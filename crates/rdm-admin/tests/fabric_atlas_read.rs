//! Functional cell for `fabric-atlas-read` (`docs/architecture/query.md:11`).

use iroh::SecretKey;
use rdm_admin::accepted::{FabricTopology, MeshTopology};
use rdm_admin::atlas::Atlas;
use rdm_admin::build::MeshDesired;
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

/// One Status call's answer.
async fn answer(client: &NodeRpcClient, to: &NodeId, req: &StatusRequest) -> StatusReply {
    match client.call::<Status>(&NodeTarget::ExactNode(to.clone()), req, &CallOptions::default()).await.0 {
        RpcOutcome::Reply(r) => r.into_value(),
        other => panic!("expected a reply to {}, got {other:?}", req.op()),
    }
}

/// `fabric.atlas.get`, or `mesh.atlas.get` of the meshes `filter` names, on op 0x23 at `to`
/// (`NodeRpc::atlas_get`, what `Fabrics::atlas_get` and `Meshes::atlas_get` call): the atlas its
/// frames assemble, or how the read ended.
async fn atlas_at(client: &NodeRpcClient, to: &NodeId, filter: Option<&rdm_core::filter::Filter>) -> Result<serde_json::Value, rdm_client::CallEnd> {
    let atlas = rdm_client::NodeRpc::new(client).atlas_get(&NodeTarget::ExactNode(to.clone()), filter, &CallOptions::default()).await?;
    Ok(serde_json::to_value(&atlas).unwrap())
}

/// The fabric-primary `admin_name` (born `admin_id`)'s `StatusService` over `topology`: the atlas it
/// answers is read from `nodes_storage` (bound on `configs`), `meshes_storage`, `builds` and the meshes
/// `atlas` holds.
#[allow(clippy::too_many_arguments)]
fn status_service(
    admin_name: &PathName,
    admin_id: &NodeId,
    fabric_id: FabricId,
    topology: Arc<tokio::sync::RwLock<Topology>>,
    meshes_storage: Arc<MemoryMeshStorage>,
    nodes_storage: Arc<dyn NodesStorage>,
    mesh_ids: BTreeMap<String, MeshId>,
    atlas: Arc<OnceLock<Arc<Atlas>>>,
    configs: rdm_admin::node_config::NodeConfigsSlot,
    builds: Arc<MemoryBuildStateAdapter>,
) -> Arc<StatusService> {
    Arc::new(StatusService {
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
        meshes_storage,
        fabric_storage: Arc::new(MemoryFabricStorage::default()),
        nodes_storage,
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
        atlas,
        fabric_recovery: Default::default(),
        configs,
        republish: Default::default(),
        pre_node_ready: Default::default(),
        rename: Default::default(),
        row_moved: Default::default(), rejoin: Default::default(),
        // The fabric's own read (`fabric.get`) answers from a view with its digest view's loads.
        loads: Some(Arc::new(BTreeMap::new)),
        digest_view: None,
        unreachable: Default::default(),
        builds,
    })
}

/// `service` served at `admin_ep` as the birth `(admin_id, admin_inc)`, as an admin serves it: Status
/// (0x1B) and the reads (Read 0x23).
fn serve_admin(admin_ep: &iroh::Endpoint, admin_id: &NodeId, admin_inc: &IncarnationId, service: Arc<StatusService>) -> iroh::protocol::Router {
    let slot = Arc::new(OnceLock::new());
    let _ = slot.set(service);
    let server = rdm_admin::read_op::serve(rdm_admin::status_rpc::serve(ServerBuilder::new(), slot.clone()), slot)
        .build(ServedBirth { node_id: admin_id.to_string(), incarnation: admin_inc.to_string() })
        .expect("build the admin's server");
    iroh::protocol::Router::builder(admin_ep.clone()).accept(rdm_transport::ALPN, server).spawn()
}

/// A caller's client at its own endpoint that resolves the admin `admin` at `admin_key`, `admin_addr`.
async fn caller_of(admin: &Node, admin_key: &SecretKey, admin_addr: std::net::SocketAddr) -> (iroh::Endpoint, Arc<NodeRpcClient>) {
    let caller_ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().expect("addr")).await.expect("bind caller ep");
    let resolver = Arc::new(LiveNodeResolver::default());
    resolver.apply(
        ResolvedNode {
            r#type: "node-admin".into(),
            mesh: admin.mesh.clone(),
            node_id: admin.node_id.clone(),
            path: admin.path.clone(),
            name: admin.name.clone(),
            endpoint_id: admin_key.public(),
            transport_addr: admin_addr,
            incarnation: admin.incarnation_id.clone().expect("the admin's birth"),
        },
        None,
    );
    let client = Arc::new(NodeRpcClient::new(caller_ep.clone(), resolver).with_caller_system("rdm"));
    (caller_ep, client)
}

// @feature: fabric-atlas-read
/// CONTRACT: docs/architecture/query.md §1 (Q439)
/// "The atlas, `fabric.atlas.get` and `mesh.atlas.get`: the topology plus what is in flight (Q439),
///  laid out as `fabric.get` answers: a list of fabrics, each with its fields as `fabric.get` answers
///  them and the current Build and its phase, and inside it its meshes in name order, each as
///  `mesh.get` answers it, with its nodes in `(path, name)` order."
/// CONTRACT (node-rpc-envelope.md "Read, op `0x23`", Q285): the atlas is read on op 0x23, and Status
/// `GetAtlas` and `GetMeshAtlas` (positions 32 and 40) are refused by name, naming op 0x23. Before the
/// admin's atlas is filled, `fabric.atlas.get` is refused `NotReady`; after it, the answer is one fabric
/// whose fields are `fabric.get`'s, under its names, with the current Build and its phase, then mesh1
/// with `mesh.get`'s fields and its live nodes in `(path, name)` order, each the topology's node row
/// read from its record. Must NOT happen: a flat node list, a fabric or a mesh spelled other than its
/// own read spells it, or the deleted node listed.
#[test]
fn fabrics_atlas_get_over_read_refuses_when_atlas_unset_and_answers_the_fabric_then_its_meshes_and_nodes(
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

        let admin_node = topology.read().await.own(&admin_id).cloned().expect("the view holds the admin");
        let service = status_service(&admin_name, &admin_id, fabric_id, topology, meshes_storage.clone(), nodes_storage.clone(), mesh_ids, atlas_slot.clone(), configs_slot.clone(), builds);
        let router = serve_admin(&admin_ep, &admin_id, &admin_inc, service);
        let (caller_ep, client) = caller_of(&admin_node, &admin_key, admin_addr).await;

        // 0. Status positions 32 and 40 are refused by name, naming op 0x23, the read's stream (Q285).
        for req in [StatusRequest::GetAtlas, StatusRequest::GetMeshAtlas { filter: rdm_core::filter::Filter::new().eq("name", ["mesh1"]) }] {
            let refused = answer(&client, &admin_id, &req).await;
            assert!(matches!(&refused, StatusReply::RejectedFilter { reason } if reason == &format!("{} is read on op 0x23 (read), a stream (Q285)", req.op())), "{} on Status is refused by name, naming op 0x23: {refused:?}", req.op());
        }

        // 1. Before the fabric-primary's atlas slot is filled, fabric.atlas.get is refused NotReady.
        let before = atlas_at(&client, &admin_id, None).await;
        assert!(matches!(&before, Err(rdm_client::CallEnd::Rejected { reason, detail }) if reason == "not-ready" && detail.contains("holds no fabric atlas yet")), "expected not-ready before the atlas is set: {before:?}");

        // 2. Install the fabric-primary's atlas, over mesh1: fabric.atlas.get answers the fabric as
        // fabric.get answers it, then mesh1 and its live nodes in (path, name) order; the deleted node is
        // not listed.
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
        let atlas_of = |read: Result<serde_json::Value, rdm_client::CallEnd>| read.unwrap_or_else(|e| panic!("the atlas read ends whole: {e}"));
        let atlas = atlas_of(atlas_at(&client, &admin_id, None).await);
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

        // 3. A node command this admin admitted is in flight on its node until it ends
        // (node-investigation.md: "its in-flight commands"): the node's row carries it accepted.
        let stop = rdm_core::OperationId::node("node.stop", &rpc_id);
        let in_flight = atlas_slot.get().expect("the atlas is installed").command_in_flight(&rpc_id, &rpc_inc, &stop);
        let during = atlas_of(atlas_at(&client, &admin_id, None).await);
        assert_eq!(during["fabrics"][0]["meshes"][0]["nodes"][0]["accepted"], serde_json::json!(stop), "the stop in flight on {rpc_name:?} is its row's accepted operation: {during}");
        drop(in_flight);
        let ended = atlas_of(atlas_at(&client, &admin_id, None).await);
        assert_eq!(ended["fabrics"][0]["meshes"][0]["nodes"][0]["accepted"], serde_json::Value::Null, "once the command ends, nothing is accepted on {rpc_name:?}: {ended}");

        // 4. mesh.atlas.get: the same fabric with only the meshes the filter names; a mesh the admin
        // does not hold leaves the fabric with no mesh; a pair the mesh schema refuses is refused by name.
        let named = atlas_of(atlas_at(&client, &admin_id, Some(&rdm_core::filter::Filter::new().eq("name", ["mesh1"]))).await);
        assert_eq!((without(&named["fabrics"][0], &["meshes"]), named["fabrics"][0]["meshes"].as_array().map(|m| m.iter().map(|m| m["name"].clone()).collect::<Vec<_>>())), (without(fabric, &["meshes"]), Some(vec![serde_json::json!("mesh1")])), "mesh.atlas.get [name: mesh1] answers the fabric and mesh1: {named}");
        let unknown = atlas_of(atlas_at(&client, &admin_id, Some(&rdm_core::filter::Filter::new().eq("name", ["mesh9"]))).await);
        assert_eq!((without(&unknown["fabrics"][0], &["meshes"]), unknown["fabrics"][0]["meshes"].clone()), (without(fabric, &["meshes"]), serde_json::json!([])), "a mesh the admin does not hold is answered as its fabric with no mesh: {unknown}");
        let refused = atlas_at(&client, &admin_id, Some(&rdm_core::filter::Filter::new().eq("color", ["red"]))).await;
        assert!(matches!(&refused, Err(rdm_client::CallEnd::Rejected { reason, detail }) if reason == "rejected-filter" && detail.contains("color")), "a pair the mesh schema refuses is refused naming it: {refused:?}");

        let _ = router.shutdown().await;
        caller_ep.close().await;
        admin_ep.close().await;
    });

    drop(rt);

    let spans = capture.all();
    assert!(
        spans.iter().any(|s| {
            s.name == "rdm.node_rpc.stream.serve.via-direct"
                && s.attributes.get("protocol").map(String::as_str) == Some("read")
                && s.attributes.get("code").map(String::as_str) == Some("UNAVAILABLE")
                && s.attributes.get("detail").map(String::as_str) == Some("not-ready")
        }),
        "expected rdm.node_rpc.stream.serve.via-direct span on read with detail=not-ready before atlas is set"
    );
    assert!(
        spans.iter().any(|s| {
            s.name == "rdm.node_rpc.stream.serve.via-direct"
                && s.attributes.get("protocol").map(String::as_str) == Some("read")
                && s.attributes.get("code").map(String::as_str) == Some("OK")
                && s.attributes.get("detail").map(String::as_str) == Some("end")
        }),
        "expected rdm.node_rpc.stream.serve.via-direct span on read with detail=end when the atlas is streamed"
    );
    let served = |op: &str| spans.iter().filter(|s| s.name == "rdm.node_admin.fabric.serve.via-atlas" && s.attributes.get("op").map(String::as_str) == Some(op)).map(|s| (s.attributes.get("meshes").cloned().unwrap_or_default(), s.attributes.get("nodes").cloned().unwrap_or_default(), s.attributes.get("accepted").cloned().unwrap_or_default())).collect::<Vec<_>>();
    assert_eq!(served("fabric.atlas.get"), vec![("mesh1".to_string(), "2".to_string(), String::new()), ("mesh1".to_string(), "2".to_string(), "mesh1.node/1 node.stop".to_string()), ("mesh1".to_string(), "2".to_string(), String::new())], "each fabric.atlas.get answered names its meshes, its nodes and the command in flight");
    assert_eq!(served("mesh.atlas.get"), vec![("mesh1".to_string(), "2".to_string(), String::new()), (String::new(), "0".to_string(), String::new())], "each mesh.atlas.get answered names only the meshes it names; the refused one answers none");
}

// @feature: fabric-atlas-read
/// CONTRACT: docs/architecture/query.md §1 (Q439, Q285)
/// "`fabric.atlas.get` answers every mesh of the current Build's topology that the answering node-admin
///  is primary for ... Both are streamed in frames on op 0x23 (Q285), never bounded as one frame"
/// CONTRACT: an atlas of more than 600 nodes (L4's 10 meshes of 20 nodes, three times over), each
/// node's record written straight to the fabric-primary's nodes store, reads whole through
/// `Fabrics::atlas_get` at the fabric-primary: its one fabric, its meshes in name order, each with every
/// node row in `(path, name)` order. The serve span names the rows and the data frames sent: every row,
/// in more than one frame. Must NOT happen: `read-over-bound` for the whole answer, a row left out, a
/// row under another mesh, or the answer sent as one frame.
#[test]
fn an_atlas_of_more_than_six_hundred_nodes_reads_whole_at_the_fabric_primary() {
    // Two meshes of 450 nodes and the admin: each mesh is more than one frame of rows.
    const PER_MESH: u32 = 450;
    let capture = Capture::new("fabric-atlas-read-streamed");
    let _guard = tracing::dispatcher::set_default(&capture.dispatch);
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("build current-thread runtime");

    let total = rt.block_on(async {
        let fabric_id = FabricId::mint();
        let admin_name = PathName::numbered("mesh1", "node-admin", 1);
        let (admin_id, admin_inc, admin_key) = (NodeId::mint(), IncarnationId::mint(), SecretKey::generate());
        let admin_ep = rdm_transport::endpoint::bind(admin_key.clone(), "127.0.0.1:0".parse().expect("addr")).await.expect("bind admin ep");
        let admin_addr = admin_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).expect("ipv4");
        let mut admin = Node::allocated(admin_name.clone(), "mesh1", "node-admin");
        admin.node_id = admin_id.clone();
        admin.incarnation_id = Some(admin_inc.clone());
        admin.endpoint_id = Some(EndpointId(admin_key.public().to_string()));
        admin.transport_addr = Some(admin_addr);
        admin.status = NodeStatus::Ready;
        admin.is_primary = true;
        admin.is_fabric_primary = true;

        // The current Build's topology: mesh1 (the admin and PER_MESH nodes) and mesh2 (PER_MESH nodes).
        let mesh1_id = MeshId::mint();
        let mut top = FabricTopology::root("fabric1", "mesh1", mesh1_id.clone());
        top.meshes.get_mut("mesh1").unwrap().nodes.extend((1..=PER_MESH).map(|i| PathName::numbered("mesh1", "node", i)));
        let mesh2 = MeshTopology::of(&MeshDesired::of("mesh2", [("node", PER_MESH)]));
        let mesh2_id = mesh2.mesh_id.clone();
        top.meshes.insert("mesh2".into(), mesh2);

        // Every node's record, written straight to the nodes store.
        let nodes_storage = Arc::new(MemoryNodesStorage::default());
        let mut ids = BTreeMap::new();
        for path in top.meshes.values().flat_map(|m| m.nodes.iter()) {
            let (r#type, id, inc) = if *path == admin_name { ("node-admin", admin_id.clone(), admin_inc.clone()) } else { ("node", NodeId::mint(), IncarnationId::mint()) };
            nodes_storage.put_node(&rdm_node::nodes_storage::fixture_record(path, r#type, id.clone(), inc, NodeStatus::Ready)).await.unwrap();
            ids.insert(path.clone(), id);
        }
        let meshes_storage = Arc::new(MemoryMeshStorage::default());
        for (name, id) in [("mesh1", &mesh1_id), ("mesh2", &mesh2_id)] {
            rdm_admin::storage::MeshStorage::put_mesh(&*meshes_storage, &rdm_admin::storage::MeshRecord::new(id.clone(), name)).await.unwrap();
        }
        let configs = crate::bound_configs_over(nodes_storage.clone(), rdm_admin::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(admin_ep.clone(), Arc::new(LiveNodeResolver::default()))))).await;
        let configs_slot: rdm_admin::node_config::NodeConfigsSlot = Arc::new(OnceLock::new());
        let _ = configs_slot.set(Arc::new(configs));
        let builds = Arc::new(MemoryBuildStateAdapter::new());
        let build_id = rdm_admin::build::BuildId::parse(&rdm_core::fixture_crockford60("b-atlas-streamed")).unwrap();
        let want: Vec<(String, Vec<(String, String, NodeId)>)> =
            top.meshes.iter().map(|(name, m)| (name.clone(), m.nodes.iter().map(|p| (p.path.clone(), p.name.clone(), ids[p].clone())).collect())).collect();
        builds
            .publish_accepted(&BuildAccepted { build_id, topology: top, change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 1_000 })
            .await
            .unwrap();

        // The fabric-primary's view and its atlas, over both meshes.
        let topology = Arc::new(tokio::sync::RwLock::new(Topology {
            fabric: Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![
                rdm_admin::model::Mesh { id: Some(mesh1_id.clone()), name: "mesh1".into(), status: ScopeStatus::Ready },
                rdm_admin::model::Mesh { id: Some(mesh2_id.clone()), name: "mesh2".into(), status: ScopeStatus::Ready },
            ],
            nodes: vec![admin.clone()],
        }));
        let atlas_slot: Arc<OnceLock<Arc<Atlas>>> = Arc::new(OnceLock::new());
        let held = Arc::new(Atlas::default());
        held.scope(["mesh1".to_string(), "mesh2".to_string()].into());
        let _ = atlas_slot.set(held);
        let mesh_ids: BTreeMap<String, MeshId> = [("mesh1".to_string(), mesh1_id), ("mesh2".to_string(), mesh2_id)].into();
        let service = status_service(&admin_name, &admin_id, fabric_id, topology, meshes_storage, nodes_storage, mesh_ids, atlas_slot, configs_slot, builds);
        let router = serve_admin(&admin_ep, &admin_id, &admin_inc, service);
        let (caller_ep, client) = caller_of(&admin, &admin_key, admin_addr).await;

        let total: usize = want.iter().map(|(_, nodes)| nodes.len()).sum();
        assert!(total > 600, "the atlas holds more than 600 nodes: {total}");
        let atlas = rdm_client::Fabrics::new(rdm_client::BuildCarrier::at_node(client, admin_id.clone()))
            .atlas_get()
            .await
            .unwrap_or_else(|e| panic!("fabric.atlas.get of {total} nodes at the fabric-primary reads whole: {e}"));
        assert_eq!(atlas.fabrics.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(), vec!["fabric1"], "one fabric");
        let got: Vec<(String, Vec<(String, String, NodeId)>)> =
            atlas.fabrics[0].meshes.iter().map(|m| (m.name.clone(), m.nodes.iter().map(|n| (n.node.path.clone(), n.node.name.clone(), n.node.node_id.clone())).collect())).collect();
        assert_eq!(got, want, "every mesh in name order, each with every node row in (path, name) order, none left out");

        let _ = router.shutdown().await;
        caller_ep.close().await;
        admin_ep.close().await;
        total
    });
    drop(rt);

    let spans = capture.all();
    let served: Vec<(String, String)> = spans
        .iter()
        .filter(|s| s.name == "rdm.node_admin.fabric.serve.via-atlas" && s.attributes.get("op").map(String::as_str) == Some("fabric.atlas.get"))
        .map(|s| (s.attributes.get("rows").cloned().unwrap_or_default(), s.attributes.get("frames").cloned().unwrap_or_default()))
        .collect();
    assert_eq!(served.len(), 1, "one fabric.atlas.get served: {served:?}");
    let (rows, frames) = &served[0];
    assert_eq!(rows, &total.to_string(), "the serve span counts every node row sent");
    assert!(frames.parse::<u32>().is_ok_and(|f| f > 2), "the rows went in more than one data frame per mesh: {frames} frames");
}

/// A nodes store that names each read made of it, in order, over the file nodes store: a point read of
/// one record (`node <id>`), the read of a mesh's records (`nodes <mesh>`) and the read of the records at
/// a path and name (`records_at <path>/<name>`).
struct NamedReads {
    records: rdm_admin::storage::FileNodesStorage,
    reads: Mutex<Vec<String>>,
}

impl NamedReads {
    /// How many reads were made so far.
    fn made(&self) -> usize {
        self.reads.lock().unwrap().len()
    }

    /// The reads made since the `from`th.
    fn since(&self, from: usize) -> Vec<String> {
        self.reads.lock().unwrap()[from..].to_vec()
    }
}

#[async_trait::async_trait]
impl NodesStorage for NamedReads {
    async fn node(&self, node_id: &NodeId) -> Result<Option<rdm_admin::storage::NodeLine>, rdm_admin::storage_error::StorageError> {
        self.reads.lock().unwrap().push(format!("node {node_id}"));
        self.records.node(node_id).await
    }
    async fn nodes(&self, mesh: &str) -> Result<Vec<NodeRecord>, rdm_admin::storage_error::StorageError> {
        self.reads.lock().unwrap().push(format!("nodes {mesh}"));
        self.records.nodes(mesh).await
    }
    async fn put_node(&self, record: &NodeRecord) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.records.put_node(record).await
    }
    async fn records_at(&self, name: &PathName) -> Result<Vec<NodeRecord>, rdm_admin::storage_error::StorageError> {
        self.reads.lock().unwrap().push(format!("records_at {}/{}", name.path, name.name));
        self.records.records_at(name).await
    }
}

// @feature: fabric-atlas-read
/// CONTRACT (query.md §1: "Node rows and property maps are not copied into an atlas to serve `node.get`
/// or acceptance", and "A current-pair lookup resolves the path and name to a node ID from the node
/// records. The reader reads the node's record at that node ID"; Q439: each atlas node row "read from the
/// node's record"; build.md §4: "the fabric-primary composes the complete topology from its records at
/// acceptance"; build-deployment.md §3: a Create naming a held path is a node's executable replacement
/// when the node's row records another executable; fabric-atlas.md R-FA2: a completed Build's lifecycle
/// steps are taken into the atlas, a step's birth its Build pinned none of named by its subject's record):
/// at the fabric-primary of two meshes of six births each, every node's record in the file nodes store,
/// Build acceptance, Build completion and the atlas reads read the records they answer, many records as
/// one store read per mesh, never one read per node: a Delete's acceptance reads each mesh's records once
/// and the deleted node's pair (its current-pair lookup and its record, the atlas row its gate reads); a
/// Create naming one held path reads each mesh's records once and that node's record at its node id, and
/// a Create naming every node's held path reads each mesh's records once more for their rows, never one
/// read per path; the completion of the Build that made the births (a create of each, and a node.delete
/// of mesh1.node/5 its Build pinned no birth of, as a mesh.delete's of its final primary is) reads that
/// one record at its node id and none for a create; `fabric.atlas.get` reads each mesh's records once for
/// its node rows, beside `fabric.get`'s cert dates at one read per mesh; `mesh.atlas.get [name: mesh2]`
/// reads mesh2's records once beside them. The acceptance's and the completion's spans name their store
/// calls (`io_calls`). Each answers what it answered before: every mesh in name order with its node rows
/// in `(path, name)` order, the Delete pinned to the birth the view holds, each Create `name-held` (every
/// path named at the executable its row records), and after the completion mesh1.node/5 no longer listed.
/// Each read's node-record store reads and wall are printed. Must NOT happen: a store read per node, per
/// path a Create names, per step a completed Build holds, or two per node row.
#[test]
fn build_acceptance_completion_and_the_atlas_reads_read_the_node_records_at_one_store_read_per_mesh() {
    let capture = Capture::new("fabric-atlas-read-store-reads");
    let _guard = tracing::dispatcher::set_default(&capture.dispatch);
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("build current-thread runtime");
    let dir = std::env::temp_dir().join(format!("atlas-store-reads-{}", NodeId::mint()));
    std::fs::create_dir_all(&dir).unwrap();
    let (births, cost, gone) = rt.block_on(async {
        let store = rdm_admin::store_root::StoreRoot::at(&dir).unwrap();
        let records = Arc::new(NamedReads { records: rdm_admin::storage::FileNodesStorage::open(&store, &rdm_admin::store_log::LogWriter::mint()).await.unwrap(), reads: Mutex::default() });
        // Each mesh: its node-admin, the mesh-primary (mesh1's the fabric-primary), and five nodes.
        let mut births = Vec::new();
        for mesh in ["mesh1", "mesh2"] {
            let mut admin = Node::allocated(PathName::numbered(mesh, "node-admin", 1), mesh, "node-admin");
            admin.is_primary = true;
            admin.is_fabric_primary = mesh == "mesh1";
            births.push(admin);
            births.extend((1..=5).map(|i| Node::allocated(PathName::numbered(mesh, "node", i), mesh, "node")));
        }
        for n in births.iter_mut() {
            n.status = NodeStatus::Ready;
            n.incarnation_id = Some(IncarnationId::mint());
            let mut row = rdm_node::nodes_storage::fixture_record(&n.location(), &n.r#type, n.node_id.clone(), n.incarnation_id.clone().unwrap(), NodeStatus::Ready);
            (row.is_primary, row.is_fabric_primary) = (n.is_primary.then_some(1), n.is_fabric_primary.then_some(1));
            // What the node's birth runs (build-deployment.md §2), the executable a Create names it at.
            row.executable = Some(crate::test_executable(&n.r#type));
            records.put_node(&row).await.unwrap();
        }
        let (mesh1_id, mesh2_id) = (MeshId::mint(), MeshId::mint());
        let view = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![
                rdm_admin::model::Mesh { id: Some(mesh1_id.clone()), name: "mesh1".into(), status: ScopeStatus::Ready },
                rdm_admin::model::Mesh { id: Some(mesh2_id.clone()), name: "mesh2".into(), status: ScopeStatus::Ready },
            ],
            nodes: births.clone(),
        };
        let meshes_storage = Arc::new(MemoryMeshStorage::default());
        for (name, id) in [("mesh1", &mesh1_id), ("mesh2", &mesh2_id)] {
            rdm_admin::storage::MeshStorage::put_mesh(&*meshes_storage, &rdm_admin::storage::MeshRecord::new(id.clone(), name)).await.unwrap();
        }
        // The current Build, accepted and complete, its topology every birth: what the atlas reads list
        // and what a Build is compiled against.
        let fp = births[0].clone();
        let builds = Arc::new(MemoryBuildStateAdapter::new());
        let accepted = rdm_admin::accepted::AcceptedStore::seeded(&*builds, view.fabric.id.clone(), FabricTopology::of_observed(&view), &fp.birth().unwrap()).await.unwrap();

        // The fabric-primary's reads over its atlas of both meshes, and its acceptance.
        let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().expect("addr")).await.expect("bind admin ep");
        let configs = crate::bound_configs_over(records.clone(), rdm_admin::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep.clone(), Arc::new(LiveNodeResolver::default()))))).await;
        let configs_slot: rdm_admin::node_config::NodeConfigsSlot = Arc::new(OnceLock::new());
        let _ = configs_slot.set(Arc::new(configs));
        // The fabric-primary's one atlas: what its acceptance and its completion write, and what its reads answer.
        let cp = crate::control_plane(builds.clone(), accepted, fp.location(), view.clone(), crate::adopted_time());
        let _ = cp.node_rows.set(records.clone());
        let atlas_slot: Arc<OnceLock<Arc<Atlas>>> = Arc::new(OnceLock::new());
        cp.atlas.scope(["mesh1".to_string(), "mesh2".to_string()].into());
        let _ = atlas_slot.set(cp.atlas.clone());
        let mesh_ids: BTreeMap<String, MeshId> = [("mesh1".to_string(), mesh1_id), ("mesh2".to_string(), mesh2_id)].into();
        let service = status_service(&fp.location(), &fp.node_id, view.fabric.id.clone(), Arc::new(tokio::sync::RwLock::new(view.clone())), meshes_storage, records.clone(), mesh_ids, atlas_slot, configs_slot, builds.clone());

        let rows = |births: &[Node], mesh: &str| -> (String, Vec<(String, String, NodeId)>) {
            let mut of: Vec<&Node> = births.iter().filter(|n| n.mesh == mesh).collect();
            of.sort_by_key(|n| n.location());
            (mesh.to_string(), of.into_iter().map(|n| (n.path.clone(), n.name.clone(), n.node_id.clone())).collect())
        };
        let answered = |atlas: &rdm_core::views::AtlasView| -> Vec<(String, Vec<(String, String, NodeId)>)> {
            atlas.fabrics[0].meshes.iter().map(|m| (m.name.clone(), m.nodes.iter().map(|n| (n.node.path.clone(), n.node.name.clone(), n.node.node_id.clone())).collect())).collect()
        };
        let mut cost: Vec<(&str, Vec<String>, std::time::Duration)> = Vec::new();

        let (from, at) = (records.made(), std::time::Instant::now());
        let atlas = service.atlas_answer("fabric.atlas.get", None).await.unwrap_or_else(|r| panic!("fabric.atlas.get is answered: {r:?}"));
        cost.push(("fabric.atlas.get", records.since(from), at.elapsed()));
        assert_eq!(answered(&atlas), vec![rows(&births, "mesh1"), rows(&births, "mesh2")], "fabric.atlas.get: both meshes in name order, each with its node rows in (path, name) order");

        let (from, at) = (records.made(), std::time::Instant::now());
        let atlas = service.atlas_answer("mesh.atlas.get", Some(&rdm_core::filter::Filter::new().eq("name", ["mesh2"]))).await.unwrap_or_else(|r| panic!("mesh.atlas.get [name: mesh2] is answered: {r:?}"));
        cost.push(("mesh.atlas.get [name: mesh2]", records.since(from), at.elapsed()));
        assert_eq!(answered(&atlas), vec![rows(&births, "mesh2")], "mesh.atlas.get [name: mesh2]: mesh2 with its node rows in (path, name) order");

        // A Create naming held paths, each at the executable its node's row records: each row is read
        // (build-deployment.md §3), none records another executable, and the Create stays name-held.
        let naming = |paths: &[PathName]| rdm_core::build::BuildChange::Create {
            meshes: Vec::new(),
            nodes: paths
                .iter()
                .map(|at| {
                    let n = births.iter().find(|n| n.location() == *at).unwrap();
                    rdm_core::build::Nodes { path: at.path.clone(), name: at.name.clone(), mesh: n.mesh.clone(), r#type: n.r#type.clone(), storage_persistence: None, executable: Some(crate::test_executable(&n.r#type)), tags: Default::default(), config: Default::default() }
                })
                .collect(),
        };
        let one = vec![PathName::numbered("mesh2", "node", 1)];
        let every: Vec<PathName> = births.iter().filter(|n| n.r#type == "node").map(|n| n.location()).collect();
        for (route, what, paths) in [("store-reads-create-one", "Create acceptance: one held path, mesh2.node/1", &one), ("store-reads-create-every", "Create acceptance: every node's held path, ten in two meshes", &every)] {
            let (from, at) = (records.made(), std::time::Instant::now());
            let refused = cp.submit(route, naming(paths)).await;
            cost.push((what, records.since(from), at.elapsed()));
            assert!(matches!(&refused, Err(rdm_admin::control_plane::Refusal::Reject(rdm_admin::build::BuildReject::NameHeld { .. }))), "{what}: every path named at the executable its row records stays name-held: {refused:?}");
        }

        let deleted = births.iter().find(|n| n.location() == PathName::numbered("mesh2", "node", 3)).cloned().unwrap();
        let (from, at) = (records.made(), std::time::Instant::now());
        let opened = cp.submit("store-reads", rdm_core::build::BuildChange::Delete { meshes: Vec::new(), node_ids: vec![deleted.node_id.clone()] }).await.unwrap_or_else(|r| panic!("the Delete of mesh2.node/3 is accepted: {r:?}"));
        cost.push(("Build acceptance: a Delete of mesh2.node/3", records.since(from), at.elapsed()));
        let pinned = builds.read_build(&opened.build_id).await.unwrap().topology.pinned;
        assert_eq!(pinned.iter().map(|(at, b)| (at.clone(), b.node_id.clone(), b.incarnation.clone())).collect::<Vec<_>>(), vec![(deleted.location(), deleted.node_id.clone(), deleted.incarnation_id.clone().unwrap())], "the Delete is pinned to the birth the view holds at mesh2.node/3");

        // The Build that made the births, complete: a create of each, and a node.delete of mesh1.node/5
        // its Build pinned no birth of, as a mesh.delete's of its final primary is.
        let gone = births.iter().find(|n| n.location() == PathName::numbered("mesh1", "node", 5)).cloned().unwrap();
        let made = rdm_admin::build::BuildId::mint();
        let mut topology = FabricTopology::of_observed(&view);
        topology.created = births.iter().map(|n| (n.location(), n.node_id.clone())).collect();
        let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
        let change = rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() };
        rdm_admin::build_state::accept(&*builds, &BuildAccepted { build_id: made.clone(), topology, change, source: None, time: now_ms }, None, fp.node_id.as_str(), "accepted").await.unwrap();
        let by = fp.birth().unwrap();
        crate::start_build(&*builds, &made, &by).await;
        let steps = rdm_admin::build_run::FramedBuilds::stepping(builds.clone(), &crate::cell_fabric(), &fp.node_id, "mesh1");
        let ops = births.iter().map(|n| (n.mesh.clone(), rdm_core::OperationId::node("node.create", &n.node_id))).chain([(gone.mesh.clone(), rdm_core::OperationId::node("node.delete", &gone.node_id))]);
        for (mesh, operation) in ops {
            let step = rdm_admin::build_state::StepWrite { build_id: made.clone(), mesh: Some(mesh), operation, step: "Complete".into(), outcome: rdm_admin::build_state::StepOutcome::Complete };
            steps.write_step(&step).await.unwrap();
        }
        let source = rdm_admin::events_storage::EventSource { fabric_id: crate::cell_fabric(), node_id: by.node_id.clone(), incarnation: by.incarnation.clone() };
        builds.events().append(&rdm_admin::events_storage::Event::build_ended(&source, now_ms, &made, &rdm_admin::events_storage::EndedState::Complete)).await.unwrap();
        let (from, at) = (records.made(), std::time::Instant::now());
        cp.take_receipts(&*builds, &made).await;
        cost.push(("Build completion: thirteen steps, twelve creates and mesh1.node/5's delete", records.since(from), at.elapsed()));
        let atlas = service.atlas_answer("fabric.atlas.get", None).await.unwrap_or_else(|r| panic!("fabric.atlas.get is answered after the completion: {r:?}"));
        let left: Vec<Node> = births.iter().filter(|n| n.node_id != gone.node_id).cloned().collect();
        assert_eq!(answered(&atlas)[0], rows(&left, "mesh1"), "the completion took mesh1.node/5's delete, its birth named by its record: fabric.atlas.get no longer lists it");
        ep.close().await;
        (births, cost, gone)
    });
    drop(rt);
    let _ = std::fs::remove_dir_all(&dir);

    let spans = capture.all();
    let io_calls_of = |what: &str, span: Option<&Captured>| -> usize {
        let span = span.unwrap_or_else(|| panic!("{what} is spanned"));
        span.attributes.get("io_calls").and_then(|v| v.parse().ok()).unwrap_or_else(|| panic!("{what}'s span names its store calls (io_calls): {:?}", span.attributes))
    };
    let accepted = |route: &str| spans.iter().find(|s| s.name == "rdm.node_admin.build.create.via-rest" && s.attributes.get("route").map(String::as_str) == Some(route));
    let io_calls = io_calls_of("the Delete's acceptance", accepted("store-reads"));
    let create_io_calls = io_calls_of("the Create of every node's held path", accepted("store-reads-create-every"));
    let completed = spans.iter().find(|s| s.name == "rdm.node_admin.fabric.update.via-atlas-receipt" && s.attributes.contains_key("io_calls"));
    let completion_io_calls = io_calls_of("the Build's completion", completed);
    for (what, reads, wall) in &cost {
        eprintln!("{what}: {} node-record store reads, wall {:.1} ms: {reads:?}", reads.len(), wall.as_secs_f64() * 1e3);
    }
    eprintln!("the Delete's acceptance span: io_calls {io_calls}; the Create's of every held path: io_calls {create_io_calls}; the completion's: io_calls {completion_io_calls}");
    let reads = |what: &str| cost.iter().find(|(w, _, _)| w.starts_with(what)).map(|(_, r, _)| r.clone()).unwrap();
    assert!(reads("fabric.atlas.get").len() <= 4, "fabric.atlas.get reads each mesh's records once for its node rows and once for fabric.get's cert dates, never a read per node row: {cost:#?}");
    assert!(reads("mesh.atlas.get").len() <= 3, "mesh.atlas.get [name: mesh2] reads mesh2's records once for its node rows beside fabric.get's cert dates, never a read per node row: {cost:#?}");
    assert!(reads("Build acceptance").len() <= 4, "Build acceptance reads each mesh's records once and the deleted node's pair (its lookup and its record), never a read per birth: {cost:#?}");
    assert!(io_calls < births.len(), "the acceptance's span names fewer store calls than the view holds births, never one per birth: io_calls {io_calls}, {} births", births.len());
    assert!(reads("Create acceptance: one").len() <= 3, "a Create naming one held path reads each mesh's records once and that node's record, never a read per birth: {cost:#?}");
    assert!(reads("Create acceptance: every").len() <= 4, "a Create naming ten held paths in two meshes reads each mesh's records once for the view and once for their rows, never a read per path: {cost:#?}");
    let paths = births.iter().filter(|n| n.r#type == "node").count();
    assert!(create_io_calls < paths, "the Create's acceptance span names fewer store calls than the {paths} paths it names, never one per path: io_calls {create_io_calls}");
    assert_eq!(reads("Build completion"), vec![format!("node {}", gone.node_id)], "the completion reads the one record that names a step's birth (mesh1.node/5's delete, its birth pinned by none) at its node id, and none for a create: {cost:#?}");
    assert!(completion_io_calls <= 1, "the completion's span names one store call, mesh1.node/5's record, never one per step: io_calls {completion_io_calls}");
}
