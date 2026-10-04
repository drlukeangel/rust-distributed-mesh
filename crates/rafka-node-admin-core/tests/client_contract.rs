//! i143.e1.s6 contract: `rafka-node-admin-client` against the real control
//! router, over HTTP. Every route decodes into the client's DTOs, every
//! mutation comes back as a Build id, and every refusal keeps its status and
//! named reason.

use rafka_node_admin_client::{BuildState, ClientError, FabricDesired, MeshDesired, NodeAdminClient, NodeStatus, ProviderKind};
use rafka_node_admin_core::build_state::MemoryBuildStateAdapter;
use rafka_node_admin_core::http::{router, ControlPlane};
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::topology::Topology;
use std::sync::Arc;

fn node(name: &str, primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::ReadyForTraffic.into_core();
    n.is_primary = primary;
    n.incarnation_id = Some(IncarnationId::mint());
    if n.kind == NodeKind::NodeAdmin {
        n.admin_api_base = Some(format!("http://127.0.0.1:1800{}", n.name.ordinal));
        n.is_fabric_primary = primary;
    }
    n
}

trait IntoCore {
    fn into_core(self) -> rafka_node_admin_core::model::NodeStatus;
}
impl IntoCore for NodeStatus {
    fn into_core(self) -> rafka_node_admin_core::model::NodeStatus {
        serde_json::from_value(serde_json::to_value(self).unwrap()).unwrap()
    }
}

fn mn() -> Topology {
    Topology {
        fabric: Fabric { name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: rafka_node_admin_core::model::ProviderKind::Process },
        meshes: vec![Mesh { id: MeshId::mint(), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
        nodes: vec![node("mesh1.admin.1", true), node("mesh1.admin.2", false), node("mesh1.rpc.1", true), node("mesh1.rpc.2", false)],
    }
}

async fn serve() -> (NodeAdminClient, Arc<ControlPlane>) {
    let cp = Arc::new(ControlPlane::new(Arc::new(MemoryBuildStateAdapter::new()), mn()));
    let app = router(cp.clone(), axum::Router::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (NodeAdminClient::new(base), cp)
}

#[tokio::test]
async fn every_read_decodes_into_the_client_views() {
    let (c, _) = serve().await;
    let nodes = c.nodes().await.unwrap();
    assert_eq!(nodes.len(), 4);
    let admin = nodes.iter().find(|n| n.name.to_string() == "mesh1.admin.1").unwrap();
    assert_eq!((admin.status, admin.is_primary, admin.is_fabric_primary), (NodeStatus::ReadyForTraffic, true, true));
    assert_eq!(admin.admin_api_base.as_deref(), Some("http://127.0.0.1:18001"));
    let fabric = c.fabric().await.unwrap();
    assert_eq!((fabric.name.as_str(), fabric.provider), ("fabric1", ProviderKind::Process));
    assert_eq!(fabric.fabric_primary.map(|p| p.to_string()), Some("mesh1.admin.1".into()));
    let mesh = c.mesh("mesh1").await.unwrap();
    assert_eq!(mesh.nodes.len(), 4);
    assert_eq!(mesh.admin_api_base.as_deref(), Some("http://127.0.0.1:18001"));
}

#[tokio::test]
async fn every_mutation_returns_a_build_and_the_build_reads_back() {
    let (c, cp) = serve().await;
    let before = cp.topology.read().await.clone();
    let spawn = c.spawn("mesh1", NodeKind::RpcNode).await.unwrap();
    let v = c.build_view(&spawn).await.unwrap();
    assert_eq!((v.build_id.clone(), v.state, v.intent["kind"].as_str()), (spawn, BuildState::Pending, Some("add_node")));
    let restart = c.restart(&"mesh1.rpc.2".parse().unwrap()).await.unwrap();
    assert_eq!(c.build_view(&restart).await.unwrap().intent["node"], "mesh1.rpc.2");
    c.remove(&"mesh1.rpc.2".parse().unwrap()).await.unwrap();
    c.build(&FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired { name: "mesh1".into(), node_admin: 2, rpc_node: 4 }] }).await.unwrap();
    c.create_mesh(&MeshDesired { name: "mesh2".into(), node_admin: 1, rpc_node: 1 }).await.unwrap();
    c.remove_mesh("mesh1").await.unwrap_err(); // the fabric's only mesh: refused below
    c.shutdown().await.unwrap();
    assert_eq!(*cp.topology.read().await, before, "the client changed nothing but Build state");
}

#[tokio::test]
async fn refusals_keep_their_status_and_named_reason() {
    let (c, _) = serve().await;
    let refused = |e: ClientError| match e {
        ClientError::Refused { status, error, .. } => (status, error),
        other => panic!("not a refusal: {other}"),
    };
    assert_eq!(refused(c.restart(&"mesh1.rpc.9".parse().unwrap()).await.unwrap_err()), (404, "unknown-node".into()));
    assert_eq!(refused(c.spawn("mesh7", NodeKind::RpcNode).await.unwrap_err()), (404, "unknown-mesh".into()));
    assert_eq!(refused(c.remove_mesh("mesh1").await.unwrap_err()), (422, "empty-fabric".into()));
    assert_eq!(refused(c.mesh("mesh9").await.unwrap_err()), (404, "not-found".into()));
    let pending = c.spawn("mesh1", NodeKind::RpcNode).await.unwrap();
    assert_eq!(refused(c.forget(&pending).await.unwrap_err()), (409, "conflict".into()), "a running Build is not history");
    let gone = NodeAdminClient::new("http://127.0.0.1:9");
    assert!(matches!(gone.nodes().await.unwrap_err(), ClientError::Transport { .. }));
}
