//! i143.e1.s6 contract: `rafka-node-admin-client` against the real control
//! router, over HTTP. Every route decodes into the client's DTOs, every
//! mutation comes back as a Build id, and every refusal keeps its status and
//! named reason.

use rafka_node_admin_client::{BuildState, ClientError, FabricDesired, MeshDesired, NodeAdminClient, NodeStatus, ProviderKind};
use rafka_node_admin_core::build::BuildId;
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
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: rafka_node_admin_core::model::ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
        nodes: vec![node("mesh1.admin.1", true), node("mesh1.admin.2", false), node("mesh1.rpc.1", true), node("mesh1.rpc.2", false)],
    }
}

async fn serve() -> (NodeAdminClient, Arc<ControlPlane>) {
    let mut t = mn();
    for n in t.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh1.admin.1") {
        n.is_primary = true;
        n.is_fabric_primary = true;
    }
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let accepted = rafka_node_admin_core::accepted::AcceptedStore::seeded(&*builds, t.fabric.id.clone(), rafka_node_admin_core::accepted::FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let cp = Arc::new(ControlPlane::new(builds, accepted, "mesh1.admin.1".parse().unwrap(), t));
    // This admin is the fabric-primary (mesh1.admin.1): it may begin a fabric shutdown.
    let _ = cp.fabric_shutdown.set(Arc::new(rafka_node_admin_core::http::ShutdownSeat {
        control: rafka_node_admin_core::shutdown::ShutdownControl::memory("mesh1.admin.1").await,
        me: "mesh1.admin.1".parse().unwrap(),
        node_id: rafka_node_admin_core::model::NodeId::mint(),
    }));
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

/// Run the Build's open attempt to convergence, as its executor would.
async fn settle(cp: &ControlPlane, id: &rafka_node_admin_client::BuildId) {
    use rafka_node_admin_core::build_state::{AttemptOutcome, BuildAttemptClaim, BuildAttemptReceipt};
    let id = BuildId(id.0.clone());
    let attempt = cp.builds.read_build(&id).await.unwrap().attempt + 1;
    cp.builds.claim_attempt(&BuildAttemptClaim { build_id: id.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
    cp.builds.append_attempt_receipt(&BuildAttemptReceipt { build_id: id, attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
}

#[tokio::test]
async fn every_mutation_returns_a_build_and_the_build_reads_back() {
    let (c, cp) = serve().await;
    let before = cp.topology.read().await.clone();
    let spawn_accepted = c.spawn("mesh1", NodeKind::RpcNode).await.unwrap();
    assert_eq!(spawn_accepted.attempt, 1, "an accepted Build starts at attempt 1");
    let spawn = spawn_accepted.build_id;
    let v = c.build_view(&spawn).await.unwrap();
    assert_eq!((v.build_id.clone(), v.state, v.submitted_change.as_ref().and_then(|c| c["kind"].as_str())), (spawn.clone(), BuildState::Pending, Some("add_node")));
    assert!(v.topology["meshes"]["mesh1"]["nodes"].as_array().is_some_and(|n| n.len() == 5), "{:?}", v.topology);
    settle(&cp, &spawn).await;
    // A restart is an attempt of the accepted Build, never a Build of its own.
    let restart_accepted = c.restart(&"mesh1.rpc.2".parse().unwrap()).await.unwrap();
    assert_eq!(restart_accepted.attempt, 2, "a restart answers the attempt it opened");
    let restart = restart_accepted.build_id;
    assert_eq!(restart, spawn);
    let v = c.build_view(&restart).await.unwrap();
    assert_eq!((v.reason.as_str(), v.action.as_ref().and_then(|a| a["path"].as_str())), ("restart", Some("mesh1.rpc.2")));
    settle(&cp, &restart).await;
    let removed = c.remove(&"mesh1.rpc.2".parse().unwrap()).await.unwrap().build_id;
    settle(&cp, &removed).await;
    let built = c.build(&FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 2), (rafka_mesh_entity::NodeKind::RpcNode, 4)])] }).await.unwrap().build_id;
    settle(&cp, &built).await;
    let mesh2 = c.create_mesh(&MeshDesired::of("mesh2".to_string(), [(rafka_mesh_entity::NodeKind::NodeAdmin, 1), (rafka_mesh_entity::NodeKind::RpcNode, 1)])).await.unwrap().build_id;
    c.remove_mesh("mesh2").await.unwrap_err(); // the Build above is still in flight: refused below
    settle(&cp, &mesh2).await;
    c.remove_mesh("mesh9").await.unwrap_err(); // no such mesh
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
    let pending = c.spawn("mesh1", NodeKind::RpcNode).await.unwrap().build_id;
    assert_eq!(refused(c.forget(&pending).await.unwrap_err()), (409, "conflict".into()), "a running Build is not history");
    assert_eq!(refused(c.spawn("mesh1", NodeKind::RpcNode).await.unwrap_err()), (409, "build-in-progress".into()), "one Build at a time");
    let gone = NodeAdminClient::new("http://127.0.0.1:9");
    assert!(matches!(gone.nodes().await.unwrap_err(), ClientError::Transport { .. }));
}

/// CONTRACT: every lifecycle state node-admin can publish on a node or on a mesh/fabric decodes into
/// the client's status enums; a state the client lacks would make a whole view undecodable.
#[test]
fn client_status_enums_decode_every_state_the_core_publishes() {
    use rafka_node_admin_core::model::{NodeStatus as Core, ScopeStatus as CoreScope};
    // Exhaustive matches: a new core variant stops this compiling until the client carries it.
    let nodes = [Core::Pending, Core::ReadyForTraffic, Core::Draining, Core::Leaving, Core::PendingReconnect, Core::Restarting, Core::Dead];
    for s in nodes {
        match s {
            Core::Pending | Core::ReadyForTraffic | Core::Draining | Core::Leaving | Core::PendingReconnect | Core::Restarting | Core::Dead => {}
        }
        let wire = serde_json::to_value(s).unwrap();
        serde_json::from_value::<NodeStatus>(wire.clone()).unwrap_or_else(|e| panic!("the client cannot decode node status {wire}: {e}"));
    }
    let scopes = [CoreScope::Pending, CoreScope::ReadyForTraffic, CoreScope::Draining, CoreScope::Retired, CoreScope::Degraded];
    for s in scopes {
        match s {
            CoreScope::Pending | CoreScope::ReadyForTraffic | CoreScope::Draining | CoreScope::Retired | CoreScope::Degraded => {}
        }
        let wire = serde_json::to_value(s).unwrap();
        serde_json::from_value::<rafka_node_admin_client::ScopeStatus>(wire.clone()).unwrap_or_else(|e| panic!("the client cannot decode scope status {wire}: {e}"));
    }
}
