//! i143.e1.s6: the Admin UI's topology flows work through the node-admin
//! client. The UI's routes run against node-admin's real control router over
//! HTTP; every flow lands as one Build in node-admin, node-admin's refusals
//! reach the UI user with their status and named reason, and the UI's
//! timeline records each Build it submitted.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use rafka_admin_ui::control::{router, BuildEvents};
use rafka_node_admin_client::{BuildId, NodeAdminClient};
use rafka_node_admin_core::build_state::MemoryBuildStateAdapter;
use rafka_node_admin_core::http::ControlPlane;
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::topology::Topology;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;

#[derive(Default)]
struct Timeline(Mutex<Vec<(String, String)>>);
impl BuildEvents for Timeline {
    fn submitted(&self, what: &str, build_id: &BuildId, _: Option<&str>, _: Option<&str>) {
        self.0.lock().unwrap().push((what.to_string(), build_id.0.clone()));
    }
}

fn node(name: &str, primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::ReadyForTraffic;
    n.is_primary = primary;
    n.incarnation_id = Some(IncarnationId::mint());
    n
}

/// Node-admin's real control API on a loopback port.
async fn node_admin() -> (NodeAdminClient, Arc<ControlPlane>) {
    let topology = Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
        nodes: vec![
            rafka_node_admin_core::model::Node { is_fabric_primary: true, ..node("mesh1.admin.1", true) },
            node("mesh1.rpc.1", true),
            node("mesh1.rpc.2", false),
        ],
    };
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let accepted = rafka_node_admin_core::accepted::AcceptedStore::seeded(&*builds, topology.fabric.id.clone(), rafka_node_admin_core::accepted::FabricTopology::of_observed(&topology), "mesh1.admin.1")
        .await
        .unwrap();
    let cp = Arc::new(ControlPlane::new(builds, accepted, "mesh1.admin.1".parse().unwrap(), topology));
    let app = rafka_node_admin_core::http::router(cp.clone(), axum::Router::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (NodeAdminClient::new(base), cp)
}

async fn call(app: &axum::Router, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(body.map(|b| Body::from(b.to_string())).unwrap_or_else(Body::empty))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

#[tokio::test]
async fn every_ui_topology_flow_is_one_build_in_node_admin() {
    let (client, cp) = node_admin().await;
    let timeline = Arc::new(Timeline::default());
    let ui = router(Some(client.clone()), timeline.clone());
    let before = cp.topology.read().await.clone();

    let flows = [
        ("POST", "/api/nodes/spawn", Some(json!({"mesh": "mesh1", "kind": "broker"})), "add_node"),
        ("POST", "/api/nodes/mesh1.rpc.2/restart", None, "restart_node"),
        ("DELETE", "/api/nodes/mesh1.rpc.2", None, "remove_node"),
        ("POST", "/api/bootstrap", None, "reconcile_fabric"),
    ];
    for (method, uri, body, kind) in flows {
        let (status, v) = call(&ui, method, uri, body).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{method} {uri}: {v}");
        let id = BuildId(v["build_id"].as_str().expect("build_id").to_string());
        let build = client.build_view(&id).await.unwrap();
        if kind == "restart_node" {
            // A restart is an attempt of the accepted Build, not a Build of its own.
            assert_eq!((build.reason.as_str(), build.action.as_ref().and_then(|a| a["path"].as_str())), ("restart", Some("mesh1.rpc.2")), "{uri}");
        } else {
            assert_eq!(build.submitted_change.as_ref().and_then(|c| c["kind"].as_str()), Some(kind), "{uri}: {:?}", build.submitted_change);
        }
        // One Build in flight at a time: run it to convergence before the next flow.
        let core_id = rafka_node_admin_core::build::BuildId(id.0.clone());
        let attempt = cp.builds.read_build(&core_id).await.unwrap().attempt + 1;
        cp.builds.claim_attempt(&rafka_node_admin_core::build_state::BuildAttemptClaim { build_id: core_id.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
        cp.builds
            .append_attempt_receipt(&rafka_node_admin_core::build_state::BuildAttemptReceipt { build_id: core_id, attempt, outcome: rafka_node_admin_core::build_state::AttemptOutcome::Converged })
            .await
            .unwrap();
    }
    let bootstrap = client.build_view(&BuildId(timeline.0.lock().unwrap().last().unwrap().1.clone())).await.unwrap();
    assert_eq!(bootstrap.submitted_change.as_ref().map(|c| c["desired"].clone()), Some(json!({"fabric": "fabric1", "meshes": [
        {"name": "mesh1", "node_admin": 2, "broker": 3, "gateway": 3, "compute": 2},
        {"name": "mesh2", "node_admin": 2, "broker": 3, "gateway": 3, "compute": 2},
    ]})));
    let whats: Vec<String> = timeline.0.lock().unwrap().iter().map(|(w, _)| w.clone()).collect();
    assert_eq!(whats, ["add node", "restart node", "remove node", "bootstrap"]);
    assert_eq!(*cp.topology.read().await, before, "the UI changed nothing itself: only Builds were submitted");
}

#[tokio::test]
async fn node_admin_refusals_reach_the_ui_with_their_status_and_reason() {
    let (client, _) = node_admin().await;
    let timeline = Arc::new(Timeline::default());
    let ui = router(Some(client), timeline.clone());
    let (s, v) = call(&ui, "POST", "/api/nodes/mesh1.rpc.9/restart", None).await;
    assert_eq!((s, v["error"].as_str()), (StatusCode::NOT_FOUND, Some("unknown-node")), "{v}");
    let (s, v) = call(&ui, "DELETE", "/api/nodes/mesh1.admin.1", None).await;
    assert_eq!((s, v["error"].as_str()), (StatusCode::UNPROCESSABLE_ENTITY, Some("would-leave-mesh-without-admin")), "{v}");
    let (s, v) = call(&ui, "POST", "/api/nodes/spawn", Some(json!({"mesh": "mesh1", "kind": "rpc_node"}))).await;
    assert_eq!((s, v["error"].as_str()), (StatusCode::UNPROCESSABLE_ENTITY, Some("kind-not-managed-in-the-r-shape")), "{v}");
    let (s, v) = call(&ui, "POST", "/api/nodes/spawn", Some(json!({"mesh": "mesh7", "kind": "gateway"}))).await;
    assert_eq!((s, v["error"].as_str()), (StatusCode::NOT_FOUND, Some("unknown-mesh")), "{v}");
    let (s, v) = call(&ui, "DELETE", "/api/nodes/not..a..path", None).await;
    assert_eq!((s, v["error"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid-request")), "{v}");
    assert!(timeline.0.lock().unwrap().is_empty(), "a refusal submits nothing");
}

#[tokio::test]
async fn without_a_node_admin_or_with_one_unreachable_the_ui_says_so_by_name() {
    let ui = router(None, Arc::new(Timeline::default()));
    let (s, v) = call(&ui, "POST", "/api/bootstrap", None).await;
    assert_eq!((s, v["error"].as_str()), (StatusCode::SERVICE_UNAVAILABLE, Some("no-node-admin")), "{v}");
    let ui = router(Some(NodeAdminClient::new("http://127.0.0.1:9")), Arc::new(Timeline::default()));
    let (s, v) = call(&ui, "POST", "/api/nodes/mesh1.rpc.1/restart", None).await;
    assert_eq!((s, v["error"].as_str()), (StatusCode::BAD_GATEWAY, Some("node-admin-unreachable")), "{v}");
}

#[tokio::test]
async fn topology_labels_mesh_primary_on_the_node_admin_cohort_only_and_serves_node_admins_connection_facts() {
    let (client, _) = node_admin().await;
    let t = rafka_admin_ui::view::topology(&client, &reqwest::Client::new(), &rafka_admin_ui::view::TopoCtx { facts: &[], cuts: &[], listeners: &[], publishers: &[] }).await.unwrap();
    let seat = |n: &str| t["nodes"].as_array().unwrap().iter().find(|x| x["name"] == n).unwrap()["seat"].as_str().unwrap().to_string();
    assert_eq!(seat("mesh1.admin.1"), "fabric primary · mesh primary");
    assert_eq!(seat("mesh1.rpc.1"), "", "an rpc node first in its cohort is not a mesh primary: {t}");
    assert_eq!(seat("mesh1.rpc.2"), "");
    // The node-admin holds no connection fact in this fixture: it answers an empty list, not an error.
    assert_eq!(t["edge_errors"], json!([]), "{t}");
    assert_eq!(t["edges"], json!([]), "{t}");
}
