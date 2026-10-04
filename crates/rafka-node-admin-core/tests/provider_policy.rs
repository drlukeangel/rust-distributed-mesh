//! i143.e2.s1 functional: the deployment provider is fabric policy.
//!
//! The first admin's `MESH_SPAWN_TYPE` becomes the fabric's provider, which the
//! fabric view advertises; a second admin inherits it from that view (or is
//! refused by name when its own value disagrees); a Build that names a
//! provider is refused and publishes nothing.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use rafka_node_admin_core::build_state::{BuildStateAdapter, MemoryBuildStateAdapter};
use rafka_node_admin_core::deployment::provider::{FabricPolicy, PolicyRefusal};
use rafka_node_admin_core::http::{router, ControlPlane};
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::topology::Topology;
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

fn first_admin(spawn_type: Option<&str>) -> (axum::Router, Arc<MemoryBuildStateAdapter>) {
    let policy = FabricPolicy::bootstrap(spawn_type).expect("bootstrap");
    let mut admin = Node::allocated("mesh1.admin.1".parse().unwrap());
    admin.status = NodeStatus::ReadyForTraffic;
    admin.is_primary = true;
    admin.is_fabric_primary = true;
    admin.admin_api_base = Some("http://127.0.0.1:18001".into());
    let topology = Topology {
        fabric: Fabric { name: "fabric1".into(), status: ScopeStatus::Pending, provider: policy.provider },
        meshes: vec![Mesh { id: MeshId::mint(), name: "mesh1".into(), status: ScopeStatus::Pending }],
        nodes: vec![admin],
    };
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    (router(Arc::new(ControlPlane::new(builds.clone(), topology)), axum::Router::new()), builds)
}

async fn call(app: &axum::Router, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(body.map(|b| Body::from(b.to_string())).unwrap_or_else(Body::empty))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let s = res.status();
    let b = res.into_body().collect().await.unwrap().to_bytes();
    (s, serde_json::from_slice(&b).unwrap_or(Value::Null))
}

/// A joining admin reads the fabric's provider from the advertised view.
fn policy_from_view(view: &Value) -> FabricPolicy {
    let provider: ProviderKind = serde_json::from_value(view["provider"].clone()).expect("fabric view names its provider");
    FabricPolicy { provider }
}

#[tokio::test]
async fn a_second_admin_inherits_the_fabric_policy_or_refuses_by_name() {
    let (first, _) = first_admin(Some("container"));
    let (_, view) = call(&first, "GET", "/api/fabric", None).await;
    assert_eq!(view["provider"], "container");
    let established = policy_from_view(&view);
    assert_eq!(FabricPolicy::inherit(established, None).unwrap().provider, ProviderKind::Container, "unset inherits");
    assert_eq!(
        FabricPolicy::inherit(established, Some("process")),
        Err(PolicyRefusal::ProviderMismatch { fabric: ProviderKind::Container, local: ProviderKind::Process })
    );
}

#[tokio::test]
async fn an_unknown_provider_never_bootstraps_a_fabric() {
    assert_eq!(FabricPolicy::bootstrap(Some("vm")), Err(PolicyRefusal::UnknownProvider { value: "vm".into() }));
}

#[tokio::test]
async fn a_build_carrying_a_provider_is_refused_and_publishes_nothing() {
    let (app, builds) = first_admin(None);
    for body in [
        json!({"fabric": "fabric1", "provider": "container", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 3}]}),
        json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 3, "provider": "process"}]}),
    ] {
        let (s, v) = call(&app, "POST", "/api/build", Some(body)).await;
        assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{v}");
        assert_eq!(v["error"], "provider-mismatch");
        assert!(v["detail"].as_str().unwrap().contains("Process"), "names the fabric's policy: {v}");
    }
    assert!(builds.facts().await.unwrap().is_empty());
    let (s, _) = call(&app, "POST", "/api/build", Some(json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 3}]}))).await;
    assert_eq!(s, StatusCode::ACCEPTED, "the same Build without a provider is accepted");
}
