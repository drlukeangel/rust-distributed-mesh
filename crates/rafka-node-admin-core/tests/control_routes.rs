//! i143.e1.s4 functional: the generic control routes only submit Build.
//!
//! Each mutation route answers 202 with a build id and its only effect is one
//! published Build intent; the observed topology is untouched on the request
//! path. Refusals are named (404/422 + `build.reject.via-<reason>`) and
//! publish nothing. `/api/shutdown` and runtime fault routes stay outside Build.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::post;
use axum::Router;
use http_body_util::BodyExt;
use rafka_node_admin_core::build_state::{
    AttemptOutcome, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildStateAdapter, MemoryBuildStateAdapter,
};
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::http::{router, ControlPlane};
use rafka_node_admin_core::model::*;
use rafka_node_admin_core::topology::Topology;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;
use tracing_subscriber::layer::SubscriberExt;

#[derive(Clone, Default)]
struct SpanNames(Arc<Mutex<Vec<(String, Option<String>)>>>);

impl<S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>> tracing_subscriber::Layer<S> for SpanNames {
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, _: &tracing::span::Id, _: tracing_subscriber::layer::Context<'_, S>) {
        self.0.lock().unwrap().push((attrs.metadata().name().to_string(), None));
    }
    fn on_record(&self, id: &tracing::span::Id, values: &tracing::span::Record<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        struct V(Option<String>);
        impl tracing::field::Visit for V {
            fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
                if f.name() == "build_id" {
                    self.0 = Some(v.to_string());
                }
            }
            fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
        }
        let mut v = V(None);
        values.record(&mut v);
        if let (Some(b), Some(span)) = (v.0, ctx.span(id)) {
            self.0.lock().unwrap().push((span.name().to_string(), Some(b)));
        }
    }
}

fn node(name: &str, primary: bool) -> Node {
    let mut n = Node::allocated(name.parse().unwrap());
    n.status = NodeStatus::ReadyForTraffic;
    n.is_primary = primary;
    if n.kind == NodeKind::NodeAdmin {
        n.admin_api_base = Some(format!("http://127.0.0.1:1800{}", n.name.ordinal));
        n.is_fabric_primary = primary;
    }
    n
}

fn mn() -> Topology {
    Topology {
        fabric: Fabric { name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: MeshId::mint(), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
        nodes: vec![
            node("mesh1.admin.1", true),
            node("mesh1.admin.2", false),
            node("mesh1.rpc.1", true),
            node("mesh1.rpc.2", false),
            node("mesh1.rpc.3", false),
        ],
    }
}

struct Harness {
    cp: Arc<ControlPlane>,
    builds: Arc<MemoryBuildStateAdapter>,
    app: Router,
    spans: SpanNames,
    _guard: tracing::subscriber::DefaultGuard,
}

fn harness(runtime_routes: Router) -> Harness {
    let spans = SpanNames::default();
    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let cp = Arc::new(ControlPlane::new(builds.clone(), mn()));
    let app = router(cp.clone(), runtime_routes);
    Harness { cp, builds, app, spans, _guard: guard }
}

async fn call(app: &Router, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
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

impl Harness {
    async fn facts(&self) -> Vec<BuildFact> {
        self.builds.facts().await.unwrap()
    }
    fn span_with_build(&self, name: &str, id: &str) -> bool {
        self.spans.0.lock().unwrap().iter().any(|(n, b)| n == name && b.as_deref() == Some(id))
    }
    fn span(&self, name: &str) -> bool {
        self.spans.0.lock().unwrap().iter().any(|(n, _)| n == name)
    }
}

#[tokio::test]
async fn spawn_delete_and_restart_only_submit_build() {
    let h = harness(Router::new());
    let before = h.cp.topology.read().await.clone();
    let cases = [
        ("POST", "/api/nodes/spawn", Some(json!({"mesh": "mesh1", "kind": "rpc_node"})), "add_node"),
        ("DELETE", "/api/nodes/mesh1.rpc.3", None, "remove_node"),
        ("POST", "/api/nodes/mesh1.rpc.2/restart", None, "restart_node"),
    ];
    for (i, (method, uri, body, kind)) in cases.into_iter().enumerate() {
        let (status, v) = call(&h.app, method, uri, body).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{uri}: {v}");
        let id = v["build_id"].as_str().expect("build_id").to_string();
        let facts = h.facts().await;
        assert_eq!(facts.len(), i + 1, "exactly one fact per mutation: {facts:?}");
        assert!(matches!(&facts[i], BuildFact::Intent(f) if f.build_id.0 == id), "the fact is the intent");
        let (_, b) = call(&h.app, "GET", &format!("/api/builds?id={id}"), None).await;
        assert_eq!(b["intent"]["kind"], kind, "{b}");
        assert_eq!(b["state"], "pending");
        assert!(h.span_with_build("rafka.node_admin.build.create.via-rest", &id), "create span carries the build id");
    }
    assert_eq!(*h.cp.topology.read().await, before, "no route touched the observed topology");
}

#[tokio::test]
async fn post_build_and_mesh_routes_submit_build() {
    let h = harness(Router::new());
    let (s, v) = call(&h.app, "POST", "/api/build", Some(json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 5}]}))).await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
    let (s, v) = call(&h.app, "POST", "/api/meshes", Some(json!({"name": "mesh2", "node_admin": 2, "rpc_node": 3}))).await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
    let (_, b) = call(&h.app, "GET", &format!("/api/builds?id={}", v["build_id"].as_str().unwrap()), None).await;
    assert_eq!(b["intent"]["kind"], "create_mesh");
    let (s, v) = call(&h.app, "DELETE", "/api/meshes/mesh1", None).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "the only mesh cannot go: {v}");
    assert_eq!(v["error"], "empty-fabric");
    assert_eq!(h.facts().await.len(), 2);
}

#[tokio::test]
async fn refusals_are_named_and_publish_nothing() {
    let h = harness(Router::new());
    let (s, v) = call(&h.app, "POST", "/api/nodes/mesh1.rpc.9/restart", None).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{v}");
    assert_eq!(v["error"], "unknown-node");
    assert!(h.span("rafka.node_admin.build.reject.via-unknown-node"));
    let (s, v) = call(&h.app, "POST", "/api/build", Some(json!({"fabric": "fabric1", "meshes": [{"name": "Mesh!", "node_admin": 1, "rpc_node": 1}]}))).await;
    assert_eq!((s, v["error"].as_str()), (StatusCode::UNPROCESSABLE_ENTITY, Some("invalid-mesh-name")), "{v}");
    assert!(h.span("rafka.node_admin.build.reject.via-invalid-mesh-name"));
    let (s, v) = call(&h.app, "POST", "/api/nodes/not-a-path/restart", None).await;
    assert_eq!((s, v["error"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid-request")), "{v}");
    let (s, v) = call(&h.app, "POST", "/api/nodes/spawn", Some(json!({"mesh": "mesh1", "kind": "broker"}))).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "a legacy role is not a node kind: {v}");
    let (s, _) = call(&h.app, "DELETE", "/api/nodes/mesh1.admin.1", None).await;
    assert_eq!(s, StatusCode::ACCEPTED, "two admins: removing one is legal");
    assert_eq!(h.facts().await.len(), 1, "refusals published nothing");
}

#[tokio::test]
async fn build_history_is_readable_and_only_finished_builds_can_be_forgotten() {
    let h = harness(Router::new());
    let (_, v) = call(&h.app, "POST", "/api/nodes/mesh1.rpc.1/restart", None).await;
    let id = v["build_id"].as_str().unwrap().to_string();
    let (s, _) = call(&h.app, "DELETE", &format!("/api/builds?id={id}"), None).await;
    assert_eq!(s, StatusCode::CONFLICT, "a pending Build is not history");
    let bid = BuildId(id.clone());
    h.builds.claim_attempt(&BuildAttemptClaim { build_id: bid.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    h.builds.append_attempt_receipt(&BuildAttemptReceipt { build_id: bid, attempt: 1, outcome: AttemptOutcome::Converged }).await.unwrap();
    let (_, b) = call(&h.app, "GET", &format!("/api/builds?id={id}"), None).await;
    assert_eq!(b["state"], "complete");
    let before = h.cp.topology.read().await.clone();
    let (s, _) = call(&h.app, "DELETE", &format!("/api/builds?id={id}"), None).await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let (s, v) = call(&h.app, "GET", &format!("/api/builds?id={id}"), None).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{v}");
    assert_eq!(*h.cp.topology.read().await, before, "history administration is not a topology mutation");
}

#[tokio::test]
async fn views_publish_nodes_meshes_and_the_owning_admin_endpoint() {
    let h = harness(Router::new());
    let (s, v) = call(&h.app, "GET", "/api/nodes", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["nodes"].as_array().unwrap().len(), 5);
    assert_eq!(v["nodes"][0]["name"], "mesh1.admin.1");
    let (_, f) = call(&h.app, "GET", "/api/fabric", None).await;
    assert_eq!(f["admin_api_base"], "http://127.0.0.1:18001");
    assert_eq!(f["meshes"][0]["admin_api_base"], "http://127.0.0.1:18001");
    let (s, m) = call(&h.app, "GET", "/api/meshes/mesh1", None).await;
    assert_eq!((s, m["primary_admin"].as_str()), (StatusCode::OK, Some("mesh1.admin.1")));
    let (s, _) = call(&h.app, "GET", "/api/meshes/nope", None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn shutdown_and_runtime_fault_routes_stay_outside_build() {
    let hit = Arc::new(Mutex::new(0));
    let hit2 = hit.clone();
    let chaos = Router::new().route(
        "/api/chaos/wedge",
        post(move || {
            let hit = hit2.clone();
            async move {
                *hit.lock().unwrap() += 1;
                "wedged"
            }
        }),
    );
    let h = harness(chaos);
    let waiter = {
        let n = h.cp.shutdown.clone();
        tokio::spawn(async move { n.notified().await })
    };
    tokio::task::yield_now().await;
    let (s, _) = call(&h.app, "POST", "/api/chaos/wedge", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(*hit.lock().unwrap(), 1);
    let (s, _) = call(&h.app, "POST", "/api/shutdown", Some(json!({}))).await;
    assert_eq!(s, StatusCode::ACCEPTED);
    tokio::time::timeout(std::time::Duration::from_secs(5), waiter).await.expect("shutdown notified").unwrap();
    assert!(h.facts().await.is_empty(), "neither shutdown nor fault control submitted a Build");
}
