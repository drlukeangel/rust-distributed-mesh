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
use rafka_node_admin_core::accepted::{AcceptedStore, FabricTopology};
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
    n.incarnation_id = Some(IncarnationId::mint());
    n.is_primary = primary;
    if n.kind == NodeKind::NodeAdmin {
        n.admin_api_base = Some(format!("http://127.0.0.1:1800{}", n.name.ordinal));
        n.is_fabric_primary = primary;
    }
    n
}

fn mn() -> Topology {
    Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic }],
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

/// This admin (`mesh1.admin.1`, the fabric-primary) holds a settled accepted Build of the
/// observed topology: the one every change compiles against.
async fn harness(runtime_routes: Router) -> Harness {
    let spans = SpanNames::default();
    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = mn();
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let cp = Arc::new(ControlPlane::new(builds.clone(), accepted, "mesh1.admin.1".parse().unwrap(), t));
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
    /// Run the Build's open attempt to convergence, as its executor would.
    async fn settle(&self, id: &str) {
        let bid = BuildId(id.into());
        let attempt = self.builds.read_build(&bid).await.unwrap().attempt + 1;
        self.builds.claim_attempt(&BuildAttemptClaim { build_id: bid.clone(), attempt, executor: "mesh1.admin.1".into() }).await.unwrap();
        self.builds.append_attempt_receipt(&BuildAttemptReceipt { build_id: bid, attempt, outcome: AttemptOutcome::Converged }).await.unwrap();
    }
    fn span_with_build(&self, name: &str, id: &str) -> bool {
        self.spans.0.lock().unwrap().iter().any(|(n, b)| n == name && b.as_deref() == Some(id))
    }
    fn span(&self, name: &str) -> bool {
        self.spans.0.lock().unwrap().iter().any(|(n, _)| n == name)
    }
}

#[tokio::test]
async fn a_change_compiles_to_the_next_build_one_at_a_time_and_a_restart_opens_an_attempt() {
    let h = harness(Router::new()).await;
    let before = h.cp.topology.read().await.clone();
    let b0 = h.cp.accepted.build_id().await.unwrap().0;
    let seed = h.facts().await.len();

    // A change: the next complete Build, and Fabric.build_id names it.
    let (status, v) = call(&h.app, "POST", "/api/nodes/spawn", Some(json!({"mesh": "mesh1", "kind": "rpc_node"}))).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{v}");
    let b1 = v["build_id"].as_str().expect("build_id").to_string();
    assert_ne!(b1, b0);
    let facts = h.facts().await;
    assert_eq!(facts.len(), seed + 1, "exactly one fact per accepted change: {facts:?}");
    assert!(matches!(&facts[seed], BuildFact::Accepted(f) if f.build_id.0 == b1), "the fact is the accepted Build");
    let (_, b) = call(&h.app, "GET", &format!("/api/builds?id={b1}"), None).await;
    assert_eq!(b["submitted_change"]["kind"], "add_node", "{b}");
    assert_eq!(b["state"], "pending");
    assert_eq!(b["reason"], "requested");
    let paths: Vec<&str> = b["topology"]["meshes"]["mesh1"]["nodes"].as_array().unwrap().iter().filter_map(|p| p.as_str()).collect();
    assert_eq!(paths, ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1", "mesh1.rpc.2", "mesh1.rpc.3", "mesh1.rpc.4"], "exact paths, the next free ordinal");
    assert!(h.span_with_build("rdm.node_admin.build.create.via-rest", &b1), "create span carries the build id");
    let (_, f) = call(&h.app, "GET", "/api/fabric", None).await;
    assert_eq!(f["build_id"], b1.as_str(), "{f}");

    // One Build in flight: the next change is refused by name until it settles.
    let (status, v) = call(&h.app, "DELETE", "/api/nodes/mesh1.rpc.3", None).await;
    assert_eq!(status, StatusCode::CONFLICT, "{v}");
    assert_eq!((v["error"].as_str(), v["current_build_id"].as_str()), (Some("build-in-progress"), Some(b1.as_str())), "{v}");
    h.settle(&b1).await;
    let (status, v) = call(&h.app, "DELETE", "/api/nodes/mesh1.rpc.3", None).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{v}");
    let b2 = v["build_id"].as_str().unwrap().to_string();
    let (_, b) = call(&h.app, "GET", &format!("/api/builds?id={b2}"), None).await;
    assert_eq!(b["submitted_change"]["kind"], "remove_node", "{b}");
    let paths: Vec<&str> = b["topology"]["meshes"]["mesh1"]["nodes"].as_array().unwrap().iter().filter_map(|p| p.as_str()).collect();
    assert_eq!(paths, ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1", "mesh1.rpc.2", "mesh1.rpc.4"], "exactly that path left");
    h.settle(&b2).await;

    // A restart changes no topology: it opens the next attempt of the accepted Build.
    let (status, v) = call(&h.app, "POST", "/api/nodes/mesh1.rpc.2/restart", None).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{v}");
    assert_eq!(v["build_id"], b2.as_str(), "the same Build: {v}");
    let (_, b) = call(&h.app, "GET", &format!("/api/builds?id={b2}"), None).await;
    assert_eq!((b["state"].as_str(), b["reason"].as_str(), b["action"]["action"].as_str(), b["action"]["path"].as_str()), (Some("pending"), Some("restart"), Some("restart"), Some("mesh1.rpc.2")), "{b}");
    assert!(matches!(h.facts().await.last(), Some(BuildFact::Opened(o)) if o.build_id.0 == b2 && o.attempt == 2));
    assert!(h.span_with_build("rdm.node_admin.build.update.via-rest", &b2));
    let (_, f) = call(&h.app, "GET", "/api/fabric", None).await;
    assert_eq!(f["build_id"], b2.as_str(), "Fabric.build_id is unchanged by a restart");
    assert_eq!(*h.cp.topology.read().await, before, "no route touched the observed topology");
}

/// CONTRACT: `POST /api/nodes/{name}/replace` mints no Build. It opens the next attempt of the
/// accepted Build, reason `replace`, fenced to the exact birth the view holds at the path, and
/// leaves `Fabric.build_id` and the topology alone. An unknown path is refused by name.
#[tokio::test]
async fn a_replace_opens_the_next_attempt_of_the_accepted_build_fenced_to_the_live_birth() {
    let h = harness(Router::new()).await;
    let b0 = h.cp.accepted.build_id().await.unwrap().0;
    let seed = h.facts().await.len();
    let (s, v) = call(&h.app, "POST", "/api/nodes/mesh1.rpc.9/replace", None).await;
    assert_eq!((s, v["error"].as_str()), (StatusCode::NOT_FOUND, Some("unknown-node")), "{v}");
    let birth = h.cp.topology.read().await.node(&"mesh1.rpc.2".parse().unwrap()).unwrap().incarnation_id.clone().unwrap();
    let (status, v) = call(&h.app, "POST", "/api/nodes/mesh1.rpc.2/replace", None).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{v}");
    assert_eq!(v["build_id"], b0.as_str(), "no Build is minted: {v}");
    let (_, b) = call(&h.app, "GET", &format!("/api/builds?id={b0}"), None).await;
    assert_eq!((b["state"].as_str(), b["reason"].as_str(), b["action"]["action"].as_str(), b["action"]["path"].as_str()), (Some("pending"), Some("replace"), Some("replace"), Some("mesh1.rpc.2")), "{b}");
    assert_eq!(b["action"]["from_incarnation"], serde_json::to_value(&birth).unwrap(), "fenced to the held birth: {b}");
    assert_eq!(h.facts().await.len(), seed + 1, "exactly one fact: the opened attempt");
    assert!(matches!(h.facts().await.last(), Some(BuildFact::Opened(o)) if o.build_id.0 == b0 && o.attempt == 2));
    assert!(h.span_with_build("rdm.node_admin.build.update.via-rest", &b0));
    let (_, f) = call(&h.app, "GET", "/api/fabric", None).await;
    assert_eq!(f["build_id"], b0.as_str(), "Fabric.build_id is unchanged by a replace");
}

#[tokio::test]
async fn post_build_and_mesh_routes_submit_build() {
    let h = harness(Router::new()).await;
    let (s, v) = call(&h.app, "POST", "/api/build", Some(json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 5}]}))).await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
    h.settle(v["build_id"].as_str().unwrap()).await;
    let (s, v) = call(&h.app, "POST", "/api/meshes", Some(json!({"name": "mesh2", "node_admin": 2, "rpc_node": 3}))).await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
    let b2 = v["build_id"].as_str().unwrap().to_string();
    let (_, b) = call(&h.app, "GET", &format!("/api/builds?id={b2}"), None).await;
    assert_eq!(b["submitted_change"]["kind"], "create_mesh");
    assert_eq!(b["topology"]["meshes"]["mesh2"]["nodes"].as_array().unwrap().len(), 5, "{b}");
    h.settle(&b2).await;
    let (s, v) = call(&h.app, "DELETE", "/api/meshes/mesh2", None).await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
    h.settle(v["build_id"].as_str().unwrap()).await;
    let (s, v) = call(&h.app, "DELETE", "/api/meshes/mesh1", None).await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "the only mesh cannot go: {v}");
    assert_eq!(v["error"], "empty-fabric");
    assert_eq!(h.facts().await.iter().filter(|f| matches!(f, BuildFact::Accepted(_))).count(), 4, "the seed and three accepted Builds");
}

#[tokio::test]
async fn refusals_are_named_and_publish_nothing() {
    let h = harness(Router::new()).await;
    let seed = h.facts().await.len();
    let (s, v) = call(&h.app, "POST", "/api/nodes/mesh1.rpc.9/restart", None).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{v}");
    assert_eq!(v["error"], "unknown-node");
    assert!(h.span("rdm.node_admin.build.reject.via-unknown-node"));
    let (s, v) = call(&h.app, "POST", "/api/build", Some(json!({"fabric": "fabric1", "meshes": [{"name": "Mesh!", "node_admin": 1, "rpc_node": 1}]}))).await;
    assert_eq!((s, v["error"].as_str()), (StatusCode::UNPROCESSABLE_ENTITY, Some("invalid-mesh-name")), "{v}");
    assert!(h.span("rdm.node_admin.build.reject.via-invalid-mesh-name"));
    let (s, v) = call(&h.app, "POST", "/api/nodes/not-a-path/restart", None).await;
    assert_eq!((s, v["error"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid-request")), "{v}");
    let (s, v) = call(&h.app, "POST", "/api/nodes/spawn", Some(json!({"mesh": "mesh1", "kind": "registry"}))).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "a role no node kind names is refused: {v}");
    let (s, _) = call(&h.app, "DELETE", "/api/nodes/mesh1.admin.1", None).await;
    assert_eq!(s, StatusCode::ACCEPTED, "two admins: removing one is legal");
    assert_eq!(h.facts().await.len(), seed + 1, "refusals published nothing");
}

#[tokio::test]
async fn build_history_is_readable_and_only_finished_builds_that_are_not_current_can_be_forgotten() {
    let h = harness(Router::new()).await;
    let b0 = h.cp.accepted.build_id().await.unwrap().0;
    // A restart opens an attempt of the accepted Build: it is in flight, not history.
    let (_, v) = call(&h.app, "POST", "/api/nodes/mesh1.rpc.1/restart", None).await;
    assert_eq!(v["build_id"], b0.as_str(), "{v}");
    let (s, _) = call(&h.app, "DELETE", &format!("/api/builds?id={b0}"), None).await;
    assert_eq!(s, StatusCode::CONFLICT, "a pending Build is not history");
    h.settle(&b0).await;
    let (_, b) = call(&h.app, "GET", &format!("/api/builds?id={b0}"), None).await;
    assert_eq!((b["state"].as_str(), b["attempt"].as_u64()), (Some("complete"), Some(2)));
    // Complete, but the accepted topology: it leaves history only once the pointer moves.
    let (s, v) = call(&h.app, "DELETE", &format!("/api/builds?id={b0}"), None).await;
    assert_eq!(s, StatusCode::CONFLICT, "{v}");
    let (s, v) = call(&h.app, "POST", "/api/nodes/spawn", Some(json!({"mesh": "mesh1", "kind": "rpc_node"}))).await;
    assert_eq!(s, StatusCode::ACCEPTED, "{v}");
    let before = h.cp.topology.read().await.clone();
    let (s, _) = call(&h.app, "DELETE", &format!("/api/builds?id={b0}"), None).await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let (s, v) = call(&h.app, "GET", &format!("/api/builds?id={b0}"), None).await;
    assert_eq!(s, StatusCode::NOT_FOUND, "{v}");
    assert_eq!(*h.cp.topology.read().await, before, "history administration is not a topology mutation");
}

#[tokio::test]
async fn views_publish_nodes_meshes_and_the_owning_admin_endpoint() {
    let h = harness(Router::new()).await;
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
    let h = harness(chaos).await;
    let seed = h.facts().await.len();
    let control = seat(&h, "mesh1.admin.1").await;
    let (s, _) = call(&h.app, "POST", "/api/chaos/wedge", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(*hit.lock().unwrap(), 1);
    let (s, body) = call(&h.app, "POST", "/api/shutdown", Some(json!({}))).await;
    assert_eq!((s, body["initiated_by"].as_str()), (StatusCode::ACCEPTED, Some("mesh1.admin.1")), "{body}");
    assert_eq!(control.held().map(|sd| sd.initiated_by), Some("mesh1.admin.1".to_string()), "the fabric-primary holds the shutdown it began");
    let (_, f) = call(&h.app, "GET", "/api/fabric", None).await;
    assert_eq!(f["shutdown"]["phase"], "frozen", "progress is visible on /api/fabric: {f}");
    assert_eq!(h.facts().await.len(), seed, "neither shutdown nor fault control submitted a Build");
}

/// A fabric shutdown seat for the admin `me` over memory storage.
async fn seat(h: &Harness, me: &str) -> Arc<rafka_node_admin_core::shutdown::ShutdownControl> {
    let control = Arc::new(
        rafka_node_admin_core::shutdown::ShutdownControl::open(Arc::new(rafka_node_admin_core::fabric_storage::MemoryFabricStorage::new()), me).await.unwrap(),
    );
    let _ = h.cp.fabric_shutdown.set(Arc::new(rafka_node_admin_core::http::ShutdownSeat {
        control: control.clone(),
        me: me.parse().unwrap(),
        node_id: rafka_node_admin_core::model::NodeId::mint(),
    }));
    control
}

#[tokio::test]
async fn only_the_fabric_primary_begins_a_fabric_shutdown() {
    let h = harness(Router::new()).await;
    let control = seat(&h, "mesh1.admin.2").await;
    let (s, body) = call(&h.app, "POST", "/api/shutdown", Some(json!({}))).await;
    assert_eq!(s, StatusCode::CONFLICT, "{body}");
    assert_eq!((body["error"].as_str(), body["fabric_primary"].as_str()), (Some("rejected-not-authority"), Some("mesh1.admin.1")), "{body}");
    assert!(control.held().is_none(), "a refused shutdown holds nothing");
}

/// CONTRACT (i143 export gate: the fabric-primary is the only topology writer): an admin that is
/// not the current fabric-primary refuses every topology-changing route by name, naming the
/// primary, and appends nothing to the Build facts.
#[tokio::test]
async fn a_node_admin_that_is_not_the_fabric_primary_refuses_every_topology_change_by_name() {
    let spans = SpanNames::default();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    let builds = Arc::new(MemoryBuildStateAdapter::new());
    let t = mn();
    let accepted = AcceptedStore::seeded(&*builds, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let before = builds.facts().await.unwrap().len();
    let cp = Arc::new(ControlPlane::new(builds.clone(), accepted, "mesh1.admin.2".parse().unwrap(), t));
    let app = router(cp, Router::new());
    for (method, uri, body) in [
        ("POST", "/api/build", Some(json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 5}]}))),
        ("POST", "/api/nodes/spawn", Some(json!({"mesh": "mesh1", "kind": "rpc_node"}))),
        ("POST", "/api/meshes", Some(json!({"name": "mesh2", "node_admin": 1, "rpc_node": 1}))),
        ("POST", "/api/nodes/mesh1.rpc.1/restart", None),
        ("POST", "/api/nodes/mesh1.rpc.1/replace", None),
    ] {
        let (s, v) = call(&app, method, uri, body).await;
        assert_eq!(s, StatusCode::CONFLICT, "{method} {uri}: {v}");
        assert_eq!((v["error"].as_str(), v["fabric_primary"].as_str()), (Some("rejected-not-authority"), Some("mesh1.admin.1")), "{method} {uri}: {v}");
    }
    assert_eq!(builds.facts().await.unwrap().len(), before, "a refused change appends no Build fact");
    assert!(spans.0.lock().unwrap().iter().any(|(n, _)| n == "rdm.node_admin.build.reject.via-not-authority"), "the refusal is spanned by name");
}

/// Builds storage that refuses every accepted Build by name and holds the rest in memory.
struct RefusingAccept(Arc<MemoryBuildStateAdapter>);

#[async_trait::async_trait]
impl rafka_node_admin_core::build_state::BuildStateAdapter for RefusingAccept {
    async fn publish_accepted(&self, accepted: &rafka_node_admin_core::build_state::BuildAccepted) -> Result<(), rafka_node_admin_core::build_state::BuildStateError> {
        Err(rafka_node_admin_core::build_state::BuildStateError::Io(format!("builds.storage refused Build {}", accepted.build_id.0)))
    }
    async fn open_attempt(&self, o: &rafka_node_admin_core::build_state::AttemptOpened) -> Result<(), rafka_node_admin_core::build_state::BuildStateError> {
        self.0.open_attempt(o).await
    }
    async fn read_build(&self, b: &BuildId) -> Result<rafka_node_admin_core::build_state::BuildProjection, rafka_node_admin_core::build_state::BuildStateError> {
        self.0.read_build(b).await
    }
    async fn list_active(&self) -> Result<Vec<rafka_node_admin_core::build_state::BuildProjection>, rafka_node_admin_core::build_state::BuildStateError> {
        self.0.list_active().await
    }
    async fn claim_attempt(&self, c: &BuildAttemptClaim) -> Result<rafka_node_admin_core::build_state::ClaimOutcome, rafka_node_admin_core::build_state::BuildStateError> {
        self.0.claim_attempt(c).await
    }
    async fn adopt_claim(&self, c: &BuildAttemptClaim) -> Result<(), rafka_node_admin_core::build_state::BuildStateError> {
        self.0.adopt_claim(c).await
    }
    async fn append_step_receipt(&self, r: &rafka_node_admin_core::build_state::BuildStepReceipt) -> Result<(), rafka_node_admin_core::build_state::BuildStateError> {
        self.0.append_step_receipt(r).await
    }
    async fn append_attempt_receipt(&self, r: &BuildAttemptReceipt) -> Result<(), rafka_node_admin_core::build_state::BuildStateError> {
        self.0.append_attempt_receipt(r).await
    }
    async fn facts(&self) -> Result<Vec<BuildFact>, rafka_node_admin_core::build_state::BuildStateError> {
        self.0.facts().await
    }
    async fn forget(&self, b: &BuildId) -> Result<(), rafka_node_admin_core::build_state::BuildStateError> {
        self.0.forget(b).await
    }
}

/// CONTRACT (i143 export gate: complete Build persistence precedes the Fabric.build_id write): a
/// change whose Build builds.storage refuses is refused with the store's reason, and
/// Fabric.build_id still names the previous Build: the pointer never names a Build that is not
/// durable.
#[tokio::test]
async fn a_build_the_store_refuses_is_never_named_by_fabric_build_id() {
    let mem = Arc::new(MemoryBuildStateAdapter::new());
    let t = mn();
    let accepted = AcceptedStore::seeded(&*mem, t.fabric.id.clone(), FabricTopology::of_observed(&t), "mesh1.admin.1").await.unwrap();
    let previous = accepted.build_id().await.expect("the seeded Build is named");
    let cp = Arc::new(ControlPlane::new(Arc::new(RefusingAccept(mem.clone())), accepted.clone(), "mesh1.admin.1".parse().unwrap(), t));
    let app = router(cp, Router::new());
    let (s, v) = call(&app, "POST", "/api/nodes/spawn", Some(json!({"mesh": "mesh1", "kind": "rpc_node"}))).await;
    assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE, "{v}");
    assert!(v["detail"].as_str().unwrap_or_default().contains("builds.storage refused Build"), "the refusal carries the store's reason: {v}");
    assert_eq!(accepted.build_id().await, Some(previous), "Fabric.build_id still names the previous Build");
}
