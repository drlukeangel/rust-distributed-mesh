//! The generic control API (PRD §7; mesh-control-plane.md §2).
//!
//! Every topology route is a front door that compiles to one [`BuildIntent`],
//! validates it against the observed topology, publishes it to the
//! [`BuildStateAdapter`] and answers `202 {"build_id"}`. A route never spawns,
//! kills or restarts anything itself: there is no lifecycle handle in
//! [`ControlPlane`] for it to call. `/api/shutdown` and runtime fault routes
//! are runtime administration, outside Build.

use crate::build::{pin, BuildId, BuildIntent, BuildReject, FabricDesired, MeshDesired};
use crate::build_state::{BuildIntentFact, BuildStateAdapter, BuildStateError};
use crate::model::{NodeKind, PathName};
use crate::topology::Topology;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};

/// What the control routes share. It holds the Build state, the observed
/// topology projection and the executor's wake-up — and no way to touch a
/// runtime.
pub struct ControlPlane {
    pub builds: Arc<dyn BuildStateAdapter>,
    pub topology: Arc<RwLock<Topology>>,
    /// Woken on every accepted Build so the executor re-plans.
    pub build_submitted: Arc<Notify>,
    /// Woken by `POST /api/shutdown`.
    pub shutdown: Arc<Notify>,
}

impl ControlPlane {
    pub fn new(builds: Arc<dyn BuildStateAdapter>, topology: Topology) -> Self {
        Self {
            builds,
            topology: Arc::new(RwLock::new(topology)),
            build_submitted: Arc::new(Notify::new()),
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Validate `intent` against observed state, pin what it means there, and
    /// publish it as a Build.
    pub async fn submit(&self, route: &'static str, intent: BuildIntent) -> Result<BuildId, Refusal> {
        let span = tracing::info_span!(
            "rafka.node_admin.build.create.via-rest",
            route,
            build_id = tracing::field::Empty,
            intent = tracing::field::Empty,
        );
        let _g = span.enter();
        let observed = self.topology.read().await.clone();
        let intent = match pin(intent, &observed) {
            Ok(pinned) => pinned,
            Err(reject) => {
                drop(_g);
                reject_span(route, &reject);
                return Err(Refusal::Reject(reject));
            }
        };
        let build_id = BuildId::mint();
        span.record("build_id", build_id.0.as_str());
        span.record("intent", serde_json::to_string(&intent).unwrap_or_default().as_str());
        let fact = BuildIntentFact {
            build_id: build_id.clone(),
            intent,
            traceparent: rafka_telemetry::current_traceparent(),
            submitted_at_ms: now_ms(),
        };
        self.builds.publish_intent(&fact).await.map_err(Refusal::State)?;
        tracing::info!(build_id = %build_id, "build accepted");
        self.build_submitted.notify_waiters();
        Ok(build_id)
    }
}

fn reject_span(route: &'static str, reject: &BuildReject) {
    // One honest name per reason (PRD §16): rafka.node_admin.build.reject.via-<reason>.
    macro_rules! emit {
        ($($reason:literal),*) => {
            match reject.reason() {
                $( $reason => tracing::info_span!(concat!("rafka.node_admin.build.reject.via-", $reason), route, detail = %reject)
                    .in_scope(|| tracing::info!(%reject, "build refused")), )*
                _ => tracing::info_span!("rafka.node_admin.build.reject.via-invalid-intent", route, detail = %reject)
                    .in_scope(|| tracing::info!(%reject, "build refused")),
            }
        };
    }
    emit!(
        "invalid-mesh-name",
        "duplicate-mesh",
        "mesh-without-admin",
        "unknown-mesh",
        "unknown-node",
        "node-not-live",
        "would-leave-mesh-without-admin",
        "mesh-already-exists",
        "fabric-mismatch",
        "empty-fabric",
        "provider-mismatch"
    );
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// A refused request, rendered with every non-leaking detail.
#[derive(Debug)]
pub enum Refusal {
    Reject(BuildReject),
    BadRequest(String),
    NotFound(String),
    Conflict(String),
    State(BuildStateError),
}

impl IntoResponse for Refusal {
    fn into_response(self) -> Response {
        let (status, error, detail) = match &self {
            Refusal::Reject(r @ (BuildReject::UnknownNode { .. } | BuildReject::UnknownMesh { .. })) => {
                (StatusCode::NOT_FOUND, r.reason().to_string(), r.to_string())
            }
            Refusal::Reject(r) => (StatusCode::UNPROCESSABLE_ENTITY, r.reason().to_string(), r.to_string()),
            Refusal::BadRequest(d) => (StatusCode::BAD_REQUEST, "invalid-request".into(), d.clone()),
            Refusal::NotFound(d) => (StatusCode::NOT_FOUND, "not-found".into(), d.clone()),
            Refusal::Conflict(d) => (StatusCode::CONFLICT, "conflict".into(), d.clone()),
            Refusal::State(e) => (StatusCode::SERVICE_UNAVAILABLE, "build-state-unavailable".into(), e.to_string()),
        };
        (status, Json(json!({ "error": error, "detail": detail }))).into_response()
    }
}

fn accepted(id: BuildId) -> Response {
    (StatusCode::ACCEPTED, Json(json!({ "build_id": id }))).into_response()
}

fn parse_path(name: &str) -> Result<PathName, Refusal> {
    name.parse().map_err(|e: crate::model::PathNameError| Refusal::BadRequest(e.to_string()))
}

fn body<T: serde::de::DeserializeOwned>(raw: &str) -> Result<T, Refusal> {
    serde_json::from_str(raw).map_err(|e| Refusal::BadRequest(e.to_string()))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SpawnBody {
    mesh: String,
    kind: NodeKind,
}

#[derive(Deserialize)]
struct BuildQuery {
    id: String,
}

type Shared = Arc<ControlPlane>;

async fn post_build(State(cp): State<Shared>, raw: String) -> Result<Response, Refusal> {
    let value: Value = body(&raw)?;
    if crate::deployment::provider::build_names_a_provider(&value) {
        let reject = BuildReject::ProviderInBuild { fabric_provider: cp.topology.read().await.fabric.provider };
        reject_span("POST /api/build", &reject);
        return Err(Refusal::Reject(reject));
    }
    let desired: FabricDesired = body(&raw)?;
    Ok(accepted(cp.submit("POST /api/build", BuildIntent::ReconcileFabric { desired }).await?))
}

async fn get_build(State(cp): State<Shared>, Query(q): Query<BuildQuery>) -> Result<Response, Refusal> {
    match cp.builds.read_build(&BuildId(q.id.clone())).await {
        Ok(p) => Ok(Json(p).into_response()),
        Err(BuildStateError::UnknownBuild(id)) => Err(Refusal::NotFound(format!("no Build {id}"))),
        Err(e) => Err(Refusal::State(e)),
    }
}

async fn delete_build(State(cp): State<Shared>, Query(q): Query<BuildQuery>) -> Result<Response, Refusal> {
    let id = BuildId(q.id);
    let p = cp.builds.read_build(&id).await.map_err(|e| match e {
        BuildStateError::UnknownBuild(id) => Refusal::NotFound(format!("no Build {id}")),
        e => Refusal::State(e),
    })?;
    if matches!(p.state, crate::build_state::BuildState::Pending | crate::build_state::BuildState::Running) {
        return Err(Refusal::Conflict(format!("Build {id} is still {:?}; only finished Builds leave history", p.state)));
    }
    cp.builds.forget(&id).await.map_err(Refusal::State)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn spawn_node(State(cp): State<Shared>, raw: String) -> Result<Response, Refusal> {
    let b: SpawnBody = body(&raw)?;
    Ok(accepted(cp.submit("POST /api/nodes/spawn", BuildIntent::AddNode { mesh: b.mesh, node_kind: b.kind, target: None }).await?))
}

async fn delete_node(State(cp): State<Shared>, Path(name): Path<String>) -> Result<Response, Refusal> {
    let node = parse_path(&name)?;
    Ok(accepted(cp.submit("DELETE /api/nodes/{name}", BuildIntent::RemoveNode { node, incarnation: None }).await?))
}

async fn restart_node(State(cp): State<Shared>, Path(name): Path<String>) -> Result<Response, Refusal> {
    let node = parse_path(&name)?;
    Ok(accepted(cp.submit("POST /api/nodes/{name}/restart", BuildIntent::RestartNode { node, from_incarnation: None }).await?))
}

async fn get_nodes(State(cp): State<Shared>) -> Json<Value> {
    let t = cp.topology.read().await;
    Json(json!({ "nodes": t.nodes }))
}

async fn get_mesh(State(cp): State<Shared>, Path(id): Path<String>) -> Result<Response, Refusal> {
    let t = cp.topology.read().await;
    t.mesh_view(&id).map(|v| Json(v).into_response()).ok_or_else(|| Refusal::NotFound(format!("no mesh {id}")))
}

async fn get_fabric(State(cp): State<Shared>) -> Json<Value> {
    Json(serde_json::to_value(cp.topology.read().await.fabric_view()).unwrap_or(Value::Null))
}

async fn create_mesh(State(cp): State<Shared>, raw: String) -> Result<Response, Refusal> {
    let desired: MeshDesired = body(&raw)?;
    Ok(accepted(cp.submit("POST /api/meshes", BuildIntent::CreateMesh { desired }).await?))
}

async fn delete_mesh(State(cp): State<Shared>, Path(id): Path<String>) -> Result<Response, Refusal> {
    let mesh = {
        let t = cp.topology.read().await;
        t.meshes.iter().find(|m| m.name == id || m.id.as_ref().is_some_and(|i| i.as_str() == id)).map(|m| m.name.clone())
    }
    .ok_or_else(|| Refusal::NotFound(format!("no mesh {id}")))?;
    Ok(accepted(cp.submit("DELETE /api/meshes/{id}", BuildIntent::RemoveMesh { mesh }).await?))
}

async fn shutdown(State(cp): State<Shared>) -> Response {
    tracing::info_span!("rafka.node_admin.fabric.update.via-shutdown").in_scope(|| tracing::info!("shutdown requested"));
    cp.shutdown.notify_waiters();
    (StatusCode::ACCEPTED, Json(json!({ "shutdown": "requested" }))).into_response()
}

/// The control router. `runtime_routes` (fault injection and the like) are
/// merged in as-is; they get no access to Build.
pub fn router(cp: Arc<ControlPlane>, runtime_routes: Router) -> Router {
    Router::new()
        .route("/api/build", post(post_build))
        .route("/api/builds", get(get_build).delete(delete_build))
        .route("/api/nodes", get(get_nodes))
        .route("/api/nodes/spawn", post(spawn_node))
        .route("/api/nodes/{name}", delete(delete_node))
        .route("/api/nodes/{name}/restart", post(restart_node))
        .route("/api/meshes", post(create_mesh))
        .route("/api/meshes/{id}", get(get_mesh).delete(delete_mesh))
        .route("/api/fabric", get(get_fabric))
        .route("/api/shutdown", post(shutdown))
        .with_state(cp)
        .merge(runtime_routes)
}
