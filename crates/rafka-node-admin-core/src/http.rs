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
    /// This admin's current desired topology (`crate::desired`).
    pub desired: Arc<crate::desired::DesiredStore>,
    pub topology: Arc<RwLock<Topology>>,
    /// Woken on every accepted Build so the executor re-plans.
    pub build_submitted: Arc<Notify>,
    /// Woken when this admin's part of a fabric shutdown is done and it should leave.
    pub shutdown: Arc<Notify>,
    /// This admin's fabric shutdown seat, set once at start.
    pub fabric_shutdown: std::sync::OnceLock<Arc<ShutdownSeat>>,
}

/// What `/api/shutdown` and `/api/fabric` need of this admin's fabric shutdown state.
pub struct ShutdownSeat {
    pub control: Arc<crate::shutdown::ShutdownControl>,
    pub me: crate::model::PathName,
    pub node_id: crate::model::NodeId,
}

impl ControlPlane {
    pub fn new(builds: Arc<dyn BuildStateAdapter>, desired: Arc<crate::desired::DesiredStore>, topology: Topology) -> Self {
        Self {
            builds,
            desired,
            topology: Arc::new(RwLock::new(topology)),
            build_submitted: Arc::new(Notify::new()),
            shutdown: Arc::new(Notify::new()),
            fabric_shutdown: std::sync::OnceLock::new(),
        }
    }

    /// Validate `intent` against observed state, pin what it means there, and
    /// publish it as a Build. A topology-changing intent first proposes the
    /// next desired-topology revision over the current one; a restart or a
    /// replacement references the current one.
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
        // Compiled against the current desired revision, never invented here.
        let current = self.desired.current().ok_or_else(|| {
            Refusal::Unavailable("this admin holds no current desired topology yet (it is not hydrated); submit through a Ready node-admin".into())
        })?;
        let next = crate::desired::apply(&intent, &current.desired, &observed).map(|d| current.next(d, &build_id));
        if let Some(next) = &next {
            match self.desired.offer(next.clone()) {
                crate::desired::Offer::Taken => {}
                other => return Err(Refusal::Conflict(format!("the desired topology moved from {} while this request compiled ({other:?}); resubmit", current.mark()))),
            }
        }
        let fact = BuildIntentFact {
            build_id: build_id.clone(),
            intent,
            traceparent: rafka_mesh_telemetry::current_traceparent(),
            submitted_at_ms: now_ms(),
            desired: Some(next.as_ref().map_or_else(|| current.mark(), |n| n.mark())),
            reason: Some(crate::build_state::BuildReason::RequestedChange),
        };
        if let Some(next) = &next {
            tracing::info_span!(
                "rafka.node_admin.desired_topology.update.via-build",
                fabric_id = %next.fabric_id,
                desired_revision = next.revision,
                previous_revision = current.revision,
                source_build_id = %build_id,
                reason = "requested-change",
            )
            .in_scope(|| tracing::info!("desired topology proposed"));
            self.builds.publish_desired(next).await.map_err(Refusal::State)?;
        }
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
    /// The admin cannot decide the request yet (not hydrated).
    Unavailable(String),
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
            Refusal::Unavailable(d) => (StatusCode::SERVICE_UNAVAILABLE, "desired-topology-unavailable".into(), d.clone()),
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
    let mut v = serde_json::to_value(cp.topology.read().await.fabric_view()).unwrap_or(Value::Null);
    // The shape the fabric should have, as this admin holds it.
    if let Some(o) = v.as_object_mut() {
        o.insert("desired".into(), serde_json::to_value(cp.desired.current()).unwrap_or(Value::Null));
        // A fabric shutdown's progress as this admin sees it: diagnostics, never authority.
        let progress = cp.fabric_shutdown.get().and_then(|s| s.control.progress());
        o.insert("shutdown".into(), serde_json::to_value(progress).unwrap_or(Value::Null));
    }
    Json(v)
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

/// `POST /api/shutdown`: only the fabric-primary begins a fabric shutdown (fabric-mesh-lifecycle.md
/// §11.1); any other admin refuses by name, naming the current fabric-primary.
async fn shutdown(State(cp): State<Shared>) -> Response {
    let Some(seat) = cp.fabric_shutdown.get() else {
        return Refusal::Unavailable("this admin holds no fabric shutdown state".into()).into_response();
    };
    let primary = cp.topology.read().await.fabric_primary().map(|n| n.name.to_string());
    if primary.as_deref() != Some(seat.me.to_string().as_str()) {
        tracing::info_span!("rafka.node_admin.fabric.reject.via-shutdown-not-authority", node = %seat.me, fabric_primary = primary.as_deref().unwrap_or(""))
            .in_scope(|| tracing::info!("a fabric shutdown is begun only by the fabric-primary"));
        return (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "rejected-not-authority",
                "detail": format!("{} is not the fabric-primary; only the fabric-primary begins a fabric shutdown", seat.me),
                "fabric_primary": primary,
            })),
        )
            .into_response();
    }
    let begun = crate::fabric_storage::FabricShutdown {
        initiated_by: seat.me.to_string(),
        initiated_by_node_id: seat.node_id.as_str().to_string(),
        initiated_at_ms: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0),
    };
    match seat.control.initiate(begun) {
        Ok(held) => {
            tracing::info_span!("rafka.node_admin.fabric.update.via-shutdown", node = %seat.me, initiated_by = %held.initiated_by)
                .in_scope(|| tracing::info!("fabric shutdown begun"));
            (StatusCode::ACCEPTED, Json(json!({ "shutdown": "accepted", "initiated_by": held.initiated_by }))).into_response()
        }
        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, Json(json!({ "error": "fabric-storage-unavailable", "detail": e.to_string() }))).into_response(),
    }
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
