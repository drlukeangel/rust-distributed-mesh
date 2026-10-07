//! The generic control API (PRD §7; mesh-control-plane.md §2).
//!
//! Every topology route is a front door that submits one [`TopologyChange`]
//! to the fabric-primary, which compiles it once against the accepted topology
//! into the next complete Build, persists it, moves `Fabric.build_id` and
//! answers `202 {"build_id"}`. A restart or replacement changes no topology:
//! it opens the next attempt of the current Build with its action. A route
//! never spawns, kills or restarts anything itself: there is no lifecycle
//! handle in [`ControlPlane`] for it to call. `/api/shutdown` and runtime fault
//! routes are runtime administration, outside Build.

use crate::accepted::{compile, AcceptedStore, AttemptAction, TopologyChange};
use crate::build::{BuildId, BuildReject, FabricDesired, MeshDesired};
use crate::build_state::{AttemptOpened, AttemptReason, BuildAccepted, BuildState, BuildStateAdapter, BuildStateError};
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
    /// `Fabric.build_id` as this admin holds it (`crate::accepted`).
    pub accepted: Arc<AcceptedStore>,
    /// This admin's path.name: topology is accepted only while it is the fabric-primary.
    pub me: PathName,
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
    pub fn new(builds: Arc<dyn BuildStateAdapter>, accepted: Arc<AcceptedStore>, me: PathName, topology: Topology) -> Self {
        Self {
            builds,
            accepted,
            me,
            topology: Arc::new(RwLock::new(topology)),
            build_submitted: Arc::new(Notify::new()),
            shutdown: Arc::new(Notify::new()),
            fabric_shutdown: std::sync::OnceLock::new(),
        }
    }

    /// Only the current fabric-primary changes topology or opens attempts.
    async fn authority(&self, route: &'static str) -> Result<(), Refusal> {
        let primary = self.topology.read().await.fabric_primary().map(|n| n.name.clone());
        if primary.as_ref() != Some(&self.me) {
            tracing::info_span!("rdm.node_admin.build.reject.via-not-authority", route, node = %self.me, fabric_primary = %primary.as_ref().map(|p| p.to_string()).unwrap_or_default())
                .in_scope(|| tracing::info!("topology is changed only by the fabric-primary"));
            return Err(Refusal::NotAuthority(primary.map(|p| p.to_string())));
        }
        Ok(())
    }

    /// The accepted Build, and the refusal when another Build is still reconciling.
    async fn current_settled(&self) -> Result<crate::build_state::BuildProjection, Refusal> {
        let current = self.accepted.current(&*self.builds).await.ok_or_else(|| {
            Refusal::Unavailable("this admin holds no accepted Build yet (Fabric.build_id is not hydrated); submit through a Ready node-admin".into())
        })?;
        if matches!(current.state, BuildState::Pending | BuildState::Running) {
            return Err(Refusal::BuildInProgress(current.build_id.clone()));
        }
        Ok(current)
    }

    /// Compile `change` once against the accepted topology into the next complete Build, persist
    /// it, move `Fabric.build_id`, and broadcast both. One accepted topology, one Build in flight.
    pub async fn submit(&self, route: &'static str, change: TopologyChange) -> Result<BuildId, Refusal> {
        let span = tracing::info_span!(
            "rdm.node_admin.build.create.via-rest",
            route,
            build_id = tracing::field::Empty,
            change = %serde_json::to_string(&change).unwrap_or_default(),
            previous_build_id = tracing::field::Empty,
        );
        let _g = span.enter();
        self.authority(route).await?;
        let current = self.current_settled().await?;
        let observed = self.topology.read().await.clone();
        let topology = match compile(&current.topology, &change, &observed) {
            Ok(t) => t,
            Err(reject) => {
                drop(_g);
                reject_span(route, &reject);
                return Err(Refusal::Reject(reject));
            }
        };
        let build_id = BuildId::mint();
        span.record("build_id", build_id.0.as_str());
        span.record("previous_build_id", current.build_id.0.as_str());
        let accepted = BuildAccepted {
            build_id: build_id.clone(),
            topology,
            submitted_change: Some(change),
            traceparent: rafka_mesh_telemetry::current_traceparent(),
            submitted_at_ms: now_ms(),
        };
        // The Build is durable before the pointer names it (never the pointer first).
        self.builds.publish_accepted(&accepted).await.map_err(Refusal::State)?;
        let record = self.accepted.point(&build_id, "accepted").await.map_err(|e| Refusal::Unavailable(format!("fabric.storage: {e}")))?;
        self.builds.publish_fabric(&record).await.map_err(Refusal::State)?;
        tracing::info!(build_id = %build_id, "build accepted");
        self.build_submitted.notify_waiters();
        Ok(build_id)
    }

    /// Open the next attempt of the accepted Build with a fenced action (a restart or a
    /// replacement of one birth). The topology is unchanged; `Fabric.build_id` stays.
    pub async fn open_attempt(&self, route: &'static str, reason: AttemptReason, path: PathName, replace: bool) -> Result<BuildId, Refusal> {
        let span = tracing::info_span!("rdm.node_admin.build.update.via-rest", route, build_id = tracing::field::Empty, attempt = tracing::field::Empty, node = %path);
        let _g = span.enter();
        self.authority(route).await?;
        let current = self.current_settled().await?;
        if !current.topology.contains(&path) {
            let reject = BuildReject::UnknownNode { node: path.to_string() };
            drop(_g);
            reject_span(route, &reject);
            return Err(Refusal::Reject(reject));
        }
        let from_incarnation = {
            let t = self.topology.read().await;
            let n = t.node(&path).ok_or_else(|| Refusal::Reject(BuildReject::UnknownNode { node: path.to_string() }))?;
            if !n.status.is_live() {
                return Err(Refusal::Reject(BuildReject::NodeNotLive { node: path.to_string() }));
            }
            n.incarnation_id.clone().ok_or_else(|| Refusal::Reject(BuildReject::NodeNotLive { node: path.to_string() }))?
        };
        let action = if replace { AttemptAction::Replace { path, from_incarnation } } else { AttemptAction::Restart { path, from_incarnation } };
        let opened = AttemptOpened {
            build_id: current.build_id.clone(),
            attempt: current.attempt + 1,
            reason,
            action: Some(action),
            opened_by: self.me.to_string(),
            opened_at_ms: now_ms(),
        };
        span.record("build_id", current.build_id.0.as_str());
        span.record("attempt", opened.attempt);
        self.builds.open_attempt(&opened).await.map_err(Refusal::State)?;
        tracing::info!(build_id = %current.build_id, attempt = opened.attempt, "attempt opened");
        self.build_submitted.notify_waiters();
        Ok(current.build_id)
    }
}

fn reject_span(route: &'static str, reject: &BuildReject) {
    // One honest name per reason (PRD §16): rdm.node_admin.build.reject.via-<reason>.
    macro_rules! emit {
        ($($reason:literal),*) => {
            match reject.reason() {
                $( $reason => tracing::info_span!(concat!("rdm.node_admin.build.reject.via-", $reason), route, detail = %reject)
                    .in_scope(|| tracing::info!(%reject, "build refused")), )*
                _ => tracing::info_span!("rdm.node_admin.build.reject.via-invalid-intent", route, detail = %reject)
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
    /// Another Build is still reconciling: one accepted topology, one Build in flight.
    BuildInProgress(BuildId),
    /// This admin is not the fabric-primary (named when known).
    NotAuthority(Option<String>),
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
            Refusal::BuildInProgress(id) => {
                return (StatusCode::CONFLICT, Json(json!({ "error": "build-in-progress", "current_build_id": id, "detail": format!("Build {id} is still reconciling; one Build at a time") })))
                    .into_response()
            }
            Refusal::NotAuthority(primary) => {
                return (
                    StatusCode::CONFLICT,
                    Json(json!({ "error": "rejected-not-authority", "fabric_primary": primary, "detail": "topology is changed only by the current fabric-primary" })),
                )
                    .into_response()
            }
            Refusal::Unavailable(d) => (StatusCode::SERVICE_UNAVAILABLE, "accepted-build-unavailable".into(), d.clone()),
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
    Ok(accepted(cp.submit("POST /api/build", TopologyChange::ReconcileFabric { desired }).await?))
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
    if matches!(p.state, BuildState::Pending | BuildState::Running) {
        return Err(Refusal::Conflict(format!("Build {id} is still {:?}; only finished Builds leave history", p.state)));
    }
    if cp.accepted.build_id().await.as_ref() == Some(&id) {
        return Err(Refusal::Conflict(format!("Build {id} is the accepted topology (Fabric.build_id); it leaves history once the pointer moves")));
    }
    cp.builds.forget(&id).await.map_err(Refusal::State)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn spawn_node(State(cp): State<Shared>, raw: String) -> Result<Response, Refusal> {
    let b: SpawnBody = body(&raw)?;
    Ok(accepted(cp.submit("POST /api/nodes/spawn", TopologyChange::AddNode { mesh: b.mesh, node_kind: b.kind }).await?))
}

async fn delete_node(State(cp): State<Shared>, Path(name): Path<String>) -> Result<Response, Refusal> {
    let node = parse_path(&name)?;
    Ok(accepted(cp.submit("DELETE /api/nodes/{name}", TopologyChange::RemoveNode { node }).await?))
}

async fn restart_node(State(cp): State<Shared>, Path(name): Path<String>) -> Result<Response, Refusal> {
    let node = parse_path(&name)?;
    Ok(accepted(cp.open_attempt("POST /api/nodes/{name}/restart", AttemptReason::Restart, node, false).await?))
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
        o.insert("build_id".into(), serde_json::to_value(cp.accepted.build_id().await).unwrap_or(Value::Null));
        // A fabric shutdown's progress as this admin sees it: diagnostics, never authority.
        let progress = cp.fabric_shutdown.get().and_then(|s| s.control.progress());
        o.insert("shutdown".into(), serde_json::to_value(progress).unwrap_or(Value::Null));
    }
    Json(v)
}

async fn create_mesh(State(cp): State<Shared>, raw: String) -> Result<Response, Refusal> {
    let desired: MeshDesired = body(&raw)?;
    Ok(accepted(cp.submit("POST /api/meshes", TopologyChange::CreateMesh { desired }).await?))
}

async fn delete_mesh(State(cp): State<Shared>, Path(id): Path<String>) -> Result<Response, Refusal> {
    // By name in the accepted topology, or by id or name in the observed view.
    let in_accepted = cp.accepted.current(&*cp.builds).await.is_some_and(|b| b.topology.meshes.contains_key(&id));
    let mesh = if in_accepted {
        Some(id.clone())
    } else {
        let t = cp.topology.read().await;
        t.meshes.iter().find(|m| m.name == id || m.id.as_ref().is_some_and(|i| i.as_str() == id)).map(|m| m.name.clone())
    }
    .ok_or_else(|| Refusal::NotFound(format!("no mesh {id}")))?;
    Ok(accepted(cp.submit("DELETE /api/meshes/{id}", TopologyChange::RemoveMesh { mesh }).await?))
}

/// `POST /api/shutdown`: only the fabric-primary begins a fabric shutdown (fabric-mesh-lifecycle.md
/// §11.1); any other admin refuses by name, naming the current fabric-primary.
async fn shutdown(State(cp): State<Shared>) -> Response {
    let Some(seat) = cp.fabric_shutdown.get() else {
        return Refusal::Unavailable("this admin holds no fabric shutdown state".into()).into_response();
    };
    let primary = cp.topology.read().await.fabric_primary().map(|n| n.name.to_string());
    if primary.as_deref() != Some(seat.me.to_string().as_str()) {
        tracing::info_span!("rdm.node_admin.fabric.reject.via-shutdown-not-authority", node = %seat.me, fabric_primary = primary.as_deref().unwrap_or(""))
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
    match seat.control.initiate(begun).await {
        Ok(held) => {
            tracing::info_span!("rdm.node_admin.fabric.update.via-shutdown", node = %seat.me, initiated_by = %held.initiated_by)
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
