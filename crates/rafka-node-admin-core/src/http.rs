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

use tracing::Instrument as _;
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
    /// The Build state.
    pub builds: Arc<dyn BuildStateAdapter>,
    /// `Fabric.build_id` as this admin holds it (`crate::accepted`).
    pub accepted: Arc<AcceptedStore>,
    /// This admin's path.name: topology is accepted only while it is the fabric-primary.
    pub me: PathName,
    /// The observed topology.
    pub topology: Arc<RwLock<Topology>>,
    /// Why the view lacks a name right now: what the digest book, the removed set and the
    /// records hold for it (set once by the admin that owns them). An unknown-node refusal from
    /// the live view carries it, so the refusal names the exclusion instead of hiding it.
    pub absence: std::sync::OnceLock<Arc<dyn Fn(&PathName) -> String + Send + Sync>>,
    /// This admin's view projected now, from the same inputs the periodic refresh projects
    /// from: what a decision on one node reads, so it never acts on a snapshot up to one refresh
    /// old (set once by the admin that owns the inputs).
    pub view_now: std::sync::OnceLock<Arc<dyn Fn() -> Topology + Send + Sync>>,
    /// What the fabric-primary's investigation holds of a peer mesh it has heard on the backbone
    /// (`crate::investigate::Ladder::peer_mesh`): a node of a mesh held unheard is not restarted,
    /// replaced or deleted from here (R-D1). Set once by the admin that owns the ladder.
    pub peer_mesh: std::sync::OnceLock<Arc<dyn Fn(&str) -> Option<crate::investigate::PeerMesh> + Send + Sync>>,
    /// The connection facts this admin holds (set once by the admin that owns its connections
    /// writer): the active Directs of the fleet, and this admin's own latest Directs and Proxies.
    pub connections: std::sync::OnceLock<Arc<std::sync::Mutex<rafka_mesh_entity::connections::ConnectionsHeld>>>,
    /// The CPU and RAM each member's latest digest carries, by node id (set once by the admin that
    /// owns the digest book). Load is served beside the view, never inside it: it changes every
    /// digest and never moves the topology.
    pub loads: std::sync::OnceLock<Arc<dyn Fn() -> std::collections::BTreeMap<String, (Option<rafka_mesh_entity::NodeLoad>, Option<rafka_mesh_entity::GossipStats>)> + Send + Sync>>,
    /// Woken on every accepted Build so the executor re-plans.
    pub build_submitted: Arc<Notify>,
    /// Woken when this admin's part of a fabric shutdown is done and it should leave.
    pub shutdown: Arc<Notify>,
    /// This admin's fabric shutdown seat, set once at start.
    pub fabric_shutdown: std::sync::OnceLock<Arc<ShutdownSeat>>,
    /// Where each attempt this admin brings into being keeps its context, for the claim to return.
    pub contexts: Arc<crate::build_claim::AttemptContexts>,
}

/// What `/api/shutdown` and `/api/fabric` need of this admin's fabric shutdown state.
pub struct ShutdownSeat {
    /// The shutdown control.
    pub control: Arc<crate::shutdown::ShutdownControl>,
    /// This admin's `path.name`.
    pub me: crate::model::PathName,
    /// This admin's node id.
    pub node_id: crate::model::NodeId,
}

impl ControlPlane {
    /// A control plane for the admin `me` over its Build state, its accepted-Build store and an
    /// initial `topology`.
    pub fn new(builds: Arc<dyn BuildStateAdapter>, accepted: Arc<AcceptedStore>, me: PathName, topology: Topology) -> Self {
        Self {
            builds,
            accepted,
            me,
            topology: Arc::new(RwLock::new(topology)),
            build_submitted: Arc::new(Notify::new()),
            shutdown: Arc::new(Notify::new()),
            fabric_shutdown: std::sync::OnceLock::new(),
            absence: std::sync::OnceLock::new(),
            view_now: std::sync::OnceLock::new(),
            connections: std::sync::OnceLock::new(),
            loads: std::sync::OnceLock::new(),
            peer_mesh: std::sync::OnceLock::new(),
            contexts: Arc::new(crate::build_claim::AttemptContexts::in_memory()),
        }
    }

    /// This control plane keeping attempt contexts in `contexts` (the admin's own data dir).
    pub fn with_contexts(mut self, contexts: Arc<crate::build_claim::AttemptContexts>) -> Self {
        self.contexts = contexts;
        self
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
    pub async fn submit(&self, route: &'static str, change: TopologyChange) -> Result<Opened, Refusal> {
        let span = tracing::info_span!(
            "rdm.node_admin.build.create.via-rest",
            route,
            build_id = tracing::field::Empty,
            change = %serde_json::to_string(&change).unwrap_or_default(),
            previous_build_id = tracing::field::Empty,
        );
        // The span is entered by the future on each poll, never by a guard held across an await
        // (a guard stays entered on the worker while the task is parked). A refusal is reported
        // beside the request span, in the caller's span.
        let outer = tracing::Span::current();
        async {
            self.authority(route).await?;
            let current = self.current_settled().await?;
            let observed = self.topology.read().await.clone();
            let topology = match compile(&current.topology, &change, &observed) {
                Ok(t) => t,
                Err(reject) => {
                    outer.in_scope(|| reject_span(route, &reject));
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
                // Strictly after the Build it succeeds: pointer rows are ordered by this stamp.
                submitted_at_ms: now_ms().max(current.submitted_at_ms + 1),
            };
            // The attempt's context is on this fabric-primary before the Build exists to be claimed.
            self.contexts
                .put(&build_id, 1, &crate::build_claim::current_context())
                .await
                .map_err(|e| Refusal::Unavailable(format!("attempt-context: {e}")))?;
            // The Build is durable before the pointer names it (never the pointer first).
            self.builds.publish_accepted(&accepted).await.map_err(Refusal::State)?;
            let record = self.accepted.point(&build_id, accepted.submitted_at_ms, "accepted").await.map_err(|e| Refusal::Unavailable(format!("fabric.storage: {e}")))?;
            self.builds.publish_fabric(&record).await.map_err(Refusal::State)?;
            tracing::info!(build_id = %build_id, "build accepted");
            self.build_submitted.notify_waiters();
            Ok::<Opened, Refusal>(Opened { build_id, attempt: 1 })
        }
        .instrument(span.clone())
        .await
    }

    /// Open the next attempt of `current` with `action`, putting its context first.
    #[allow(clippy::too_many_arguments)]
    async fn open_with(&self, current: &crate::build_state::BuildProjection, reason: AttemptReason, action: AttemptAction, route: &'static str, outer: &tracing::Span, node_name: &str, span: tracing::Span) -> Result<Opened, Refusal> {
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
        // The context is on this fabric-primary before the attempt is open to be claimed.
        self.contexts
            .put(&opened.build_id, opened.attempt, &crate::build_claim::current_context())
            .await
            .map_err(|e| Refusal::Unavailable(format!("attempt-context: {e}")))?;
        if let Err(e) = self.builds.open_attempt(&opened).await {
            return Err(match e {
                BuildStateError::AttemptTaken { .. } => {
                    outer.in_scope(|| {
                        tracing::info_span!("rdm.node_admin.build.reject.via-attempt-taken", route, build_id = %current.build_id, attempt = opened.attempt, node = %node_name, detail = %e)
                            .in_scope(|| tracing::info!(detail = %e, "attempt refused: another action holds the number"))
                    });
                    Refusal::AttemptTaken(e.to_string())
                }
                e => Refusal::State(e),
            });
        }
        tracing::info!(build_id = %current.build_id, attempt = opened.attempt, "attempt opened");
        self.build_submitted.notify_waiters();
        Ok(Opened { build_id: current.build_id.clone(), attempt: opened.attempt })
    }

    /// Open the next attempt of the accepted Build with a fenced action (a restart or a
    /// replacement of one birth). The topology is unchanged; `Fabric.build_id` stays.
    pub async fn open_attempt(&self, route: &'static str, reason: AttemptReason, path: PathName, replace: bool, named_birth: Option<crate::model::IncarnationId>) -> Result<Opened, Refusal> {
        let span = tracing::info_span!("rdm.node_admin.build.update.via-rest", route, build_id = tracing::field::Empty, attempt = tracing::field::Empty, node = %path);
        let outer = tracing::Span::current();
        async {
            self.authority(route).await?;
            let current = self.current_settled().await?;
            if !current.topology.contains(&path) {
                let reject = BuildReject::UnknownNode { node: path.to_string() };
                outer.in_scope(|| reject_span(route, &reject));
                return Err(Refusal::Reject(reject));
            }
            let from_incarnation = {
                let t = match self.view_now.get() {
                    Some(project) => project(),
                    None => self.topology.read().await.clone(),
                };
                let Some(n) = t.node(&path) else {
                    // A node being replaced is the node nobody hears: the Build names it, the heard
                    // view need not. The caller names the exact birth to retire.
                    if let (true, Some(birth)) = (replace, named_birth) {
                        drop(t);
                        outer.in_scope(|| {
                            tracing::info_span!("rdm.node_admin.build.update.via-named-birth", route, node = %path, incarnation_id = %birth.0)
                                .in_scope(|| tracing::info!("the node is in the accepted Build and silent in the view: the caller's named birth is fenced"))
                        });
                        let named_path = path.to_string();
                        let action = AttemptAction::Replace { path, from_incarnation: birth };
                        return self.open_with(&current, reason, action, route, &outer, &named_path, span.clone()).await;
                    }
                    let why = self.absence.get().map(|f| f(&path)).unwrap_or_else(|| "no absence reporter".to_string());
                    let reject = BuildReject::UnknownNode { node: path.to_string() };
                    drop(t);
                    outer.in_scope(|| {
                        tracing::info_span!("rdm.node_admin.build.reject.via-unknown-node", route, detail = %reject, view = %why)
                            .in_scope(|| tracing::info!(%reject, %why, "build refused: the live view lacks the node"))
                    });
                    return Err(Refusal::Reject(reject));
                };
                // A requested replacement may name a birth that does not answer (PendingReconnect,
                // Dead): the decommission retires its exact runtime before the new node is created.
                let unheard = matches!(n.status, crate::model::NodeStatus::PendingReconnect | crate::model::NodeStatus::Dead);
                if !n.status.is_live() && !(replace && unheard) {
                    let reason = if replace { "the node's status is neither live nor an unheard birth a replacement may retire" } else { "a restart needs a live node" };
                    return Err(not_live(&outer, route, &path, n.status, reason));
                }
                // A node of a peer mesh this fabric-primary holds unheard is not its to restart, replace or delete:
                // the mesh's own authority, once heard again, reconciles it (R-D1).
                if path.mesh != self.me.mesh {
                    if let Some(unheard_ms) = self.peer_mesh.get().and_then(|f| f(&path.mesh)).and_then(|p| p.unheard_ms) {
                        let reject = BuildReject::UnheardMesh { node: path.to_string(), mesh: path.mesh.clone(), unheard_ms };
                        outer.in_scope(|| reject_span(route, &reject));
                        return Err(Refusal::Reject(reject));
                    }
                }
                match n.incarnation_id.clone() {
                    Some(incarnation) => incarnation,
                    None => return Err(not_live(&outer, route, &path, n.status, "the node holds no incarnation id to fence the attempt to")),
                }
            };
            let node_name = path.to_string();
            let action = if replace { AttemptAction::Replace { path, from_incarnation } } else { AttemptAction::Restart { path, from_incarnation } };
            self.open_with(&current, reason, action, route, &outer, &node_name, span.clone()).await
        }
        .instrument(span.clone())
        .await
    }
}

/// The restart/replace refusal `node-not-live`, spanned beside the request with the node, the
/// status the live view holds for it and why that status refuses the operation
/// (`rdm.node_admin.node.reject.via-not-live`).
fn not_live(outer: &tracing::Span, route: &'static str, path: &PathName, status: crate::model::NodeStatus, reason: &'static str) -> Refusal {
    let held = serde_json::to_value(status).ok().and_then(|v| v.as_str().map(str::to_string)).unwrap_or_else(|| format!("{status:?}"));
    let reject = BuildReject::NodeNotLive { node: path.to_string() };
    outer.in_scope(|| {
        tracing::info_span!("rdm.node_admin.node.reject.via-not-live", route, node = %path, status = %held, reason, detail = %reject)
            .in_scope(|| tracing::info!(%reject, status = %held, reason, "build refused: the node is not live"))
    });
    Refusal::Reject(reject)
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
        "provider-mismatch",
        "unheard-mesh"
    );
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// A refused request, rendered with every non-leaking detail.
#[derive(Debug)]
pub enum Refusal {
    /// The request names a Build the topology rules refuse.
    Reject(BuildReject),
    /// The request is not valid.
    BadRequest(String),
    /// The thing requested does not exist.
    NotFound(String),
    /// The request conflicts with the current state.
    Conflict(String),
    /// Another Build is still reconciling: one accepted topology, one Build in flight.
    BuildInProgress(BuildId),
    /// This admin is not the fabric-primary (named when known).
    NotAuthority(Option<String>),
    /// The attempt number this request computed was opened first by another action.
    AttemptTaken(String),
    /// The admin cannot decide the request yet (not hydrated).
    Unavailable(String),
    /// The Build state could not be read or written.
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
            Refusal::AttemptTaken(d) => (StatusCode::CONFLICT, "attempt-taken".into(), d.clone()),
            Refusal::Unavailable(d) => (StatusCode::SERVICE_UNAVAILABLE, "accepted-build-unavailable".into(), d.clone()),
            Refusal::State(e) => (StatusCode::SERVICE_UNAVAILABLE, "build-state-unavailable".into(), e.to_string()),
        };
        (status, Json(json!({ "error": error, "detail": detail }))).into_response()
    }
}

/// The Build and the attempt a request answered 202 for: the caller waits for exactly this
/// attempt. A Build that is accepted starts at attempt 1; an opened attempt is the one it opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Opened {
    /// The Build.
    pub build_id: BuildId,
    /// The attempt.
    pub attempt: u32,
}

fn accepted(o: Opened) -> Response {
    (StatusCode::ACCEPTED, Json(json!({ "build_id": o.build_id, "attempt": o.attempt }))).into_response()
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
    Ok(accepted(cp.open_attempt("POST /api/nodes/{name}/restart", AttemptReason::Restart, node, false, None).await?))
}

#[derive(Deserialize)]
struct ReplaceQuery {
    /// The exact birth to retire, for a node the Build names that the view does not hold.
    incarnation: Option<String>,
}

async fn replace_node(State(cp): State<Shared>, Path(name): Path<String>, Query(q): Query<ReplaceQuery>) -> Result<Response, Refusal> {
    let node = parse_path(&name)?;
    let birth = q.incarnation.map(crate::model::IncarnationId);
    Ok(accepted(cp.open_attempt("POST /api/nodes/{name}/replace", AttemptReason::Replace, node, true, birth).await?))
}

async fn get_nodes(State(cp): State<Shared>) -> Json<Value> {
    let t = cp.topology.read().await;
    let mut nodes = serde_json::to_value(t.births().collect::<Vec<_>>()).unwrap_or(Value::Null);
    // Each node's CPU and RAM and mesh-channel counts from its latest digest as this admin holds it; absent when its
    // digest carried none (a member of a peer mesh is held without its load).
    if let (Some(loads), Some(list)) = (cp.loads.get(), nodes.as_array_mut()) {
        let loads = loads();
        for n in list.iter_mut() {
            if let Some((l, g)) = n["node_id"].as_str().and_then(|id| loads.get(id)) {
                if let Some(l) = l {
                    n["load"] = serde_json::to_value(l).unwrap_or(Value::Null);
                }
                if let Some(g) = g {
                    n["gossip"] = serde_json::to_value(g).unwrap_or(Value::Null);
                }
            }
        }
    }
    Json(json!({ "nodes": nodes }))
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

/// `GET /api/connections`: the connection facts this admin holds, read-only. Each names its
/// source, destination, kind (`direct` or `proxy`, a proxy with its carrier), state and when it
/// was observed. An admin that holds none lists none.
async fn get_connections(State(cp): State<Shared>) -> Json<Value> {
    use rafka_mesh_entity::connections::{ConnectionKind, ConnectionState, NodeConnection};
    let mut facts: Vec<NodeConnection> = Vec::new();
    let mut complete = false;
    if let Some(held) = cp.connections.get() {
        let held = held.lock().unwrap();
        complete = held.is_complete();
        facts.extend(held.active_directs().into_iter().cloned());
        for own in held.own_latest_directs().into_iter().chain(held.own_active_proxies()) {
            let key = own.index();
            match facts.iter().position(|f| f.index() == key) {
                Some(i) if facts[i].stamp() >= own.stamp() => {}
                Some(i) => facts[i] = own.clone(),
                None => facts.push(own.clone()),
            }
        }
    }
    let connections: Vec<Value> = facts
        .iter()
        .map(|f| {
            json!({
                "source": f.source.name.to_string(),
                "destination": f.destination.name.to_string(),
                "kind": match f.kind { ConnectionKind::Direct => "direct", ConnectionKind::Proxy => "proxy" },
                "state": match f.state { ConnectionState::Connected => "connected", ConnectionState::Disconnected => "disconnected", ConnectionState::Failed => "failed" },
                "carrier": f.carrier.as_ref().map(|c| c.name.to_string()),
                "reason": f.reason,
                "logged_at_ms": f.logged_at_ms,
            })
        })
        .collect();
    Json(json!({"node": cp.me.to_string(), "complete": complete, "connections": connections}))
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
        .route("/api/nodes/{name}/replace", post(replace_node))
        .route("/api/meshes", post(create_mesh))
        .route("/api/meshes/{id}", get(get_mesh).delete(delete_mesh))
        .route("/api/fabric", get(get_fabric))
        .route("/api/connections", get(get_connections))
        .route("/api/shutdown", post(shutdown))
        .with_state(cp)
        .merge(runtime_routes)
}
