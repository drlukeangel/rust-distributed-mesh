//! The generic control API (PRD §7; mesh-control-plane.md §2).
//!
//! Every topology route is a front door that submits one [`TopologyChange`]
//! to the fabric-primary, which compiles it once against the accepted topology
//! into the next complete Build, persists it, which makes it the current Build, and
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
    /// The Fabric identity as this admin holds it (`crate::accepted`).
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
    /// from (the node rows of the births that do not publish read with it): what a decision on one
    /// node reads, so it never acts on a snapshot up to one refresh old (set once by the admin that
    /// owns the inputs).
    pub view_now: std::sync::OnceLock<Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Topology> + Send>> + Send + Sync>>,
    /// What the fabric-primary's investigation holds of a peer mesh it has heard on the backbone
    /// (`crate::investigate::Ladder::peer_mesh`): a node of a mesh held unheard is not restarted,
    /// replaced or deleted from here (R-D1). Set once by the admin that owns the ladder.
    pub peer_mesh: std::sync::OnceLock<Arc<dyn Fn(&str) -> Option<crate::investigate::PeerMesh> + Send + Sync>>,
    /// The record the fabric-primary holds of the parked predecessor a replace left at a `<path>.old`
    /// (`crate::node_rows::NodeRows::parked_at`): what admits its `node.delete`, fenced to that exact
    /// birth. No view names the path but the replace's executor's (set once by the admin that owns
    /// the row writer).
    pub parked_old: std::sync::OnceLock<Arc<dyn Fn(&PathName) -> Option<crate::model::Node> + Send + Sync>>,
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
    /// The rafka-time this admin stamps its Builds, attempts and shutdown with.
    pub time: rafka_mesh_transport::clock::RafkaTime,
    /// Where this fabric-primary admits a Build or an attempt: held from the check that no Build is
    /// reconciling to the durable write that makes the next one, by every path that starts work
    /// (a submitted change, an opened attempt, the drift check).
    pub admission: Arc<tokio::sync::Mutex<()>>,
    /// Whether the initial fabric has first reached ready-for-traffic: until it has, a workflow that
    /// moves a seat or writes a caller's row is refused `fabric-not-ready` (states.md, "The initial
    /// fabric before ready-for-traffic").
    pub first_ready: crate::fabric_state::FirstReady,
    /// The status rows of this admin's mesh on the fabric's store root: the ready-for-traffic row of the
    /// fabric among them is the durable fact that it first reached ready-for-traffic (states.md, "The
    /// initial fabric before ready-for-traffic"). Held in memory, empty, until the admin hands its own
    /// ([`ControlPlane::with_first_ready`]).
    pub ready_rows: Arc<dyn crate::status_storage::StatusStorage>,
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
    /// initial `topology`, stamping with `time`: the rafka-time its process adopted. A control
    /// plane is never built without one, because every Build, attempt and shutdown it opens is
    /// stamped from it.
    pub fn new(builds: Arc<dyn BuildStateAdapter>, accepted: Arc<AcceptedStore>, me: PathName, topology: Topology, time: rafka_mesh_transport::clock::RafkaTime) -> Self {
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
            parked_old: std::sync::OnceLock::new(),
            contexts: Arc::new(crate::build_claim::AttemptContexts::in_memory()),
            time,
            admission: Arc::new(tokio::sync::Mutex::new(())),
            first_ready: crate::fabric_state::FirstReady::new(),
            ready_rows: Arc::new(crate::status_storage::MemoryStatusStorage::default()),
        }
    }

    /// This control plane reading whether the initial fabric first reached ready-for-traffic from
    /// `first_ready`, the handle its admin's fabric state sets, and `ready_rows`, the status rows of
    /// its admin's mesh on the fabric's store root, where the durable fact is.
    pub fn with_first_ready(mut self, first_ready: crate::fabric_state::FirstReady, ready_rows: Arc<dyn crate::status_storage::StatusStorage>) -> Self {
        self.first_ready = first_ready;
        self.ready_rows = ready_rows;
        self
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

    /// The one admission rule before the initial fabric is ready-for-traffic (states.md, "The initial
    /// fabric before ready-for-traffic"; R-PB1, R-PC1): `workflow` moves a seat or writes a caller's
    /// row, so it is refused `fabric-not-ready` until the fabric has first reached ready-for-traffic,
    /// naming the workflow and the fabric's status. It runs before anything is read, claimed or
    /// written: a refused workflow opens no attempt and accepts no Build. Decided on the durable fact
    /// (states.md): what this admin already knows, else a point read of the fabric's ready-for-traffic
    /// row among its mesh's status rows on the store root, which the fabric state it hears by gossip
    /// can lag (an outside successor of the fabric seat). A read that fails is refused by name.
    async fn admit_once_ready(&self, route: &'static str, workflow: String, outer: &tracing::Span) -> Result<(), Refusal> {
        let (status, fabric_id, fabric_primary_mesh) = {
            let t = self.topology.read().await;
            (t.fabric.status, t.fabric.id.clone(), t.fabric_primary().map(|n| n.mesh.clone()))
        };
        match self.first_ready.reached_on_root(&*self.ready_rows, &fabric_id, status, fabric_primary_mesh.as_deref()).await {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(e) => {
                let detail = format!("{}: {workflow} cannot be admitted: whether fabric {fabric_id} first reached ready-for-traffic could not be read from the status rows of mesh {} on the store root (it is {}): {e}", self.me, self.me.mesh, status.name());
                outer.in_scope(|| {
                    tracing::info_span!("rdm.node_admin.build.reject.via-fabric-ready-unreadable", route, workflow = %workflow, status = status.name(), detail = %detail)
                        .in_scope(|| tracing::info!("build refused: the ready-for-traffic row could not be read"))
                });
                return Err(Refusal::Unavailable(detail));
            }
        }
        let reject = BuildReject::FabricNotReady { workflow: workflow.clone(), status: status.name().to_string() };
        outer.in_scope(|| {
            tracing::info_span!("rdm.node_admin.build.reject.via-fabric-not-ready", route, workflow = %workflow, status = status.name(), detail = %reject)
                .in_scope(|| tracing::info!(%reject, "build refused: the initial fabric has not first reached ready-for-traffic"))
        });
        Err(Refusal::Reject(reject))
    }

    /// The current Build (R-BI3: the Build entities read last in, first out, a failed one skipped),
    /// and the refusal when it is still reconciling. The admission is taken first and returned with
    /// the Build: the caller holds it through its write.
    async fn current_settled(&self) -> Result<(tokio::sync::OwnedMutexGuard<()>, crate::build_state::BuildProjection), Refusal> {
        let admission = self.admission.clone().lock_owned().await;
        // "Never written" and "could not be read" are distinct refusals: the store's own error is named.
        let current = self.builds.current().await.map_err(Refusal::State)?.ok_or_else(|| {
            Refusal::Unavailable("this admin holds no current Build yet (no Build in its builds store is in progress or completed); submit through a Ready node-admin".into())
        })?;
        if matches!(current.state, BuildState::Pending | BuildState::Running) {
            tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", build_id = %current.build_id, state = ?current.state)
                .in_scope(|| tracing::info!("another Build is reconciling: one Build in flight"));
            return Err(Refusal::BuildInProgress(current.build_id.clone()));
        }
        Ok((admission, current))
    }

    /// Compile `change` once against the current Build's topology into the next complete Build and
    /// accept it: written once into the builds store, where it is the current Build from then on.
    /// One accepted topology, one Build in flight.
    pub async fn submit(&self, route: &'static str, change: TopologyChange) -> Result<Opened, Refusal> {
        let span = tracing::info_span!(
            "rdm.node_admin.build.create.via-rest",
            route,
            build_id = tracing::field::Empty,
            change = %serde_json::to_string(&change).unwrap_or_default(),
            previous_build_id = tracing::field::Empty,
            minted_meshes = tracing::field::Empty,
        );
        self.submit_in(span, route, change).await
    }

    /// [`ControlPlane::submit`] under `span`, which names the surface the change came through.
    pub async fn submit_in(&self, span: tracing::Span, route: &'static str, change: TopologyChange) -> Result<Opened, Refusal> {
        self.submit_checked(span, route, change, None).await
    }

    /// [`ControlPlane::submit_in`], the stopped-node collector's candidate (when one) rechecked under
    /// the admission before anything is compiled or written (fabric-node-lifecycle.md §6.9 Recheck).
    pub(crate) async fn submit_checked(&self, span: tracing::Span, route: &'static str, change: TopologyChange, collect: Option<&crate::stopped_gc::Candidate>) -> Result<Opened, Refusal> {
        // The span is entered by the future on each poll, never by a guard held across an await
        // (a guard stays entered on the worker while the task is parked). A refusal is reported
        // beside the request span, in the caller's span.
        let outer = tracing::Span::current();
        async {
            self.authority(route).await?;
            let (_admitted, current) = self.current_settled().await?;
            if let Some(candidate) = collect {
                if let Err(reject) = self.recheck_stopped(candidate).await {
                    outer.in_scope(|| reject_span(route, &reject));
                    return Err(Refusal::Reject(reject));
                }
            }
            let observed = self.topology.read().await.clone();
            let topology = match compile(&current.topology, &change, &observed) {
                Ok(t) => t,
                Err(reject) => {
                    outer.in_scope(|| reject_span(route, &reject));
                    return Err(Refusal::Reject(reject));
                }
            };
            let operations = crate::accepted::plan(&topology, &observed, None).operations;
            // A Build that deletes a node or a mesh writes caller state: it waits for the initial fabric
            // to first reach ready-for-traffic (R-PC1).
            if let Some(workflow) = operations.iter().find_map(|op| match op {
                crate::build::BuildOperation::RetireNode { node, exited: None } => Some(format!("node.delete {node}")),
                crate::build::BuildOperation::ShutdownMesh { mesh, .. } => Some(format!("mesh.delete {mesh}")),
                _ => None,
            }) {
                self.admit_once_ready(route, workflow, &outer).await?;
            }
            // A Build whose plan would end a seat holder while no other node-admin of its mesh can take
            // its seat is refused before it exists: node.primary.update has no other node of its type (R-MT1).
            if let Some(reject) = operations.iter().find_map(|op| {
                let (node, action) = match op {
                    crate::build::BuildOperation::RetireNode { node, .. } => (node, "retire"),
                    crate::build::BuildOperation::RestartNode { node } => (node, "restart"),
                    crate::build::BuildOperation::ReplaceNode { node, .. } => (node, "replace"),
                    crate::build::BuildOperation::DeleteConnections { node, .. } => (node, "node.connections.delete"),
                    crate::build::BuildOperation::StopNode { node, .. } => (node, "stop"),
                    _ => return None,
                };
                last_admin(node, action, &observed)
            }) {
                outer.in_scope(|| reject_span(route, &reject));
                return Err(Refusal::Reject(reject));
            }
            let build_id = BuildId::mint();
            span.record("build_id", build_id.as_str());
            span.record("previous_build_id", current.build_id.as_str());
            // The meshes this Build brings into the accepted topology, each under the id `compile`
            // minted for it now, once (node-identity-and-state.md §2.7, "Who mints it"): every run of
            // the mesh's create-mesh, by any executor and any attempt, claims with that id.
            let minted: Vec<String> = topology.meshes.values().filter(|m| !current.topology.meshes.values().any(|c| c.mesh_id == m.mesh_id)).map(|m| format!("{}={}", m.name, m.mesh_id)).collect();
            if !minted.is_empty() {
                span.record("minted_meshes", minted.join(",").as_str());
            }
            // Strictly after every Build accepted before it, a failed one above the current Build
            // included: the acceptance-order index is ordered by this stamp (R-BI3).
            let newest = self.builds.newest_accepted_at_ms().await.map_err(Refusal::State)?.unwrap_or(current.submitted_at_ms);
            let accepted = BuildAccepted {
                build_id: build_id.clone(),
                topology,
                submitted_change: Some(change),
                submitted_at_ms: self.time.now_ms().max(newest.max(current.submitted_at_ms) + 1),
            };
            // The attempt's context is on this fabric-primary before the Build exists to be claimed.
            self.contexts
                .put(&build_id, 1, &crate::build_claim::current_context())
                .await
                .map_err(|e| Refusal::Unavailable(format!("attempt-context: {e}")))?;
            // The Build topic carries the Fabric record, its identity, before the Build is accepted: a
            // refusal here (the seat moved) accepts nothing.
            if let Some(identity) = self.accepted.record().await.map_err(|e| Refusal::Unavailable(format!("fabric.storage: {e}")))? {
                self.builds.publish_fabric(&identity).await.map_err(Refusal::State)?;
            }
            // The Build is current the moment it is durable in the builds store: nothing else is written.
            // It is the door's last write: the reconcile loop drives a current Build at once, and its first
            // step can move the fabric seat (a retire of this admin's own mesh), which refuses every gated
            // write after it, so the door answers the Build it accepted whatever the drive does next.
            crate::build_state::accept(&*self.builds, &accepted, Some(&current.build_id), &self.me.to_string(), "accepted").await.map_err(Refusal::State)?;
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
            opened_at_ms: self.time.now_ms(),
        };
        span.record("build_id", current.build_id.as_str());
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

    /// Open the next attempt of the current Build with a fenced action (a restart or a replacement
    /// of one birth). The topology is unchanged; the Build stays the current one.
    pub async fn open_attempt(&self, route: &'static str, reason: AttemptReason, path: PathName, kind: ActionKind, named_birth: Option<crate::model::IncarnationId>) -> Result<Opened, Refusal> {
        let span = tracing::info_span!("rdm.node_admin.build.update.via-rest", route, build_id = tracing::field::Empty, attempt = tracing::field::Empty, node = %path);
        self.open_attempt_in(span, route, reason, path, kind, named_birth).await
    }

    /// [`ControlPlane::open_attempt`] under `span`, which names the surface the request came through.
    pub async fn open_attempt_in(&self, span: tracing::Span, route: &'static str, reason: AttemptReason, path: PathName, kind: ActionKind, named_birth: Option<crate::model::IncarnationId>) -> Result<Opened, Refusal> {
        let replace = kind == ActionKind::Replace;
        let outer = tracing::Span::current();
        async {
            self.authority(route).await?;
            // A caller's node workflow writes a row, and a node.connections.delete of a seat holder moves its seats: both
            // wait for the initial fabric to first reach ready-for-traffic (R-PB1, R-PC1).
            let holds_a_seat = || async {
                let t = self.topology.read().await;
                t.node(&path).is_some_and(|n| t.holds_fabric_seat(&n.node_id) || t.holds_mesh_seat(&n.mesh, &n.node_id))
            };
            let workflow = match kind {
                ActionKind::DeleteConnections if holds_a_seat().await => Some(format!("node.connections.delete {path} (a seat holder)")),
                ActionKind::DeleteConnections => None,
                _ => Some(format!("node.{} {path}", kind.name())),
            };
            if let Some(workflow) = workflow {
                self.admit_once_ready(route, workflow, &outer).await?;
            }
            let (_admitted, current) = self.current_settled().await?;
            // A delete acts only on a `.old` path: the parked predecessor a replacement left there, found
            // in the record the fabric-primary holds of it (below), and only a delete acts on it from that
            // record. Any other action's `.old` or `.new` name is its accepted path's (node-replace.md: a
            // predecessor renamed `<name>.old`, a successor born `<name>.new`): a node renamed while
            // stopped is started by its row's name (R-JN1, node-start.md step 2), and its birth is fenced
            // below in the view, which lists it at its row's name.
            let parked_old = kind == ActionKind::Delete && path.old;
            // A birth of a mesh deleted at mesh.deleted (its last admin, alive): no accepted Build names
            // the mesh, and only a delete acts on it (mesh-delete.md step 7).
            let deleted_mesh = kind == ActionKind::Delete && !path.old && !current.topology.meshes.contains_key(&path.mesh) && self.of_deleted_mesh(&path).await;
            if (kind == ActionKind::Delete && !(path.old || deleted_mesh)) || (!parked_old && !deleted_mesh && !current.topology.contains(&path.base())) {
                let reject = BuildReject::UnknownNode { node: path.to_string() };
                outer.in_scope(|| reject_span(route, &reject));
                return Err(Refusal::Reject(reject));
            }
            let from_incarnation = {
                // The parked predecessor at a `.old` path is admitted from the record the
                // fabric-primary holds of it (ops-naming "Node data": the fabric-primary holds the whole
                // record of every node), never from a view: only the replace's executor's view names it.
                let t = match self.view_now.get() {
                    Some(project) => project().await,
                    None => self.topology.read().await.clone(),
                };
                let held = if parked_old { self.parked_old.get().and_then(|held| held(&path)) } else { t.node(&path).cloned() };
                let Some(n) = held else {
                    // A node being replaced is the node nobody hears: the Build names it, the heard
                    // view need not. The caller names the exact birth to retire.
                    if let (true, Some(birth)) = (replace, named_birth) {
                        drop(t);
                        outer.in_scope(|| {
                            tracing::info_span!("rdm.node_admin.build.update.via-named-birth", route, node = %path, incarnation_id = %birth.as_str())
                                .in_scope(|| tracing::info!("the node is in the accepted Build and silent in the view: the caller's named birth is fenced"))
                        });
                        let named_path = path.to_string();
                        let action = AttemptAction::Replace { path, from_incarnation: birth };
                        return self.open_with(&current, reason, action, route, &outer, &named_path, span.clone()).await;
                    }
                    let why = if parked_old {
                        format!("{}: this fabric-primary holds no record of a parked predecessor at {path} (no replace it drove moved a row there, or its Deleted row is written)", self.me)
                    } else {
                        self.absence.get().map(|f| f(&path)).unwrap_or_else(|| "no absence reporter".to_string())
                    };
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
                // A start is the transition out of Leaving, and only for a parked process; every other action needs a live node.
                let startable = kind == ActionKind::Start && n.parked;
                // A delete acts on the parked predecessor, or on one whose exit is already proven, or on
                // the last admin of a deleted mesh, live or stopped: its node.delete stops it first.
                let deletable = kind == ActionKind::Delete && (n.parked || n.status == crate::model::NodeStatus::Dead || deleted_mesh);
                let needs_live = !matches!(kind, ActionKind::Start | ActionKind::Delete);
                if (kind == ActionKind::Start && !startable) || (kind == ActionKind::Delete && !deletable) || (needs_live && !n.status.is_live() && !(replace && unheard)) {
                    let reason = match kind {
                        ActionKind::Replace => "the node's status is neither live nor an unheard birth a replacement may retire",
                        ActionKind::Restart => "a restart needs a live node",
                        ActionKind::DeleteConnections => "node.connections.delete needs a live node",
                        ActionKind::Stop => "a stop needs a live node",
                        ActionKind::Start => "a start needs a parked node",
                        ActionKind::Delete => "a delete of a .old path needs the parked predecessor or one whose exit is proven; any other delete through this route is of a deleted mesh's birth",
                    };
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
                // A seat holder's seat moves by node.primary.update to another node-admin of its mesh;
                // with none, the update has no other node of its type and the action is refused (R-MT1).
                if kind != ActionKind::Start {
                    if let Some(reject) = last_admin(&path, kind.name(), &t) {
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
            let action = match kind {
                ActionKind::Restart => AttemptAction::Restart { path, from_incarnation },
                ActionKind::Replace => AttemptAction::Replace { path, from_incarnation },
                ActionKind::DeleteConnections => AttemptAction::DeleteConnections { path, from_incarnation },
                ActionKind::Stop => AttemptAction::Stop { path, from_incarnation },
                ActionKind::Start => AttemptAction::Start { path, from_incarnation },
                ActionKind::Delete => AttemptAction::Delete { path, from_incarnation },
            };
            self.open_with(&current, reason, action, route, &outer, &node_name, span.clone()).await
        }
        .instrument(span.clone())
        .await
    }
}

impl ControlPlane {
    /// Whether `path` is a birth of a mesh deleted at mesh.deleted, as the live view holds it: the
    /// mesh's status is deleted (its row Dead), and its last admin lives on until its own node.delete
    /// (mesh-delete.md steps 6-7).
    pub async fn of_deleted_mesh(&self, path: &PathName) -> bool {
        let t = match self.view_now.get() {
            Some(project) => project().await,
            None => self.topology.read().await.clone(),
        };
        t.meshes.iter().any(|m| m.name == path.mesh && m.status == crate::model::ScopeStatus::Dead)
    }

    /// Open the next attempt of the accepted Build as the recovery of `path` from its exited birth
    /// `(node_id, incarnation)` (node-recovery.md "Execution"): one topology Build in flight, so a
    /// Build still Pending or Running answers `BuildInProgress`. No live-status check: the node is
    /// silent or absent by definition.
    ///
    /// Keyed on the exited birth: once a node workflow that deletes a birth completed for that exact
    /// birth (its recovery, whose last operation `update-node:<path>` completed, or a
    /// `replace-node:<path>` / `retire-node:<path>` the birth exited inside), a submit answers
    /// [`Recovery::AlreadyApplied`] naming that workflow's attempt, and nothing is opened. A node-admin is node-recovered only while its mesh holds
    /// another live admin to recover it (mesh-recovery.md:8-9); otherwise it is mesh recovery.
    pub async fn open_recovery(&self, route: &'static str, path: PathName, node_id: crate::model::NodeId, incarnation: crate::model::IncarnationId) -> Result<Recovery, Refusal> {
        let span = tracing::info_span!("rdm.node_admin.build.update.via-recovery", route, build_id = tracing::field::Empty, attempt = tracing::field::Empty, node = %path, node_id = %node_id, incarnation_id = %incarnation.as_str(), outcome = tracing::field::Empty);
        let outer = tracing::Span::current();
        let r = async {
            self.authority(route).await?;
            // Keyed on the exited birth across the Build log: a birth a node workflow already
            // deleted (its recovery, or a replace or retire it exited inside: node-recovery.md
            // "Failure and receipts") is answered as applied by that workflow's attempt, whatever
            // Build is accepted or in flight now (a later topology Build holds none of its
            // receipts), and nothing is opened.
            if let Some((build_id, attempt, operation)) = self.builds.recovery_of(&path, &node_id, &incarnation).await.map_err(Refusal::State)? {
                outer.in_scope(|| {
                    tracing::info_span!("rdm.node_admin.build.reject.via-recovery-already-applied", route, build_id = %build_id, attempt, operation = %operation, node = %path, node_id = %node_id, incarnation_id = %incarnation.as_str())
                        .in_scope(|| tracing::info!(operation = %operation, "the exited birth was deleted by {operation} and its workflow completed at this attempt: its exit belongs to that workflow, nothing is opened"))
                });
                return Ok(Recovery::AlreadyApplied(Opened { build_id, attempt }));
            }
            let (_admitted, current) = match self.current_settled().await {
                Err(Refusal::BuildInProgress(held)) => {
                    outer.in_scope(|| {
                        tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", route, node = %path, node_id = %node_id, incarnation_id = %incarnation.as_str(), current_build_id = %held)
                            .in_scope(|| tracing::info!("recovery refused: one Build in flight, and {held} is still reconciling"))
                    });
                    return Err(Refusal::BuildInProgress(held));
                }
                other => other?,
            };
            if !current.topology.contains(&path) {
                let reject = BuildReject::UnknownNode { node: path.to_string() };
                outer.in_scope(|| reject_span(route, &reject));
                return Err(Refusal::Reject(reject));
            }
            // A node-admin is node-recovered by its mesh's live primary (mesh-recovery.md:8-9); one
            // with no other live admin in its mesh is that mesh's lost mesh-primary: mesh recovery.
            if path.kind == crate::model::NodeKind::NodeAdmin && crate::executor::node_admin_recoverer(&path, &*self.topology.read().await).is_none() {
                let reject = BuildReject::NodeAdminNotNodeRecovered { node: path.to_string() };
                outer.in_scope(|| reject_span(route, &reject));
                return Err(Refusal::Reject(reject));
            }
            let node_name = path.to_string();
            let action = AttemptAction::Recover { path: path.clone(), node_id: node_id.clone(), from_incarnation: incarnation.clone() };
            self.open_with(&current, AttemptReason::Recovery, action, route, &outer, &node_name, span.clone()).await.map(Recovery::Opened)
        }
        .instrument(span.clone())
        .await;
        span.record("outcome", match &r {
            Ok(Recovery::Opened(_)) => "admitted",
            Ok(Recovery::AlreadyApplied(_)) => "already-applied",
            Err(Refusal::BuildInProgress(_)) => "build-in-progress",
            Err(_) => "refused",
        });
        r
    }

    /// A finished Build leaves history. The current Build, and a Build still reconciling, stay.
    pub async fn forget_build(&self, id: &BuildId) -> Result<(), Refusal> {
        let p = self.builds.read_build(id).await.map_err(|e| match e {
            BuildStateError::UnknownBuild(id) => Refusal::NotFound(format!("no Build {id}")),
            e => Refusal::State(e),
        })?;
        if matches!(p.state, BuildState::Pending | BuildState::Running) {
            return Err(Refusal::Conflict(format!("Build {id} is still {:?}; only finished Builds leave history", p.state)));
        }
        if self.builds.current_build().await.map_err(Refusal::State)?.build_id() == Some(id) {
            return Err(Refusal::Conflict(format!("Build {id} is the current Build (the newest in the builds store that is in progress or completed); it leaves history once a later Build is current")));
        }
        self.builds.forget(id).await.map_err(Refusal::State)
    }
}

/// What an attempt opened at a node route does to the exact birth it is fenced to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionKind {
    /// `restart-node:<path>`.
    Restart,
    /// `replace-node:<path>`.
    Replace,
    /// `node.connections.delete:<path>`.
    DeleteConnections,
    /// `stop-node:<path>`.
    Stop,
    /// `start-node:<path>`.
    Start,
    /// `delete-node:<path>.old`.
    Delete,
}

impl ActionKind {
    /// The action's name: what a refusal says the request would do to the node.
    pub fn name(self) -> &'static str {
        match self {
            Self::Restart => "restart",
            Self::Replace => "replace",
            Self::DeleteConnections => "node.connections.delete",
            Self::Stop => "stop",
            Self::Start => "start",
            Self::Delete => "delete",
        }
    }
}

/// The refusal of a `action` of `node` when `node` holds its mesh's primary seat or the fabric seat in
/// `t` and its node.primary.update has no successor (the approved proposal
/// node-primary-update-fabric-mesh-and-node-type.md, Part C1 "The no-successor cases": "refused by name,
/// `cannot transfer: no secondary of type <node_type> in mesh <mesh>`, until a secondary is available
/// ... the workflow that commanded it ... fails by name with that reason. The one exception is
/// R-MT1's: the delete of a mesh's only node skips the update"), or, for that delete, when the fabric
/// seat it holds has no other mesh-primary to go to (C2 step 3): the same search the holder's
/// pre-check runs (`election::commanded_successor`), so nothing is admitted and no attempt opens.
fn last_admin(node: &PathName, action: &str, t: &Topology) -> Option<BuildReject> {
    let target = t.node(node)?;
    let fabric_seat = t.fabric_primary().is_some_and(|fp| fp.node_id == target.node_id);
    let mesh_seat = t.cohort_primary(&node.mesh, crate::model::NodeKind::NodeAdmin).is_some_and(|p| p.node_id == target.node_id);
    let moving = match (mesh_seat, fabric_seat) {
        (true, true) => crate::election::Moving::BothSeats,
        (true, false) => crate::election::Moving::MeshSeat,
        _ => return None,
    };
    // The delete of a mesh's only node, by a desired-state retire or by the node.delete route (the
    // final primary a mesh.delete left alive, mesh-delete.md step 7), skips the update; a fabric seat on
    // it still moves, out of its mesh (C2 search step 2), and is refused with C2 step 3's text when no
    // other mesh-primary can take it.
    let moving = if matches!(action, "retire" | "delete") && t.only_node(&node.mesh, &target.node_id) {
        if !fabric_seat {
            return None;
        }
        crate::election::Moving::FabricSeatOutside
    } else {
        moving
    };
    let nodes: Vec<crate::model::Node> = t.members().cloned().collect();
    let refusal = crate::election::commanded_successor(&nodes, &crate::election::Incumbency::default(), target, moving).err()?;
    Some(BuildReject::LastAdmin { node: node.to_string(), mesh: node.mesh.clone(), action: action.to_string(), refusal })
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

pub(crate) fn reject_span(route: &'static str, reject: &BuildReject) {
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
        "unheard-mesh",
        "node-admin-not-node-recovered",
        "last-admin",
        "node-not-stopped",
        "referenced-by-open-build"
    );
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

/// What a recovery submit answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Recovery {
    /// The recovery is the attempt opened.
    Opened(Opened),
    /// The exited birth was already recovered, by this attempt of the Build: nothing was opened.
    AlreadyApplied(Opened),
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
    let id = BuildId::parse(&q.id).map_err(|e| Refusal::BadRequest(e.to_string()))?;
    match cp.builds.read_build(&id).await {
        Ok(p) => Ok(Json(p).into_response()),
        Err(BuildStateError::UnknownBuild(id)) => Err(Refusal::NotFound(format!("no Build {id}"))),
        Err(e) => Err(Refusal::State(e)),
    }
}

async fn delete_build(State(cp): State<Shared>, Query(q): Query<BuildQuery>) -> Result<Response, Refusal> {
    let id = BuildId::parse(&q.id).map_err(|e| Refusal::BadRequest(e.to_string()))?;
    cp.forget_build(&id).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn spawn_node(State(cp): State<Shared>, raw: String) -> Result<Response, Refusal> {
    let b: SpawnBody = body(&raw)?;
    Ok(accepted(cp.submit("POST /api/nodes/spawn", TopologyChange::AddNode { mesh: b.mesh, node_kind: b.kind }).await?))
}

async fn delete_node(State(cp): State<Shared>, Path(name): Path<String>) -> Result<Response, Refusal> {
    let node = parse_path(&name)?;
    // `node.delete` of the parked predecessor a replacement left at `<path>.old`, or of the last
    // admin of a deleted mesh (mesh-delete.md step 7: a separate node.delete after mesh.deleted): the
    // topology never names that path, so the delete is the next attempt of the accepted Build, fenced
    // to the exact birth (node-decommission.md).
    if node.old || cp.of_deleted_mesh(&node).await {
        return Ok(accepted(cp.open_attempt("DELETE /api/nodes/{name}", AttemptReason::Delete, node, ActionKind::Delete, None).await?));
    }
    Ok(accepted(cp.submit("DELETE /api/nodes/{name}", TopologyChange::RemoveNode { node }).await?))
}

async fn restart_node(State(cp): State<Shared>, Path(name): Path<String>) -> Result<Response, Refusal> {
    let node = parse_path(&name)?;
    Ok(accepted(cp.open_attempt("POST /api/nodes/{name}/restart", AttemptReason::Restart, node, ActionKind::Restart, None).await?))
}

async fn delete_connections_node(State(cp): State<Shared>, Path(name): Path<String>) -> Result<Response, Refusal> {
    let node = parse_path(&name)?;
    Ok(accepted(cp.open_attempt("POST /api/nodes/{name}/connections/delete", AttemptReason::DeleteConnections, node, ActionKind::DeleteConnections, None).await?))
}

async fn stop_node(State(cp): State<Shared>, Path(name): Path<String>) -> Result<Response, Refusal> {
    let node = parse_path(&name)?;
    Ok(accepted(cp.open_attempt("POST /api/nodes/{name}/stop", AttemptReason::Stop, node, ActionKind::Stop, None).await?))
}

#[derive(Deserialize)]
struct ReplaceQuery {
    /// The exact birth to retire, for a node the Build names that the view does not hold.
    incarnation: Option<String>,
}

async fn replace_node(State(cp): State<Shared>, Path(name): Path<String>, Query(q): Query<ReplaceQuery>) -> Result<Response, Refusal> {
    let node = parse_path(&name)?;
    let birth = q.incarnation.as_deref().map(crate::model::IncarnationId::parse).transpose().map_err(|e| Refusal::BadRequest(e.to_string()))?;
    Ok(accepted(cp.open_attempt("POST /api/nodes/{name}/replace", AttemptReason::Replace, node, ActionKind::Replace, birth).await?))
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

async fn get_fabric(State(cp): State<Shared>) -> Result<Json<Value>, Refusal> {
    // The current Build (R-BI3), read from the builds store: a store that cannot be read is refused
    // by name, never shown as a fabric with no Build.
    let current = cp.builds.current_build().await.map_err(Refusal::State)?.into_build_id();
    let mut v = serde_json::to_value(cp.topology.read().await.fabric_view()).unwrap_or(Value::Null);
    // The shape the fabric should have, as this admin holds it.
    if let Some(o) = v.as_object_mut() {
        o.insert("build_id".into(), serde_json::to_value(current).unwrap_or(Value::Null));
        // A fabric shutdown's progress as this admin sees it: diagnostics, never authority.
        let progress = cp.fabric_shutdown.get().and_then(|s| s.control.progress());
        o.insert("shutdown".into(), serde_json::to_value(progress).unwrap_or(Value::Null));
    }
    Ok(Json(v))
}

/// `GET /api/connections`: the connection facts this admin holds, read-only. Each names its entry's
/// minted id, its source, destination, kind (`direct` or `proxy`, a proxy with its carrier), state and when it
/// was observed. An admin that holds none lists none.
async fn get_connections(State(cp): State<Shared>) -> Json<Value> {
    let (facts, complete) = match cp.connections.get() {
        Some(held) => {
            let held = held.lock().unwrap();
            (crate::views::held_connections(&held), held.is_complete())
        }
        None => (Vec::new(), false),
    };
    let connections: Vec<Value> = facts.iter().map(|f| serde_json::to_value(crate::views::connection_view(f)).unwrap_or(Value::Null)).collect();
    Json(json!({"node": cp.me.to_string(), "complete": complete, "connections": connections}))
}

async fn create_mesh(State(cp): State<Shared>, raw: String) -> Result<Response, Refusal> {
    let desired: MeshDesired = body(&raw)?;
    Ok(accepted(cp.submit("POST /api/meshes", TopologyChange::CreateMesh { desired }).await?))
}

async fn delete_mesh(State(cp): State<Shared>, Path(id): Path<String>) -> Result<Response, Refusal> {
    // By name in the accepted topology, or by id or name in the observed view.
    let in_accepted = cp.builds.current().await.map_err(Refusal::State)?.is_some_and(|b| b.topology.meshes.contains_key(&id));
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
        initiated_at_ms: cp.time.now_ms(),
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
        .route("/api/nodes/{name}/connections/delete", post(delete_connections_node))
        .route("/api/nodes/{name}/stop", post(stop_node))
        .route("/api/meshes", post(create_mesh))
        .route("/api/meshes/{id}", get(get_mesh).delete(delete_mesh))
        .route("/api/fabric", get(get_fabric))
        .route("/api/connections", get(get_connections))
        .route("/api/shutdown", post(shutdown))
        .with_state(cp)
        .merge(runtime_routes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Fabric, FabricId, Mesh, MeshId, Node, NodeStatus, ProviderKind, ScopeStatus};

    fn node(name: &str, primary: bool, fabric: bool) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.status = NodeStatus::ReadyForTraffic;
        n.incarnation_id = Some(crate::model::IncarnationId::mint());
        n.is_primary = primary;
        n.is_fabric_primary = fabric;
        n
    }

    /// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1 "The
    /// no-successor cases": "The one exception is R-MT1's: the delete of a mesh's only node skips the
    /// update"; mesh-delete.md step 7: "A separate node.delete deletes the mesh-primary"): mesh1's final
    /// primary, the only node a mesh.delete left in mesh1 and still its mesh-primary, is deleted through
    /// the node.delete route (`delete`) and through a desired-state retire (`retire`) with no
    /// node.primary.update and no refusal. A stop of that node is refused with Q7's text, and so is the
    /// delete of mesh2's primary while mesh2 still holds another node and no other node-admin. What must
    /// NOT happen: the node.delete mesh-delete.md step 7 names refused `last-admin`.
    #[test]
    fn the_delete_of_a_meshs_only_node_skips_the_update_on_every_delete_route() {
        let t = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
            meshes: vec![
                Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic },
                Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic },
            ],
            nodes: vec![node("mesh1.admin.1", true, false), node("mesh2.admin.1", true, true), node("mesh2.rpc.1", true, false)],
        };
        let only: PathName = "mesh1.admin.1".parse().unwrap();
        for action in ["delete", "retire"] {
            assert!(last_admin(&only, action, &t).is_none(), "{action} of mesh1's only node skips the update: {:?}", last_admin(&only, action, &t));
        }
        let refusal = |path: &str, action: &str| match last_admin(&path.parse().unwrap(), action, &t) {
            Some(BuildReject::LastAdmin { refusal, .. }) => refusal,
            other => panic!("{action} of {path} is refused last-admin: {other:?}"),
        };
        assert_eq!(refusal("mesh1.admin.1", "stop"), "cannot transfer: no secondary of type node-admin in mesh mesh1");
        assert_eq!(refusal("mesh2.admin.1", "delete"), "cannot transfer: no secondary of type node-admin in mesh mesh2");
    }

    /// CONTRACT (mesh-delete.md "Resume after the primary's loss", step 1: "The lost primary's exact
    /// provider Exited is recorded as its roster receipt", step 4: "The new mesh-primary is then deleted
    /// by the separate node.delete of step 7"; the approved proposal
    /// node-primary-update-fabric-mesh-and-node-type.md, Part C1 "The no-successor cases": "The one
    /// exception is R-MT1's: the delete of a mesh's only node skips the update"; elections §4.1: only
    /// the provider's exit proof vacates a seat): mesh2's lost primary `mesh2.admin.1` is proven exited
    /// (its exit-proof row read: `exit_proven`, `Dead`), and `mesh2.admin.2`, the final primary the
    /// roster amendment admitted, is the only node of mesh2. Its node.delete (`delete`) skips the
    /// update at the Build door (`last_admin`) and at the drive (`executor::seat_move`), and the fabric
    /// primary executes it. A member `Dead` by the true-offline mark alone (no exit proof) is still a
    /// node of the mesh: the same delete moves the seat first, and with no Ready secondary it is refused
    /// with Q7's text.
    /// Must NOT happen: a birth proven exited counted as the mesh's other node (step 7's delete refused
    /// `last-admin`), or a birth only marked offline discounted.
    #[test]
    fn a_birth_proven_exited_is_not_the_other_node_of_its_mesh_when_its_final_primary_is_deleted() {
        let mut lost = node("mesh2.admin.1", false, false);
        lost.status = NodeStatus::Dead;
        lost.exit_proven = true;
        let mut t = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
            meshes: vec![
                Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic },
                Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic },
            ],
            nodes: vec![node("mesh1.admin.1", true, true), lost, node("mesh2.admin.2", true, false)],
        };
        let last: PathName = "mesh2.admin.2".parse().unwrap();
        let delete = crate::build::BuildOperation::RetireNode { node: last.clone(), exited: None };
        assert!(last_admin(&last, "delete", &t).is_none(), "the delete of mesh2's only node skips the update: {:?}", last_admin(&last, "delete", &t));
        assert_eq!(crate::executor::seat_move(&delete, &t), None, "no seat moves before it");
        assert_eq!(crate::executor::executor_for(&delete, &t).map(|p| p.to_string()), Some("mesh1.admin.1".to_string()), "the fabric primary executes it");
        for n in t.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh2.admin.1") {
            n.exit_proven = false;
        }
        match last_admin(&last, "delete", &t) {
            Some(BuildReject::LastAdmin { refusal, .. }) => assert_eq!(refusal, "cannot transfer: no secondary of type node-admin in mesh mesh2"),
            other => panic!("a birth marked offline is still mesh2's other node: the delete is refused last-admin: {other:?}"),
        }
        assert!(crate::executor::seat_move(&delete, &t).is_some_and(|m| m.mesh_seat), "a birth marked offline is still mesh2's other node: the seat moves first");
    }

    /// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C2
    /// "Successor search for the commanded transfer": "2. only when that mesh has none ... the lowest
    /// NodeId among the other ReadyForTraffic mesh-primaries; 3. neither found: refused by name,
    /// `cannot transfer fabric seat: no other node-admin ReadyForTraffic in mesh <mesh>, no other
    /// mesh-primary ReadyForTraffic` ... the commanding workflow fails with that reason"; C1: "the
    /// delete of a mesh's only node skips the update"): the fabric primary `mesh2.admin.1` is mesh2's
    /// only node. Its retire and delete skip node.primary.update, and its fabric seat moves out of
    /// mesh2: with mesh1's primary ReadyForTraffic the Build door admits them; with no other
    /// mesh-primary ReadyForTraffic the door refuses them `last-admin` with C2 step 3's text.
    /// Must NOT happen: a Build admitted whose fabric seat has nowhere to go.
    #[test]
    fn the_delete_of_a_fabric_primary_that_is_its_meshs_only_node_needs_another_ready_mesh_primary() {
        let mut t = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
            meshes: vec![
                Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic },
                Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic },
            ],
            nodes: vec![node("mesh1.admin.1", true, false), node("mesh1.rpc.1", true, false), node("mesh2.admin.1", true, true)],
        };
        let fp: PathName = "mesh2.admin.1".parse().unwrap();
        for action in ["retire", "delete"] {
            assert!(last_admin(&fp, action, &t).is_none(), "{action}: mesh1's Ready primary takes the fabric seat: {:?}", last_admin(&fp, action, &t));
        }
        for n in t.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh1.admin.1") {
            n.status = NodeStatus::Pending;
        }
        for action in ["retire", "delete"] {
            match last_admin(&fp, action, &t) {
                Some(BuildReject::LastAdmin { refusal, .. }) => assert_eq!(refusal, "cannot transfer fabric seat: no other node-admin ReadyForTraffic in mesh mesh2, no other mesh-primary ReadyForTraffic", "{action}"),
                other => panic!("{action}: no other mesh-primary is ReadyForTraffic: refused last-admin: {other:?}"),
            }
        }
    }
}
