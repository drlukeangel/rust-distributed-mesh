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
use crate::build::{BuildId, BuildReject, MeshDesired};
use crate::build_state::{AttemptOpened, AttemptReason, BuildAccepted, BuildState, BuildStateAdapter, BuildStateError};
use crate::model::{NodeKind, PathName};
use crate::topology::Topology;
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
    /// Each mesh's in-flight row (R-BI7 caveat 3, `crate::mesh_claims`): what a drift attempt this
    /// admin opens as its mesh's primary takes before any fact of it is written.
    pub claims: Arc<crate::mesh_claims::MeshClaims>,
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
    /// initial fabric before ready-for-traffic"), and each exited birth's exit-proof row is the durable
    /// proof of its departure (node-recovery.md: "Exited is the proof, recorded durably"), which a drift
    /// lane's take-over from a departed claimant reads. Held in memory, empty, until the admin hands its
    /// own ([`ControlPlane::with_first_ready`]).
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
            claims: Arc::new(crate::mesh_claims::MeshClaims::in_memory()),
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

    /// This control plane taking each mesh's in-flight row in `claims` (the fabric's store root).
    pub fn with_claims(mut self, claims: Arc<crate::mesh_claims::MeshClaims>) -> Self {
        self.claims = claims;
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
            // A topology Build in progress refuses a second topology change fabric-wide (build.md §1,
            // R-BI7): named by the mesh its claimed attempt acts in and its claimant, when claimed.
            let held = holding(&current, None).unwrap_or_else(|| InProgress { current_build_id: current.build_id.clone(), mesh: None, held_by: None });
            tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", build_id = %current.build_id, state = ?current.state, mesh = %held.mesh.as_deref().unwrap_or(""), held_by = %held.held_by.as_deref().unwrap_or(""))
                .in_scope(|| tracing::info!("another Build is reconciling: one topology Build in flight"));
            return Err(Refusal::BuildInProgress(held));
        }
        Ok((admission, current))
    }

    /// The current Build (R-BI3) for an attempt that acts in `mesh` alone with `action`, and the
    /// refusal when an attempt holds `mesh` in this admin's fold (R-BI7 caveat 3: one attempt in
    /// flight per mesh, not per fabric): the fabric lane's claimed attempt whose executor is of
    /// `mesh`, or `mesh`'s own drift lane's open attempt. An attempt in progress in another mesh
    /// refuses nothing. The lane's attempt that is opened with `action` and claimed by no one is
    /// not a refusal: it is answered as the attempt to claim (build.md :365 as the approved proposal
    /// A4 words it: claims are insert-and-fail on the attempt, so its first claim takes it,
    /// whoever opened it). The admission is taken first and returned with the Build.
    async fn mesh_settled(&self, mesh: &str, action: &AttemptAction) -> Result<(tokio::sync::OwnedMutexGuard<()>, crate::build_state::BuildProjection, Option<u32>), Refusal> {
        let admission = self.admission.clone().lock_owned().await;
        let read = || async {
            self.builds.current().await.map_err(Refusal::State)?.ok_or_else(|| {
                Refusal::Unavailable("this admin holds no current Build yet (no Build in its builds store is in progress or completed); submit through a Ready node-admin".into())
            })
        };
        let mut current = read().await?;
        // The mesh's own drift attempt whose claimant departed with it unfinished ends on that
        // departure's exact proof, and the mesh is free for this attempt (`end_departed_attempt`).
        if holding(&current, Some(mesh)).is_some() && self.end_departed_attempt(&current, mesh).await? {
            current = read().await?;
        }
        if let Some(attempt) = open_unclaimed(&current, mesh, action) {
            return Ok((admission, current, Some(attempt)));
        }
        if let Some(mut held) = holding(&current, Some(mesh)) {
            // An attempt opened and claimed by no one has no claimant in the fold: the holder is the
            // admin whose in-flight row of the mesh names it, the one that opened it.
            if held.held_by.is_none() {
                let row = self.claims.held(mesh).await.map_err(|e| {
                    Refusal::Unavailable(format!("{}: mesh {mesh} is held by an attempt of {} that no one claimed, and the mesh's in-flight row naming its holder could not be read: {e}", self.me, held.current_build_id))
                })?;
                held.held_by = row.filter(|r| r.build_id == held.current_build_id && r.lane.as_deref() == Some(mesh)).map(|r| r.claimant);
            }
            tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", build_id = %held.current_build_id, mesh, held_by = %held.held_by.as_deref().unwrap_or(""))
                .in_scope(|| tracing::info!("an attempt holds the mesh: one attempt in flight per mesh"));
            return Err(Refusal::BuildInProgress(held));
        }
        Ok((admission, current, None))
    }

    /// The exit-proof row of the exact birth this admin's view names at `claimant` in `mesh`, a point
    /// read: `None` while no exit of that birth is recorded (silence is never departure: build.md
    /// §5.2, R-BI4), or while the view names no birth there. A row that cannot be read is refused by
    /// name, never taken as no proof.
    async fn departure_of(&self, claimant: &PathName, mesh: &str) -> Result<Option<(crate::model::Node, crate::status_storage::NodeExitRow)>, Refusal> {
        let Some(n) = self.topology.read().await.node(claimant).cloned() else { return Ok(None) };
        let Some(incarnation) = n.incarnation_id.clone() else { return Ok(None) };
        match self.ready_rows.node_exit_of(mesh, &n.node_id, &incarnation).await {
            Ok(row) => Ok(row.map(|row| (n, row))),
            Err(e) => Err(Refusal::Unavailable(format!("{}: the exit-proof row of {claimant} ({}) at {} among mesh {mesh}'s status rows could not be read to prove its departure: {e}", self.me, n.node_id, incarnation.as_str()))),
        }
    }

    /// End `mesh`'s drift attempt of `current` whose claimant departed with it unfinished, when its
    /// departure is proven (R-BI7 caveat 2: each mesh-primary claims its mesh's drift attempts on its
    /// mesh's rows; build.md §5.2 as the approved proposal A4 words it: "The next attempt is claimed
    /// after the running attempt's terminal receipt (Complete, Failed or HandedOff), or, when the
    /// executor is lost with the attempt unfinished, after exact departure proof. A live run is
    /// reattached, never re-claimed"). The lane's running attempt, claimed by another admin than this
    /// one, whose exact birth in this admin's view has its exit recorded, ends `Failed` naming that
    /// departure (on this admin's Build log and the Build topic, so every fold holds the lane free and
    /// the exits the attempt was recovering are owed a submit again: node-recovery.md "Failure and
    /// receipts"), and its in-flight row of the mesh is released. Answers whether it ended one. An
    /// attempt this admin claimed, an attempt whose claimant is not proven departed, and the fabric
    /// lane's attempt (its drive takes it over) are left holding the mesh.
    async fn end_departed_attempt(&self, current: &crate::build_state::BuildProjection, mesh: &str) -> Result<bool, Refusal> {
        let fabric_holds = current.state == BuildState::Running && current.executor.as_deref().and_then(|e| e.parse::<PathName>().ok()).is_some_and(|e| e.mesh == mesh);
        let Some(lane) = current.lanes.get(mesh).filter(|l| !fabric_holds && l.state == BuildState::Running) else { return Ok(false) };
        let Some(claimant) = lane.claimant.as_deref().and_then(|c| c.parse::<PathName>().ok()) else { return Ok(false) };
        if claimant == self.me {
            return Ok(false);
        }
        let Some((n, row)) = self.departure_of(&claimant, mesh).await? else { return Ok(false) };
        let attempt = lane.attempt;
        let incarnation = row.incarnation_id.as_str().to_string();
        let reason = format!(
            "{claimant} ({} at {incarnation}) departed with attempt {attempt} of mesh {mesh}'s lane unfinished: its exit is proven ({}{}) and recorded; {} takes the lane over",
            n.node_id,
            row.proof,
            row.exit_code.map(|c| format!(", exit code {c}")).unwrap_or_default(),
            self.me
        );
        let span = tracing::info_span!("rdm.node_admin.build.update.via-departed-claimant", build_id = %current.build_id, lane = mesh, attempt, claimant = %claimant, node_id = %n.node_id, incarnation_id = %incarnation, exit_proof = %row.proof, node = %self.me, outcome = tracing::field::Empty);
        let verdict = crate::build_state::BuildAttemptReceipt { mesh: Some(mesh.to_string()), build_id: current.build_id.clone(), attempt, outcome: crate::build_state::AttemptOutcome::Failed { reason } };
        if let Err(e) = self.builds.append_attempt_receipt(&verdict).await {
            span.record("outcome", "verdict-unwritten");
            span.in_scope(|| tracing::info!(error = %e, "the departed claimant's attempt could not be ended: the mesh stays held by it"));
            return Err(Refusal::State(e));
        }
        let held = crate::mesh_claims::InFlight { build_id: current.build_id.clone(), lane: Some(mesh.to_string()), attempt, claimant: claimant.to_string() };
        if let Err(e) = self.claims.release(mesh, &held).await {
            span.record("outcome", "in-flight-unreleased");
            span.in_scope(|| tracing::info!(error = %e, "the departed claimant's attempt is ended and mesh's in-flight row could not be released: the mesh stays held by it"));
            return Err(Refusal::Unavailable(format!("{}: attempt {attempt} of mesh {mesh}'s lane of {} is ended on {claimant}'s departure and mesh {mesh}'s in-flight row could not be released: {e}", self.me, current.build_id)));
        }
        span.record("outcome", "ended");
        span.in_scope(|| tracing::info!("the mesh's drift attempt ends with its departed claimant, and its in-flight row is released"));
        Ok(true)
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
            // A mesh whose own drift attempt is open is held by it: a topology operation on that mesh
            // is refused, naming the Build, the mesh and the mesh-primary holding it (R-BI7 caveat 3).
            if let Some(held) = operations.iter().filter_map(op_mesh).find_map(|mesh| holding(&current, Some(&mesh))) {
                outer.in_scope(|| {
                    tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", route, build_id = %held.current_build_id, mesh = %held.mesh.as_deref().unwrap_or(""), held_by = %held.held_by.as_deref().unwrap_or(""))
                        .in_scope(|| tracing::info!("a topology operation acts in a mesh its own drift attempt holds: one attempt in flight per mesh"))
                });
                return Err(Refusal::BuildInProgress(held));
            }
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
            Ok::<Opened, Refusal>(Opened { build_id, attempt: 1, mesh: None })
        }
        .instrument(span.clone())
        .await
    }

    /// Open the next attempt of `current` in `lane` (`None` the fabric lane, `Some(mesh)` that mesh's
    /// drift lane) with `action`, putting its context first.
    #[allow(clippy::too_many_arguments)]
    async fn open_with(&self, current: &crate::build_state::BuildProjection, lane: Option<&str>, reason: AttemptReason, action: AttemptAction, route: &'static str, outer: &tracing::Span, node_name: &str, span: tracing::Span) -> Result<Opened, Refusal> {
        let opened = AttemptOpened {
            mesh: lane.map(str::to_string),
            build_id: current.build_id.clone(),
            attempt: next_in_lane(current, lane),
            reason,
            action: Some(action),
            opened_by: self.me.to_string(),
            opened_at_ms: self.time.now_ms(),
        };
        span.record("build_id", current.build_id.as_str());
        span.record("attempt", opened.attempt);
        // The context is on this fabric-primary before the attempt is open to be claimed.
        self.contexts
            .put_lane(&opened.build_id, lane, opened.attempt, &crate::build_claim::current_context())
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
        tracing::info!(build_id = %current.build_id, attempt = opened.attempt, lane = lane.unwrap_or("fabric"), "attempt opened");
        self.build_submitted.notify_waiters();
        Ok(Opened { build_id: current.build_id.clone(), attempt: opened.attempt, mesh: opened.mesh })
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
            // The node's mesh held by its own drift attempt refuses the action, naming the mesh-primary
            // holding it (R-BI7 caveat 3).
            if let Some(held) = holding(&current, Some(&path.mesh)) {
                outer.in_scope(|| {
                    tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", route, build_id = %held.current_build_id, mesh = %path.mesh, held_by = %held.held_by.as_deref().unwrap_or(""), node = %path)
                        .in_scope(|| tracing::info!("the node's mesh is held by its own drift attempt: one attempt in flight per mesh"))
                });
                return Err(Refusal::BuildInProgress(held));
            }
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
                        return self.open_with(&current, None, reason, action, route, &outer, &named_path, span.clone()).await;
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
            self.open_with(&current, None, reason, action, route, &outer, &node_name, span.clone()).await
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

    /// Open and claim the recovery of `path` from its exited birth `(node_id, incarnation)` as the
    /// next attempt of the current Build in `path`'s mesh's drift lane (R-BI7: "the mesh-primary
    /// opens the recovery attempt of the current Build itself: claim on its mesh's rows, then run; no
    /// call to the fabric-primary"; node-recovery.md "Execution"). No live-status check: the node is
    /// silent or absent by definition.
    ///
    /// In order: keyed on the exited birth, once a node workflow that deletes a birth completed for
    /// that exact birth (its recovery, whose last operation `create-node:<path>` completed, or a
    /// `replace-node:<path>` / `retire-node:<path>` the birth exited inside), a submit answers
    /// [`Recovery::AlreadyApplied`] naming that workflow's attempt, whoever asks, and nothing is
    /// opened. A node-admin is node-recovered only while its mesh holds another live admin to recover
    /// it (mesh-recovery.md:8-9); otherwise it is mesh recovery. The claimant is the member's
    /// recoverer, its mesh's primary node-admin as this admin's view holds the seat: any other admin
    /// is refused [`Refusal::NotClaimant`] naming who may claim. An attempt holding the mesh (a
    /// topology operation's claimed attempt there, or the mesh's own drift attempt) answers
    /// `BuildInProgress` naming the Build, the mesh and its claimant; an attempt in another mesh
    /// refuses nothing. Then the mesh's in-flight row is taken on the store root before any fact is
    /// written, the attempt is opened in the mesh's lane, and its claim is decided by this admin. The
    /// lane's attempt already opened with this very recovery and claimed by no one (its opener lost
    /// between its open and its claim) is claimed as it stands: never opened again, and nothing ends it.
    pub async fn open_recovery(&self, route: &'static str, path: PathName, node_id: crate::model::NodeId, incarnation: crate::model::IncarnationId) -> Result<Recovery, Refusal> {
        let span = tracing::info_span!("rdm.node_admin.build.update.via-recovery", route, build_id = tracing::field::Empty, attempt = tracing::field::Empty, mesh = %path.mesh, node = %path, node_id = %node_id, incarnation_id = %incarnation.as_str(), open_attempt = tracing::field::Empty, outcome = tracing::field::Empty);
        let outer = tracing::Span::current();
        let r = async {
            // Keyed on the exited birth across the Build log: a birth a node workflow already
            // deleted (its recovery, or a replace or retire it exited inside: node-recovery.md
            // "Failure and receipts") is answered as applied by that workflow's attempt, whatever
            // Build is accepted or in flight now (a later topology Build holds none of its
            // receipts), and nothing is opened.
            if let Some((build_id, mesh, attempt, operation)) = self.builds.recovery_of(&path, &node_id, &incarnation).await.map_err(Refusal::State)? {
                outer.in_scope(|| {
                    tracing::info_span!("rdm.node_admin.build.reject.via-recovery-already-applied", route, build_id = %build_id, lane = mesh.as_deref().unwrap_or("fabric"), attempt, operation = %operation, node = %path, node_id = %node_id, incarnation_id = %incarnation.as_str())
                        .in_scope(|| tracing::info!(operation = %operation, "the exited birth was deleted by {operation} and its workflow completed at this attempt: its exit belongs to that workflow, nothing is opened"))
                });
                return Ok(Recovery::AlreadyApplied(Opened { build_id, attempt, mesh }));
            }
            let t = self.topology.read().await.clone();
            // A node-admin is node-recovered by its mesh's live primary (mesh-recovery.md:8-9); one
            // with no other live admin in its mesh is that mesh's lost mesh-primary: mesh recovery.
            let recoverer = if path.kind == crate::model::NodeKind::NodeAdmin {
                let Some(recoverer) = crate::executor::node_admin_recoverer(&path, &t) else {
                    let reject = BuildReject::NodeAdminNotNodeRecovered { node: path.to_string() };
                    outer.in_scope(|| reject_span(route, &reject));
                    return Err(Refusal::Reject(reject));
                };
                Some(recoverer)
            } else {
                t.cohort_primary(&path.mesh, NodeKind::NodeAdmin).map(|n| n.name.clone())
            };
            // The claimant of a drift attempt is its mesh's own primary, as this admin's view holds
            // the seat (R-BI7 caveat 2): decided from this admin's view, never from who asked.
            if recoverer.as_ref() != Some(&self.me) {
                let fabric_primary = t.fabric_primary().map(|n| n.name.to_string());
                let mesh_primary = recoverer.map(|n| n.to_string());
                outer.in_scope(|| {
                    tracing::info_span!("rdm.node_admin.build.reject.via-not-claimant", route, node = %self.me, mesh = %path.mesh, fabric_primary = %fabric_primary.as_deref().unwrap_or(""), mesh_primary = %mesh_primary.as_deref().unwrap_or(""), member = %path)
                        .in_scope(|| tracing::info!("a drift attempt is claimed by its mesh's primary: this admin is not it in its view"))
                });
                return Err(Refusal::NotClaimant { mesh: path.mesh.clone(), fabric_primary, mesh_primary });
            }
            let action = AttemptAction::Recover { path: path.clone(), node_id: node_id.clone(), from_incarnation: incarnation.clone() };
            let (_admitted, current, open) = self.mesh_settled(&path.mesh, &action).await?;
            if !current.topology.contains(&path) {
                let reject = BuildReject::UnknownNode { node: path.to_string() };
                outer.in_scope(|| reject_span(route, &reject));
                return Err(Refusal::Reject(reject));
            }
            // The mesh's in-flight row decides first, on the store root: whichever claims first on the
            // mesh's rows wins (R-BI7 caveat 3). Nothing of the attempt is written before it.
            let lane = Some(path.mesh.as_str());
            // The lane's attempt opened with this very recovery and claimed by no one is claimed as
            // it stands; otherwise the lane's next attempt is opened for it.
            let attempt = open.unwrap_or_else(|| next_in_lane(&current, lane));
            span.record("open_attempt", if open.is_some() { "held-open" } else { "opened-here" });
            let row = crate::mesh_claims::InFlight { build_id: current.build_id.clone(), lane: lane.map(str::to_string), attempt, claimant: self.me.to_string() };
            let mut taken = self.claims.take(&path.mesh, &row).await;
            // A row a departed claimant took for a drift attempt that holds no lane in this admin's
            // fold (its run ended and the row was never released, or nothing of it was written before
            // the claimant was lost) is released on that departure's exact proof, and taken again.
            if let Ok(crate::mesh_claims::Taken::Held(held)) = &taken {
                let departed = match held.claimant.parse::<PathName>() {
                    Ok(c) if held.lane.as_deref() == Some(path.mesh.as_str()) && c != self.me => self.departure_of(&c, &path.mesh).await?.map(|(n, row)| (c, n, row)),
                    _ => None,
                };
                if let Some((claimant, n, exit)) = departed {
                    let release = tracing::info_span!("rdm.node_admin.build.update.via-departed-claimant", build_id = %held.build_id, lane = %path.mesh, attempt = held.attempt, claimant = %claimant, node_id = %n.node_id, incarnation_id = %exit.incarnation_id.as_str(), exit_proof = %exit.proof, node = %self.me, outcome = tracing::field::Empty);
                    if let Err(e) = self.claims.release(&path.mesh, held).await {
                        release.record("outcome", "in-flight-unreleased");
                        return Err(Refusal::Unavailable(format!("{}: mesh {}'s in-flight row names attempt {} of {} by {claimant}, which departed; the row could not be released: {e}", self.me, path.mesh, held.attempt, held.build_id)));
                    }
                    release.record("outcome", "row-released");
                    release.in_scope(|| tracing::info!("the mesh's in-flight row of a departed claimant's attempt that holds no lane is released"));
                    taken = self.claims.take(&path.mesh, &row).await;
                }
            }
            match taken {
                Ok(crate::mesh_claims::Taken::Won) => {}
                Ok(crate::mesh_claims::Taken::Held(held)) => {
                    let held = InProgress { current_build_id: held.build_id, mesh: Some(path.mesh.clone()), held_by: Some(held.claimant) };
                    outer.in_scope(|| {
                        tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", route, build_id = %held.current_build_id, mesh = %path.mesh, held_by = %held.held_by.as_deref().unwrap_or(""), node = %path)
                            .in_scope(|| tracing::info!("the mesh's in-flight row names another attempt: one attempt in flight per mesh"))
                    });
                    return Err(Refusal::BuildInProgress(held));
                }
                Err(e) => return Err(Refusal::Unavailable(format!("{}: the in-flight row of mesh {} could not be taken for attempt {attempt} of {}: {e}", self.me, path.mesh, current.build_id))),
            }
            let node_name = path.to_string();
            let opened = match open {
                // Its open is on the Build log already, whoever wrote it: nothing is written again.
                Some(attempt) => Opened { build_id: current.build_id.clone(), attempt, mesh: lane.map(str::to_string) },
                None => match self.open_with(&current, lane, AttemptReason::Recovery, action, route, &outer, &node_name, span.clone()).await {
                    Ok(o) => o,
                    Err(e) => {
                        // Nothing of the attempt was written: the row taken for it goes.
                        let _ = self.claims.release(&path.mesh, &row).await;
                        return Err(e);
                    }
                },
            };
            let claim = crate::build_state::BuildAttemptClaim { build_id: opened.build_id.clone(), attempt: opened.attempt, executor: self.me.to_string(), claimant: Some(self.me.to_string()), mesh: opened.mesh.clone() };
            match crate::build_claim::decide_claim(&self.me, &*self.builds, &self.claims, &self.contexts, &claim, &path.mesh).await {
                rafka_node_rpc_contract::build_claim::BuildClaimReply::Won { .. } => Ok(Recovery::Opened(opened)),
                other => Err(Refusal::Unavailable(format!("{}: attempt {} of {} in mesh {}'s lane was opened and its claim not won: {other:?}", self.me, opened.attempt, opened.build_id, path.mesh))),
            }
        }
        .instrument(span.clone())
        .await;
        if let Ok(Recovery::Opened(o) | Recovery::AlreadyApplied(o)) = &r {
            span.record("build_id", o.build_id.as_str());
            span.record("attempt", o.attempt);
        }
        span.record("outcome", match &r {
            Ok(Recovery::Opened(_)) => "admitted",
            Ok(Recovery::AlreadyApplied(_)) => "already-applied",
            Err(Refusal::BuildInProgress(_)) => "build-in-progress",
            Err(Refusal::NotClaimant { .. }) => "not-claimant",
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
    /// Another attempt holds what the request needs: a topology Build in progress (a second topology
    /// change), or the attempt holding the mesh the request acts in (one attempt in flight per mesh,
    /// R-BI7 caveat 3).
    BuildInProgress(InProgress),
    /// This admin may not open or claim on the mesh the request acts in: a topology attempt is the
    /// fabric-primary's, a drift attempt its mesh's primary's (R-BI7 caveat 2), as its view holds them.
    NotClaimant {
        /// The mesh the request acts in.
        mesh: String,
        /// The fabric-primary this admin's view holds, when one.
        fabric_primary: Option<String>,
        /// The mesh's primary this admin's view holds (the member's recoverer), when one.
        mesh_primary: Option<String>,
    },
    /// This admin is not the fabric-primary (named when known).
    NotAuthority(Option<String>),
    /// The attempt number this request computed was opened first by another action.
    AttemptTaken(String),
    /// The admin cannot decide the request yet (not hydrated).
    Unavailable(String),
    /// The Build state could not be read or written.
    State(BuildStateError),
}

/// The Build and the attempt a request answered 202 for: the caller waits for exactly this
/// attempt. A Build that is accepted starts at attempt 1; an opened attempt is the one it opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Opened {
    /// The Build.
    pub build_id: BuildId,
    /// The attempt, numbered in its lane.
    pub attempt: u32,
    /// The attempt's lane: `None` the fabric lane, `Some(mesh)` that mesh's drift lane (R-BI7).
    pub mesh: Option<String>,
}

/// What holds the Build or the mesh a refused request needed (R-BI7 caveat 3; envelope
/// `BuildInProgress { current_build_id, mesh, held_by }`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InProgress {
    /// The Build of the holding attempt.
    pub current_build_id: BuildId,
    /// The mesh the holding attempt acts in, when known (a claimed attempt's).
    pub mesh: Option<String>,
    /// The admin that claimed the holding attempt, as its claim recorded it (never re-read from a
    /// later seat view); `None` for an attempt not yet claimed or claimed before claimants were
    /// recorded.
    pub held_by: Option<String>,
}

/// The attempt of `p` holding `mesh` in this admin's fold (R-BI7 caveat 3), or, for `None`, the
/// fabric lane's attempt in progress named by the mesh it acts in: the fabric lane's claimed
/// (running) attempt acts in its executor's mesh; a drift lane's open attempt holds its own mesh.
/// A fabric-lane attempt opened and not yet claimed holds no mesh: its claim takes the mesh's row.
fn holding(p: &crate::build_state::BuildProjection, mesh: Option<&str>) -> Option<InProgress> {
    let fabric_mesh = (p.state == BuildState::Running).then(|| p.executor.as_deref().and_then(|e| e.parse::<PathName>().ok()).map(|e| e.mesh)).flatten();
    match mesh {
        None => (matches!(p.state, BuildState::Pending | BuildState::Running)).then(|| InProgress {
            current_build_id: p.build_id.clone(),
            held_by: fabric_mesh.as_ref().and(p.claimant.clone()),
            mesh: fabric_mesh,
        }),
        Some(mesh) => {
            if fabric_mesh.as_deref() == Some(mesh) {
                return Some(InProgress { current_build_id: p.build_id.clone(), mesh: Some(mesh.to_string()), held_by: p.claimant.clone() });
            }
            p.lanes.get(mesh).filter(|l| l.is_open()).map(|l| InProgress { current_build_id: p.build_id.clone(), mesh: Some(mesh.to_string()), held_by: l.claimant.clone() })
        }
    }
}

/// The attempt of `mesh`'s drift lane of `p` that is opened with `action` and claimed by no one,
/// while the fabric lane's attempt does not hold `mesh`: the lane stands at its open (`Pending`, the
/// open one past the highest claim). An attempt handed off or claimed is not one.
fn open_unclaimed(p: &crate::build_state::BuildProjection, mesh: &str, action: &AttemptAction) -> Option<u32> {
    let fabric_holds = p.state == BuildState::Running && p.executor.as_deref().and_then(|e| e.parse::<PathName>().ok()).is_some_and(|e| e.mesh == mesh);
    let lane = p.lanes.get(mesh).filter(|l| !fabric_holds && l.state == BuildState::Pending && l.opened == l.attempt + 1)?;
    (lane.action.as_ref() == Some(action)).then_some(lane.opened)
}

/// The next attempt of `p` in `lane`: the fabric lane's numbering is the Build's; a drift lane's
/// is its mesh's, from 1.
fn next_in_lane(p: &crate::build_state::BuildProjection, lane: Option<&str>) -> u32 {
    match lane {
        None => p.attempt + 1,
        Some(mesh) => p.lanes.get(mesh).map_or(0, |l| l.attempt) + 1,
    }
}

/// The mesh `op` acts in.
fn op_mesh(op: &crate::build::BuildOperation) -> Option<String> {
    match op {
        crate::build::BuildOperation::CreateMesh { mesh, .. } | crate::build::BuildOperation::ShutdownMesh { mesh, .. } => Some(mesh.clone()),
        other => other.node().map(|n| n.mesh.clone()),
    }
}

/// What a recovery submit answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Recovery {
    /// The recovery is the attempt opened.
    Opened(Opened),
    /// The exited birth was already recovered, by this attempt of the Build: nothing was opened.
    AlreadyApplied(Opened),
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
