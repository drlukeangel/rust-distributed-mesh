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
use crate::build::{BuildId, BuildReject};
use crate::build_state::{AttemptOpened, AttemptReason, BuildAccepted, BuildState, BuildStateAdapter, BuildStateError};
use crate::model::{NodeKind, PathName};
use crate::topology::Topology;
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
    /// Fabric recovery in a view (Q122): its executor opens the re-create of the node-admin at the
    /// fabric-primary's path.name, in the fabric lane.
    pub fabric_recovery: std::sync::OnceLock<crate::fabric_recovery::Reader>,
    /// This admin's seat book: a node-admin it holds as its mesh's primary replaced by the election is
    /// an unheard mesh-primary, the fabric-primary's to recover (node-recovery.md step 2).
    pub seats: std::sync::OnceLock<rdm_transport::membership::SeatBook>,
    /// What the fabric-primary's investigation holds of a peer mesh it has heard on the backbone
    /// (`crate::investigate::Ladder::peer_mesh`): a node of a mesh held unheard is not restarted,
    /// stopped or deleted from here (R-D1). Set once by the admin that owns the ladder.
    pub peer_mesh: std::sync::OnceLock<Arc<dyn Fn(&str) -> Option<crate::investigate::PeerMesh> + Send + Sync>>,
    /// The connection facts this admin holds (set once by the admin that owns its connections
    /// writer): the active Directs of the fleet, and this admin's own latest Directs and Proxies.
    pub connections: std::sync::OnceLock<Arc<std::sync::Mutex<rdm_core::connections::ConnectionsHeld>>>,
    /// The CPU and RAM each member's latest digest carries, by node id (set once by the admin that
    /// owns the digest book). Load is served beside the view, never inside it: it changes every
    /// digest and never moves the topology.
    pub loads: std::sync::OnceLock<Arc<dyn Fn() -> std::collections::BTreeMap<String, (Option<rdm_core::NodeLoad>, Option<rdm_core::GossipStats>)> + Send + Sync>>,
    /// Woken on every accepted Build so the executor re-plans.
    pub build_submitted: Arc<Notify>,
    /// The fabric's climb told of a Build this admin accepted, before the door answers it: a resize or
    /// roll puts the fabric back to pending inside the acceptance (states.md, Resize and roll), and the
    /// view this admin answers from holds it. Set once by the admin that owns the climb.
    pub resize: std::sync::OnceLock<Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>>,
    /// Where each attempt this admin brings into being keeps its context, for the claim to return.
    pub contexts: Arc<crate::build_claim::AttemptContexts>,
    /// Each mesh's in-flight row (R-BI7 caveat 3, `crate::mesh_claims`): what a drift attempt this
    /// admin opens as its mesh's primary takes before any fact of it is written.
    pub claims: Arc<dyn crate::storage::MeshStorage>,
    /// The rafka-time this admin stamps its Builds and attempts with.
    pub time: rdm_transport::clock::RafkaTime,
    /// Where this fabric-primary admits a Build or an attempt: held from the check that no Build is
    /// reconciling to the durable write that makes the next one, by every path that starts work
    /// (a submitted change, an opened attempt, the drift check).
    pub admission: Arc<tokio::sync::Mutex<()>>,
    /// Whether the initial fabric has first reached ready-for-traffic: until it has, a workflow that
    /// moves a seat or writes a caller's row is refused `fabric-not-ready` (states.md, "The initial
    /// fabric before ready-for-traffic").
    pub first_ready: crate::fabric_state::FirstReady,
    /// The fabric store: the fabric's ready-for-traffic fact on it is the durable fact that it first
    /// reached ready-for-traffic (states.md, "The initial fabric before ready-for-traffic"). Held in
    /// memory, empty, until the admin hands its own ([`ControlPlane::with_first_ready`]).
    pub ready_rows: Arc<dyn crate::fabric_storage::FabricStorage>,
    /// The exact departure proof of a drift lane's claimant (build.md: "after exact departure proof"),
    /// set once the admin's drive is built; none proves nothing.
    pub departure: std::sync::OnceLock<Arc<dyn crate::build_drive::DepartureProof>>,
}

impl ControlPlane {
    /// A control plane for the admin `me` over its Build state, its accepted-Build store and an
    /// initial `topology`, stamping with `time`: the rafka-time its process adopted. A control
    /// plane is never built without one, because every Build and attempt it opens is
    /// stamped from it.
    pub fn new(builds: Arc<dyn BuildStateAdapter>, accepted: Arc<AcceptedStore>, me: PathName, topology: Topology, time: rdm_transport::clock::RafkaTime) -> Self {
        Self {
            builds,
            accepted,
            me,
            topology: Arc::new(RwLock::new(topology)),
            build_submitted: Arc::new(Notify::new()),
            resize: std::sync::OnceLock::new(),
            absence: std::sync::OnceLock::new(),
            view_now: std::sync::OnceLock::new(),
            connections: std::sync::OnceLock::new(),
            loads: std::sync::OnceLock::new(),
            peer_mesh: std::sync::OnceLock::new(),
            fabric_recovery: std::sync::OnceLock::new(),
            seats: std::sync::OnceLock::new(),
            contexts: Arc::new(crate::build_claim::AttemptContexts::in_memory()),
            claims: Arc::new(crate::storage::MemoryMeshStorage::default()),
            time,
            admission: Arc::new(tokio::sync::Mutex::new(())),
            first_ready: crate::fabric_state::FirstReady::new(),
            ready_rows: Arc::new(crate::fabric_storage::MemoryFabricStorage::new()),
            departure: std::sync::OnceLock::new(),
        }
    }

    /// This control plane reading whether the initial fabric first reached ready-for-traffic from
    /// `first_ready`, the handle its admin's fabric state sets, and `ready_rows`, the fabric store, where
    /// the durable fact is.
    pub fn with_first_ready(mut self, first_ready: crate::fabric_state::FirstReady, ready_rows: Arc<dyn crate::fabric_storage::FabricStorage>) -> Self {
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
    pub fn with_claims(mut self, claims: Arc<dyn crate::storage::MeshStorage>) -> Self {
        self.claims = claims;
        self
    }

    /// This admin's currently held `path.name`: what its Build state or its view names its birth,
    /// falling back to `self.me` before its birth is in the view.
    pub async fn held_name(&self) -> PathName {
        if let Some(held) = self.builds.held_name() {
            return held;
        }
        self.topology.read().await.held_name_of(&self.me, None)
    }

    /// Only the current fabric-primary changes topology or opens attempts.
    async fn require_fabric_primary(&self, route: &'static str) -> Result<(), Refusal> {
        let me = self.held_name().await;
        let primary = self.topology.read().await.fabric_primary().map(|n| n.name.clone());
        if primary.as_ref() != Some(&me) {
            tracing::info_span!("rdm.node_admin.build.reject.via-not-authority", route, node = %me, fabric_primary = %primary.as_ref().map(|p| p.to_string()).unwrap_or_default())
                .in_scope(|| tracing::info!("topology is changed only by the fabric-primary"));
            return Err(Refusal::NotPrimary(primary.map(|p| p.to_string())));
        }
        Ok(())
    }

    /// The one admission rule before the initial fabric is ready-for-traffic (states.md, "The initial
    /// fabric before ready-for-traffic"; R-PB1, R-PC1): `workflow` moves a seat or writes a caller's
    /// row, so it is refused `fabric-not-ready` until the fabric has first reached ready-for-traffic,
    /// naming the workflow and the fabric's status. It runs before anything is read, claimed or
    /// written: a refused workflow opens no attempt and accepts no Build. Decided on the durable fact
    /// (states.md): what this admin already knows, else a point read of the fabric's ready-for-traffic
    /// fact on the fabric store, which the fabric state it hears by gossip can lag. A read that fails is
    /// refused by name.
    async fn admit_once_ready(&self, route: &'static str, workflow: String, outer: &tracing::Span) -> Result<(), Refusal> {
        let me = self.held_name().await;
        let (status, fabric_id) = {
            let t = self.topology.read().await;
            (t.fabric.status, t.fabric.id.clone())
        };
        match self.first_ready.reached_on_root(&*self.ready_rows, &fabric_id, status).await {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(e) => {
                let detail = format!("{me}: {workflow} cannot be admitted: whether fabric {fabric_id} first reached ready-for-traffic could not be read from the fabric store (it is {}): {e}", status.name());
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
        let (admission, current) = self.current_read().await?;
        settled(&current)?;
        Ok((admission, current))
    }

    /// The current Build (R-BI3), in progress or not, with the admission taken first and returned with
    /// it: what fabric.delete, the one Build that may interrupt the Build in progress (build.md §1),
    /// is compiled against.
    async fn current_read(&self) -> Result<(tokio::sync::OwnedMutexGuard<()>, crate::build_state::BuildProjection), Refusal> {
        let admission = self.admission.clone().lock_owned().await;
        // "Never written" and "could not be read" are distinct refusals: the store's own error is named.
        let current = self.builds.current().await.map_err(Refusal::State)?.ok_or_else(|| {
            Refusal::Unavailable("this admin holds no current Build yet (no Build in its builds store is in progress or completed); submit through a Ready node-admin".into())
        })?;
        Ok((admission, current))
    }

    /// The fabric.delete in force, which a second fabric.delete answers and accepts nothing beside
    /// (build.md §1: "No other Build ends another"; a successor reads the delete as the current Build
    /// and continues it): the newest Build accepted, when it is a fabric.delete, in progress, complete,
    /// or failed with its intent retained. Its attempt is the one its drive covers now; `true` when
    /// that attempt follows a failed one.
    pub async fn fabric_delete_in_force(&self) -> Result<Option<(Opened, bool)>, Refusal> {
        let in_force = crate::build_state::fabric_delete_in_force(&*self.builds).await.map_err(Refusal::State)?;
        Ok(in_force.map(|(p, failed)| (Opened { build_id: p.build_id.clone(), attempt: p.attempt_to_drive(), mesh: None }, failed)))
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
                let me = self.held_name().await;
                let row = self.claims.in_flight(mesh).await.map_err(|e| {
                    Refusal::Unavailable(format!("{me}: mesh {mesh} is held by an attempt of {} that no one claimed, and the mesh's in-flight row naming its holder could not be read: {e}", held.current_build_id))
                })?;
                held.held_by = row.filter(|r| r.build_id == held.current_build_id && r.lane.as_deref() == Some(mesh)).map(|r| r.claimant);
            }
            tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", build_id = %held.current_build_id.log(), mesh, held_by = %held.held_by.as_deref().unwrap_or(""))
                .in_scope(|| tracing::info!("an attempt holds the mesh: one attempt in flight per mesh"));
            return Err(Refusal::BuildInProgress(held));
        }
        Ok((admission, current, None))
    }

    /// The exact birth this admin's view names at `claimant` when its departure is proven (build.md:
    /// "after exact departure proof"; silence is never departure, R-BI4): the provider reads its exact
    /// runtime exited. `None` while it is not, while the view names no birth there, or while no
    /// departure proof is held.
    async fn departure_of(&self, claimant: &PathName) -> Result<Option<crate::model::Node>, Refusal> {
        let Some(n) = self.topology.read().await.node(claimant).cloned().filter(|n| n.incarnation_id.is_some()) else { return Ok(None) };
        let Some(proof) = self.departure.get() else { return Ok(None) };
        Ok(proof.proven(claimant).await.ok().map(|()| n))
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
        if fabric_holds {
            return self.end_fabric_attempt_of_a_lost_claimant(current, mesh).await;
        }
        let Some(lane) = current
            .lanes
            .get(mesh)
            .filter(|l| l.state == BuildState::Running)
        else {
            return Ok(false);
        };
        // The admin that runs the attempt departed: its executor (the claimant too, for an attempt its
        // mesh-primary claimed at its own door; the fabric-primary decided the claim of a reported one).
        let Some(executor) = lane
            .executor
            .as_deref()
            .or(lane.claimant.as_deref())
            .and_then(|c| c.parse::<PathName>().ok())
        else {
            return Ok(false);
        };
        let claimant = lane
            .claimant
            .clone()
            .unwrap_or_else(|| executor.to_string());
        let me = self.held_name().await;
        if executor == me || executor == self.me {
            return Ok(false);
        }
        let Some(n) = self.departure_of(&executor).await? else {
            return Ok(false);
        };
        let attempt = lane.attempt;
        let incarnation = n
            .incarnation_id
            .as_ref()
            .map(|i| i.as_str().to_string())
            .unwrap_or_default();
        let reason = format!("{executor} ({} at {incarnation}) departed with attempt {attempt} of mesh {mesh}'s lane unfinished: its departure is proven; {me} takes the lane over", n.node_id);
        let span = tracing::info_span!("rdm.node_admin.build.update.via-departed-claimant", build_id = %current.build_id.log(), lane = mesh, attempt, claimant = %claimant, executor = %executor, node_id = %n.node_id, incarnation_id = %incarnation, node = %me, outcome = tracing::field::Empty);
        let verdict = crate::build_state::BuildAttemptReceipt {
            mesh: Some(mesh.to_string()),
            build_id: current.build_id.clone(),
            attempt,
            outcome: crate::build_state::AttemptOutcome::Failed {
                reason,
                code: rdm_core::outcome::Code::FailedPrecondition,
            },
        };
        if let Err(e) = self.builds.append_attempt_receipt(&verdict).await {
            span.record("outcome", "verdict-unwritten");
            span.in_scope(|| tracing::info!(error = %e, "the departed claimant's attempt could not be ended: the mesh stays held by it"));
            return Err(Refusal::State(e));
        }
        let held = crate::mesh_claims::InFlight {
            build_id: current.build_id.clone(),
            lane: Some(mesh.to_string()),
            attempt,
            claimant,
            claimant_node: None,
        };
        if let Err(e) = self.claims.release_in_flight(mesh, &held).await {
            span.record("outcome", "in-flight-unreleased");
            span.in_scope(|| tracing::info!(error = %e, "the departed claimant's attempt is ended and mesh's in-flight row could not be released: the mesh stays held by it"));
            return Err(Refusal::Unavailable(format!("{me}: attempt {attempt} of mesh {mesh}'s lane of {} is ended on {executor}'s departure and mesh {mesh}'s in-flight row could not be released: {e}", current.build_id)));
        }
        span.record("outcome", "ended");
        span.in_scope(|| tracing::info!("the mesh's drift attempt ends with its departed claimant, and its in-flight row is released"));
        Ok(true)
    }

    /// End the fabric lane's attempt of `current` acting in `mesh` whose claimant departed with it unfinished,
    /// on that departure's exact proof: the next one in the same order starts the workflow again, resuming the
    /// attempt from its recorded progress (fabric-recovery.md, Q122; build.md "after exact departure proof").
    async fn end_fabric_attempt_of_a_lost_claimant(&self, current: &crate::build_state::BuildProjection, mesh: &str) -> Result<bool, Refusal> {
        let Some(claimant) = current.claimant.as_deref().and_then(|c| c.parse::<PathName>().ok()) else { return Ok(false) };
        let me = self.held_name().await;
        if claimant == me || claimant == self.me {
            return Ok(false);
        }
        let Some(n) = self.departure_of(&claimant).await? else { return Ok(false) };
        let attempt = current.attempt;
        let incarnation = n.incarnation_id.as_ref().map(|i| i.as_str().to_string()).unwrap_or_default();
        let reason = format!("{claimant} ({} at {incarnation}) departed with attempt {attempt} of the fabric lane unfinished, acting in mesh {mesh}: its departure is proven; {me} starts the workflow again", n.node_id);
        let span = tracing::info_span!("rdm.node_admin.build.update.via-departed-claimant", build_id = %current.build_id.log(), lane = "fabric", attempt, claimant = %claimant, node_id = %n.node_id, incarnation_id = %incarnation, node = %me, outcome = tracing::field::Empty);
        let verdict = crate::build_state::BuildAttemptReceipt { mesh: None, build_id: current.build_id.clone(), attempt, outcome: crate::build_state::AttemptOutcome::Failed { reason, code: rdm_core::outcome::Code::FailedPrecondition } };
        if let Err(e) = self.builds.append_attempt_receipt(&verdict).await {
            span.record("outcome", "verdict-unwritten");
            span.in_scope(|| tracing::info!(error = %e, "the departed claimant's fabric-lane attempt could not be ended: the mesh stays held by it"));
            return Err(Refusal::State(e));
        }
        let held = crate::mesh_claims::InFlight { build_id: current.build_id.clone(), lane: None, attempt, claimant: claimant.to_string(), claimant_node: None };
        if let Err(e) = self.claims.release_in_flight(mesh, &held).await {
            span.record("outcome", "in-flight-unreleased");
            span.in_scope(|| tracing::info!(error = %e, "the departed claimant's fabric-lane attempt is ended and the mesh's in-flight row could not be released"));
            return Err(Refusal::Unavailable(format!("{me}: attempt {attempt} of {}'s fabric lane is ended on {claimant}'s departure and mesh {mesh}'s in-flight row could not be released: {e}", current.build_id)));
        }
        span.record("outcome", "ended");
        span.in_scope(|| tracing::info!("the fabric lane's attempt ends with its departed claimant, and the mesh's in-flight row is released"));
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
    /// the admission before anything is compiled or written (node-identity-and-state.md §6.9 Recheck).
    pub(crate) async fn submit_checked(&self, span: tracing::Span, route: &'static str, change: TopologyChange, collect: Option<&crate::stopped_gc::Candidate>) -> Result<Opened, Refusal> {
        // The span is entered by the future on each poll, never by a guard held across an await
        // (a guard stays entered on the worker while the task is parked). A refusal is reported
        // beside the request span, in the caller's span.
        let outer = tracing::Span::current();
        async {
            self.require_fabric_primary(route).await?;
            // A roll replaces members and moves primaries: refused `fabric-not-ready` until the initial
            // fabric first reached ready-for-traffic, as every recipe is (proposal roll.md step 1).
            if change.is_roll() {
                self.admit_once_ready(route, "roll".into(), &outer).await?;
            }
            // node.delete and mesh.delete write caller state: refused `fabric-not-ready` until the initial
            // fabric first reached ready-for-traffic (states.md "The initial fabric before
            // ready-for-traffic"), before the Build in progress is read, as the roll, a recipe and a node
            // workflow are: the Build that changes the initial fabric is in progress until its ready-for-traffic state is set (Q154).
            let standalone_delete = match &change {
                TopologyChange::RemoveNode { node } => Some(format!("node.delete {node}")),
                TopologyChange::RemoveMesh { mesh } => Some(format!("mesh.delete {mesh}")),
                _ => None,
            };
            if let Some(workflow) = standalone_delete {
                self.admit_once_ready(route, workflow, &outer).await?;
            }
            // fabric.delete is the one Build that may interrupt the Build in progress (build.md §1): it is
            // compiled against the current Build whatever its state, and a fabric.delete in force answers
            // a second one, which accepts nothing.
            let deletes = matches!(change, TopologyChange::DeleteFabric);
            let (_admitted, current) = if deletes { self.current_read().await? } else { self.current_settled().await? };
            if deletes {
                if let Some((resumed, _)) = self.fabric_delete_in_force().await? {
                    span.record("build_id", resumed.build_id.as_str());
                    tracing::info!(build_id = %resumed.build_id.log(), attempt = resumed.attempt, "a fabric.delete is in force: this one answers it and accepts nothing");
                    return Ok(resumed);
                }
            }
            if let TopologyChange::Roll { bindings } = &change {
                let provider = self.topology.read().await.fabric.provider;
                if let Some(reject) = roll_set_refusal(bindings, &current.topology, provider).await {
                    outer.in_scope(|| reject_span(route, &reject));
                    return Err(Refusal::Reject(reject));
                }
            }
            if let Some(candidate) = collect {
                if let Err(reject) = self.recheck_stopped(candidate).await {
                    outer.in_scope(|| reject_span(route, &reject));
                    return Err(Refusal::Reject(reject));
                }
            }
            let observed = self.topology.read().await.clone();
            // node.delete's target (node-delete.md "Operation contract"): a live birth or a stopped one.
            // An unheard birth is its mesh-primary's to recover on node investigation's verdict
            // (node-recovery.md step 3): it is refused `node-not-live` by name.
            if let TopologyChange::RemoveNode { node } = &change {
                if let Some(n) = observed.node(node).filter(|n| !n.status.is_live() && !n.parked) {
                    return Err(not_live(&outer, route, node, n.status, "a delete needs a live node or a stopped one"));
                }
            }
            let topology = match compile(&current.topology, &change, &observed) {
                Ok(t) => t,
                Err(reject) => {
                    outer.in_scope(|| reject_span(route, &reject));
                    return Err(Refusal::Reject(reject));
                }
            };
            let operations = crate::accepted::plan(&topology, &observed, None).operations;
            // A Build whose plan holds the fabric delete, the whole plan, is exempt from the
            // build-in-progress refusal, R-BI7 and R-PC1: it interrupts the Build in progress, and its own
            // mesh.delete and node.delete steps, and only those, run before the first ready-for-traffic
            // (build.md §1; states.md, "The initial fabric before ready-for-traffic").
            let interrupts = operations.iter().any(|op| matches!(op, crate::build::BuildOperation::DeleteFabric { .. }));
            if deletes && !interrupts {
                settled(&current)?;
            }
            // A mesh whose own drift attempt is open is held by it: a topology operation on that mesh
            // is refused, naming the Build, the mesh and the mesh-primary holding it (R-BI7 caveat 3).
            if let Some(held) = operations.iter().filter_map(op_mesh).find_map(|mesh| holding(&current, Some(&mesh))).filter(|_| !interrupts) {
                outer.in_scope(|| {
                    tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", route, build_id = %held.current_build_id.log(), mesh = %held.mesh.as_deref().unwrap_or(""), held_by = %held.held_by.as_deref().unwrap_or(""))
                        .in_scope(|| tracing::info!("a topology operation acts in a mesh its own drift attempt holds: one attempt in flight per mesh"))
                });
                return Err(Refusal::BuildInProgress(held));
            }
            // A Build that deletes a node or a mesh writes caller state: it waits for the initial fabric
            // to first reach ready-for-traffic (R-PC1).
            if let Some(workflow) = operations.iter().filter(|_| !interrupts).find_map(|op| match op {
                crate::build::BuildOperation::RetireNode { node, exited: None } => Some(format!("node.delete {node}")),
                crate::build::BuildOperation::ShutdownMesh { mesh, .. } => Some(format!("mesh.delete {mesh}")),
                _ => None,
            }) {
                self.admit_once_ready(route, workflow, &outer).await?;
            }
            // A Build whose plan would end a seat holder while no other node of its type in its mesh can
            // take its seat is refused before it exists: node.primary.update has no successor (R-MT1, Q7).
            if let Some(reject) = operations.iter().find_map(|op| {
                let (node, action) = match op {
                    crate::build::BuildOperation::RetireNode { node, .. } => (node, "retire"),
                    crate::build::BuildOperation::RestartNode { node } => (node, "restart"),
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
            let me = self.held_name().await;
            crate::build_state::accept(&*self.builds, &accepted, Some(&current.build_id), &me.to_string(), "accepted").await.map_err(Refusal::State)?;
            tracing::info!(build_id = %build_id.log(), "build accepted");
            // A resize or roll puts the fabric back to pending (states.md, Resize and roll): the climb takes
            // the accepted Build before the door answers it and before the executor is woken.
            if let Some(resize) = self.resize.get() {
                resize().await;
            }
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
        let me = self.held_name().await;
        let opened = AttemptOpened {
            mesh: lane.map(str::to_string),
            build_id: current.build_id.clone(),
            attempt: next_in_lane(current, lane),
            reason,
            action: Some(action),
            opened_by: me.to_string(),
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
                        tracing::info_span!("rdm.node_admin.build.reject.via-attempt-taken", route, build_id = %current.build_id.log(), attempt = opened.attempt, node = %node_name, detail = %e)
                            .in_scope(|| tracing::info!(detail = %e, "attempt refused: another action holds the number"))
                    });
                    Refusal::AttemptTaken(e.to_string())
                }
                e => Refusal::State(e),
            });
        }
        tracing::info!(build_id = %current.build_id.log(), attempt = opened.attempt, lane = lane.unwrap_or("fabric"), "attempt opened");
        self.build_submitted.notify_waiters();
        Ok(Opened { build_id: current.build_id.clone(), attempt: opened.attempt, mesh: opened.mesh })
    }

    /// Open the next attempt of the current Build with a fenced action (a restart, connections delete,
    /// stop, start or delete of one birth). The topology is unchanged; the Build stays the current one.
    pub async fn open_attempt(&self, route: &'static str, reason: AttemptReason, path: PathName, kind: ActionKind) -> Result<Opened, Refusal> {
        let span = tracing::info_span!("rdm.node_admin.build.update.via-rest", route, build_id = tracing::field::Empty, attempt = tracing::field::Empty, node = %path);
        self.open_attempt_in(span, route, reason, path, kind).await
    }

    /// Open the next attempt of the accepted Build as the recipe `steps` (`BuildChange::Recipe`;
    /// build.md §1: "Rafka's replace recipe is submitted as one Build of node primitives";
    /// lifecycles/node-replace.md), under `span`. The topology is unchanged; the Build stays the
    /// current one. Refused by name: before the initial fabric first reaches ready-for-traffic
    /// (`fabric-not-ready`, R-PC1); a recipe with no step, one naming an operation twice, or one
    /// acting on a path that holds no birth and that no earlier step makes
    /// ([`crate::accepted::recipe_action`]); a step in a mesh held by its own drift attempt (R-BI7
    /// caveat 3); a step on a node of a peer mesh this fabric-primary holds unheard (R-D1); a
    /// node.connections.delete, stop or delete of a seat holder while no other node of its type in its
    /// mesh can take its seat (R-MT1, Q7).
    pub async fn open_recipe_in(&self, span: tracing::Span, route: &'static str, steps: Vec<rdm_core::build::RecipeStep>) -> Result<Opened, Refusal> {
        let outer = tracing::Span::current();
        async {
            self.require_fabric_primary(route).await?;
            let summary = steps.iter().map(crate::build::recipe_step_key).collect::<Vec<_>>().join(" ");
            self.admit_once_ready(route, format!("recipe ({summary})"), &outer).await?;
            let (_admitted, current) = self.current_settled().await?;
            for step in &steps {
                let node = step.node();
                if let Some(held) = holding(&current, Some(&node.mesh)) {
                    outer.in_scope(|| {
                        tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", route, build_id = %held.current_build_id.log(), mesh = %node.mesh, held_by = %held.held_by.as_deref().unwrap_or(""), node = %node)
                            .in_scope(|| tracing::info!("the recipe acts in a mesh held by its own drift attempt: one attempt in flight per mesh"))
                    });
                    return Err(Refusal::BuildInProgress(held));
                }
            }
            let t = match self.view_now.get() {
                Some(project) => project().await,
                None => self.topology.read().await.clone(),
            };
            for step in &steps {
                let node = step.node();
                if node.mesh != self.me.mesh {
                    if let Some(unheard_ms) = self.peer_mesh.get().and_then(|f| f(&node.mesh)).and_then(|p| p.unheard_ms) {
                        let reject = BuildReject::UnheardMesh { node: node.to_string(), mesh: node.mesh.clone(), unheard_ms };
                        outer.in_scope(|| reject_span(route, &reject));
                        return Err(Refusal::Reject(reject));
                    }
                }
                let action = match step {
                    rdm_core::build::RecipeStep::Stop { .. } => Some("stop"),
                    rdm_core::build::RecipeStep::Delete { .. } => Some("delete"),
                    // A seat holder's seats move at its node.connections.delete { All }, the first step of
                    // a replace (node-connections-delete.md step 0; node-replace.md step 1).
                    rdm_core::build::RecipeStep::DeleteConnections { .. } => Some("node.connections.delete"),
                    _ => None,
                };
                if let Some(reject) = action.and_then(|a| last_admin(node, a, &t)) {
                    outer.in_scope(|| reject_span(route, &reject));
                    return Err(Refusal::Reject(reject));
                }
            }
            let action = match crate::accepted::recipe_action(steps, &t) {
                Ok(a) => a,
                Err(reject) => {
                    outer.in_scope(|| reject_span(route, &reject));
                    return Err(Refusal::Reject(reject));
                }
            };
            drop(t);
            self.open_with(&current, None, AttemptReason::Recipe, action, route, &outer, &summary, span.clone()).await
        }
        .instrument(span.clone())
        .await
    }

    /// [`ControlPlane::open_attempt`] under `span`, which names the surface the request came through.
    pub async fn open_attempt_in(&self, span: tracing::Span, route: &'static str, reason: AttemptReason, path: PathName, kind: ActionKind) -> Result<Opened, Refusal> {
        let outer = tracing::Span::current();
        async {
            self.require_fabric_primary(route).await?;
            // A caller's node workflow writes a row, and a node.connections.delete of a seat holder moves its seats: both
            // wait for the initial fabric to first reach ready-for-traffic (R-PB1, R-PC1).
            let holds_a_seat = || async {
                let t = self.topology.read().await;
                t.node(&path).is_some_and(|n| t.is_fabric_primary(&n.node_id) || t.is_mesh_primary(&n.mesh, &n.node_id))
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
                    tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", route, build_id = %held.current_build_id.log(), mesh = %path.mesh, held_by = %held.held_by.as_deref().unwrap_or(""), node = %path)
                        .in_scope(|| tracing::info!("the node's mesh is held by its own drift attempt: one attempt in flight per mesh"))
                });
                return Err(Refusal::BuildInProgress(held));
            }
            // A delete acts only on a `.old` path: the stopped birth a node.name.update renamed there,
            // found in the view (below), which reads a renamed birth by its node id through the node-row
            // id index (R12). Any other action's `.old` name is its accepted path's (node-replace.md: a
            // predecessor renamed `<name>.old` live and stopped there): a stopped `.old` node is started by
            // its row's name (rollback step 2), and its birth is fenced below in the view, which lists it at
            // its row's name.
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
                // A stopped birth at a `.old` path is admitted from the view, which reads a renamed
                // birth by its node id through the node-row id index (R12).
                let t = match self.view_now.get() {
                    Some(project) => project().await,
                    None => self.topology.read().await.clone(),
                };
                let held = t.node(&path).cloned();
                let Some(n) = held else {
                    let me = self.held_name().await;
                    let why = if parked_old {
                        format!("{me}: this fabric-primary's view holds no stopped birth at {path} (no node.name.update renamed one to it, or its Deleted row is written)")
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
                // A start is the transition out of Leaving, and only for a parked process; every other action needs a live node.
                let startable = kind == ActionKind::Start && n.parked;
                // A delete acts on the parked predecessor, or on one whose exit is already proven, or on
                // the last admin of a deleted mesh, live or stopped: its node.delete stops it first.
                let deletable = kind == ActionKind::Delete && (n.parked || n.status == crate::model::NodeStatus::Dead || deleted_mesh);
                let needs_live = !matches!(kind, ActionKind::Start | ActionKind::Delete);
                if (kind == ActionKind::Start && !startable) || (kind == ActionKind::Delete && !deletable) || (needs_live && !n.status.is_live()) {
                    let reason = match kind {
                        ActionKind::Restart => restart_refusal(&current, &path, &n),
                        ActionKind::DeleteConnections => "node.connections.delete needs a live node",
                        ActionKind::Stop => "a stop needs a live node",
                        ActionKind::Start => "a start needs a parked node",
                        ActionKind::Delete => "a delete of a .old path needs the parked predecessor or one whose exit is proven; any other delete through this route is of a deleted mesh's birth",
                    };
                    return Err(not_live(&outer, route, &path, n.status, reason));
                }
                // A node of a peer mesh this fabric-primary holds unheard is not its to restart, stop or delete:
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
    /// Open the termination of the deleted birth `(node_id, incarnation)` at `path`, heard again and
    /// reported (node-investigation.md: "The node that refused it reports it to the fabric-primary, which
    /// terminates its runtime"; Q110, Q128), as the next attempt of the current Build, executed by this
    /// fabric-primary. Only a birth a node workflow deleted is terminated this way (any other is not
    /// known here: `UnknownNode`); one whose termination completed answers `AlreadyApplied`.
    pub async fn open_terminate(&self, span: tracing::Span, route: &'static str, path: PathName, node_id: crate::model::NodeId, incarnation: crate::model::IncarnationId) -> Result<Recovery, Refusal> {
        let outer = tracing::Span::current();
        async {
            self.require_fabric_primary(route).await?;
            if self.builds.recovery_of(&path, &node_id, &incarnation).await.map_err(Refusal::State)?.is_none() {
                let reject = BuildReject::UnknownNode { node: path.to_string() };
                outer.in_scope(|| reject_span(route, &reject));
                return Err(Refusal::Reject(reject));
            }
            let (_admitted, current) = self.current_settled().await?;
            let operation = format!("node.terminate:{path}@{}", incarnation.as_str());
            if let Some(done) = current.steps.iter().find(|s| s.operation == operation && s.step == "Complete" && s.outcome == crate::build_state::StepOutcome::Complete) {
                return Ok(Recovery::AlreadyApplied(Opened { build_id: current.build_id.clone(), attempt: done.attempt, mesh: None }));
            }
            let node_name = path.to_string();
            let action = AttemptAction::Terminate { path, node_id, from_incarnation: incarnation };
            self.open_with(&current, None, AttemptReason::Delete, action, route, &outer, &node_name, span.clone()).await.map(Recovery::Opened)
        }
        .instrument(span.clone())
        .await
    }

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
    /// that exact birth (its recovery, whose last operation `node.create:<path>` completed, or a
    /// `node.delete:<path>` / `delete-node:<path>` the birth exited inside), a submit answers
    /// [`Recovery::AlreadyApplied`] naming that workflow's attempt, whoever asks, and nothing is
    /// opened. A node-admin is node-recovered only while its mesh holds another live admin to recover
    /// it (mesh-recovery.md:8-9); otherwise it is mesh recovery. The claimant is the member's
    /// recoverer, its mesh's primary node-admin as this admin's view holds the seat: any other admin
    /// is refused [`Refusal::NotClaimant`] naming who may claim. An attempt holding the mesh (a
    /// topology operation's claimed attempt there, or the mesh's own drift attempt) answers
    /// `BuildInProgress` naming the Build, the mesh and its claimant; an attempt in another mesh
    /// refuses nothing. Then the mesh's in-flight row is taken on the store root before any fact is
    /// written, the attempt is opened in the mesh's lane (the fabric lane for an unheard mesh-primary, which the
    /// fabric-primary recovers), and its claim is decided by this admin. The
    /// lane's attempt already opened with this very recovery and claimed by no one (its opener lost
    /// between its open and its claim) is claimed as it stands: never opened again, and nothing ends it.
    pub async fn open_recovery(&self, route: &'static str, path: PathName, node_id: crate::model::NodeId, incarnation: crate::model::IncarnationId) -> Result<Recovery, Refusal> {
        let span = tracing::info_span!("rdm.node_admin.build.update.via-recovery", route, build_id = tracing::field::Empty, attempt = tracing::field::Empty, mesh = %path.mesh, node = %path, node_id = %node_id, incarnation_id = %incarnation.as_str(), open_attempt = tracing::field::Empty, outcome = tracing::field::Empty);
        let outer = tracing::Span::current();
        let r = async {
            // Keyed on the exited birth across the Build log: a birth a node workflow already
            // deleted (its recovery, or a delete or retire it exited inside: node-recovery.md
            // "Failure and receipts") is answered as applied by that workflow's attempt, whatever
            // Build is accepted or in flight now (a later topology Build holds none of its
            // receipts), and nothing is opened.
            if let Some((build_id, mesh, attempt, operation)) = self.builds.recovery_of(&path, &node_id, &incarnation).await.map_err(Refusal::State)? {
                outer.in_scope(|| {
                    tracing::info_span!("rdm.node_admin.build.reject.via-recovery-already-applied", route, build_id = %build_id.log(), lane = mesh.as_deref().unwrap_or("fabric"), attempt, operation = %operation, node = %path, node_id = %node_id, incarnation_id = %incarnation.as_str())
                        .in_scope(|| tracing::info!(operation = %operation, "the exited birth was deleted by {operation} and its workflow completed at this attempt: its exit belongs to that workflow, nothing is opened"))
                });
                return Ok(Recovery::AlreadyApplied(Opened { build_id, attempt, mesh }));
            }
            // Under an in-force fabric.delete nothing is recovered for being absent and nothing is
            // re-created (fabric-delete.md "Failure"; fabric-recovery.md step 4, Q155): the delete's own
            // node.delete of the birth is its end. Refused naming the delete before anything is read or
            // written, its attempts included.
            if let Some((delete, _)) = crate::build_state::fabric_delete_in_force(&*self.builds).await.map_err(Refusal::State)? {
                outer.in_scope(|| {
                    tracing::info_span!("rdm.node_admin.build.reject.via-fabric-delete-in-force", route, build_id = %delete.build_id.log(), node = %path, node_id = %node_id, incarnation_id = %incarnation.as_str())
                        .in_scope(|| tracing::info!("a fabric.delete is in force: nothing is recovered and nothing is re-created"))
                });
                return Err(Refusal::BuildInProgress(InProgress { current_build_id: delete.build_id.clone(), mesh: Some(path.mesh.clone()), held_by: None }));
            }
            let me = self.held_name().await;
            let t = self.topology.read().await.clone();
            // A node-admin is node-recovered by its mesh's live primary (mesh-recovery.md:8-9); one
            // with no other live admin in its mesh is that mesh's lost mesh-primary: mesh recovery.
            // An unheard mesh-primary is recovered by the fabric-primary (node-recovery.md sequence
            // step 1: "for an unheard mesh-primary, the fabric-primary opens it").
            // Fabric recovery's re-create of the node-admin at the fabric-primary's path.name is its
            // executor's, in the fabric lane (fabric-recovery.md step 3, Q122): nobody is fabric-primary.
            let fabric_recovery = self.fabric_recovery.get().and_then(|read| read(&t)).filter(|fr| path.kind == NodeKind::NodeAdmin && fr.target.node_id == node_id && fr.target.incarnation == incarnation);
            // Unheard as the primary: the view still names it, or the election has replaced it and the
            // fabric-primary runs its recovery "after the election has named the new mesh-primary"
            // (node-recovery.md step 2).
            let replaced = self.seats.get().is_some_and(|seats| seats.replaced(rdm_core::Seat::MeshPrimary, &path.mesh, &incarnation));
            let unheard_mesh_primary = fabric_recovery.is_some()
                || (path.kind == crate::model::NodeKind::NodeAdmin && (replaced || t.node_type_primary(&path.mesh, NodeKind::NodeAdmin).is_some_and(|p| p.name == path && p.node_id == node_id && !p.status.is_live())));
            let recoverer = if let Some(fr) = &fabric_recovery {
                fr.executor.clone()
            } else if unheard_mesh_primary {
                t.fabric_primary().map(|n| n.name.clone())
            } else if path.kind == crate::model::NodeKind::NodeAdmin {
                let Some(recoverer) = crate::executor::node_admin_recoverer(&path, &t) else {
                    let reject = BuildReject::NodeAdminNotNodeRecovered { node: path.to_string() };
                    outer.in_scope(|| reject_span(route, &reject));
                    return Err(Refusal::Reject(reject));
                };
                Some(recoverer)
            } else {
                t.node_type_primary(&path.mesh, NodeKind::NodeAdmin).map(|n| n.name.clone())
            };
            // A mesh-primary reports its drift up as build.create { Change(Recover) }: the fabric-primary
            // opens the attempt at `(build_id, mesh, attempt)` and the mesh-primary claims it through
            // BuildClaim (node-recovery.md "Execution", Q118). Any other admin that is not the recoverer
            // opens nothing.
            let reported = recoverer.as_ref().is_some_and(|r| *r != me) && route == "build.create" && t.fabric_primary().is_some_and(|n| n.name == me);
            if recoverer.as_ref() != Some(&me) && !reported {
                let fabric_primary = t.fabric_primary().map(|n| n.name.to_string());
                let mesh_primary = recoverer.map(|n| n.to_string());
                outer.in_scope(|| {
                    tracing::info_span!("rdm.node_admin.build.reject.via-not-claimant", route, node = %me, mesh = %path.mesh, fabric_primary = %fabric_primary.as_deref().unwrap_or(""), mesh_primary = %mesh_primary.as_deref().unwrap_or(""), member = %path)
                        .in_scope(|| tracing::info!("a drift attempt is claimed by its mesh's primary: this admin is not it in its view"))
                });
                return Err(Refusal::NotClaimant { mesh: path.mesh.clone(), fabric_primary, mesh_primary });
            }
            let action = AttemptAction::Recover { path: path.clone(), node_id: node_id.clone(), from_incarnation: incarnation.clone(), backbone: route == "fabric-recovery-backbone" };
            // A reported recovery whose attempt still runs under the reporter is reattached, never
            // claimed again (build.md: "A live run is reattached, never re-claimed"): the fabric-primary
            // lost its stream to the run, and the reporter's next report finds it.
            if let Some(claimant) = recoverer.as_ref().filter(|_| reported) {
                let current = self.builds.current().await.map_err(Refusal::State)?;
                let live = current.as_ref().and_then(|c| c.lanes.get(&path.mesh).filter(|l| l.state == BuildState::Running && l.action.as_ref() == Some(&action) && l.executor.as_deref() == Some(claimant.to_string().as_str())).map(|l| Opened { build_id: c.build_id.clone(), attempt: l.attempt, mesh: Some(path.mesh.clone()) }));
                if let Some(opened) = live {
                    span.record("open_attempt", "reattached");
                    return Ok(Recovery::Reported { opened, claimant: claimant.clone() });
                }
            }
            let (_admitted, current, open) = self.mesh_settled(&path.mesh, &action).await?;
            if !current.topology.contains(&path) {
                let reject = BuildReject::UnknownNode { node: path.to_string() };
                outer.in_scope(|| reject_span(route, &reject));
                return Err(Refusal::Reject(reject));
            }
            // The mesh's in-flight row decides, on the store root: whichever claims first on the
            // mesh's rows wins (R-BI7 caveat 3). The row names its claim's lane key (Q35), so it is
            // taken in the claim's own writer's turn; a row standing for another attempt that still
            // holds the mesh refuses here, before anything of this attempt is written.
            // An unheard mesh-primary is recovered by the fabric-primary from the fabric lane, as a lost
            // mesh's node-admin is (node-recovery.md, "for an unheard mesh-primary, the fabric-primary
            // opens it"; mesh-investigation.md): the mesh's lane is written by its primary (build.md, the
            // Build journal's writers), which is the unheard birth. The mesh's in-flight row still holds
            // the mesh.
            // Fabric recovery's re-create runs in its target mesh's lane: free while the fabric lane holds an
            // attempt the lost fabric-primary claimed, which the restored fabric-primary resumes.
            let lane = if unheard_mesh_primary && fabric_recovery.is_none() && !reported { None } else { Some(path.mesh.as_str()) };
            // The lane's attempt opened with this very recovery and claimed by no one is claimed as
            // it stands; otherwise the lane's next attempt is opened for it.
            // A fabric-lane attempt opened with this very recovery and claimed by no one (an executor's open whose
            // claim another attempt refused) is claimed as it stands (fabric-recovery.md: "resuming the attempt
            // from its recorded progress").
            let open = if lane.is_some() { open } else { (current.state == BuildState::Pending && current.opened == current.attempt + 1 && current.attempt_actions.get(&current.opened) == Some(&action)).then_some(current.opened) };
            let attempt = open.unwrap_or_else(|| next_in_lane(&current, lane));
            span.record("open_attempt", if open.is_some() { "held-open" } else { "opened-here" });
            let row = crate::mesh_claims::InFlight { build_id: current.build_id.clone(), lane: lane.map(str::to_string), attempt, claimant: me.to_string(), claimant_node: None };
            let standing = match self.claims.in_flight(&path.mesh).await {
                Ok(standing) => standing.filter(|held| !held.same_claim(&row)),
                Err(e) => return Err(Refusal::Unavailable(format!("{me}: the in-flight row of mesh {} could not be read for attempt {attempt} of {}: {e}", path.mesh, current.build_id))),
            };
            if let Some(held) = standing {
                // A row a departed claimant took for a drift attempt that holds no lane in this admin's
                // fold (its run ended and the row was never released, or nothing of it was written
                // before the claimant was lost) is released on that departure's exact proof.
                let departed = match held.claimant.parse::<PathName>() {
                    Ok(c) if held.lane.as_deref() == Some(path.mesh.as_str()) && c != me && c != self.me => self.departure_of(&c).await?.map(|n| (c, n)),
                    _ => None,
                };
                if let Some((claimant, n)) = departed {
                    let release = tracing::info_span!("rdm.node_admin.build.update.via-departed-claimant", build_id = %held.build_id.log(), lane = %path.mesh, attempt = held.attempt, claimant = %claimant, node_id = %n.node_id, incarnation_id = %n.incarnation_id.as_ref().map(|i| i.as_str().to_string()).unwrap_or_default(), node = %me, outcome = tracing::field::Empty);
                    if let Err(e) = self.claims.release_in_flight(&path.mesh, &held).await {
                        release.record("outcome", "in-flight-unreleased");
                        return Err(Refusal::Unavailable(format!("{me}: mesh {}'s in-flight row names attempt {} of {} by {claimant}, which departed; the row could not be released: {e}", path.mesh, held.attempt, held.build_id)));
                    }
                    release.record("outcome", "row-released");
                    release.in_scope(|| tracing::info!("the mesh's in-flight row of a departed claimant's attempt that holds no lane is released"));
                } else if self.builds.in_flight_live(&held).await.map_err(Refusal::State)? {
                    return Err(in_progress(route, &outer, &path, held.build_id, Some(held.claimant)));
                }
            }
            let node_name = path.to_string();
            let opened = match open {
                // Its open is on the Build log already, whoever wrote it: nothing is written again.
                Some(attempt) => Opened { build_id: current.build_id.clone(), attempt, mesh: lane.map(str::to_string) },
                None => self.open_with(&current, lane, AttemptReason::Recovery, action, route, &outer, &node_name, span.clone()).await?,
            };
            // A reported drift: opened here, claimed by the mesh-primary through BuildClaim.
            if let Some(claimant) = recoverer.filter(|_| reported) {
                return Ok(Recovery::Reported { opened, claimant });
            }
            let claim = crate::build_state::BuildAttemptClaim { build_id: opened.build_id.clone(), attempt: opened.attempt, executor: me.to_string(), claimant: Some(me.to_string()), mesh: opened.mesh.clone() };
            match crate::build_claim::decide_claim(&me, &*self.builds, &*self.claims, &self.contexts, &claim, &path.mesh).await {
                rdm_core::build_claim::BuildClaimReply::Won { .. } => Ok(Recovery::Opened(opened)),
                // Another attempt took the mesh's row between the read above and this claim: the
                // attempt stays open, claimed by no one, and the next recovery claims it as it stands.
                rdm_core::build_claim::BuildClaimReply::BuildInProgress { current_build_id, held_by, .. } => match BuildId::parse(&current_build_id) {
                    Ok(id) => Err(in_progress(route, &outer, &path, id, held_by)),
                    Err(e) => Err(Refusal::Unavailable(format!("{me}: mesh {}'s in-flight row names a Build id that does not parse ({current_build_id:?}: {e})", path.mesh))),
                },
                other => Err(Refusal::Unavailable(format!("{me}: attempt {} of {} in mesh {}'s lane was opened and its claim not won: {other:?}", opened.attempt, opened.build_id, path.mesh))),
            }
        }
        .instrument(span.clone())
        .await;
        if let Ok(
            Recovery::Opened(o)
            | Recovery::AlreadyApplied(o)
            | Recovery::Reported { opened: o, .. },
        ) = &r
        {
            span.record("build_id", o.build_id.as_str());
            span.record("attempt", o.attempt);
        }
        span.record(
            "outcome",
            match &r {
                Ok(Recovery::Opened(_)) => "admitted",
                Ok(Recovery::Reported { .. }) => "reported",
                Ok(Recovery::AlreadyApplied(_)) => "already-applied",
                Err(Refusal::BuildInProgress(_)) => "build-in-progress",
                Err(Refusal::NotClaimant { .. }) => "not-claimant",
                Err(_) => "refused",
            },
        );
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

/// The refusal of a recovery while another attempt's in-flight row holds `path`'s mesh (R-BI7 caveat
/// 3), spanned under `outer`: the Build and the claimant the row names.
fn in_progress(route: &'static str, outer: &tracing::Span, path: &PathName, current_build_id: BuildId, held_by: Option<String>) -> Refusal {
    let held = InProgress { current_build_id, mesh: Some(path.mesh.clone()), held_by };
    outer.in_scope(|| {
        tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", route, build_id = %held.current_build_id.log(), mesh = %path.mesh, held_by = %held.held_by.as_deref().unwrap_or(""), node = %path)
            .in_scope(|| tracing::info!("the mesh's in-flight row names another attempt: one attempt in flight per mesh"))
    });
    Refusal::BuildInProgress(held)
}

/// What an attempt opened at a node route does to the exact birth it is fenced to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionKind {
    /// `node.restart:<path>`.
    Restart,
    /// `node.connections.delete:<path>`.
    DeleteConnections,
    /// `node.stop:<path>`.
    Stop,
    /// `node.start:<path>`.
    Start,
    /// `delete-node:<path>.old`.
    Delete,
}

impl ActionKind {
    /// The action's name: what a refusal says the request would do to the node.
    pub fn name(self) -> &'static str {
        match self {
            Self::Restart => "restart",
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
/// pre-check runs (`election::commanded_successor`), so nothing is admitted and no attempt opens. A
/// node of any other type that holds its cohort's seat is refused [`BuildReject::NoSecondary`] when no
/// other node of its type in its mesh can take the seat (`election::commanded_node_type_successor`, Q7).
fn last_admin(node: &PathName, action: &str, t: &Topology) -> Option<BuildReject> {
    let target = t.node(node)?;
    // The holder of any other cohort's seat: its mesh-primary's node.primary.update needs another node
    // of its type (Q7); the delete of a type's only node in its mesh skips the update, whether the node
    // leaves the topology (a removal) or is re-created at the same path.name (node recovery, a roll's
    // replacement, the replace recipe's Delete step): R5, node-primary-update.md "No-successor cases".
    if target.kind != crate::model::NodeKind::NodeAdmin {
        let holds = t.node_type_primary(&node.mesh, target.kind).is_some_and(|p| p.node_id == target.node_id);
        let only_of_cohort = t.nodes_of_type(&node.mesh, target.kind).all(|n| n.node_id == target.node_id);
        if !holds || (matches!(action, "retire" | "delete") && only_of_cohort) {
            return None;
        }
        let nodes: Vec<crate::model::Node> = t.members().cloned().collect();
        let refusal = crate::election::commanded_node_type_successor(&nodes, &crate::election::Incumbency::default(), target).err()?;
        return Some(BuildReject::NoSecondary { node: node.to_string(), mesh: node.mesh.clone(), action: action.to_string(), refusal });
    }
    let fabric_seat = t.fabric_primary().is_some_and(|fp| fp.node_id == target.node_id);
    let mesh_seat = t.node_type_primary(&node.mesh, crate::model::NodeKind::NodeAdmin).is_some_and(|p| p.node_id == target.node_id);
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
        if !fabric_seat || (!t.meshes.is_empty() && t.meshes.iter().all(|m| m.status == crate::model::ScopeStatus::Dead)) {
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

/// A roll's set refused by name (proposal roll.md step 1), or `None` when it is accepted: validated as
/// the bootstrap admin validates its own at start (`BindingSet::validate`, every file hashed, on the
/// blocking pool): every launch id `topology` uses declared and bound, `node_admin` always; every file
/// present and hashing to its sha; every binding checked against the fabric's `provider` (a process
/// fabric binds no image, a container fabric binds its one image). The set is held to its own
/// candidate: a roll names the build it rolls to.
async fn roll_set_refusal(set: &rdm_core::binding::BindingSet, topology: &crate::accepted::FabricTopology, provider: crate::model::ProviderKind) -> Option<BuildReject> {
    let required: Vec<&'static str> = topology.meshes.values().flat_map(|m| m.nodes.iter().map(|p| p.kind.name())).collect::<std::collections::BTreeSet<_>>().into_iter().collect();
    let set = set.clone();
    let image = crate::admin::provider_image(provider);
    let checked = crate::blocking::blocking_io(move || {
        let candidate = set.candidate.sha.clone();
        set.validate(&rdm_core::binding::Expect { candidate_sha: &candidate, required: &required, provider_image: image }).map(|_| ())
    })
    .await;
    checked.err().map(|e| BuildReject::BindingsRefused { defect: e.name().to_string(), detail: e.to_string() })
}

/// Why a fresh `node.restart` of the birth `n` at `path`, which is not live, is refused `node-not-live`
/// (R-PV1, the fresh-request versus resumption rule): a birth whose own restart is open in `current`
/// is resumed by the caller's resubmit of that Build (build.create with its existing build_id), which
/// continues from its receipts; a stopped (parked) birth is started by node.start; a birth in any
/// other status needs to be live again.
fn restart_refusal(current: &crate::build_state::BuildProjection, path: &PathName, n: &crate::model::Node) -> &'static str {
    if n.incarnation_id.as_ref().is_some_and(|inc| open_restart(current, path, inc).is_some()) {
        "a restart of this birth is open: the caller resubmits its Build, which resumes it from its receipts"
    } else if n.parked {
        "a fresh restart of a stopped node is refused: node.start starts it"
    } else {
        "a restart needs a live node"
    }
}

/// The attempt of `current` whose `node.restart` of the exact birth `incarnation` at `path` is open:
/// the latest attempt that acted on `path` carried that restart, and no `Complete` of
/// `node.restart:<path>` is receipted in it (node-restart.md: "A failed or uncertain start does not
/// produce node.restarted"; gossip.md: NodeRestarting stays until node.started). `None`: no restart of
/// that birth is open.
pub(crate) fn open_restart(current: &crate::build_state::BuildProjection, path: &PathName, incarnation: &crate::model::IncarnationId) -> Option<u32> {
    let acts_on = |a: &AttemptAction| match a {
        AttemptAction::Restart { path: p, .. } | AttemptAction::DeleteConnections { path: p, .. } | AttemptAction::Stop { path: p, .. } | AttemptAction::Start { path: p, .. } | AttemptAction::Recover { path: p, .. } | AttemptAction::Delete { path: p, .. } => p == path,
        AttemptAction::Recipe { steps, .. } => steps.iter().any(|s| s.node() == path),
        AttemptAction::Terminate { .. } => false,
    };
    let (attempt, action) = current.attempt_actions.iter().rev().find(|(_, a)| acts_on(a))?;
    let AttemptAction::Restart { from_incarnation, .. } = action else { return None };
    let operation = format!("node.restart:{path}");
    let completed = current.steps.iter().any(|s| s.attempt == *attempt && s.mesh.is_none() && s.operation == operation && s.step == "Complete" && s.outcome == crate::build_state::StepOutcome::Complete);
    (from_incarnation == incarnation && !completed).then_some(*attempt)
}

/// The restart/stop/start/delete refusal `node-not-live`, spanned beside the request with the node, the
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
        "no-secondary",
        "node-not-stopped",
        "referenced-by-open-build",
        "empty-recipe",
        "recipe-step-repeated",
        "recipe-node-unknown",
        "bindings-refused"
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
    NotPrimary(Option<String>),
    /// The attempt number this request computed was opened first by another action.
    AttemptTaken(String),
    /// The admin cannot decide the request yet (it holds no Build).
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

/// The refusal of a second topology change while `current` is in progress (build.md §1, R-BI7): named
/// by the mesh its claimed attempt acts in and its claimant, when claimed.
fn settled(current: &crate::build_state::BuildProjection) -> Result<(), Refusal> {
    if !matches!(current.state, BuildState::Pending | BuildState::Running) {
        return Ok(());
    }
    let held = holding(current, None).unwrap_or_else(|| InProgress { current_build_id: current.build_id.clone(), mesh: None, held_by: None });
    tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", build_id = %current.build_id.log(), state = ?current.state, mesh = %held.mesh.as_deref().unwrap_or(""), held_by = %held.held_by.as_deref().unwrap_or(""))
        .in_scope(|| tracing::info!("another Build is reconciling: one topology Build in flight"));
    Err(Refusal::BuildInProgress(held))
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
    /// A mesh's drift reported by its mesh-primary (Q118): the attempt opened here in the mesh's
    /// lane, which `claimant` claims through BuildClaim and this fabric-primary runs naming the mesh.
    Reported {
        /// The attempt.
        opened: Opened,
        /// The mesh-primary that reported it: the attempt's claimant and executor.
        claimant: PathName,
    },
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

    /// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1: "One
    /// primitive moves the primary of any (mesh, node_type) cohort ... A mesh-primary commands the
    /// primaries of its mesh's other cohorts (Q3)"; step 1: "The holder is commanded from above, as a
    /// step of the Build that stops, deletes, restarts, replaces ..."; Q7, ruled: "`node.primary.update`
    /// for a type with no other node of that type in the mesh is refused by name, `cannot transfer: no
    /// secondary of type <node_type> in mesh <mesh>` ... the commanding workflow fails with that reason";
    /// "a mesh's sole broker ... cannot be stopped or restarted until a secondary is ReadyForTraffic"):
    /// mesh1.rpc.1 holds mesh1's rpc seat with mesh1.rpc.2 beside it, and mesh2.rpc.1 is mesh2's sole
    /// rpc node. The stop of mesh1.rpc.1 moves its seat first (`executor::seat_move` names it) and no
    /// admin executes the stop until it has; the stop of mesh1.rpc.2, which holds no seat, moves nothing
    /// and its mesh's primary executes it. The Build door admits a stop of mesh1.rpc.1 and refuses a
    /// stop or restart of mesh2.rpc.1 by name with Q7's text; its delete, by the node.delete route or the
    /// replace recipe's Delete step, skips the update as its removal does (R5).
    /// Must NOT happen: a cohort holder stopped with its seat unmoved, or the sole node of its type in a
    /// mesh stopped or restarted while its mesh has other nodes.
    /// CONTRACT (R-PV1, the fresh-request versus resumption rule; node-rpc-envelope.md build.create:
    /// "existing build_id for resubmission"): attempt 2 of a Build carried the restart of mesh1.rpc.2's
    /// birth and failed after its stop completed (StopNode and AwaitNodeLeft Complete, StartNode
    /// Failed). That restart is open: a fresh node.restart of the parked birth is refused naming the
    /// resubmit. The resubmit's attempt 3 carries the same restart; once it receipts `Complete` of
    /// `node.restart:mesh1.rpc.2` nothing is open, and a fresh restart of the birth parked again (by a
    /// node.stop) is refused naming node.start. Must NOT happen: a restart of another birth at the
    /// path read as open, or an open restart read after a later action on the path.
    #[test]
    fn a_restart_open_on_a_birth_is_named_on_its_fresh_restarts_refusal_and_a_stopped_birth_names_node_start() {
        use crate::build_state::{fold, AttemptOutcome, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildStepReceipt, StepOutcome};
        let path: PathName = "mesh1.rpc.2".parse().unwrap();
        let inc = crate::model::IncarnationId::mint();
        let build_id = BuildId::mint();
        let accepted = BuildFact::Accepted(BuildAccepted { build_id: build_id.clone(), topology: crate::accepted::FabricTopology::root("fabric1", "mesh1", MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 1 });
        let opened = |attempt: u32, action: AttemptAction| BuildFact::Opened(AttemptOpened { build_id: build_id.clone(), attempt, reason: AttemptReason::Restart, action: Some(action), opened_by: "mesh1.admin.1".into(), opened_at_ms: 2, mesh: None });
        let claim = |attempt: u32| BuildFact::Claim(BuildAttemptClaim { build_id: build_id.clone(), attempt, executor: "mesh1.admin.1".into(), claimant: Some("mesh1.admin.1".into()), mesh: None });
        let step = |attempt: u32, operation: &str, step: &str, outcome: StepOutcome| BuildFact::Step(BuildStepReceipt { build_id: build_id.clone(), attempt, operation: operation.into(), step: step.into(), outcome, output: None, executor: Some("mesh1.admin.1".into()), mesh: None });
        let ended = |attempt: u32, outcome: AttemptOutcome| BuildFact::Attempt(BuildAttemptReceipt { build_id: build_id.clone(), attempt, outcome, mesh: None });
        let restart_op = format!("node.restart:{path}");
        let restart = AttemptAction::Restart { path: path.clone(), from_incarnation: inc.clone() };
        let mut facts = vec![accepted, claim(1), ended(1, AttemptOutcome::Converged), opened(2, restart.clone()), claim(2)];
        for (s, outcome) in [("NodeRestarting", StepOutcome::Complete), ("StopNode", StepOutcome::Complete), ("AwaitNodeLeft", StepOutcome::Complete), ("StartNode", StepOutcome::failed("start-node was not answered Started"))] {
            facts.push(step(2, &restart_op, s, outcome));
        }
        facts.push(ended(2, AttemptOutcome::Failed { reason: "StartNode".into() , code: rdm_core::outcome::Code::FailedPrecondition }));
        let current = |facts: &[BuildFact]| fold(facts).remove(&build_id).expect("the Build folds");
        let mut parked = Node::allocated(path.clone());
        parked.incarnation_id = Some(inc.clone());
        parked.status = NodeStatus::Leaving;
        parked.parked = true;
        assert_eq!(open_restart(&current(&facts), &path, &inc), Some(2), "the failed restart is open");
        assert_eq!(open_restart(&current(&facts), &path, &crate::model::IncarnationId::mint()), None, "a restart of another birth at the path is not this one's");
        assert_eq!(restart_refusal(&current(&facts), &path, &parked), "a restart of this birth is open: the caller resubmits its Build, which resumes it from its receipts");
        // The resubmit claims attempt 3, which carries the restart, and completes it.
        facts.push(claim(3));
        assert_eq!(open_restart(&current(&facts), &path, &inc), Some(3), "the resubmit's attempt carries the open restart");
        for s in ["NodeRestarting", "StartNode", "WaitForNodeReady", "Complete"] {
            facts.push(step(3, &restart_op, s, StepOutcome::Complete));
        }
        facts.push(ended(3, AttemptOutcome::Converged));
        assert_eq!(open_restart(&current(&facts), &path, &inc), None, "a completed restart is not open");
        // A node.stop of the birth after it: a fresh restart of the stopped birth names node.start.
        facts.push(opened(4, AttemptAction::Stop { path: path.clone(), from_incarnation: inc.clone() }));
        facts.push(claim(4));
        assert_eq!(open_restart(&current(&facts), &path, &inc), None, "a later action on the path closes nothing open");
        assert_eq!(restart_refusal(&current(&facts), &path, &parked), "a fresh restart of a stopped node is refused: node.start starts it");
        let mut silent = parked.clone();
        silent.parked = false;
        silent.status = NodeStatus::PendingReconnect;
        assert_eq!(restart_refusal(&current(&facts), &path, &silent), "a restart needs a live node");
    }

    #[test]
    fn an_rpc_node_type_primary_moves_before_its_stop_and_the_sole_node_of_its_type_is_refused_by_name() {
        let t = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
            meshes: vec![
                Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic },
                Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic },
            ],
            nodes: vec![node("mesh1.admin.1", true, true), node("mesh1.rpc.1", true, false), node("mesh1.rpc.2", false, false), node("mesh2.admin.1", true, false), node("mesh2.rpc.1", true, false)],
        };
        let stop = |path: &str| crate::build::BuildOperation::StopNode { node: path.parse().unwrap(), from_incarnation: t.node(&path.parse().unwrap()).unwrap().incarnation_id.clone().unwrap() };
        let holder = stop("mesh1.rpc.1");
        assert_eq!(crate::executor::seat_move(&holder, &t).map(|m| m.holder.name.to_string()), Some("mesh1.rpc.1".to_string()), "the rpc seat moves before its holder's stop");
        assert_eq!(crate::executor::executor_for(&holder, &t), None, "no admin executes the holder's stop until its seat moved");
        let other = stop("mesh1.rpc.2");
        assert_eq!(crate::executor::seat_move(&other, &t), None, "a node that holds no seat moves none");
        assert_eq!(crate::executor::executor_for(&other, &t).map(|p| p.to_string()), Some("mesh1.admin.1".to_string()), "its mesh's primary executes it");
        assert!(last_admin(&"mesh1.rpc.1".parse().unwrap(), "stop", &t).is_none(), "mesh1's rpc holder has a secondary: admitted");
        for action in ["stop", "restart"] {
            match last_admin(&"mesh2.rpc.1".parse().unwrap(), action, &t) {
                Some(reject) => assert_eq!((reject.reason(), reject.to_string().as_str()), ("no-secondary", "cannot transfer: no secondary of type rpc-node in mesh mesh2"), "{action}"),
                None => panic!("{action} of mesh2's sole rpc node is refused by name"),
            }
        }
        let retire_sole = crate::build::BuildOperation::RetireNode { node: "mesh2.rpc.1".parse().unwrap(), exited: None };
        assert!(last_admin(&"mesh2.rpc.1".parse().unwrap(), "retire", &t).is_none(), "retiring the sole member of a non-admin cohort skips node.primary.update");
        assert_eq!(crate::executor::seat_move(&retire_sole, &t), None, "retiring the sole member of a non-admin cohort moves no seat");
        // R5 (Luke 2026-10-24, arm (a); node-primary-update.md "No-successor cases": "the delete of a
        // type's only node in its mesh, whether the node leaves the topology (a removal) or is re-created
        // at the same `path.name`"): the replace recipe's Delete step and the node.delete route of a
        // type's only node skip the update as a removal does, refused `no-secondary` at no step.
        assert!(last_admin(&"mesh2.rpc.1".parse().unwrap(), "delete", &t).is_none(), "the delete of a type's only node skips node.primary.update: {:?}", last_admin(&"mesh2.rpc.1".parse().unwrap(), "delete", &t));
        let delete_step = crate::build::BuildOperation::RecipeStep { step: rdm_core::build::RecipeStep::Delete { node: "mesh2.rpc.1".parse().unwrap() }, birth: None };
        assert_eq!(crate::executor::seat_move(&delete_step, &t), None, "the recipe's delete of a type's only node moves no primary");
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
        for action in ["stop", "restart"] {
            assert_eq!(refusal("mesh1.admin.1", action), "cannot transfer: no secondary of type node-admin in mesh mesh1");
        }
        assert!(crate::executor::seat_move(&crate::build::BuildOperation::StopNode { node: only.clone(), from_incarnation: crate::model::IncarnationId::mint() }, &t).is_some_and(|m| m.mesh_seat), "stop of mesh1's only node never skips the seat move");
        assert!(crate::executor::seat_move(&crate::build::BuildOperation::RestartNode { node: only.clone() }, &t).is_some_and(|m| m.mesh_seat), "restart of mesh1's only node never skips the seat move");
        assert_eq!(crate::executor::seat_move(&crate::build::BuildOperation::RetireNode { node: only, exited: None }, &t), None, "the delete of mesh1's only node skips the seat move");
        assert_eq!(refusal("mesh2.admin.1", "delete"), "cannot transfer: no secondary of type node-admin in mesh mesh2");
    }

    /// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C2
    /// "Successor search for the commanded transfer": "2. only when that mesh has none ... the lowest
    /// NodeId among the other ReadyForTraffic mesh-primaries; 3. neither found: refused by name,
    /// `cannot transfer fabric-primary: no other node-admin ReadyForTraffic in mesh <mesh>, no other
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
                Some(BuildReject::LastAdmin { refusal, .. }) => assert_eq!(refusal, "cannot transfer fabric-primary: no other node-admin ReadyForTraffic in mesh mesh2, no other mesh-primary ReadyForTraffic", "{action}"),
                other => panic!("{action}: no other mesh-primary is ReadyForTraffic: refused last-admin: {other:?}"),
            }
        }
    }

    /// CONTRACT (proposal roll.md step 1: "The fabric-primary validates it as the bootstrap admin
    /// validates its own at start (`BindingSet::validate`): every launch id the accepted topology uses
    /// declared and bound, `node_admin` always; every file present and hashing to its sha; every binding
    /// checked against the fabric's provider ... Any defect is refused by name and nothing is
    /// accepted"): a set whose files hash to their shas and bind every kind the topology uses is
    /// accepted under the process provider; a sha that does not match, a launch id the topology uses
    /// with no binding, and, under the container provider, a binding without the image are each refused
    /// `bindings-refused` naming the defect and the launch id. Must NOT happen: a roll accepted onto a
    /// file whose bytes are not the bound ones, or onto a set that cannot launch a member.
    #[tokio::test]
    async fn a_rolls_set_is_validated_at_acceptance_and_each_defect_is_refused_by_name() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("roll-set-{}-{}", std::process::id(), rand::random::<u32>()));
        std::fs::create_dir_all(&dir).unwrap();
        let bind = |id: &str| {
            let p = dir.join(id);
            std::fs::write(&p, format!("#!/bin/sh\n# {id}\n")).unwrap();
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
            rdm_core::binding::Binding { launch_id: id.into(), sha256: rdm_core::binding::sha256_file(&p).unwrap(), executable: p, image: None }
        };
        let good = rdm_core::binding::BindingSet { candidate: rdm_core::binding::Candidate { sha: "next".into(), build: "b2".into() }, launch_ids: vec!["node_admin".into(), "rpc_node".into()], bindings: vec![bind("node_admin"), bind("rpc_node")] };
        let topology = crate::accepted::FabricTopology { fabric: "fabric1".into(), meshes: [("mesh1".to_string(), crate::accepted::MeshTopology::of(&crate::build::MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 1)])))].into(), bindings: None };
        assert_eq!(roll_set_refusal(&good, &topology, ProviderKind::Process).await, None, "a complete, hash-true set is accepted");
        let refused = |r: Option<BuildReject>| match r {
            Some(BuildReject::BindingsRefused { defect, detail }) => (defect, detail),
            other => panic!("refused bindings-refused: {other:?}"),
        };
        let mut mismatched = good.clone();
        mismatched.bindings[1].sha256 = "0".repeat(64);
        let (defect, detail) = refused(roll_set_refusal(&mismatched, &topology, ProviderKind::Process).await);
        assert_eq!(defect, "hash-mismatch");
        assert!(detail.contains("rpc_node"), "{detail}");
        let unbound = rdm_core::binding::BindingSet { launch_ids: vec!["node_admin".into()], bindings: vec![good.bindings[0].clone()], ..good.clone() };
        let (defect, detail) = refused(roll_set_refusal(&unbound, &topology, ProviderKind::Process).await);
        assert_eq!((defect.as_str(), detail.contains("rpc_node")), ("not-required-by-the-set", true), "{detail}");
        let (defect, _) = refused(roll_set_refusal(&good, &topology, ProviderKind::Container).await);
        assert_eq!(defect, "no-image-bound", "a container fabric's set binds its one image");
        let reject = BuildReject::BindingsRefused { defect: "hash-mismatch".into(), detail: "x".into() };
        assert_eq!(reject.reason(), "bindings-refused");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
