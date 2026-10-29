//! The generic control API (PRD §7; mesh-control-plane.md §2).
//!
//! Every topology route is a front door that submits one [`TopologyChange`]
//! to the fabric-primary, which compiles it once against the accepted topology
//! into the next complete Build, persists it, which makes it the current Build, and
//! answers `202 {"build_id"}`. A stop, a start or a replacement changes no topology:
//! it opens the next attempt of the current Build with its action. A route
//! never spawns, kills or restarts anything itself: there is no lifecycle
//! handle in [`ControlPlane`] for it to call. `/api/shutdown` and runtime fault
//! routes are runtime administration, outside Build.

use tracing::Instrument as _;
use crate::accepted::{AcceptedStore, AttemptAction, TopologyChange};
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
    /// Why the view lacks a name right now: what the digest view, the removed set and the
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
    /// This admin's primary view: a node-admin it holds as its mesh's primary replaced by the election is
    /// an unheard mesh-primary, the fabric-primary's to recover (node-recovery.md step 2).
    pub seats: std::sync::OnceLock<rdm_transport::topology::PrimaryView>,
    /// What the fabric-primary's investigation holds of a peer mesh it has heard on the backbone
    /// (`crate::investigate::Ladder::peer_mesh`): a node of a mesh held unheard is not restarted,
    /// stopped or deleted from here (R-D1). Set once by the admin that owns the ladder.
    pub peer_mesh: std::sync::OnceLock<Arc<dyn Fn(&str) -> Option<crate::investigate::PeerMesh> + Send + Sync>>,
    /// Node investigation of one exact birth (node-investigation.md; Q107 item 1), the one primitive:
    /// gate 5 admits a delete of a birth neither stopped nor heard live only on its `exited` verdict
    /// (build.md §4.4; Q219). Set once by the admin that runs it; unset, no verdict is held.
    pub investigation: std::sync::OnceLock<Arc<dyn crate::seat_revision::PathProbe>>,
    /// The connection facts this admin holds (set once by the admin that owns its connections
    /// writer): the active Directs of the fleet, and this admin's own latest Directs and Proxies.
    pub connections: std::sync::OnceLock<Arc<std::sync::Mutex<rdm_core::connections::ConnectionsHeld>>>,
    /// The CPU and RAM each member's latest digest carries, by node id (set once by the admin that
    /// owns the digest view). Load is served beside the view, never inside it: it changes every
    /// digest and never moves the topology.
    pub loads: std::sync::OnceLock<Arc<dyn Fn() -> std::collections::BTreeMap<String, (Option<rdm_core::NodeLoad>, Option<rdm_core::GossipStats>)> + Send + Sync>>,
    /// Woken on every accepted Build, every attempt opened and every mesh drift attempt's end on this
    /// admin: the reconcile loop runs its next pass on it (`ReconcileWake`).
    pub build_submitted: Arc<Notify>,
    /// The fabric's change told of a Build this admin accepted, before the door answers it: a resize or
    /// an executable replacement puts the fabric back to pending inside the acceptance (states.md,
    /// Resize and executable replacement), and the
    /// view this admin answers from holds it. Set once by the admin that owns the change.
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
    /// The exact departure proof of a drift lane's claimant (build.md: "after exact departure proof"),
    /// set once the admin's drive is built; none proves nothing.
    pub departure: std::sync::OnceLock<Arc<dyn crate::build_drive::DepartureProof>>,
    /// The node rows (nodes.storage, every mesh's rows on the fabric's shared root), set once the admin
    /// opens them: a complete-topology request is compiled against what each row records of its
    /// executable (proposal executable-bindings-at-create.md rule 5, R4).
    pub node_rows: std::sync::OnceLock<Arc<dyn crate::storage::NodesStorage>>,
    /// The node id this admin was born with, set with `node_rows`: a recovery it opens and a claim it
    /// decides for itself are refused by name while the rows hold this birth deleted (Q181,
    /// [`crate::node_rows::deleted_birth`]).
    pub node_id: std::sync::OnceLock<crate::model::NodeId>,
    /// This admin's `RDM_EXECUTABLE_DIRS` (proposal executable-bindings-at-create.md §2): under the
    /// container provider, an executable a request names outside them is refused by name at acceptance.
    pub executable_dirs: Vec<std::path::PathBuf>,
    /// The `topology_storage_provider` names this admin's wiring registers (storage.md §2): as the
    /// fabric-primary, a request naming any other is refused by name at acceptance.
    pub topology_storage_providers: Vec<String>,
    /// This admin's atlas (fabric-atlas.md, Q224): what Build acceptance of a node action
    /// reads the node's birth and lifecycle from, never the gossip view.
    pub atlas: Arc<crate::atlas::Atlas>,
    /// This admin's storage changes: a fabric.delete admitted during a storage change interrupts its
    /// switch (storage.md §2 "Changing it"). Filled once the admin holds them.
    pub storage_changes: std::sync::OnceLock<Arc<crate::storage_change::StorageChanges>>,
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
            investigation: std::sync::OnceLock::new(),
            fabric_recovery: std::sync::OnceLock::new(),
            seats: std::sync::OnceLock::new(),
            contexts: Arc::new(crate::build_claim::AttemptContexts::in_memory()),
            claims: Arc::new(crate::storage::MemoryMeshStorage::default()),
            time,
            admission: Arc::new(tokio::sync::Mutex::new(())),
            departure: std::sync::OnceLock::new(),
            node_rows: std::sync::OnceLock::new(),
            node_id: std::sync::OnceLock::new(),
            executable_dirs: Vec::new(),
            topology_storage_providers: vec![rdm_core::launch::TOPOLOGY_STORAGE_FILE.into()],
            atlas: Arc::default(),
            storage_changes: std::sync::OnceLock::new(),
        }
    }

    /// This control plane holding `names`, the `topology_storage_provider` names its admin's wiring
    /// registers.
    pub fn with_topology_storage_providers(mut self, names: Vec<String>) -> Self {
        self.topology_storage_providers = names;
        self
    }

    /// This control plane holding `dirs`, the admin's `RDM_EXECUTABLE_DIRS`.
    pub fn with_executable_dirs(mut self, dirs: Vec<std::path::PathBuf>) -> Self {
        self.executable_dirs = dirs;
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
            tracing::info_span!("rdm.node_admin.build.reject.via-fabric-primary-required", route, node = %me, fabric_primary = %primary.as_ref().map(|p| p.to_string()).unwrap_or_default())
                .in_scope(|| tracing::info!("topology is changed only by the fabric-primary"));
            return Err(Refusal::NotPrimary(primary.map(|p| p.to_string())));
        }
        Ok(())
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
        // The mesh's own drift attempt whose claimant exited with it unfinished ends on that
        // departure's exact proof, and the mesh is free for this attempt (`end_deleted_attempt`).
        if holding(&current, Some(mesh)).is_some() && self.end_deleted_attempt(&current, mesh).await? {
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
    /// Why this admin's own birth (`node_id`, at its held name) is a deleted birth on the node rows
    /// (`crate::node_rows::deleted_birth`, Q181), if it is one; `None` before the rows and the birth are
    /// set. An unreadable row refuses by name: nothing is opened on a birth the rows cannot vouch for.
    async fn deleted_self(&self) -> Result<Option<String>, Refusal> {
        let (Some(rows), Some(node_id)) = (self.node_rows.get(), self.node_id.get()) else { return Ok(None) };
        let me = self.held_name().await;
        crate::node_rows::deleted_birth(&**rows, &me, node_id).await.map_err(|e| Refusal::Unavailable(format!("{me}: its own node row ({node_id}) could not be read: {e}")))
    }

    async fn departure_of(&self, claimant: &PathName) -> Result<Option<crate::model::Node>, Refusal> {
        let Some(n) = self.topology.read().await.node(claimant).cloned().filter(|n| n.incarnation_id.is_some()) else { return Ok(None) };
        let Some(proof) = self.departure.get() else { return Ok(None) };
        Ok(proof.proven(claimant).await.ok().map(|()| n))
    }

    /// End `mesh`'s drift attempt of `current` whose claimant exited with it unfinished, when its
    /// departure is proven (R-BI7 caveat 2: each mesh-primary claims its mesh's drift attempts on its
    /// mesh's rows; build.md §5.2 as the approved proposal A4 words it: "The next attempt is claimed
    /// after the running attempt's terminal receipt (Complete, Failed or HandedOff), or, when the
    /// executor is lost with the attempt unfinished, after exact departure proof. A live run is
    /// reattached, never re-claimed"). The lane's running attempt, claimed by another admin than this
    /// one, whose exact birth in this admin's view has its exit recorded, ends `Failed` naming that
    /// departure (on the fabric's Build journal on the store root, so every fold holds the lane free and
    /// the exits the attempt was recovering are owed a submit again: node-recovery.md "Failure and
    /// receipts"), and its in-flight row of the mesh is released. Answers whether it ended one. An
    /// attempt this admin claimed, an attempt whose claimant is not proven exited, and the fabric
    /// lane's attempt (its drive takes it over) are left holding the mesh.
    async fn end_deleted_attempt(&self, current: &crate::build_state::BuildProjection, mesh: &str) -> Result<bool, Refusal> {
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
        // The admin that runs the attempt deleted: its executor (the claimant too, for an attempt its
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
        let reason = format!("{executor} ({} at {incarnation}) exited with attempt {attempt} of mesh {mesh}'s lane unfinished: its departure is proven; {me} takes the lane over", n.node_id);
        let span = tracing::info_span!("rdm.node_admin.build.update.via-claimant-departure", build_id = %current.build_id.log(), lane = mesh, attempt, claimant = %claimant, executor = %executor, node_id = %n.node_id, incarnation_id = %incarnation, node = %me, outcome = tracing::field::Empty);
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
            span.in_scope(|| tracing::info!(error = %e, "the deleted claimant's attempt could not be ended: the mesh stays held by it"));
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
            span.in_scope(|| tracing::info!(error = %e, "the deleted claimant's attempt is ended and mesh's in-flight row could not be released: the mesh stays held by it"));
            return Err(Refusal::Unavailable(format!("{me}: attempt {attempt} of mesh {mesh}'s lane of {} is ended on {executor}'s departure and mesh {mesh}'s in-flight row could not be released: {e}", current.build_id)));
        }
        span.record("outcome", "ended");
        span.in_scope(|| tracing::info!("the mesh's drift attempt ends with its deleted claimant, and its in-flight row is released"));
        Ok(true)
    }

    /// End the fabric lane's attempt of `current` acting in `mesh` whose claimant exited with it unfinished,
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
        let reason = format!("{claimant} ({} at {incarnation}) exited with attempt {attempt} of the fabric lane unfinished, acting in mesh {mesh}: its departure is proven; {me} starts the workflow again", n.node_id);
        let span = tracing::info_span!("rdm.node_admin.build.update.via-claimant-departure", build_id = %current.build_id.log(), lane = "fabric", attempt, claimant = %claimant, node_id = %n.node_id, incarnation_id = %incarnation, node = %me, outcome = tracing::field::Empty);
        let verdict = crate::build_state::BuildAttemptReceipt { mesh: None, build_id: current.build_id.clone(), attempt, outcome: crate::build_state::AttemptOutcome::Failed { reason, code: rdm_core::outcome::Code::FailedPrecondition } };
        if let Err(e) = self.builds.append_attempt_receipt(&verdict).await {
            span.record("outcome", "verdict-unwritten");
            span.in_scope(|| tracing::info!(error = %e, "the deleted claimant's fabric-lane attempt could not be ended: the mesh stays held by it"));
            return Err(Refusal::State(e));
        }
        let held = crate::mesh_claims::InFlight { build_id: current.build_id.clone(), lane: None, attempt, claimant: claimant.to_string(), claimant_node: None };
        if let Err(e) = self.claims.release_in_flight(mesh, &held).await {
            span.record("outcome", "in-flight-unreleased");
            span.in_scope(|| tracing::info!(error = %e, "the deleted claimant's fabric-lane attempt is ended and the mesh's in-flight row could not be released"));
            return Err(Refusal::Unavailable(format!("{me}: attempt {attempt} of {}'s fabric lane is ended on {claimant}'s departure and mesh {mesh}'s in-flight row could not be released: {e}", current.build_id)));
        }
        span.record("outcome", "ended");
        span.in_scope(|| tracing::info!("the fabric lane's attempt ends with its deleted claimant, and the mesh's in-flight row is released"));
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
        // (a guard stays entered on the worker while the task is held). A refusal is reported
        // beside the request span, in the caller's span.
        let outer = tracing::Span::current();
        async {
            self.require_fabric_primary(route).await?;
            // fabric.delete is the one Build that may interrupt the Build in progress (build.md §1): it is
            // compiled against the current Build whatever its state, and a fabric.delete in force answers
            // a second one, which accepts nothing.
            let deletes = matches!(change, TopologyChange::DeleteFabric);
            let (_admitted, current) = if deletes { self.current_read().await? } else { self.current_settled().await? };
            if deletes {
                // A storage change in progress is resolved before anything of the delete is written: its
                // commit record read, and this fabric-primary unblocked onto the provider it decided
                // (storage.md §2: fabric.delete "interrupts the change as it interrupts any Build";
                // i149 R12).
                if let Some(changes) = self.storage_changes.get() {
                    changes.interrupt_for_delete();
                }
                if let Some((resumed, _)) = self.fabric_delete_in_force().await? {
                    span.record("build_id", resumed.build_id.as_str());
                    tracing::info!(build_id = %resumed.build_id.log(), attempt = resumed.attempt, "a fabric.delete is in force: this one answers it and accepts nothing");
                    return Ok(resumed);
                }
            }
            // A complete-topology request is built from a read of the current Build (proposal
            // executable-bindings-at-create.md rule 1): one built from an older read is refused by name,
            // nothing accepted, so a list from a stale read never removes a node another caller added.
            // It compiles against what the rows record of each path's executable.
            let recorded = match &change {
                TopologyChange::Reconcile { based_on, topology_storage_provider, .. } => {
                    if based_on != &current.build_id {
                        let reject = BuildReject::BasedOnNotCurrent { based_on: based_on.to_string(), current: current.build_id.to_string() };
                        outer.in_scope(|| reject_span(route, &reject));
                        return Err(Refusal::Reject(reject));
                    }
                    // A provider this fabric-primary's wiring does not register is refused by name, nothing
                    // accepted and nothing moved (storage.md §2 "Changing it").
                    if let Some(provider) = topology_storage_provider.as_ref().filter(|p| !self.topology_storage_providers.contains(p)) {
                        let reject = BuildReject::TopologyStorageProviderNotRegistered { provider: provider.clone(), registered: self.topology_storage_providers.clone() };
                        outer.in_scope(|| reject_span(route, &reject));
                        return Err(Refusal::Reject(reject));
                    }
                    // A registered provider other than the Fabric row's is opened now, on this fabric-primary's
                    // scope, and closed again: one that does not open is refused by name, nothing accepted and
                    // nothing moved (storage.md §2 "Changing it": "does not register or cannot open").
                    if let Some(provider) = topology_storage_provider.as_ref() {
                        let named = self.accepted.topology_storage_provider().await.map_err(|e| Refusal::Unavailable(format!("fabric.storage: {e}")))?;
                        if named.as_deref() != Some(provider.as_str()) {
                            let changes = self.storage_changes.get().ok_or_else(|| Refusal::Unavailable(format!("{}: no topology storage handle is held yet to open `{provider}`", self.me)))?;
                            if let Err(detail) = changes.handle.opens(provider).await {
                                let reject = BuildReject::TopologyStorageProviderNotOpened { provider: provider.clone(), detail };
                                outer.in_scope(|| reject_span(route, &reject));
                                return Err(Refusal::Reject(reject));
                            }
                        }
                    }
                    let rows = self.node_rows.get().ok_or_else(|| Refusal::Unavailable(format!("{}: no nodes.storage is open yet to read what each node's row records", self.me)))?;
                    crate::accepted::recorded_executables(&**rows, &current.topology).await.map_err(Refusal::Unavailable)?
                }
                _ => crate::accepted::Recorded::new(),
            };
            if let Some(candidate) = collect {
                if let Err(reject) = self.recheck_stopped(candidate).await {
                    outer.in_scope(|| reject_span(route, &reject));
                    return Err(Refusal::Reject(reject));
                }
            }
            let mut observed = self.topology.read().await.clone();
            // Each birth as the ops left it (R-FA1: the fabric-primary accepts or denies from the atlas, the
            // gossip view context only): a node-admin a start just made ready is a successor here, whatever
            // the gossip view has heard of it; a birth an op deleted is not in it.
            self.atlas.apply(&mut observed);
            // node.delete's target (node-delete.md "Operation contract"; build.md §4.4 gate 5): a live
            // birth, a stopped one, or one whose exact runtime node investigation reads exited
            // ([`crate::build::delete_admits`]). A birth the view marks Dead with neither fact is refused
            // `node-not-live` by name: silence never says the runtime exited (Q219).
            let mut exited_pin: Option<(PathName, crate::build::PinnedBirth)> = None;
            if let TopologyChange::RemoveNode { node } = &change {
                // The node's mesh held by its own attempt is the gate before the node's own gates
                // (build.md §4.4): refused naming the mesh, whatever the node's state.
                if let Some(held) = holding(&current, Some(&node.mesh)) {
                    outer.in_scope(|| {
                        tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", route, build_id = %held.current_build_id.log(), mesh = %node.mesh, held_by = %held.held_by.as_deref().unwrap_or(""), node = %node)
                            .in_scope(|| tracing::info!("the node's mesh is held by its own drift attempt: one attempt in flight per mesh"))
                    });
                    return Err(Refusal::BuildInProgress(held));
                }
                if let Some(n) = self.atlas_row(node, &observed).await? {
                    match self.delete_gate(&n, &observed, &outer).await {
                        Some(crate::build::DeleteAdmitted::Exited) => exited_pin = n.incarnation.clone().map(|incarnation| (node.clone(), crate::build::PinnedBirth { node_id: n.node_id.clone(), incarnation })),
                        Some(_) => {}
                        None => return Err(not_live(&outer, route, node, n.status, "a delete needs a live node, a stopped one, or one whose exit node investigation proved")),
                    }
                }
            }
            let mut topology = match crate::accepted::compile_against(&current.topology, &change, &observed, &recorded) {
                Ok(t) => t,
                Err(reject) => {
                    outer.in_scope(|| reject_span(route, &reject));
                    return Err(Refusal::Reject(reject));
                }
            };
            // The fabric's topology_storage_provider the Build realizes: the request's, else the Fabric row's,
            // unchanged (storage.md §2: "a Build that omits the property leaves it unchanged", Q166).
            let current_provider = self.accepted.topology_storage_provider().await.map_err(|e| Refusal::Unavailable(format!("fabric.storage: {e}")))?;
            topology.topology_storage_provider = match &change {
                TopologyChange::Reconcile { topology_storage_provider: Some(p), .. } => Some(p.clone()),
                _ => current_provider.clone(),
            };
            // A Build naming another provider than the Fabric row's is a storage change: its change_id is
            // minted now and recorded with the accepted Build, before its first command (storage.md §2
            // "Changing it"). Any other Build is none, whatever Build it was compiled against.
            // The Build's id, minted once: a storage change accepted now is keyed by it.
            let build_id = BuildId::mint();
            topology.storage_change = match (&change, &current_provider) {
                (TopologyChange::Reconcile { topology_storage_provider: Some(to), .. }, Some(from)) if to != from => Some(rdm_core::build::StorageChange { change_id: rdm_core::ChangeId::mint(), from: from.clone(), to: to.clone(), build_id: build_id.clone() }),
                // A fabric.delete interrupting a storage change carries it: the delete sends each
                // mesh-primary `mesh.unblock` onto the provider the change's `commit` record decided
                // before its teardown.
                (TopologyChange::DeleteFabric, _) => current.topology.storage_change.clone().filter(|_| matches!(current.state, crate::build_state::BuildState::Pending | crate::build_state::BuildState::Running)),
                _ => None,
            };
            // Every create names its executable (build-deployment.md §2.1): whatever change compiled it, a
            // path created with none is refused by name, nothing accepted.
            if let Some(reject) = created_without_executable(&current.topology, &topology) {
                outer.in_scope(|| reject_span(route, &reject));
                return Err(Refusal::Reject(reject));
            }
            // Every executable the request creates a node on or replaces one with is checked now, before
            // anything is accepted: its place under the container provider, then its hash; the launch
            // hashes it again.
            if matches!(change, TopologyChange::Reconcile { .. }) {
                let provider = self.topology.read().await.fabric.provider;
                if let Some(reject) = named_executable_refusal(&current.topology, &topology, &recorded, provider, &self.executable_dirs).await {
                    outer.in_scope(|| reject_span(route, &reject));
                    return Err(Refusal::Reject(reject));
                }
            }
            // Every node this Build deletes is pinned to the exact birth the view holds at its path now
            // (build.md §4.4): its node.delete carries that birth, and the executing mesh-primary
            // refuses another `birth-replaced`. A birth with no incarnation cannot be pinned.
            let mut pinned = Vec::new();
            for n in crate::accepted::surplus_births(&topology, &observed) {
                match crate::build::PinnedBirth::of(n) {
                    Some(birth) => pinned.push((n.name.clone(), birth)),
                    None => return Err(not_live(&outer, route, &n.name, n.status, "the node holds no incarnation id to pin its delete to")),
                }
            }
            // The birth gate 5 admitted on its exited verdict is pinned too, whatever the view marks it.
            if let Some((at, birth)) = exited_pin.take() {
                if !pinned.iter().any(|(p, _)| *p == at) {
                    pinned.push((at, birth));
                }
            }
            topology.pinned = pinned;
            // A peer mesh held unheard is held as a mesh (R-D1): its births under a silence mark are planned
            // as they stand, and gate 6 below reads what the request changes there.
            let held: std::collections::BTreeSet<String> = topology.meshes.keys().filter(|m| **m != self.me.mesh && self.peer_mesh.get().and_then(|f| f(m)).is_some_and(|p| p.unheard_ms.is_some())).cloned().collect();
            let operations = crate::accepted::plan_holding(&topology, &observed, &held).operations;
            // A Build whose plan holds the fabric delete, the whole plan, is exempt from the
            // build-in-progress refusal, R-BI7 and R-PC1: it interrupts the Build in progress, and its own
            // mesh.delete and node.delete steps, and only those, run before the first ready
            // (build.md §1; states.md, "The initial fabric before ready").
            let interrupts = operations.iter().any(|op| matches!(op, crate::build::BuildOperation::DeleteFabric { .. }));
            if deletes && !interrupts {
                settled(&current)?;
            }
            // The replacements the request owes act in their meshes as every topology operation does:
            // held by a mesh's open drift attempt (a recovery's), and refused before the initial fabric
            // first reached ready. A replacement's own guards are the planner's (R4, R5), so
            // the no-successor check below reads the topology's operations alone.
            let replacements = match &change {
                TopologyChange::Reconcile { .. } => crate::accepted::replacement_operations(&topology, &observed, &recorded),
                _ => Vec::new(),
            };
            // A replaced member's delete is pinned to the birth this acceptance saw at its path (Q218; Q228
            // decided by Q218 and Q221): a birth replaced before the delete runs is refused `birth-replaced`.
            let more = crate::accepted::replacement_pins(&replacements, &topology.pinned);
            topology.pinned.extend(more);
            // A mesh whose own drift attempt is open is held by it: a topology operation on that mesh
            // is refused, naming the Build, the mesh and the mesh-primary holding it (R-BI7 caveat 3).
            if let Some(held) = operations.iter().chain(replacements.iter()).filter_map(op_mesh).find_map(|mesh| holding(&current, Some(&mesh))).filter(|_| !interrupts) {
                outer.in_scope(|| {
                    tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", route, build_id = %held.current_build_id.log(), mesh = %held.mesh.as_deref().unwrap_or(""), held_by = %held.held_by.as_deref().unwrap_or(""))
                        .in_scope(|| tracing::info!("a topology operation acts in a mesh its own drift attempt holds: one attempt in flight per mesh"))
                });
                return Err(Refusal::BuildInProgress(held));
            }
            // Gate 6 (build.md §4.4; Q221): every node the Build acts on is of a mesh the fabric-primary's
            // view hears, refused by name naming the mesh and how long it has gone unheard (R-D1). Each
            // operation is checked again before its claim or dispatch (the drive's own check).
            if let Some(reject) = operations.iter().chain(replacements.iter()).filter_map(|op| op.node()).filter(|n| n.mesh != self.me.mesh).find_map(|n| {
                let unheard_ms = self.peer_mesh.get().and_then(|f| f(&n.mesh)).and_then(|p| p.unheard_ms)?;
                Some(BuildReject::UnheardMesh { node: n.to_string(), mesh: n.mesh.clone(), unheard_ms })
            }) {
                outer.in_scope(|| reject_span(route, &reject));
                return Err(Refusal::Reject(reject));
            }
            // A Build whose plan would end a seat holder while no other node of its type in its mesh can
            // take its seat is refused before it exists: node.primary.update has no successor (R-MT1, Q7).
            if let Some(reject) = operations.iter().find_map(|op| {
                let (node, action) = match op {
                    crate::build::BuildOperation::DeleteLiveNode { node, .. } => (node, "delete"),
                    crate::build::BuildOperation::DeleteConnections { node, .. } => (node, "node.connection.delete"),
                    crate::build::BuildOperation::StopNode { node, .. } => (node, "stop"),
                    _ => return None,
                };
                last_admin(node, action, &observed)
            }) {
                outer.in_scope(|| reject_span(route, &reject));
                return Err(Refusal::Reject(reject));
            }
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
            // The Build is current the moment it is durable in the builds store: nothing else is written.
            // It is the door's last write: the reconcile loop drives a current Build at once, and its first
            // step can move the fabric seat (a delete of this admin's own mesh), which refuses every gated
            // write after it, so the door answers the Build it accepted whatever the drive does next.
            let me = self.held_name().await;
            crate::build_state::accept(&*self.builds, &accepted, Some(&current.build_id), &me.to_string(), "accepted").await.map_err(Refusal::State)?;
            tracing::info!(build_id = %build_id.log(), "build accepted");
            // A resize or an executable replacement puts the fabric back to pending (states.md): the change takes
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
            // The request's context, in the open's own record: a refused open writes nothing, and
            // a claim of the attempt the moment it lands takes this context (build.md §4.4).
            context: Some(crate::build_claim::current_context()),
        };
        span.record("build_id", current.build_id.as_str());
        span.record("attempt", opened.attempt);
        // The attempt is opened with its context in one record: a refusal (`attempt-taken`, the number
        // another writer's open holds) writes nothing (build.md §4.4), so the context of the attempt
        // that holds the number stays its own.
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
        if let Some(action) = &opened.action {
            self.atlas.note_accepted(&opened.build_id, opened.attempt, action);
        }
        self.build_submitted.notify_waiters();
        Ok(Opened { build_id: current.build_id.clone(), attempt: opened.attempt, mesh: opened.mesh })
    }

    /// Open the next attempt of the current Build with a targeted action (a connections delete,
    /// stop, start or delete of one birth). The topology is unchanged; the Build stays the current one.
    pub async fn open_attempt(&self, route: &'static str, reason: AttemptReason, path: PathName, kind: ActionKind) -> Result<Opened, Refusal> {
        let span = tracing::info_span!("rdm.node_admin.build.update.via-rest", route, build_id = tracing::field::Empty, attempt = tracing::field::Empty, node = %path);
        self.open_attempt_in(span, route, reason, path, kind).await
    }

    /// Open the next attempt of the accepted Build as `steps` (`BuildChange::Steps`;
    /// build.md §1: "Rafka's replace steps (one Build of node primitives ...)";
    /// lifecycles/node-replace.md), under `span`. The topology is unchanged; the Build stays the
    /// current one. Refused by name: a Build with no step, one naming an operation twice, or one
    /// acting on a path that holds no birth and that no earlier step makes
    /// (`crate::accepted::steps_action`); a step in a mesh held by its own drift attempt (R-BI7
    /// caveat 3); a step on a node of a peer mesh this fabric-primary holds unheard (R-D1); a
    /// node.connection.delete, stop or delete of a seat holder while no other node of its type in its
    /// mesh can take its seat (R-MT1, Q7).
    pub async fn open_steps_in(&self, span: tracing::Span, route: &'static str, steps: Vec<rdm_core::build::Step>) -> Result<Opened, Refusal> {
        let outer = tracing::Span::current();
        async {
            self.require_fabric_primary(route).await?;
            let summary = steps.iter().map(crate::build::step_key).collect::<Vec<_>>().join(" ");
            let (_admitted, current) = self.current_settled().await?;
            for step in &steps {
                let node = step.node();
                if let Some(held) = holding(&current, Some(&node.mesh)) {
                    outer.in_scope(|| {
                        tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", route, build_id = %held.current_build_id.log(), mesh = %node.mesh, held_by = %held.held_by.as_deref().unwrap_or(""), node = %node)
                            .in_scope(|| tracing::info!("the steps act in a mesh held by its own drift attempt: one attempt in flight per mesh"))
                    });
                    return Err(Refusal::BuildInProgress(held));
                }
            }
            let mut t = match self.view_now.get() {
                Some(project) => project().await,
                None => self.topology.read().await.clone(),
            };
            // Each birth as the ops left it (R-FA1): a primary's successor, a step's node.
            self.atlas.apply(&mut t);
            // A node stands at every path a step acts on, before its mesh is read heard and before a
            // primary's successor is looked for (build.md §4.4's order).
            let action = match crate::accepted::steps_action(steps.clone(), &t) {
                Ok(a) => a,
                Err(reject) => {
                    outer.in_scope(|| reject_span(route, &reject));
                    return Err(Refusal::Reject(reject));
                }
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
                let step_action = match step {
                    rdm_core::build::Step::Stop { .. } => Some("stop"),
                    rdm_core::build::Step::Delete { .. } => Some("delete"),
                    // A seat holder's seats move at its node.connection.delete { All }, the first step of
                    // a replace (node-connections-delete.md step 0; node-replace.md step 1).
                    rdm_core::build::Step::DeleteConnections { .. } => Some("node.connection.delete"),
                    _ => None,
                };
                if let Some(reject) = step_action.and_then(|a| last_admin(node, a, &t)) {
                    outer.in_scope(|| reject_span(route, &reject));
                    return Err(Refusal::Reject(reject));
                }
            }
            drop(t);
            self.open_with(&current, None, AttemptReason::Steps, action, route, &outer, &summary, span.clone()).await
        }
        .instrument(span.clone())
        .await
    }

    /// [`ControlPlane::open_attempt`] under `span`, which names the surface the request came through.
    pub async fn open_attempt_in(&self, span: tracing::Span, route: &'static str, reason: AttemptReason, path: PathName, kind: ActionKind) -> Result<Opened, Refusal> {
        let outer = tracing::Span::current();
        async {
            self.require_fabric_primary(route).await?;
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
            // its row's name (rollback step 2), and its birth is targeted below in the view, which lists it at
            // its row's name.
            let stopped_old = kind == ActionKind::Delete && path.old;
            // A birth of a mesh deleted at mesh.deleted (its last admin, alive): no accepted Build names
            // the mesh, and only a delete acts on it (mesh-delete.md step 7).
            let deleted_mesh = kind == ActionKind::Delete && !path.old && !current.topology.meshes.contains_key(&path.mesh) && self.of_deleted_mesh(&path).await;
            if (kind == ActionKind::Delete && !(path.old || deleted_mesh)) || (!stopped_old && !deleted_mesh && !current.topology.contains(&path.base())) {
                let reject = BuildReject::UnknownNode { node: path.to_string(), step: None };
                outer.in_scope(|| reject_span(route, &reject));
                return Err(Refusal::Reject(reject));
            }
            let (node_id, from_incarnation) = {
                // A stopped birth at a `.old` path is admitted from the view, which reads a renamed
                // birth by its node id through the node-row id index (R12).
                let mut t = match self.view_now.get() {
                    Some(project) => project().await,
                    None => self.topology.read().await.clone(),
                };
                // Each birth as the ops left it (R-FA1): a primary's successor a stop or a start the
                // fabric-primary ran moved is read so, whatever the gossip view has heard of it.
                self.atlas.apply(&mut t);
                // The birth and its lifecycle are the atlas's: the node row's current birth and what the
                // ops left it (fabric-atlas.md R-FA1), never the gossip view.
                let held = self.atlas_row(&path, &t).await?;
                let Some(n) = held else {
                    let me = self.held_name().await;
                    let why = if stopped_old {
                        format!("{me}: this fabric-primary's view holds no stopped birth at {path} (no node.name.update renamed one to it, or its Deleted row is written)")
                    } else {
                        self.absence.get().map(|f| f(&path)).unwrap_or_else(|| "no absence reporter".to_string())
                    };
                    let reject = BuildReject::UnknownNode { node: path.to_string(), step: None };
                    drop(t);
                    outer.in_scope(|| {
                        tracing::info_span!("rdm.node_admin.build.reject.via-unknown-node", route, detail = %reject, view = %why)
                            .in_scope(|| tracing::info!(%reject, %why, "build refused: the live view lacks the node"))
                    });
                    return Err(Refusal::Reject(reject));
                };
                // A start is the transition out of Leaving, and only for a stopped process; every other action needs a live node.
                let startable = kind == ActionKind::Start && n.stopped;
                let live = n.live;
                // Gate 5 for a delete ([`crate::build::delete_admits`]): the stopped predecessor, or one whose
                // exit node investigation proved, or the last admin of a deleted mesh, live or stopped: its
                // node.delete stops it first. A Dead mark alone admits nothing (Q219).
                let deletable = kind == ActionKind::Delete
                    && match self.delete_gate(&n, &t, &outer).await {
                        Some(crate::build::DeleteAdmitted::Stopped | crate::build::DeleteAdmitted::Exited) => true,
                        Some(crate::build::DeleteAdmitted::Live) => deleted_mesh,
                        None => false,
                    };
                let needs_live = !matches!(kind, ActionKind::Start | ActionKind::Delete);
                if (kind == ActionKind::Start && !startable) || (kind == ActionKind::Delete && !deletable) || (needs_live && !live) {
                    let reason = match kind {
                        ActionKind::DeleteConnections => "node.connection.delete needs a live node",
                        ActionKind::Stop => "a stop needs a live node",
                        ActionKind::Start => "a start needs a stopped node",
                        ActionKind::Delete => "a delete of a .old path needs the stopped predecessor or one whose exit is proven; any other delete through this route is of a deleted mesh's birth",
                    };
                    return Err(not_live(&outer, route, &path, n.status, reason));
                }
                // A node of a peer mesh this fabric-primary holds unheard is not its to stop or delete:
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
                // The exact birth the gates pinned (build.md §4.4): its node id and incarnation, which the
                // executing mesh-primary verifies before any effect.
                match n.incarnation.clone() {
                    Some(incarnation) => (n.node_id.clone(), incarnation),
                    None => return Err(not_live(&outer, route, &path, n.status, "the node holds no incarnation id to pin the attempt to")),
                }
            };
            let node_name = path.to_string();
            let action = match kind {
                ActionKind::DeleteConnections => AttemptAction::DeleteConnections { path, node_id, from_incarnation },
                ActionKind::Stop => AttemptAction::Stop { path, node_id, from_incarnation },
                ActionKind::Start => AttemptAction::Start { path, node_id, from_incarnation },
                ActionKind::Delete => AttemptAction::Delete { path, node_id, from_incarnation },
            };
            self.open_with(&current, None, reason, action, route, &outer, &node_name, span.clone()).await
        }
        .instrument(span.clone())
        .await
    }
}

impl ControlPlane {
    /// Gate 5 of a delete of the atlas row `n` ([`crate::build::delete_admits`]): node investigation of
    /// its exact birth (`observed`'s birth of that node id and incarnation) is asked only when the birth
    /// is neither stopped nor heard live, and its verdict is spanned on `outer`. No investigation held:
    /// no verdict.
    pub(crate) async fn delete_gate(&self, n: &crate::atlas::AtlasRow, observed: &crate::topology::Topology, outer: &tracing::Span) -> Option<crate::build::DeleteAdmitted> {
        use crate::model::NodeStatus;
        let heard_dead = n.status == NodeStatus::Dead || n.observed == Some(NodeStatus::Dead);
        if !crate::build::delete_needs_verdict(n.stopped, n.live, heard_dead) {
            return crate::build::delete_admits(n.stopped, n.live, heard_dead, None);
        }
        let finding = match (self.investigation.get(), n.incarnation.clone()) {
            (Some(probe), Some(incarnation)) => {
                let birth = observed.births().find(|b| b.node_id == n.node_id && b.incarnation_id.as_ref() == Some(&incarnation)).cloned().unwrap_or_else(|| {
                    let mut b = crate::model::Node::allocated(n.name.clone());
                    b.node_id = n.node_id.clone();
                    b.incarnation_id = Some(incarnation.clone());
                    b
                });
                Some(probe.investigate(&birth).await)
            }
            _ => None,
        };
        let admitted = crate::build::delete_admits(n.stopped, n.live, heard_dead, finding.as_ref());
        let verdict = finding.as_ref().map(|f| f.name()).unwrap_or("none-held");
        let detail = finding.as_ref().map(|f| f.detail()).unwrap_or_default();
        outer.in_scope(|| tracing::info!(node = %n.name, node_id = %n.node_id, verdict, detail = %detail, admitted = admitted.map(|a| a.name()).unwrap_or("none"), "gate 5: the birth is neither stopped nor heard live, so node investigation of its exact birth decides the delete"));
        admitted
    }

    /// Open the termination of the deleted birth `(node_id, incarnation)` at `path`, heard again and
    /// reported (node-investigation.md: "The node that refused it reports it to the fabric-primary, which
    /// terminates its runtime"; Q110, Q128), as the next attempt of the current Build, executed by this
    /// fabric-primary. Only a deleted birth is terminated this way: one a node workflow deleted, or one
    /// this fabric-primary's atlas holds deleted, whose recovery's re-create has not completed (Q217:
    /// "if they come back and they are deleted they get actually deleted"); any other is not known here
    /// (`UnknownNode`). One whose termination completed answers `AlreadyApplied`.
    pub async fn open_terminate(&self, span: tracing::Span, route: &'static str, path: PathName, node_id: crate::model::NodeId, incarnation: crate::model::IncarnationId) -> Result<Recovery, Refusal> {
        let outer = tracing::Span::current();
        async {
            self.require_fabric_primary(route).await?;
            let (_admitted, current) = self.current_read().await?;
            // A node workflow deleted it: the current Build holds its `NodeDeleted` receipt for this exact
            // birth (the fabric-primary writes every receipt), whether or not the recovery's re-create
            // has completed and whether or not this atlas has heard the deleting mesh-primary's row.
            let deleted_here = current.steps.iter().filter_map(|s| crate::build_state::delete_step_birth(s, "NodeDeleted")).any(|(p, n, i)| p == path && n == node_id && i == incarnation);
            if !deleted_here && !self.atlas.holds_deleted(&node_id, &incarnation) && self.builds.recovery_of(&path, &node_id, &incarnation).await.map_err(Refusal::State)?.is_none() {
                let reject = BuildReject::UnknownNode { node: path.to_string(), step: None };
                outer.in_scope(|| reject_span(route, &reject));
                return Err(Refusal::Reject(reject));
            }
            settled(&current)?;
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
        let span = tracing::info_span!("rdm.node_admin.build.update.via-recovery", route, build_id = tracing::field::Empty, attempt = tracing::field::Empty, mesh = %path.mesh, node = %path, node_id = %node_id, incarnation_id = %incarnation.as_str(), open_attempt = tracing::field::Empty, mesh_primary = tracing::field::Empty, admins = tracing::field::Empty, executor = tracing::field::Empty, outcome = tracing::field::Empty);
        let outer = tracing::Span::current();
        let r = async {
            // A deleted birth opens nothing (node-investigation.md: "A node that comes back after it was
            // re-created is a deleted birth ... No admit reason exists for it"; Q181): this admin's own
            // birth, read on the store's node rows before anything is read or written, whatever its own
            // view says.
            if let Some(why) = self.deleted_self().await? {
                let me = self.held_name().await;
                let node_id = self.node_id.get().map(|i| i.to_string()).unwrap_or_default();
                outer.in_scope(|| {
                    tracing::info_span!("rdm.node_admin.build.reject.via-deleted-birth", route, node = %me, node_id = %node_id, member = %path, reason = %why)
                        .in_scope(|| tracing::info!("this admin's own birth is a deleted birth on the node rows: it opens no recovery"))
                });
                return Err(Refusal::Conflict(why));
            }
            // Keyed on the exited birth across the Build log: a birth a node workflow already
            // deleted (its recovery, or a delete it exited inside: node-recovery.md
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
            // An unheard mesh-primary's recovery the fabric-primary opens in the fabric lane is claimed for
            // the mesh-primary the election named in the birth's place, the one writer of its mesh's rows
            // (node-recovery.md step 2: the fabric-primary runs it "after the election has named the new
            // mesh-primary"; "Execution": "the mesh-primary claims it ... the mesh-primary runs it"), and for
            // this admin only as acting primary of a mesh with no other node-admin
            // (node-identity-and-state.md: "The fabric-primary writes a row only as acting primary of a mesh
            // with no primary"). The writer is decided here, once, before anything is opened: while this
            // admin's view names no mesh-primary in the birth's place and the mesh holds another node-admin,
            // the election has not named it here yet, and nothing is opened or claimed; the submit is owed
            // and made again.
            let fabric_lane = unheard_mesh_primary && fabric_recovery.is_none() && !reported;
            let executor = if fabric_lane {
                let now = match self.view_now.get() {
                    Some(project) => project().await,
                    None => t.clone(),
                };
                let named = crate::executor::unheard_mesh_primary_executor(&path, &node_id, &now);
                let mesh_primary = now.node_type_primary(&path.mesh, NodeKind::NodeAdmin).map(|n| n.name.to_string()).unwrap_or_default();
                let admins: Vec<String> = crate::executor::held_admins(&path.mesh, Some(&path), &now).into_iter().map(|n| n.name.to_string()).collect();
                span.record("mesh_primary", mesh_primary.as_str());
                span.record("admins", admins.join(",").as_str());
                match named {
                    Some(executor) => executor,
                    None => {
                        let why = format!(
                            "{me}: the recovery of mesh {}'s unheard mesh-primary {path} ({node_id}) is not opened: this admin's view names {} mesh {}'s primary and the mesh holds node-admin(s) {}, so the election has not named the mesh-primary in its place here; the submit is made again",
                            path.mesh,
                            if mesh_primary.is_empty() { "no node-admin".to_string() } else { mesh_primary.clone() },
                            path.mesh,
                            admins.join(", ")
                        );
                        outer.in_scope(|| {
                            tracing::info_span!("rdm.node_admin.build.reject.via-mesh-primary-not-named", route, node = %me, mesh = %path.mesh, member = %path, node_id = %node_id, mesh_primary = %mesh_primary, admins = %admins.join(","))
                                .in_scope(|| tracing::info!("an unheard mesh-primary's recovery is claimed for the mesh-primary the election names in its place: none is named in this admin's view yet"))
                        });
                        return Err(Refusal::Unavailable(why));
                    }
                }
            } else {
                me.clone()
            };
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
                let reject = BuildReject::UnknownNode { node: path.to_string(), step: None };
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
            let lane = if fabric_lane { None } else { Some(path.mesh.as_str()) };
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
                // A row a deleted claimant took for a drift attempt that holds no lane in this admin's
                // fold (its run ended and the row was never released, or nothing of it was written
                // before the claimant was lost) is released on that departure's exact proof.
                let deleted = match held.claimant.parse::<PathName>() {
                    Ok(c) if held.lane.as_deref() == Some(path.mesh.as_str()) && c != me && c != self.me => self.departure_of(&c).await?.map(|n| (c, n)),
                    _ => None,
                };
                if let Some((claimant, n)) = deleted {
                    let release = tracing::info_span!("rdm.node_admin.build.update.via-claimant-departure", build_id = %held.build_id.log(), lane = %path.mesh, attempt = held.attempt, claimant = %claimant, node_id = %n.node_id, incarnation_id = %n.incarnation_id.as_ref().map(|i| i.as_str().to_string()).unwrap_or_default(), node = %me, outcome = tracing::field::Empty);
                    if let Err(e) = self.claims.release_in_flight(&path.mesh, &held).await {
                        release.record("outcome", "in-flight-unreleased");
                        return Err(Refusal::Unavailable(format!("{me}: mesh {}'s in-flight row names attempt {} of {} by {claimant}, which deleted; the row could not be released: {e}", path.mesh, held.attempt, held.build_id)));
                    }
                    release.record("outcome", "row-released");
                    release.in_scope(|| tracing::info!("the mesh's in-flight row of a deleted claimant's attempt that holds no lane is released"));
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
            // An unheard mesh-primary's recovery opened here in the fabric lane is claimed for the executor
            // decided above, and the fabric-primary's drive calls build.attempt.run on it: "the
            // fabric-primary opens the attempt ...; the mesh-primary claims it through BuildClaim, and the
            // fabric-primary calls build.attempt.run naming the mesh; the mesh-primary runs it and reports
            // each receipt up that stream, and the fabric-primary writes every Build record"
            // (node-recovery.md "Execution").
            span.record("executor", executor.to_string().as_str());
            let claim = crate::build_state::BuildAttemptClaim { build_id: opened.build_id.clone(), attempt: opened.attempt, executor: executor.to_string(), claimant: Some(me.to_string()), mesh: opened.mesh.clone() };
            let own = self.node_rows.get().zip(self.node_id.get()).filter(|_| executor == me).map(|(rows, id)| (&**rows, id));
            match crate::build_claim::decide_claim(&me, &*self.builds, &*self.claims, &self.contexts, &claim, &path.mesh, own).await {
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

/// What an attempt opened at a node route does to the exact birth it is targeted to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionKind {
    /// `node.connection.delete:<path>`.
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
            Self::DeleteConnections => "node.connection.delete",
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
    // leaves the topology (a removal) or is re-created at the same path.name (node recovery, an executable
    // replacement, the replace's Delete step): R5, node-primary-update.md "No-successor cases".
    if target.kind != crate::model::NodeKind::NodeAdmin {
        let holds = t.node_type_primary(&node.mesh, target.kind).is_some_and(|p| p.node_id == target.node_id);
        let only_of_cohort = t.nodes_of_type(&node.mesh, target.kind).all(|n| n.node_id == target.node_id);
        if !holds || (action == "delete" && only_of_cohort) {
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
    // The delete of a mesh's only node, by a desired-state delete or by the node.delete route (the
    // final primary a mesh.delete left alive, mesh-delete.md step 7), skips the update; a fabric seat on
    // it still moves, out of its mesh (C2 search step 2), and is refused with C2 step 3's text when no
    // other mesh-primary can take it.
    let moving = if action == "delete" && t.only_node(&node.mesh, &target.node_id) {
        if !fabric_seat || (!t.meshes.is_empty() && t.meshes.iter().all(|m| m.status == crate::model::ScopeStatus::Dead)) {
            return None;
        }
        crate::election::Moving::FabricSeatOutside
    } else {
        moving
    };
    let refusal = crate::election::commanded_successor_in(t, &crate::election::Incumbency::default(), target, moving).err()?;
    Some(BuildReject::LastAdmin { node: node.to_string(), mesh: node.mesh.clone(), action: action.to_string(), refusal })
}

/// The first executable a complete-topology request names that cannot launch, refused by name before
/// anything is accepted (proposal executable-bindings-at-create.md §2: "the fabric-primary refuses a
/// missing file or a wrong sha by name at acceptance"; "an executable outside them is refused by name
/// at acceptance"): each binding of a path `next` creates, or replaces because its row records another,
/// in the topology's order, first held, under the container provider, to `executable_dirs` (a
/// containerized admin reads only what is mounted, so its place is checked before its file is read),
/// then checked as its launch checks it: its launch id and image per kind, then its hash. Each distinct
/// file (path and sha) is hashed once, however many kinds share it, and every one at once, each on the
/// blocking pool (rdm#148); no file is read past the first binding refused before its file is read. The
/// refusal is the first binding's in that order, whichever check refuses it.
async fn named_executable_refusal(current: &crate::accepted::FabricTopology, next: &crate::accepted::FabricTopology, recorded: &crate::accepted::Recorded, provider: crate::model::ProviderKind, executable_dirs: &[std::path::PathBuf]) -> Option<BuildReject> {
    let mut checked: std::collections::BTreeSet<(std::path::PathBuf, String, String)> = std::collections::BTreeSet::new();
    let mut to_hash: Vec<(&PathName, &rdm_core::binding::Binding)> = Vec::new();
    let mut refused_unread = None;
    for (path, spec) in next.meshes.values().flat_map(|m| m.spec_by_path.iter()) {
        let Some(binding) = spec.executable.as_ref() else { continue };
        let created = !current.contains(path);
        let replaced = recorded.get(path).is_some_and(|r| r.sha256.as_deref() != Some(binding.sha256.as_str()));
        if !(created || replaced) || !checked.insert((binding.executable.clone(), binding.sha256.clone(), path.kind.name().to_string())) {
            continue;
        }
        if provider == crate::model::ProviderKind::Container && !executable_dirs.iter().any(|d| binding.executable.starts_with(d)) {
            let named = executable_dirs.iter().map(|d| d.display().to_string()).collect::<Vec<_>>().join(",");
            let detail = format!("{} is outside every directory {} names ({named:?}): a containerized admin mounts only those, so no launch could read it", binding.executable.display(), rdm_core::launch::ENV_EXECUTABLE_DIRS);
            refused_unread = Some(BuildReject::ExecutableRefused { node: path.to_string(), detail });
            break;
        }
        if let Err(detail) = crate::admin::runner::binding_fits(binding, path.kind, provider) {
            refused_unread = Some(BuildReject::ExecutableRefused { node: path.to_string(), detail });
            break;
        }
        to_hash.push((path, binding));
    }
    // Each distinct file once, hashed as the first binding naming it (whose refusal names its launch id),
    // spanned with every launch id that shares it.
    let mut files: std::collections::BTreeMap<(&std::path::Path, &str), (&rdm_core::binding::Binding, Vec<&str>)> = std::collections::BTreeMap::new();
    for (_, b) in &to_hash {
        let (_, ids) = files.entry((b.executable.as_path(), b.sha256.as_str())).or_insert((b, Vec::new()));
        if !ids.contains(&b.launch_id.as_str()) {
            ids.push(b.launch_id.as_str());
        }
    }
    let hashed: std::collections::BTreeMap<(&std::path::Path, &str), Result<(), String>> =
        futures_util::future::join_all(files.iter().map(|(file, (b, ids))| async move { (*file, crate::admin::runner::hash_executable(b, &ids.join(","), "request").await) })).await.into_iter().collect();
    to_hash
        .iter()
        .find_map(|(path, b)| hashed.get(&(b.executable.as_path(), b.sha256.as_str())).and_then(|r| r.clone().err()).map(|detail| BuildReject::ExecutableRefused { node: path.to_string(), detail }))
        .or(refused_unread)
}

/// The first path `next` creates that names no executable (proposal executable-bindings-at-create.md
/// rule 1: "a created path whose create spec names no executable is refused by name at acceptance,
/// nothing runs"), whatever change compiled it: a count form creates paths with none.
fn created_without_executable(current: &crate::accepted::FabricTopology, next: &crate::accepted::FabricTopology) -> Option<BuildReject> {
    next.meshes
        .values()
        .flat_map(|m| m.spec_by_path.iter())
        .find(|(path, spec)| !current.contains(path) && spec.executable.is_none())
        .map(|(path, _)| BuildReject::ExecutableMissing { node: path.to_string() })
}

/// The stop/start/delete refusal `node-not-live`, spanned beside the request with the node, the
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
    // One honest name per reason (PRD §16), each a literal at its own arm: rdm.node_admin.build.reject.via-<reason>.
    let span = match reject.reason() {
        "invalid-mesh-name" => tracing::info_span!("rdm.node_admin.build.reject.via-invalid-mesh-name", route, detail = %reject),
        "duplicate-mesh" => tracing::info_span!("rdm.node_admin.build.reject.via-duplicate-mesh", route, detail = %reject),
        "mesh-without-admin" => tracing::info_span!("rdm.node_admin.build.reject.via-mesh-without-admin", route, detail = %reject),
        "unknown-mesh" => tracing::info_span!("rdm.node_admin.build.reject.via-unknown-mesh", route, detail = %reject),
        "unknown-node" => tracing::info_span!("rdm.node_admin.build.reject.via-unknown-node", route, detail = %reject),
        "node-not-live" => tracing::info_span!("rdm.node_admin.build.reject.via-node-not-live", route, detail = %reject),
        "would-leave-mesh-without-admin" => tracing::info_span!("rdm.node_admin.build.reject.via-would-leave-mesh-without-admin", route, detail = %reject),
        "mesh-already-exists" => tracing::info_span!("rdm.node_admin.build.reject.via-mesh-already-exists", route, detail = %reject),
        "fabric-mismatch" => tracing::info_span!("rdm.node_admin.build.reject.via-fabric-mismatch", route, detail = %reject),
        "empty-fabric" => tracing::info_span!("rdm.node_admin.build.reject.via-empty-fabric", route, detail = %reject),
        "provider-mismatch" => tracing::info_span!("rdm.node_admin.build.reject.via-provider-mismatch", route, detail = %reject),
        "unheard-mesh" => tracing::info_span!("rdm.node_admin.build.reject.via-unheard-mesh", route, detail = %reject),
        "node-admin-not-node-recovered" => tracing::info_span!("rdm.node_admin.build.reject.via-node-admin-not-node-recovered", route, detail = %reject),
        "last-admin" => tracing::info_span!("rdm.node_admin.build.reject.via-last-admin", route, detail = %reject),
        "no-secondary" => tracing::info_span!("rdm.node_admin.build.reject.via-no-secondary", route, detail = %reject),
        "node-not-stopped" => tracing::info_span!("rdm.node_admin.build.reject.via-node-not-stopped", route, detail = %reject),
        "referenced-by-open-build" => tracing::info_span!("rdm.node_admin.build.reject.via-referenced-by-open-build", route, detail = %reject),
        "empty-steps" => tracing::info_span!("rdm.node_admin.build.reject.via-empty-steps", route, detail = %reject),
        "step-repeated" => tracing::info_span!("rdm.node_admin.build.reject.via-step-repeated", route, detail = %reject),
        "topology-storage-provider-not-registered" => tracing::info_span!("rdm.node_admin.build.reject.via-topology-storage-provider-not-registered", route, detail = %reject),
        "topology-storage-provider-not-opened" => tracing::info_span!("rdm.node_admin.build.reject.via-topology-storage-provider-not-opened", route, detail = %reject),
        _ => tracing::info_span!("rdm.node_admin.build.reject.via-invalid-intent", route, detail = %reject),
    };
    span.in_scope(|| tracing::info!(%reject, "build refused"));
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
        crate::build::BuildOperation::CreateMesh { mesh, .. } | crate::build::BuildOperation::ShutdownMesh { mesh, .. } | crate::build::BuildOperation::BlockStorage { mesh, .. } | crate::build::BuildOperation::UnblockStorage { mesh, .. } => Some(mesh.clone()),
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
        n.status = NodeStatus::Ready;
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
    /// "a mesh's sole broker ... cannot be stopped or restarted until a secondary is Ready"):
    /// mesh1.rpc.1 holds mesh1's rpc seat with mesh1.rpc.2 beside it, and mesh2.rpc.1 is mesh2's sole
    /// rpc node. The stop of mesh1.rpc.1 moves its seat first (`executor::seat_move` names it) and no
    /// admin executes the stop until it has; the stop of mesh1.rpc.2, which holds no seat, moves nothing
    /// and its mesh's primary executes it. The Build door admits a stop of mesh1.rpc.1 and refuses a
    /// stop of mesh2.rpc.1 by name with Q7's text; its delete, by the node.delete route or the
    /// replace's Delete step, skips the update as its removal does (R5).
    /// Must NOT happen: a cohort holder stopped with its seat unmoved, or the sole node of its type in a
    /// mesh stopped while its mesh has other nodes.
    #[test]
    fn an_rpc_node_type_primary_moves_before_its_stop_and_the_sole_node_of_its_type_is_refused_by_name() {
        let t = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![
                Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready },
                Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready },
            ],
            nodes: vec![node("mesh1.admin.1", true, true), node("mesh1.rpc.1", true, false), node("mesh1.rpc.2", false, false), node("mesh2.admin.1", true, false), node("mesh2.rpc.1", true, false)],
        };
        let stop = |path: &str| crate::build::BuildOperation::StopNode { node: path.parse().unwrap(), node_id: crate::model::NodeId::mint(), from_incarnation: t.node(&path.parse().unwrap()).unwrap().incarnation_id.clone().unwrap() };
        let holder = stop("mesh1.rpc.1");
        assert_eq!(crate::executor::seat_move(&holder, &t).map(|m| m.holder.name.to_string()), Some("mesh1.rpc.1".to_string()), "the rpc seat moves before its holder's stop");
        assert_eq!(crate::executor::executor_for(&holder, &t), None, "no admin executes the holder's stop until its seat moved");
        let other = stop("mesh1.rpc.2");
        assert_eq!(crate::executor::seat_move(&other, &t), None, "a node that holds no seat moves none");
        assert_eq!(crate::executor::executor_for(&other, &t).map(|p| p.to_string()), Some("mesh1.admin.1".to_string()), "its mesh's primary executes it");
        assert!(last_admin(&"mesh1.rpc.1".parse().unwrap(), "stop", &t).is_none(), "mesh1's rpc holder has a secondary: admitted");
        for action in ["stop"] {
            match last_admin(&"mesh2.rpc.1".parse().unwrap(), action, &t) {
                Some(reject) => assert_eq!((reject.reason(), reject.to_string().as_str()), ("no-secondary", "cannot transfer: no secondary of type rpc-node in mesh mesh2"), "{action}"),
                None => panic!("{action} of mesh2's sole rpc node is refused by name"),
            }
        }
        let delete_sole = crate::build::BuildOperation::DeleteLiveNode { node: "mesh2.rpc.1".parse().unwrap(), exited: None, birth: None };
        assert!(last_admin(&"mesh2.rpc.1".parse().unwrap(), "delete", &t).is_none(), "deleting the sole member of a non-admin cohort skips node.primary.update");
        assert_eq!(crate::executor::seat_move(&delete_sole, &t), None, "deleting the sole member of a non-admin cohort moves no seat");
        // R5 (Luke 2026-10-24, arm (a); node-primary-update.md "No-successor cases": "the delete of a
        // type's only node in its mesh, whether the node leaves the topology (a removal) or is re-created
        // at the same `path.name`"): the replace's Delete step and the node.delete route of a
        // type's only node skip the update as a removal does, refused `no-secondary` at no step.
        assert!(last_admin(&"mesh2.rpc.1".parse().unwrap(), "delete", &t).is_none(), "the delete of a type's only node skips node.primary.update: {:?}", last_admin(&"mesh2.rpc.1".parse().unwrap(), "delete", &t));
        let delete_step = crate::build::BuildOperation::Step { step: rdm_core::build::Step::Delete { node: "mesh2.rpc.1".parse().unwrap() }, birth: None, entry: None };
        assert_eq!(crate::executor::seat_move(&delete_step, &t), None, "the Build's delete of a type's only node moves no primary");
    }

    /// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1 "The
    /// no-successor cases": "The one exception is R-MT1's: the delete of a mesh's only node skips the
    /// update"; mesh-delete.md step 7: "A separate node.delete deletes the mesh-primary"): mesh1's final
    /// primary, the only node a mesh.delete left in mesh1 and still its mesh-primary, is deleted through
    /// the node.delete route (`delete`) and through a desired-state delete (`delete`) with no
    /// node.primary.update and no refusal. A stop of that node is refused with Q7's text, and so is the
    /// delete of mesh2's primary while mesh2 still holds another node and no other node-admin. What must
    /// NOT happen: the node.delete mesh-delete.md step 7 names refused `last-admin`.
    #[test]
    fn the_delete_of_a_meshs_only_node_skips_the_update_on_every_delete_route() {
        let t = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![
                Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready },
                Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready },
            ],
            nodes: vec![node("mesh1.admin.1", true, false), node("mesh2.admin.1", true, true), node("mesh2.rpc.1", true, false)],
        };
        let only: PathName = "mesh1.admin.1".parse().unwrap();
        for action in ["delete"] {
            assert!(last_admin(&only, action, &t).is_none(), "{action} of mesh1's only node skips the update: {:?}", last_admin(&only, action, &t));
        }
        let refusal = |path: &str, action: &str| match last_admin(&path.parse().unwrap(), action, &t) {
            Some(BuildReject::LastAdmin { refusal, .. }) => refusal,
            other => panic!("{action} of {path} is refused last-admin: {other:?}"),
        };
        assert_eq!(refusal("mesh1.admin.1", "stop"), "cannot transfer: no secondary of type node-admin in mesh mesh1");
        assert!(crate::executor::seat_move(&crate::build::BuildOperation::StopNode { node: only.clone(), node_id: crate::model::NodeId::mint(), from_incarnation: crate::model::IncarnationId::mint() }, &t).is_some_and(|m| m.mesh_seat), "stop of mesh1's only node never skips the seat move");
        assert_eq!(crate::executor::seat_move(&crate::build::BuildOperation::DeleteLiveNode { node: only, exited: None, birth: None }, &t), None, "the delete of mesh1's only node skips the seat move");
        assert_eq!(refusal("mesh2.admin.1", "delete"), "cannot transfer: no secondary of type node-admin in mesh mesh2");
    }

    /// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C2
    /// "Successor search for the commanded transfer": "2. only when that mesh has none ... the lowest
    /// NodeId among the other Ready mesh-primaries; 3. neither found: refused by name,
    /// `cannot transfer fabric-primary: no other node-admin Ready in mesh <mesh>, no other
    /// mesh-primary Ready` ... the commanding workflow fails with that reason"; C1: "the
    /// delete of a mesh's only node skips the update"): the fabric primary `mesh2.admin.1` is mesh2's
    /// only node. Its delete skips node.primary.update, and its fabric seat moves out of
    /// mesh2: with mesh1's primary Ready the Build door admits them; with no other
    /// mesh-primary Ready the door refuses them `last-admin` with C2 step 3's text.
    /// Must NOT happen: a Build admitted whose fabric seat has nowhere to go.
    #[test]
    fn the_delete_of_a_fabric_primary_that_is_its_meshs_only_node_needs_another_ready_mesh_primary() {
        let mut t = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![
                Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready },
                Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready },
            ],
            nodes: vec![node("mesh1.admin.1", true, false), node("mesh1.rpc.1", true, false), node("mesh2.admin.1", true, true)],
        };
        let fp: PathName = "mesh2.admin.1".parse().unwrap();
        for action in ["delete"] {
            assert!(last_admin(&fp, action, &t).is_none(), "{action}: mesh1's Ready primary takes the fabric seat: {:?}", last_admin(&fp, action, &t));
        }
        for n in t.nodes.iter_mut().filter(|n| n.name.to_string() == "mesh1.admin.1") {
            n.status = NodeStatus::Pending;
        }
        for action in ["delete"] {
            match last_admin(&fp, action, &t) {
                Some(BuildReject::LastAdmin { refusal, .. }) => assert_eq!(refusal, "cannot transfer fabric-primary: no other node-admin Ready in mesh mesh2, no other mesh-primary Ready", "{action}"),
                other => panic!("{action}: no other mesh-primary is Ready: refused last-admin: {other:?}"),
            }
        }
    }

    /// CONTRACT (fabric-atlas.md R-FA2: "execution progress writes it from each attempt's receipts as
    /// they stream up `build.attempt.run`, before the caller is answered `Complete`"; R-FA3: "a delayed
    /// event never moves it backward"; node-start.md "Completion": the mesh-primary's Ready on
    /// the birth; build.md §4.4: a primary's stop has a successor, read from the fabric-primary's view):
    /// the fabric-primary took the `Complete` receipt of `node.start:mesh2.admin.2` (attempt 16) into
    /// its atlas. Its view with the atlas applied holds mesh2.admin.2 ready, so a stop of
    /// mesh2's primary mesh2.admin.1 has a successor, both while the backbone projection still holds the
    /// birth stopped and once mesh2's projection published at the start's `node.joined` (Pending, the
    /// stop over) reaches it after the start's `Complete`, as #3224's spans show it (v32 installed at
    /// +14469, the stop refused `last-admin` at +14485, v33 Ready at +14501). An observer's
    /// liveness mark on the birth after its start (`Dead`) stands. Must NOT happen: the stop refused
    /// `cannot transfer: no secondary of type node-admin in mesh mesh2` because the projection's
    /// Pending, published before the start completed, stood over the start's receipt.
    #[test]
    fn a_node_admin_whose_start_completed_is_its_primarys_successor_while_the_projection_still_holds_it_pending() {
        use crate::accepted::{AttemptAction, FabricTopology};
        use crate::build_state::{AttemptReason, BuildProjection, BuildState, BuildStepReceipt, StepOutcome};
        let t = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![
                Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready },
                Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready },
            ],
            nodes: vec![node("mesh1.admin.1", true, true), node("mesh2.admin.1", true, false), node("mesh2.admin.2", false, false)],
        };
        let started: PathName = "mesh2.admin.2".parse().unwrap();
        let primary: PathName = "mesh2.admin.1".parse().unwrap();
        let from = t.node(&started).unwrap().incarnation_id.clone().unwrap();
        let build_id = crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap();
        let start = AttemptAction::Start { path: started.clone(), node_id: t.node(&started).unwrap().node_id.clone(), from_incarnation: from.clone() };
        let operation = format!("node.start:{started}");
        let p = BuildProjection {
            build_id: build_id.clone(),
            topology: FabricTopology { section_of: None, storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: Default::default(), pinned: Default::default() },
            submitted_change: None,
            submitted_at_ms: 0,
            state: BuildState::Pending,
            attempt: 16,
            executor: None,
            steps: vec![BuildStepReceipt { mesh: None, build_id, attempt: 16, operation: operation.clone(), step: "Complete".into(), outcome: StepOutcome::Complete, output: None, executor: None }],
            last_failure: None,
            reason: AttemptReason::Start,
            action: Some(start.clone()),
            attempt_actions: [(16, start)].into_iter().collect(),
            attempt_executors: Default::default(),
            opened: 16,
            failed_at: None,
            completed_at: None,
            reported: Default::default(),
            claimant: None,
            lanes: Default::default(),
        };
        let atlas = crate::atlas::Atlas::default();
        assert_eq!(atlas.take_receipt(&p, 16, &operation), crate::atlas::Taken::Effect(Some("started")), "the start's Complete receipt is the atlas's");
        let viewed = |status: NodeStatus, stopped: bool| {
            let mut view = t.clone();
            for n in view.nodes.iter_mut().filter(|n| n.name == started) {
                n.status = status;
                n.stopped = stopped;
                n.routable = false;
            }
            atlas.apply(&mut view);
            view
        };
        for (status, stopped, projection) in [(NodeStatus::Leaving, true, "the projection carrying its stop"), (NodeStatus::Pending, false, "the projection published at its node.joined, the stop over")] {
            let view = viewed(status, stopped);
            let n = view.node(&started).unwrap();
            assert_eq!((n.status, n.stopped), (NodeStatus::Ready, false), "{projection}: the started birth is ready in the fabric-primary's view with the atlas applied");
            assert!(last_admin(&primary, "stop", &view).is_none(), "{projection}: the stop of mesh2's primary has its successor: {:?}", last_admin(&primary, "stop", &view));
        }
        let view = viewed(NodeStatus::Dead, false);
        assert_eq!(view.node(&started).unwrap().status, NodeStatus::Dead, "an observer's liveness mark after the start stands");
        assert!(matches!(last_admin(&primary, "stop", &view), Some(BuildReject::LastAdmin { .. })), "a Dead birth is no successor");
    }

    /// CONTRACT (proposal executable-bindings-at-create.md §2: "the fabric-primary refuses a missing file or a
    /// wrong sha by name at acceptance"; rule 4, the launch hashes again): every executable a complete-topology
    /// request creates a node on, or replaces a node's with, is checked as its launch checks it, before
    /// anything is accepted: a hash-true file of the path's kind passes; a missing file, a sha its bytes do not
    /// hash to, and a binding of another kind are each refused `executable-refused` naming the path. A path
    /// whose row already records the executable is not hashed again. Must NOT happen: a Build accepted onto
    /// an executable no launch could run.
    #[tokio::test]
    async fn every_executable_a_request_creates_or_replaces_a_node_on_is_hashed_at_acceptance() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("named-executable-{}-{}", std::process::id(), rand::random::<u32>()));
        std::fs::create_dir_all(&dir).unwrap();
        let bind = |id: &str| {
            let p = dir.join(id);
            std::fs::write(&p, format!("#!/bin/sh\n# {id}\n")).unwrap();
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
            rdm_core::binding::Binding { launch_id: id.into(), sha256: rdm_core::binding::sha256_file(&p).unwrap(), executable: p, image: None }
        };
        let current = crate::accepted::FabricTopology { section_of: None, pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: [("mesh1".to_string(), crate::accepted::MeshTopology::of(&crate::build::MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)])))].into() };
        let with = |path: &str, b: rdm_core::binding::Binding| {
            let mut next = current.clone();
            let p: PathName = path.parse().unwrap();
            let m = next.meshes.get_mut("mesh1").unwrap();
            m.nodes.insert(p.clone());
            let mut spec = current.meshes["mesh1"].spec_by_path.get(&p).cloned().unwrap_or(rdm_core::meta::NodeSpec { path: p.clone(), storage_persistence: rdm_core::meta::StorageMeta::Ephemeral, placement: Default::default(), executable: None, tags: Default::default(), config: Default::default() });
            spec.executable = Some(b);
            m.spec_by_path.insert(p, spec);
            next
        };
        let rows = crate::accepted::Recorded::new();
        let good = bind("rpc_node");
        assert_eq!(named_executable_refusal(&current, &with("mesh1.rpc.2", good.clone()), &rows, ProviderKind::Process, &[]).await, None, "a hash-true file of the path's kind");
        let refused = |r: Option<BuildReject>| match r {
            Some(BuildReject::ExecutableRefused { node, detail }) => (node, detail),
            other => panic!("refused executable-refused: {other:?}"),
        };
        let missing = rdm_core::binding::Binding { executable: dir.join("absent"), ..good.clone() };
        let wrong = rdm_core::binding::Binding { sha256: "0".repeat(64), ..good.clone() };
        let admin = bind("node_admin");
        for (b, what) in [(missing, "a missing file"), (wrong, "a sha its bytes do not hash to"), (admin.clone(), "a binding of another kind")] {
            let (node, detail) = refused(named_executable_refusal(&current, &with("mesh1.rpc.2", b), &rows, ProviderKind::Process, &[]).await);
            assert_eq!(node, "mesh1.rpc.2", "{what}: {detail}");
        }
        // A held path whose row already records the executable is not hashed again: its file may be gone.
        let gone = rdm_core::binding::Binding { executable: dir.join("gone"), ..admin.clone() };
        let recorded: crate::accepted::Recorded = [("mesh1.admin.1".parse().unwrap(), crate::accepted::RecordedRow { config: None, tags: None, storage_persistence: None, placement: None, node_id: crate::model::NodeId::mint(), sha256: Some(gone.sha256.clone()) })].into();
        assert_eq!(named_executable_refusal(&current, &with("mesh1.admin.1", gone), &recorded, ProviderKind::Process, &[]).await, None);
        assert_eq!(BuildReject::ExecutableRefused { node: "x".into(), detail: "y".into() }.reason(), "executable-refused");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A topology of mesh1 holding one node-admin, and that topology with each `(path, binding)` created.
    fn creating(created: &[(&str, rdm_core::binding::Binding)]) -> (crate::accepted::FabricTopology, crate::accepted::FabricTopology) {
        let current = crate::accepted::FabricTopology { section_of: None, pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: [("mesh1".to_string(), crate::accepted::MeshTopology::of(&crate::build::MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1)])))].into() };
        let mut next = current.clone();
        let m = next.meshes.get_mut("mesh1").unwrap();
        for (path, b) in created {
            let p: PathName = path.parse().unwrap();
            m.nodes.insert(p.clone());
            m.spec_by_path.insert(p.clone(), rdm_core::meta::NodeSpec { path: p, storage_persistence: rdm_core::meta::StorageMeta::Ephemeral, placement: Default::default(), executable: Some(b.clone()), tags: Default::default(), config: Default::default() });
        }
        (current, next)
    }

    /// An executable file `name` in `dir`, padded (sparse) to `len` bytes, bound as `launch_id`.
    fn bound_file(dir: &std::path::Path, name: &str, launch_id: &str, len: u64) -> rdm_core::binding::Binding {
        use std::os::unix::fs::PermissionsExt;
        let p = dir.join(name);
        std::fs::write(&p, format!("#!/bin/sh\n# {name}\n")).unwrap();
        std::fs::OpenOptions::new().write(true).open(&p).unwrap().set_len(len).unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
        rdm_core::binding::Binding { launch_id: launch_id.into(), sha256: rdm_core::binding::sha256_file(&p).unwrap(), executable: p, image: None }
    }

    /// CONTRACT (rdm#148; build-deployment.md: every create names its executable, checked at acceptance
    /// as its launch checks it): a request creating a broker, a gateway and a compute node on the one
    /// file the three kinds share hashes that file once at acceptance: one
    /// `rdm.node_admin.deployment.resolve.via-executable-hash` span with `from = request`, and each
    /// kind's launch id is still checked. Must NOT happen: the shared file read once per kind.
    #[tokio::test]
    async fn a_file_several_kinds_share_is_hashed_once_at_acceptance() {
        let dir = crate::test_support::tempdir("hash-once-shared");
        let node = bound_file(&dir, "rdm-node", "broker", 4 << 20);
        let as_kind = |id: &str| rdm_core::binding::Binding { launch_id: id.into(), ..node.clone() };
        let (current, next) = creating(&[("mesh1.broker.1", as_kind("broker")), ("mesh1.gateway.1", as_kind("gateway")), ("mesh1.compute.1", as_kind("compute"))]);
        let capture = crate::span_capture::Capture::new("rdm148-hash-once-shared");
        let refused = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            named_executable_refusal(&current, &next, &crate::accepted::Recorded::new(), ProviderKind::Process, &[]).await
        };
        assert_eq!(refused, None, "the shared file is hash-true for every kind");
        let hashes: Vec<_> = capture.named("rdm.node_admin.deployment.resolve.via-executable-hash").into_iter().filter(|s| s.attributes.get("from").map(String::as_str) == Some("request")).collect();
        assert_eq!(hashes.len(), 1, "one hash of the one file the three kinds share: {hashes:#?}");
        assert_eq!(hashes[0].attributes.get("outcome").map(String::as_str), Some("hashed"), "{hashes:#?}");
        // Each kind's launch id is still checked: the shared file bound for the node-admin at a broker path.
        let (current, next) = creating(&[("mesh1.broker.1", as_kind("broker")), ("mesh1.gateway.1", as_kind("node_admin"))]);
        match named_executable_refusal(&current, &next, &crate::accepted::Recorded::new(), ProviderKind::Process, &[]).await {
            Some(BuildReject::ExecutableRefused { node, detail }) => assert_eq!(node, "mesh1.gateway.1", "{detail}"),
            other => panic!("refused executable-refused at mesh1.gateway.1: {other:?}"),
        }
        eprintln!("span rdm.node_admin.deployment.resolve.via-executable-hash trace_id={} {:?}", hashes[0].trace_id, hashes[0].attributes);
    }

    /// CONTRACT (rdm#148): a request creating nodes on three distinct files hashes them at once at
    /// acceptance, each on the blocking pool (Q30): three
    /// `rdm.node_admin.deployment.resolve.via-executable-hash` spans with `from = request`, every one open
    /// while every other is (the latest start before the earliest end). Must NOT happen: the files hashed
    /// one after another, the acceptance waiting out the sum of their hashes.
    #[tokio::test]
    async fn distinct_files_a_request_names_are_hashed_at_once_at_acceptance() {
        let dir = crate::test_support::tempdir("hash-once-distinct");
        let (current, next) = creating(&[("mesh1.rpc.1", bound_file(&dir, "rpc", "rpc_node", 64 << 20)), ("mesh1.broker.1", bound_file(&dir, "broker", "broker", 64 << 20)), ("mesh1.gateway.1", bound_file(&dir, "gateway", "gateway", 64 << 20))]);
        let capture = crate::span_capture::Capture::new("rdm148-hash-once-distinct");
        let refused = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            named_executable_refusal(&current, &next, &crate::accepted::Recorded::new(), ProviderKind::Process, &[]).await
        };
        assert_eq!(refused, None, "every file is hash-true for its kind");
        let hashes: Vec<_> = capture.named("rdm.node_admin.deployment.resolve.via-executable-hash").into_iter().filter(|s| s.attributes.get("from").map(String::as_str) == Some("request")).collect();
        assert_eq!(hashes.len(), 3, "one hash per distinct file: {hashes:#?}");
        let (latest_start, earliest_end) = (hashes.iter().map(|s| s.start).max().unwrap(), hashes.iter().map(|s| s.end).min().unwrap());
        assert!(latest_start < earliest_end, "the three hashes overlap in time: {:#?}", hashes.iter().map(|s| (s.attributes.get("executable"), s.start, s.end)).collect::<Vec<_>>());
        eprintln!("span rdm.node_admin.deployment.resolve.via-executable-hash trace_id={} {:?}", hashes[0].trace_id, hashes.iter().map(|s| s.attributes.get("elapsed_ms")).collect::<Vec<_>>());
    }

    /// CONTRACT (rdm#148: the first refusal in the same order as before the hashes ran at once): of
    /// several refused executables the one at the first path in the topology's order is returned,
    /// whichever check refuses it: an earlier path's wrong sha before a later path's wrong launch id,
    /// and an earlier path's wrong launch id before a later path's missing file. Must NOT happen: the
    /// refusal of whichever hash finished first.
    #[tokio::test]
    async fn the_first_refused_executable_in_path_order_is_the_refusal() {
        let dir = crate::test_support::tempdir("hash-once-order");
        let (b, g) = (bound_file(&dir, "broker", "broker", 1 << 20), bound_file(&dir, "gateway", "gateway", 1 << 20));
        let wrong_sha = rdm_core::binding::Binding { sha256: "0".repeat(64), ..b.clone() };
        let wrong_kind = rdm_core::binding::Binding { launch_id: "compute".into(), ..g.clone() };
        let missing = rdm_core::binding::Binding { executable: dir.join("absent"), ..g.clone() };
        let rows = crate::accepted::Recorded::new();
        // mesh1.broker.1 sorts before mesh1.gateway.1.
        for (created, first, what) in [
            ([("mesh1.broker.1", wrong_sha.clone()), ("mesh1.gateway.1", wrong_kind.clone())], "mesh1.broker.1", "a wrong sha before a wrong launch id"),
            ([("mesh1.broker.1", rdm_core::binding::Binding { launch_id: "compute".into(), ..b.clone() }), ("mesh1.gateway.1", missing.clone())], "mesh1.broker.1", "a wrong launch id before a missing file"),
            ([("mesh1.broker.1", b.clone()), ("mesh1.gateway.1", missing.clone())], "mesh1.gateway.1", "a later missing file once the earlier path passes"),
        ] {
            let (current, next) = creating(&created);
            assert_eq!(next.meshes["mesh1"].spec_by_path.keys().find(|p| p.kind != NodeKind::NodeAdmin).map(|p| p.to_string()).as_deref(), Some("mesh1.broker.1"), "the topology's order");
            match named_executable_refusal(&current, &next, &rows, ProviderKind::Process, &[]).await {
                Some(BuildReject::ExecutableRefused { node, detail }) => assert_eq!(node, first, "{what}: {detail}"),
                other => panic!("{what}: refused executable-refused: {other:?}"),
            }
        }
    }

    /// CONTRACT (proposal executable-bindings-at-create.md §2: "A containerized admin's executable
    /// directories become its launch configuration, `RDM_EXECUTABLE_DIRS` ...; an executable outside them
    /// is refused by name at acceptance"; build-deployment.md §2.5: "before anything destructive"): under
    /// the container provider an executable a request creates a node on is refused `executable-refused`,
    /// naming the path and `RDM_EXECUTABLE_DIRS`, when it lies outside every configured directory, before
    /// its file is read (a containerized fabric-primary cannot read outside its mounts, so a hash first
    /// would misreport it missing); with none configured, every executable is outside. One inside a
    /// configured directory is accepted. Under the process provider the directories are not consulted.
    /// Must NOT happen: a container fabric accepting a Build whose launch could not read its executable.
    #[tokio::test]
    async fn an_executable_outside_the_executable_dirs_is_refused_by_name_under_the_container_provider() {
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!("executable-dirs-{}-{}", std::process::id(), rand::random::<u32>()));
        let (inside, outside) = (root.join("inside"), root.join("outside"));
        std::fs::create_dir_all(&inside).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let bind = |dir: &std::path::Path, image: Option<&str>| {
            let p = dir.join("rpc_node");
            std::fs::write(&p, "#!/bin/sh\n# rpc_node\n").unwrap();
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
            rdm_core::binding::Binding { launch_id: "rpc_node".into(), sha256: rdm_core::binding::sha256_file(&p).unwrap(), executable: p, image: image.map(str::to_string) }
        };
        let image = Some(crate::deployment::container::RUNTIME_IMAGE);
        let current = crate::accepted::FabricTopology { section_of: None, pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: [("mesh1".to_string(), crate::accepted::MeshTopology::of(&crate::build::MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 0)])))].into() };
        let with = |b: rdm_core::binding::Binding| {
            let mut next = current.clone();
            let p: PathName = "mesh1.rpc.1".parse().unwrap();
            let m = next.meshes.get_mut("mesh1").unwrap();
            m.nodes.insert(p.clone());
            m.spec_by_path.insert(p.clone(), rdm_core::meta::NodeSpec { path: p, storage_persistence: rdm_core::meta::StorageMeta::Ephemeral, placement: Default::default(), executable: Some(b), tags: Default::default(), config: Default::default() });
            next
        };
        let rows = crate::accepted::Recorded::new();
        let dirs = vec![inside.clone()];
        assert_eq!(named_executable_refusal(&current, &with(bind(&inside, image)), &rows, ProviderKind::Container, &dirs).await, None, "inside a configured directory");
        let out = bind(&outside, image);
        for (configured, what) in [(dirs.clone(), "outside every configured directory"), (Vec::new(), "none configured")] {
            match named_executable_refusal(&current, &with(out.clone()), &rows, ProviderKind::Container, &configured).await {
                Some(BuildReject::ExecutableRefused { node, detail }) => {
                    assert_eq!(node, "mesh1.rpc.1", "{what}: {detail}");
                    assert!(detail.contains("RDM_EXECUTABLE_DIRS") && detail.contains(&out.executable.display().to_string()), "{what}: {detail}");
                }
                other => panic!("{what}: refused executable-refused: {other:?}"),
            }
        }
        // Outside, and absent: refused for its place, never misreported as missing.
        let absent = rdm_core::binding::Binding { executable: root.join("elsewhere").join("rpc_node"), ..out.clone() };
        match named_executable_refusal(&current, &with(absent), &rows, ProviderKind::Container, &dirs).await {
            Some(BuildReject::ExecutableRefused { detail, .. }) => assert!(detail.contains("RDM_EXECUTABLE_DIRS"), "{detail}"),
            other => panic!("refused executable-refused naming RDM_EXECUTABLE_DIRS: {other:?}"),
        }
        assert_eq!(named_executable_refusal(&current, &with(bind(&outside, None)), &rows, ProviderKind::Process, &dirs).await, None, "the process provider reads the host");
        let _ = std::fs::remove_dir_all(&root);
    }

    /// CONTRACT (proposal executable-bindings-at-create.md rule 1: "a created path whose config names no
    /// executable is refused by name at acceptance, nothing runs"; §2: "every create now names its
    /// executable"): whatever change compiled the next topology, a path it creates whose entry names no
    /// executable is refused `executable-missing` naming the path: a count form (`AddNode`) creates one.
    /// A held path naming none (a Build written before the field) and a created path naming one pass.
    /// Must NOT happen: a create accepted with nothing naming what it runs.
    #[test]
    fn a_path_created_naming_no_executable_is_refused_by_name_whatever_the_change() {
        let current = crate::accepted::FabricTopology { section_of: None, pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: [("mesh1".to_string(), crate::accepted::MeshTopology::of(&crate::build::MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 1), (NodeKind::RpcNode, 1)])))].into() };
        let observed = crate::topology::Topology { fabric: crate::model::Fabric { id: crate::model::FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: ProviderKind::Process }, meshes: vec![], nodes: vec![] };
        let grown = crate::accepted::compile(&current, &crate::accepted::TopologyChange::AddNode { mesh: "mesh1".into(), node_kind: NodeKind::RpcNode }, &observed).unwrap();
        assert_eq!(created_without_executable(&current, &grown), Some(BuildReject::ExecutableMissing { node: "mesh1.rpc.2".into() }));
        assert_eq!(created_without_executable(&current, &current), None, "held paths naming none are history");
        let mut named = grown.clone();
        named.meshes.get_mut("mesh1").unwrap().spec_by_path.get_mut(&"mesh1.rpc.2".parse().unwrap()).unwrap().executable = Some(rdm_core::binding::Binding { launch_id: "rpc_node".into(), executable: "/x/rpc".into(), sha256: "0".repeat(64), image: None });
        assert_eq!(created_without_executable(&current, &named), None);
        assert_eq!(BuildReject::ExecutableMissing { node: "x".into() }.reason(), "executable-missing");
    }
}
