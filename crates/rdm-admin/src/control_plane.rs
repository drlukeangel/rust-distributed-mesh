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
use crate::model::{IncarnationId, NodeId, PathName};
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
    /// This admin's path and name: topology is accepted only while it is the fabric-primary.
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
    /// fabric-primary's path and name, in the fabric lane.
    pub fabric_recovery: std::sync::OnceLock<crate::fabric_recovery::Reader>,
    /// This admin's primary view: a node-admin it holds as its mesh's primary replaced by the election is
    /// an unheard mesh-primary, the fabric-primary's to recover (node-recovery.md step 2).
    pub primaries: std::sync::OnceLock<rdm_transport::topology::PrimaryView>,
    /// How long a peer mesh has gone unheard on the backbone, in milliseconds, once at or past the
    /// staleness floor (`DigestView::mesh_unheard`; gossip.md: its mesh-primary is marked silent at the
    /// floor, as any birth): a node of a mesh held unheard is not restarted, stopped or deleted from
    /// here (R-D1). `None` for a mesh heard within the floor or never heard on the backbone. Set once by
    /// the admin that owns the digest view.
    pub mesh_unheard: std::sync::OnceLock<Arc<dyn Fn(&str) -> Option<u64> + Send + Sync>>,
    /// Node investigation of one exact birth (node-investigation.md; Q107 item 1), the one primitive:
    /// gate 5 admits a delete of a birth neither stopped nor heard live only on its `exited` verdict
    /// (build.md §4.4; Q219). Set once by the admin that runs it; unset, no verdict is held.
    pub investigation: std::sync::OnceLock<Arc<dyn crate::path_investigation::PathProbe>>,
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
    pub departure: std::sync::OnceLock<Arc<dyn crate::build_drive::ExecutorInspection>>,
    /// The node rows (nodes.storage, every mesh's rows on the fabric's shared root), set once the admin
    /// opens them: a complete-topology request is compiled against what each row records of its
    /// executable (proposal executable-bindings-at-create.md rule 5, R4).
    pub node_rows: std::sync::OnceLock<Arc<dyn crate::storage::NodesStorage>>,
    /// The node id this admin was born with, set with `node_rows`: a recovery it opens and a claim it
    /// decides for itself are refused by name while the rows hold this birth deleted (Q181,
    /// [`crate::node_rows::deleted_birth`]).
    pub node_id: std::sync::OnceLock<crate::model::NodeId>,
    /// The incarnation established by this process at boot.
    pub incarnation: std::sync::OnceLock<crate::model::IncarnationId>,
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
            mesh_unheard: std::sync::OnceLock::new(),
            investigation: std::sync::OnceLock::new(),
            fabric_recovery: std::sync::OnceLock::new(),
            primaries: std::sync::OnceLock::new(),
            contexts: Arc::new(crate::build_claim::AttemptContexts::in_memory()),
            claims: Arc::new(crate::storage::MemoryMeshStorage::default()),
            time,
            admission: Arc::new(tokio::sync::Mutex::new(())),
            departure: std::sync::OnceLock::new(),
            node_rows: std::sync::OnceLock::new(),
            node_id: std::sync::OnceLock::new(),
            incarnation: std::sync::OnceLock::new(),
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

    /// This admin's currently held `(path, name)`: what its Build state or its view names its birth,
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
        let primary = self.topology.read().await.fabric_primary().map(|n| n.node_id.clone());
        if !primary.as_ref().is_some_and(|id| self.node_id.get() == Some(id)) {
            tracing::info_span!("rdm.node_admin.build.reject.via-fabric-primary-required", route, node_path = %me.path, node_name = %me.name, fabric_primary = %primary.as_ref().map(|p| p.to_string()).unwrap_or_default())
                .in_scope(|| tracing::info!("topology is changed only by the fabric-primary"));
            return Err(Refusal::NotPrimary(primary));
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
    pub(crate) async fn submit_checked(&self, span: tracing::Span, route: &'static str, mut change: TopologyChange, collect: Option<&crate::stopped_gc::Candidate>) -> Result<Opened, Refusal> {
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
            // A storage change in progress is the fabric-primary's own workflow, never a Build: every
            // Build but fabric.delete is refused `build-in-progress` beside it (storage.md §2 "Changing it").
            if !deletes {
                if let Some(running) = self.storage_changes.get().and_then(|c| c.in_progress()) {
                    outer.in_scope(|| {
                        tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", route, change_id = %running.change_id, held_by = %self.node_id.get().map(|id| id.as_str()).unwrap_or(""))
                            .in_scope(|| tracing::info!("a storage change is in progress: no Build is admitted beside it"))
                    });
                    return Err(Refusal::StorageChangeInProgress { change_id: running.change_id, held_by: self.node_id.get().cloned() });
                }
            }
            let (_admitted, current) = if deletes { self.current_read().await? } else { self.current_settled().await? };
            if deletes {
                // A storage change in progress is ended before anything of the delete is written: its
                // rdm.storage.committed event read, every mesh-primary sent mesh.unblock onto the provider
                // it decided, and its rdm.storage.ended event written failed (storage.md §2: fabric.delete
                // "interrupts the change as it interrupts any Build").
                if let Some(changes) = self.storage_changes.get() {
                    changes.interrupt_for_delete().await;
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
                TopologyChange::Reconcile { based_on, .. } => {
                    if based_on != &current.build_id {
                        let reject = BuildReject::BasedOnNotCurrent { based_on: based_on.to_string(), current: current.build_id.to_string() };
                        outer.in_scope(|| reject_span(route, &reject));
                        return Err(Refusal::Reject(reject));
                    }
                    let rows = self.node_rows.get().ok_or_else(|| Refusal::Unavailable(format!("path={} name={}: no nodes.storage is open yet to read what each node's row records", self.me.path, self.me.name)))?;
                    crate::accepted::recorded_executables(&**rows, &current.topology).await.map_err(Refusal::Unavailable)?
                }
                _ => crate::accepted::Recorded::new(),
            };
            // The Fabric's types (Day 0's `RDM_NODE_TYPES` on its record): what a created node's type is
            // read against, and what fills a created node's executable when it names none.
            let declared = self.accepted.types().await.map_err(|e| Refusal::Unavailable(format!("fabric.storage: {e}")))?;
            with_type_defaults(&current.topology, &mut change, &declared);
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
            self.apply_atlas(&mut observed).await?;
            // node.delete's target (node-delete.md "Operation contract"; build.md §4.4 gate 5): a live
            // birth, a stopped one, or one whose exact runtime node investigation reads exited
            // ([`crate::build::delete_admits`]). A birth the view marks Dead with neither fact is refused
            // `node-not-live` by name: silence never says the runtime exited (Q219).
            let mut exited_pin: Option<(PathName, crate::build::PinnedBirth)> = None;
            if let TopologyChange::RemoveNode { node } = &change {
                if let Some(candidate) = collect {
                    let exact = observed.node(node).is_some_and(|n| n.node_id == candidate.node_id && n.incarnation_id.as_ref() == Some(&candidate.incarnation) && n.stopped);
                    if !exact {
                        let reject = BuildReject::NodeNotStopped { node: node.clone(), found: "the admitted removal location no longer names the scanned stopped birth".into() };
                        outer.in_scope(|| reject_span(route, &reject));
                        return Err(Refusal::Reject(reject));
                    }
                }
                if let Some(n) = self.atlas_row(node, &observed).await? {
                // The node's mesh held by its own attempt is the gate before the node's own gates
                // (build.md §4.4): refused naming the mesh, whatever the node's state.
                if let Some(held) = holding(&current, Some(&n.mesh)) {
                    outer.in_scope(|| {
                        tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", route, build_id = %held.current_build_id.log(), mesh = %n.mesh, held_by = %held.held_by.as_ref().map(|id| id.as_str()).unwrap_or(""), node_path = %node.path, node_name = %node.name)
                            .in_scope(|| tracing::info!("the node's mesh is held by its own drift attempt: one attempt in flight per mesh"))
                    });
                    return Err(Refusal::BuildInProgress(held));
                }
                    match self.delete_gate(&n, &observed, &outer).await {
                        Some(crate::build::DeleteAdmitted::Exited) => exited_pin = n.incarnation.clone().map(|incarnation| (node.clone(), crate::build::PinnedBirth { r#type: n.r#type.clone(), mesh: n.mesh.clone(), node_id: n.node_id.clone(), incarnation })),
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
            // The fabric's topology_storage_provider at the acceptance, the Fabric row's: a Build never
            // changes it (storage.md §2: a storage change is fabric.storage.update, never a Build).
            topology.topology_storage_provider = self.accepted.topology_storage_provider().await.map_err(|e| Refusal::Unavailable(format!("fabric.storage: {e}")))?;
            // The Build's id, minted once.
            let build_id = BuildId::mint();
            // Every create names its executable (build-deployment.md §2.1): whatever change compiled it, a
            // path created with none is refused by name, nothing accepted.
            if let Some(reject) = created_of_unknown_type(&current.topology, &topology, &declared).or_else(|| created_without_executable(&current.topology, &topology)) {
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
            let held: std::collections::BTreeSet<String> = topology.meshes.keys().filter(|m| observed.node(&self.me).is_none_or(|own| **m != own.mesh) && self.mesh_unheard.get().and_then(|f| f(m)).is_some()).cloned().collect();
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
                    tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", route, build_id = %held.current_build_id.log(), mesh = %held.mesh.as_deref().unwrap_or(""), held_by = %held.held_by.as_ref().map(|id| id.as_str()).unwrap_or(""))
                        .in_scope(|| tracing::info!("a topology operation acts in a mesh its own drift attempt holds: one attempt in flight per mesh"))
                });
                return Err(Refusal::BuildInProgress(held));
            }
            // Gate 6 (build.md §4.4; Q221): every node the Build acts on is of a mesh the fabric-primary's
            // view hears, refused by name naming the mesh and how long it has gone unheard (R-D1). Each
            // operation is checked again before its claim or dispatch (the drive's own check).
            if let Some(reject) = operations.iter().chain(replacements.iter()).find_map(|op| {
                let n = op.node()?;
                let mesh = op.mesh()?;
                if observed.node(&self.me).is_some_and(|own| own.mesh == mesh) { return None; }
                let unheard_ms = self.mesh_unheard.get().and_then(|f| f(mesh))?;
                Some(BuildReject::UnheardMesh { node: n.clone(), mesh: mesh.to_string(), unheard_ms })
            }) {
                outer.in_scope(|| reject_span(route, &reject));
                return Err(Refusal::Reject(reject));
            }
            // A Build whose plan would end a primary while no other node of its type in its mesh can
            // take its role is refused before it exists: node.primary.update has no successor (R-MT1, Q7).
            if let Some(reject) = operations.iter().find_map(|op| {
                let (node, action) = match op {
                    crate::build::BuildOperation::DeleteLiveNode { node, .. } => (node, "delete"),
                    crate::build::BuildOperation::DeleteConnections { node, .. } => (node, "node.connection.delete"),
                    crate::build::BuildOperation::StopNode { node, .. } => (node, "stop"),
                    _ => return None,
                };
                last_admin(node, action, &observed, observed.node(&self.me).map(|n| n.mesh.as_str()).unwrap_or_default())
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
            // step can move the fabric-primary role (a delete of this admin's own mesh), which refuses every gated
            // write after it, so the door answers the Build it accepted whatever the drive does next.
            let me = self.held_name().await;
            crate::build_state::accept(&*self.builds, &accepted, Some(&current.build_id), self.node_id.get().ok_or_else(|| Refusal::Unavailable("accepting admin ID is not bound".into()))?.as_str(), "accepted").await.map_err(Refusal::State)?;
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

    /// Open the next attempt of `current` with `action`, putting its context first.
    async fn open_with(&self, current: &crate::build_state::BuildProjection, reason: AttemptReason, action: AttemptAction, route: &'static str, outer: &tracing::Span, span: tracing::Span) -> Result<Opened, Refusal> {
        let opener = self.node_id.get().cloned().ok_or_else(|| Refusal::Unavailable("attempt opener node ID is not bound".into()))?;
        let opened = AttemptOpened {
            mesh: None,
            build_id: current.build_id.clone(),
            attempt: current.attempt + 1,
            reason,
            action: Some(action),
            opened_by: opener,
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
                        tracing::info_span!("rdm.node_admin.build.reject.via-attempt-taken", route, build_id = %current.build_id.log(), attempt = opened.attempt, action = ?opened.action, detail = %e)
                            .in_scope(|| tracing::info!(detail = %e, "attempt refused: another action holds the number"))
                    });
                    Refusal::AttemptTaken(e.to_string())
                }
                e => Refusal::State(e),
            });
        }
        tracing::info!(build_id = %current.build_id.log(), attempt = opened.attempt, "attempt opened");
        if let Some(action) = &opened.action {
            self.atlas.note_accepted(&opened.build_id, opened.attempt, action);
        }
        self.build_submitted.notify_waiters();
        Ok(Opened { build_id: current.build_id.clone(), attempt: opened.attempt, mesh: opened.mesh })
    }

    /// Open the next attempt of the accepted Build as `steps` (`BuildChange::Steps`;
    /// build.md §1: "Rafka's replace steps (one Build of node primitives ...)";
    /// lifecycles/node-replace.md), under `span`. The topology is unchanged; the Build stays the
    /// current one. Refused by name: a Build with no step, one naming an operation twice, or one
    /// acting on a path that holds no birth and that no earlier step makes
    /// (`crate::accepted::steps_action`); a step in a mesh held by its own drift attempt (R-BI7
    /// caveat 3); a step on a node of a peer mesh this fabric-primary holds unheard (R-D1); a
    /// node.connection.delete, stop or delete of a primary while no other node of its type in its
    /// mesh can take its role (R-MT1, Q7).
    pub async fn open_steps_in(&self, span: tracing::Span, route: &'static str, steps: Vec<rdm_core::build::Step>) -> Result<Opened, Refusal> {
        let outer = tracing::Span::current();
        async {
            self.require_fabric_primary(route).await?;
            let (_admitted, current) = self.current_settled().await?;
            let mut t = match self.view_now.get() {
                Some(project) => project().await,
                None => self.topology.read().await.clone(),
            };
            // Each birth as the ops left it (R-FA1): a primary's successor, a step's node.
            self.apply_atlas(&mut t).await?;
            // A node stands at every path a step acts on, before its mesh is read heard and before a
            // primary's successor is looked for (build.md §4.4's order).
            let action = match crate::accepted::steps_action(steps.clone(), &t) {
                Ok(a) => a,
                Err(reject) => {
                    outer.in_scope(|| reject_span(route, &reject));
                    return Err(Refusal::Reject(reject));
                }
            };
            let AttemptAction::Steps { pinned, .. } = &action else { unreachable!("steps action") };
            let births = crate::accepted::step_births(&steps, pinned).map_err(Refusal::Reject)?;
            for (step, (_, mesh, _)) in steps.iter().zip(&births) {
                if let Some(held) = holding(&current, Some(mesh)) { return Err(Refusal::BuildInProgress(held)); }
                let node = step.node();
                if t.node(&self.me).is_none_or(|own| own.mesh != *mesh) {
                    if let Some(unheard_ms) = self.mesh_unheard.get().and_then(|f| f(mesh)) {
                        let reject = BuildReject::UnheardMesh { node: node.clone(), mesh: mesh.clone(), unheard_ms };
                        outer.in_scope(|| reject_span(route, &reject));
                        return Err(Refusal::Reject(reject));
                    }
                }
                let step_action = match step {
                    rdm_core::build::Step::Stop { .. } => Some("stop"),
                    rdm_core::build::Step::Delete { .. } => Some("delete"),
                    // A primary's roles move at its node.connection.delete { All }, the first step of
                    // a replace (node-connections-delete.md step 0; node-replace.md step 1).
                    rdm_core::build::Step::DeleteConnections { .. } => Some("node.connection.delete"),
                    _ => None,
                };
                if let Some(reject) = step_action.and_then(|a| last_admin(node, a, &t, t.node(&self.me).map(|n| n.mesh.as_str()).unwrap_or_default())) {
                    outer.in_scope(|| reject_span(route, &reject));
                    return Err(Refusal::Reject(reject));
                }
            }
            drop(t);
            self.open_with(&current, AttemptReason::Steps, action, route, &outer, span.clone()).await
        }
        .instrument(span.clone())
        .await
    }

    /// Open the next attempt of the current Build with the delete of one birth the topology never names
    /// (a stopped retained predecessor, or the last admin of a deleted mesh), under `span`, which names
    /// the surface the request came through. The topology is unchanged; the Build stays the current one.
    pub async fn open_delete_in(&self, span: tracing::Span, route: &'static str, path: PathName) -> Result<Opened, Refusal> {
        let outer = tracing::Span::current();
        async {
            self.require_fabric_primary(route).await?;
            let (_admitted, current) = self.current_settled().await?;
            let mut t = match self.view_now.get() {
                Some(project) => project().await,
                None => self.topology.read().await.clone(),
            };
            self.apply_atlas(&mut t).await?;
            // build.md §4.4, "A node stands at the path": none stands there, refused by name on its span.
            let Some(n) = self.atlas_row(&path, &t).await? else {
                let reject = BuildReject::UnknownNode { node: path.clone(), step: None };
                outer.in_scope(|| reject_span(route, &reject));
                return Err(Refusal::Reject(reject));
            };
            let mesh = n.mesh.clone();
            if let Some(held) = holding(&current, Some(&mesh)) {
                return Err(Refusal::BuildInProgress(held));
            }
            // The pair resolves through its name claim to the node's record; a node a rename moved off its
            // accepted entry (a replacement's retained predecessor) is acted on as that entry's node,
            // whatever its new name spells.
            let predecessor = n.entry_left(&current.topology).is_some();
            let deleted_mesh = !predecessor
                && !current.topology.meshes.contains_key(&mesh)
                && t.meshes.iter().any(|m| m.name == mesh && m.status == crate::model::ScopeStatus::Dead);
            if !(predecessor || deleted_mesh) {
                let reject = BuildReject::UnknownNode { node: path.clone(), step: None };
                outer.in_scope(|| reject_span(route, &reject));
                return Err(Refusal::Reject(reject));
            }
            let (node_id, from_incarnation) = {
                // Gate 5 for a delete ([`crate::build::delete_admits`]): the stopped predecessor, or one whose
                // exit node investigation proved, or the last admin of a deleted mesh, live or stopped: its
                // node.delete stops it first. A Dead mark alone admits nothing (Q219).
                let deletable = match self.delete_gate(&n, &t, &outer).await {
                    Some(crate::build::DeleteAdmitted::Stopped | crate::build::DeleteAdmitted::Exited) => true,
                    Some(crate::build::DeleteAdmitted::Live) => deleted_mesh,
                    None => false,
                };
                if !deletable {
                    return Err(not_live(&outer, route, &path, n.status, "a delete of a retained predecessor needs that stopped birth or one whose exit is proven; any other delete through this route is of a deleted mesh's birth"));
                }
                // A node of a peer mesh this fabric-primary holds unheard is not its to delete: the mesh's
                // own primary, once heard again, reconciles it (R-D1).
                if mesh != t.node(&self.me).map(|n| n.mesh.as_str()).unwrap_or_default() {
                    if let Some(unheard_ms) = self.mesh_unheard.get().and_then(|f| f(&mesh)) {
                        let reject = BuildReject::UnheardMesh { node: path.clone(), mesh: mesh.clone(), unheard_ms };
                        outer.in_scope(|| reject_span(route, &reject));
                        return Err(Refusal::Reject(reject));
                    }
                }
                // A primary's role moves by node.primary.update to another node-admin of its mesh;
                // with none, the update has no other node of its type and the delete is refused (R-MT1).
                if let Some(reject) = last_admin(&path, ActionKind::Delete.name(), &t, t.node(&self.me).map(|n| n.mesh.as_str()).unwrap_or_default()) {
                    outer.in_scope(|| reject_span(route, &reject));
                    return Err(Refusal::Reject(reject));
                }
                // The exact birth the gates pinned (build.md §4.4): its node id and incarnation, which the
                // executing mesh-primary verifies before any effect.
                match n.incarnation.clone() {
                    Some(incarnation) => (n.node_id.clone(), incarnation),
                    None => return Err(not_live(&outer, route, &path, n.status, "the node holds no incarnation id to pin the attempt to")),
                }
            };
            let action = AttemptAction::Delete { r#type: n.r#type.clone(), mesh, path, node_id, from_incarnation };
            self.open_with(&current, AttemptReason::Delete, action, route, &outer, span.clone()).await
        }
        .instrument(span.clone())
        .await
    }
}

/// A caller's node command refused by the primary above the node before anything is sent to the
/// node, by name (node-stop.md, node-start.md; Q342, D40): `reason` is the refusal's name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandRefusal {
    /// `unknown-node`, `node-not-live`, `no-secondary`, `receiver-not-primary`, `rejected-stale-incarnation`.
    pub reason: &'static str,
    /// What was refused, for the reader.
    pub detail: String,
    /// The incarnation this primary holds, for `rejected-stale-incarnation`.
    pub held: Option<IncarnationId>,
}

impl ControlPlane {
    /// The admission of a caller's node command `kind` on the exact birth `(node_id, incarnation)` at
    /// this admin, the primary above the node: its mesh-primary, the fabric-primary for a mesh-primary
    /// (node-stop.md, node-start.md "Entry", "Admission"; Q342, D40). A node command is never a Build or
    /// part of one: nothing is opened and the current Build is neither read for it nor holds it. The
    /// birth the command acts on is answered with its `(path, name)`, read from this admin's atlas
    /// (fabric-atlas.md R-FA1). Refused by name, nothing written: `unknown-node` (no record of the
    /// node), `rejected-stale-incarnation` (another birth of it), `receiver-not-primary` (this admin is not
    /// the primary above it), `node-not-live` (a stop or a connections delete needs a live birth, a start
    /// a stopped one), `no-secondary` (a primary's stop with no other node of its type to take its role,
    /// R-MT1).
    pub async fn admit_node_command(&self, node_id: &NodeId, incarnation: &IncarnationId, kind: ActionKind) -> Result<(PathName, crate::build::PinnedBirth), CommandRefusal> {
        let refuse = |reason: &'static str, detail: String| CommandRefusal { reason, detail, held: None };
        let mut t = match self.view_now.get() {
            Some(project) => project().await,
            None => self.topology.read().await.clone(),
        };
        self.apply_atlas(&mut t).await.map_err(|r| refuse("not-ready", format!("{r:?}")))?;
        let Some(n) = self.atlas_row_id(node_id, &t).await.map_err(|r| refuse("not-ready", format!("{r:?}")))? else {
            return Err(refuse("unknown-node", format!("this primary holds no record of node {node_id}")));
        };
        if n.incarnation.as_ref() != Some(incarnation) {
            return Err(CommandRefusal { reason: "rejected-stale-incarnation", detail: format!("node {node_id} holds incarnation {:?}, not {}", n.incarnation.as_ref().map(|i| i.as_str()), incarnation.as_str()), held: n.incarnation.clone() });
        }
        let me = self.node_id.get().cloned();
        let mesh_primary = t.node_type_primary(&n.mesh, rdm_core::NODE_ADMIN).is_some_and(|p| p.node_id == n.node_id);
        let above = if mesh_primary { t.fabric_primary().map(|fp| fp.node_id.clone()) } else { t.node_type_primary(&n.mesh, rdm_core::NODE_ADMIN).map(|p| p.node_id.clone()) };
        if above.is_none() || above != me {
            return Err(refuse("receiver-not-primary", format!("node {node_id} at path {:?}, name {:?} is commanded by {}, the {} above it", n.name.path, n.name.name, above.map(|a| a.to_string()).unwrap_or_else(|| "no primary in this view".into()), if mesh_primary { "fabric-primary" } else { "mesh-primary" })));
        }
        let path = n.name.clone();
        let refused_state = match kind {
            ActionKind::Start if !n.stopped => Some("a start needs a stopped node"),
            ActionKind::Stop if !n.live => Some("a stop needs a live node"),
            ActionKind::DeleteConnections if !n.live => Some("node.connection.delete needs a live node"),
            _ => None,
        };
        if let Some(reason) = refused_state {
            let outer = tracing::Span::current();
            let _ = not_live(&outer, "node-command", &path, n.status, reason);
            return Err(refuse("node-not-live", format!("node {node_id} at path {:?}, name {:?} is {:?}: {reason}", path.path, path.name, n.status)));
        }
        if kind != ActionKind::Start {
            let own_mesh = t.nodes.iter().find(|n| Some(&n.node_id) == me.as_ref()).map(|n| n.mesh.clone()).unwrap_or_default();
            if let Some(reject) = last_admin(&path, kind.name(), &t, &own_mesh) {
                reject_span("node-command", &reject);
                return Err(refuse(reject.reason(), reject.to_string()));
            }
        }
        Ok((path, crate::build::PinnedBirth { r#type: n.r#type.clone(), mesh: n.mesh.clone(), node_id: n.node_id.clone(), incarnation: incarnation.clone() }))
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
                    let mut b = crate::model::Node::allocated(n.name.clone(), n.mesh.clone(), &n.r#type);
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
        outer.in_scope(|| tracing::info!(node_path = %n.name.path, node_name = %n.name.name, node_id = %n.node_id, verdict, detail = %detail, admitted = admitted.map(|a| a.name()).unwrap_or("none"), "gate 5: the birth is neither stopped nor heard live, so node investigation of its exact birth decides the delete"));
        admitted
    }

    /// Whether `path` names a birth the accepted topology never names there, deleted by the next attempt
    /// of the accepted Build: a node a rename moved off its accepted entry (its record's provenance names
    /// the entry, [`crate::atlas::AtlasRow::entry_left`]), or a birth of a mesh deleted at mesh.deleted, as
    /// the live view holds it: the mesh's status is deleted (its row Dead), and its last admin lives on
    /// until its own node.delete (mesh-delete.md steps 6-7).
    pub async fn of_deleted_mesh(&self, path: &PathName) -> Result<bool, Refusal> {
        let t = match self.view_now.get() {
            Some(project) => project().await,
            None => self.topology.read().await.clone(),
        };
        let Some(n) = self.atlas_row(path, &t).await? else { return Ok(false) };
        // A node a rename moved off the entry the accepted topology names (the pair resolved through its
        // claim to the node's record, whose provenance names the entry): the topology never names its pair.
        let current = self.builds.current().await.map_err(Refusal::State)?;
        let left = current.as_ref().is_some_and(|c| n.entry_left(&c.topology).is_some());
        Ok(left || t.meshes.iter().any(|m| m.name == n.mesh && m.status == crate::model::ScopeStatus::Dead))
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

/// What an attempt opened at a node route does to the exact birth it is targeted to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionKind {
    /// `node.connection.delete:<path>`.
    DeleteConnections,
    /// `node.stop:<path>`.
    Stop,
    /// `node.start:<path>`.
    Start,
    /// `node.delete` targeting the retained predecessor by node ID.
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

/// The refusal of a `action` of `node` when `node` holds its mesh's primary role or the fabric-primary role in
/// `t` and its node.primary.update has no successor (the approved proposal
/// node-primary-update-fabric-mesh-and-node-type.md, Part C1 "The no-successor cases": "refused by name,
/// `cannot transfer: no secondary of type <type> in mesh <mesh>`, until a secondary is available
/// ... the workflow that commanded it ... fails by name with that reason. The one exception is
/// R-MT1's: the delete of a mesh's only node skips the update"), or, for that delete, when the fabric
/// role it holds has no other mesh-primary to go to (C2 step 3): the same search the holder's
/// pre-check runs (`election::commanded_successor`), so nothing is admitted and no attempt opens. A
/// node of any other type that holds its node-type primary role is refused [`BuildReject::NoSecondary`] when no
/// other node of its type in its mesh can take the role (`election::commanded_node_type_successor`, Q7).
/// `own_mesh` is the mesh of the admin deciding: the mesh-primary of another mesh is the writer of its
/// own mesh's records and pre-checks its successor from its own view (node-primary-update.md step 2;
/// D64), so this admin sends the command down and decides nothing for it from its projection.
fn last_admin(node: &PathName, action: &str, t: &Topology, own_mesh: &str) -> Option<BuildReject> {
    let target = t.node(node)?;
    // The primary of any other node type: its mesh-primary's node.primary.update needs another node
    // of its type (Q7); the delete of a type's only node in its mesh skips the update, whether the node
    // leaves the topology (a removal) or is re-created at the same path and name (node recovery, an executable
    // replacement, the replace's Delete step): R5, node-primary-update.md "No-successor cases".
    if !rdm_core::is_node_admin(&target.r#type) {
        let holds = t.node_type_primary(&target.mesh, &target.r#type).is_some_and(|p| p.node_id == target.node_id);
        let only_of_node_type = t.nodes_of_type(&target.mesh, &target.r#type).all(|n| n.node_id == target.node_id);
        if !holds || (action == "delete" && only_of_node_type) {
            return None;
        }
        let nodes: Vec<crate::model::Node> = t.members().cloned().collect();
        let refusal = crate::election::commanded_node_type_successor(&nodes, &crate::election::Incumbency::default(), target).err()?;
        return Some(BuildReject::NoSecondary { node: node.clone(), mesh: target.mesh.clone(), action: action.to_string(), refusal });
    }
    let fabric_role = t.fabric_primary().is_some_and(|fp| fp.node_id == target.node_id);
    let mesh_role = t.node_type_primary(&target.mesh, "node-admin").is_some_and(|p| p.node_id == target.node_id);
    let moving = match (mesh_role, fabric_role) {
        (true, true) => crate::election::Moving::BothRoles,
        (true, false) if target.mesh != own_mesh => return None,
        (true, false) => crate::election::Moving::MeshRole,
        _ => return None,
    };
    // The delete of a mesh's only node, by a desired-state delete or by the node.delete route (the
    // final primary a mesh.delete left alive, mesh-delete.md step 7), skips the update; a fabric-primary role on
    // it still moves, out of its mesh (C2 search step 2), and is refused with C2 step 3's text when no
    // other mesh-primary can take it.
    let moving = if action == "delete" && t.only_node(&target.mesh, &target.node_id) {
        if !fabric_role || (!t.meshes.is_empty() && t.meshes.iter().all(|m| m.status == crate::model::ScopeStatus::Dead)) {
            return None;
        }
        crate::election::Moving::FabricRoleOutside
    } else {
        moving
    };
    let refusal = crate::election::commanded_successor_in(t, &crate::election::Incumbency::default(), target, moving).err()?;
    Some(BuildReject::LastAdmin { node: node.clone(), mesh: target.mesh.clone(), action: action.to_string(), refusal })
}

/// The first executable a complete-topology request names that cannot launch, refused by name before
/// anything is accepted (proposal executable-bindings-at-create.md §2: "the fabric-primary refuses a
/// missing file or a wrong sha by name at acceptance"; "an executable outside them is refused by name
/// at acceptance"): each binding of a path `next` creates, or replaces because its row records another,
/// in the topology's order, first held, under the container provider, to `executable_dirs` (a
/// containerized admin reads only what is mounted, so its place is checked before its file is read),
/// then checked as its launch checks it: its launch id and image per type, then its hash. Each distinct
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
        let replaced = recorded.get(path).is_some_and(|r| r.mesh != spec.mesh || r.r#type != spec.r#type || r.sha256.as_deref() != Some(binding.sha256.as_str()));
        if !(created || replaced) || !checked.insert((binding.executable.clone(), binding.sha256.clone(), spec.r#type.to_string())) {
            continue;
        }
        if provider == crate::model::ProviderKind::Container && !executable_dirs.iter().any(|d| binding.executable.starts_with(d)) {
            let named = executable_dirs.iter().map(|d| d.display().to_string()).collect::<Vec<_>>().join(",");
            let detail = format!("{} is outside every directory {} names ({named:?}): a containerized admin mounts only those, so no launch could read it", binding.executable.display(), rdm_core::launch::ENV_EXECUTABLE_DIRS);
            refused_unread = Some(BuildReject::ExecutableRefused { node: path.clone(), detail });
            break;
        }
        if let Err(detail) = crate::admin::runner::binding_fits(binding, &spec.r#type, provider) {
            refused_unread = Some(BuildReject::ExecutableRefused { node: path.clone(), detail });
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
        .find_map(|(path, b)| hashed.get(&(b.executable.as_path(), b.sha256.as_str())).and_then(|r| r.clone().err()).map(|detail| BuildReject::ExecutableRefused { node: (*path).clone(), detail }))
        .or(refused_unread)
}

/// A node a complete-topology request creates that names no executable of its own takes its declared
/// type's (node-identity-and-state.md §2: each declared type overrides `node`'s defaults, `executable`
/// among them), before the request is compiled.
fn with_type_defaults(current: &crate::accepted::FabricTopology, change: &mut TopologyChange, declared: &[rdm_core::NodeType]) {
    if let TopologyChange::Reconcile { nodes, .. } = change {
        for spec in nodes.iter_mut().filter(|s| s.executable.is_none() && !current.contains(&s.path)) {
            spec.executable = rdm_core::NodeType::of(declared, &spec.r#type).and_then(|t| t.executable.clone());
        }
    }
}

/// The first node `next` creates whose type is neither `node` nor `node-admin` and that `declared`, the
/// Fabric's types, does not name: refused by name, nothing accepted (node-identity-and-state.md §2).
fn created_of_unknown_type(current: &crate::accepted::FabricTopology, next: &crate::accepted::FabricTopology, declared: &[rdm_core::NodeType]) -> Option<BuildReject> {
    next.meshes
        .values()
        .flat_map(|m| m.spec_by_path.iter())
        .find(|(path, spec)| !current.contains(path) && rdm_core::NodeType::undeclared(declared, &spec.r#type).is_some())
        .map(|(path, spec)| BuildReject::UnknownType { node: path.clone(), r#type: spec.r#type.clone() })
}

/// The first path `next` creates that names no executable (proposal executable-bindings-at-create.md
/// rule 1: "a created path whose create spec names no executable is refused by name at acceptance,
/// nothing runs"), whatever change compiled it: a count form creates paths with none.
fn created_without_executable(current: &crate::accepted::FabricTopology, next: &crate::accepted::FabricTopology) -> Option<BuildReject> {
    next.meshes
        .values()
        .flat_map(|m| m.spec_by_path.iter())
        .find(|(path, spec)| !current.contains(path) && spec.executable.is_none())
        .map(|(path, _)| BuildReject::ExecutableMissing { node: path.clone() })
}

/// The stop/start/delete refusal `node-not-live`, spanned beside the request with the node, the
/// status the live view holds for it and why that status refuses the operation
/// (`rdm.node_admin.node.reject.via-not-live`).
fn not_live(outer: &tracing::Span, route: &'static str, path: &PathName, status: crate::model::NodeStatus, reason: &'static str) -> Refusal {
    let held = serde_json::to_value(status).ok().and_then(|v| v.as_str().map(str::to_string)).unwrap_or_else(|| format!("{status:?}"));
    let reject = BuildReject::NodeNotLive { node: path.clone() };
    outer.in_scope(|| {
        tracing::info_span!("rdm.node_admin.node.reject.via-not-live", route, node_path = %path.path, node_name = %path.name, status = %held, reason, detail = %reject)
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
        "unknown-type" => tracing::info_span!("rdm.node_admin.build.reject.via-unknown-type", route, detail = %reject),
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
    /// A storage change is in progress (storage.md §2 "Changing it"): the fabric-primary's own workflow,
    /// beside which no Build but fabric.delete is admitted, refused `build-in-progress` by name.
    StorageChangeInProgress {
        /// The change in progress.
        change_id: rdm_core::ChangeId,
        /// The fabric-primary running it.
        held_by: Option<crate::model::NodeId>,
    },
    /// This admin may not open or claim on the mesh the request acts in: a topology attempt is the
    /// fabric-primary's, a drift attempt its mesh's primary's (R-BI7 caveat 2), as its view holds them.
    NotClaimant {
        /// The mesh the request acts in.
        mesh: String,
        /// The fabric-primary this admin's view holds, when one.
        fabric_primary: Option<crate::model::NodeId>,
        /// The mesh's primary this admin's view holds (the member's recoverer), when one.
        mesh_primary: Option<crate::model::NodeId>,
    },
    /// This admin is not the fabric-primary (named when known).
    NotPrimary(Option<crate::model::NodeId>),
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
    /// later role view); `None` for an attempt not yet claimed or claimed before claimants were
    /// recorded.
    pub held_by: Option<crate::model::NodeId>,
}

/// The attempt of `p` holding `mesh` in this admin's fold (R-BI7 caveat 3), or, for `None`, the
/// fabric lane's attempt in progress named by the mesh it acts in: the fabric lane's claimed
/// (running) attempt acts in its executor's mesh; a drift lane's open attempt holds its own mesh.
/// A fabric-lane attempt opened and not yet claimed holds no mesh: its claim takes the mesh's row.
fn holding(p: &crate::build_state::BuildProjection, mesh: Option<&str>) -> Option<InProgress> {
    let fabric_mesh = (p.state == BuildState::Running).then(|| p.attempt_claimed_meshes.get(&p.attempt).cloned()).flatten();
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
            drift_attempt_holding(p, mesh)
        }
    }
}

/// `mesh`'s own drift lane's open attempt of `p`, which holds the mesh whatever the fabric lane holds.
fn drift_attempt_holding(p: &crate::build_state::BuildProjection, mesh: &str) -> Option<InProgress> {
    p.lanes.get(mesh).filter(|l| l.is_open()).map(|l| InProgress { current_build_id: p.build_id.clone(), mesh: Some(mesh.to_string()), held_by: l.claimant.clone() })
}

/// The refusal of a second topology change while `current` is in progress (build.md §1, R-BI7): named
/// by the mesh its claimed attempt acts in and its claimant, when claimed.
fn settled(current: &crate::build_state::BuildProjection) -> Result<(), Refusal> {
    if !matches!(current.state, BuildState::Pending | BuildState::Running) {
        return Ok(());
    }
    let held = holding(current, None).unwrap_or_else(|| InProgress { current_build_id: current.build_id.clone(), mesh: None, held_by: None });
    tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", build_id = %current.build_id.log(), state = ?current.state, mesh = %held.mesh.as_deref().unwrap_or(""), held_by = %held.held_by.as_ref().map(|id| id.as_str()).unwrap_or(""))
        .in_scope(|| tracing::info!("another Build is reconciling: one topology Build in flight"));
    Err(Refusal::BuildInProgress(held))
}

/// The mesh `op` acts in.
fn op_mesh(op: &crate::build::BuildOperation) -> Option<String> {
    match op {
        crate::build::BuildOperation::CreateMesh { mesh, .. } | crate::build::BuildOperation::ShutdownMesh { mesh, .. } => Some(mesh.clone()),
        other => other.mesh().map(str::to_owned),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Fabric, FabricId, Mesh, MeshId, Node, NodeStatus, ProviderKind, ScopeStatus};

    fn node(name: PathName, mesh: &str, r#type: &str, primary: bool, fabric: bool) -> Node {
        let mut n = Node::allocated(name, mesh, r#type);
        n.status = NodeStatus::Ready;
        n.incarnation_id = Some(crate::model::IncarnationId::mint());
        n.is_primary = primary;
        n.is_fabric_primary = fabric;
        n
    }

    /// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1: "One
    /// primitive moves the primary of any (mesh, type) cohort ... A mesh-primary commands the
    /// primaries of its mesh's other cohorts (Q3)"; step 1: "The primary is commanded from above, as a
    /// step of the Build that stops, deletes, restarts, replaces ..."; Q7, ruled: "`node.primary.update`
    /// for a type with no other node of that type in the mesh is refused by name, `cannot transfer: no
    /// secondary of type <type> in mesh <mesh>` ... the commanding workflow fails with that reason";
    /// "a mesh's sole broker ... cannot be stopped or restarted until a secondary is Ready"):
    /// mesh1.node.1 holds mesh1's rpc role with mesh1.node.2 beside it, and mesh2.node.1 is mesh2's sole
    /// node. The stop of mesh1.node.1 moves its role first (`executor::role_move` names it) and no
    /// admin executes the stop until it has; the stop of mesh1.node.2, which holds no role, moves nothing
    /// and its mesh's primary executes it. The Build door admits a stop of mesh1.node.1 and refuses a
    /// stop of mesh2.node.1 by name with Q7's text; its delete, by the node.delete route or the
    /// replace's Delete step, skips the update as its removal does (R5).
    /// Must NOT happen: a node-type primary stopped with its role unmoved, or the sole node of its type in a
    /// mesh stopped while its mesh has other nodes.
    #[test]
    fn an_rpc_node_type_primary_moves_before_its_stop_and_the_sole_node_of_its_type_is_refused_by_name() {
        let t = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![
                Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready },
                Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready },
            ],
            nodes: vec![node(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", true, true), node(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", true, false), node(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node", false, false), node(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", true, false), node(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", true, false)],
        };
        let stop = |path: PathName| crate::build::BuildOperation::StopNode { mesh: t.node(&path).unwrap().mesh.clone(), r#type: t.node(&path).unwrap().r#type.clone(), node: path.clone(), node_id: t.node(&path).unwrap().node_id.clone(), from_incarnation: t.node(&path).unwrap().incarnation_id.clone().unwrap() };
        let primary_stop = stop(PathName::new("mesh1.node", "1").unwrap());
        assert_eq!(crate::executor::role_move(&primary_stop, &t).map(|m| m.primary.name.clone()), Some(PathName::new("mesh1.node", "1").unwrap()), "the rpc role moves before its primary's stop");
        assert_eq!(crate::executor::executor_for(&primary_stop, &t), None, "no admin executes the primary's stop until its role moved");
        let other = stop(PathName::new("mesh1.node", "2").unwrap());
        assert_eq!(crate::executor::role_move(&other, &t), None, "a node that holds no primary role moves none");
        assert_eq!(crate::executor::executor_for(&other, &t), Some(t.node(&PathName::new("mesh1.node-admin", "1").unwrap()).unwrap().node_id.clone()), "its mesh's primary executes it");
        assert!(last_admin(&PathName::new("mesh1.node", "1").unwrap(), "stop", &t, "mesh1").is_none(), "mesh1's rpc primary has a secondary: admitted");
        for action in ["stop"] {
            match last_admin(&PathName::new("mesh2.node", "1").unwrap(), action, &t, "mesh1") {
                Some(reject) => assert_eq!((reject.reason(), reject.to_string().as_str()), ("no-secondary", "cannot transfer: no secondary of type node in mesh mesh2"), "{action}"),
                None => panic!("{action} of mesh2's sole node is refused by name"),
            }
        }
        let delete_sole = crate::build::BuildOperation::DeleteLiveNode { mesh: "mesh2".into(), r#type: "node".into(), node: PathName::new("mesh2.node", "1").unwrap(), exited: None, birth: None };
        assert!(last_admin(&PathName::new("mesh2.node", "1").unwrap(), "delete", &t, "mesh1").is_none(), "deleting the sole member of a non-admin node type skips node.primary.update");
        assert_eq!(crate::executor::role_move(&delete_sole, &t), None, "deleting the sole member of a non-admin node type moves no role");
        // R5 (Luke 2026-10-24, arm (a); node-primary-update.md "No-successor cases": "the delete of a
        // type's only node in its mesh, whether the node leaves the topology (a removal) or is re-created
        // at the same `(path, name)`"): the replace's Delete step and the node.delete route of a
        // type's only node skip the update as a removal does, refused `no-secondary` at no step.
        assert!(last_admin(&PathName::new("mesh2.node", "1").unwrap(), "delete", &t, "mesh1").is_none(), "the delete of a type's only node skips node.primary.update: {:?}", last_admin(&PathName::new("mesh2.node", "1").unwrap(), "delete", &t, "mesh1"));
        let delete_step = crate::build::BuildOperation::Step { rename_intent: crate::accepted::RenameIntent::Ordinary, recipe_step: Some(0), mesh: "mesh2".into(), r#type: "node".into(), step: rdm_core::build::Step::Delete { node: PathName::new("mesh2.node", "1").unwrap() }, birth: None, entry: None };
        assert_eq!(crate::executor::role_move(&delete_step, &t), None, "the Build's delete of a type's only node moves no primary");
    }

    /// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C1 "The
    /// no-successor cases": "The one exception is R-MT1's: the delete of a mesh's only node skips the
    /// update"; mesh-delete.md step 7: "A separate node.delete deletes the mesh-primary"): mesh1's final
    /// primary, the only node a mesh.delete left in mesh1 and still its mesh-primary, is deleted through
    /// the node.delete route (`delete`) and through a desired-state delete (`delete`) with no
    /// node.primary.update and no refusal. A stop of that node, decided by the fabric-primary of mesh2, is
    /// sent down, since mesh1's primary pre-checks its own successor (node-primary-update.md step 2; D64);
    /// the delete of mesh2's primary, the fabric-primary itself, while mesh2 still holds another node and
    /// no other node-admin, is refused with Q7's text. What must NOT happen: the node.delete
    /// mesh-delete.md step 7 names refused `last-admin`.
    #[test]
    fn the_delete_of_a_meshs_only_node_skips_the_update_on_every_delete_route() {
        let t = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![
                Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready },
                Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready },
            ],
            nodes: vec![node(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", true, false), node(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", true, true), node(PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", true, false)],
        };
        let only: PathName = PathName::new("mesh1.node-admin", "1").unwrap();
        for action in ["delete"] {
            assert!(last_admin(&only, action, &t, "mesh2").is_none(), "{action} of mesh1's only node skips the update: {:?}", last_admin(&only, action, &t, "mesh2"));
        }
        let refusal = |path: PathName, action: &str| match last_admin(&path, action, &t, "mesh2") {
            Some(BuildReject::LastAdmin { refusal, .. }) => refusal,
            other => panic!("{action} of {path:?} is refused last-admin: {other:?}"),
        };
        // The fabric-primary (mesh2's) sends a stop of mesh1's primary down: mesh1's primary, the writer of
        // its mesh's records, pre-checks its successor over its own view and refuses by name
        // (node-primary-update.md step 2; D64). The fabric-primary decides nothing for it from its projection.
        assert!(last_admin(&PathName::new("mesh1.node-admin", "1").unwrap(), "stop", &t, "mesh2").is_none(), "the stop of another mesh's primary is that primary's own pre-check");
        assert!(crate::executor::role_move(&crate::build::BuildOperation::StopNode { mesh: "mesh1".into(), r#type: "node-admin".into(), node: only.clone(), node_id: t.node(&only).unwrap().node_id.clone(), from_incarnation: t.node(&only).unwrap().incarnation_id.clone().unwrap() }, &t).is_some_and(|m| m.mesh_role), "stop of mesh1's only node never skips the role move");
        assert_eq!(crate::executor::role_move(&crate::build::BuildOperation::DeleteLiveNode { mesh: "mesh1".into(), r#type: "node-admin".into(), node: only, exited: None, birth: None }, &t), None, "the delete of mesh1's only node skips the role move");
        assert_eq!(refusal(PathName::new("mesh2.node-admin", "1").unwrap(), "delete"), "cannot transfer: no secondary of type node-admin in mesh mesh2");
    }

    /// CONTRACT (the approved proposal node-primary-update-fabric-mesh-and-node-type.md, Part C2
    /// "Successor search for the commanded transfer": "2. only when that mesh has none ... the lowest
    /// NodeId among the other Ready mesh-primaries; 3. neither found: refused by name,
    /// `cannot transfer fabric-primary: no other node-admin Ready in mesh <mesh>, no other
    /// mesh-primary Ready` ... the commanding workflow fails with that reason"; C1: "the
    /// delete of a mesh's only node skips the update"): the fabric primary `mesh2.node-admin.1` is mesh2's
    /// only node. Its delete skips node.primary.update, and its fabric role moves out of
    /// mesh2: with mesh1's primary Ready the Build door admits them; with no other
    /// mesh-primary Ready the door refuses them `last-admin` with C2 step 3's text.
    /// Must NOT happen: a Build admitted whose fabric-primary role has nowhere to go.
    #[test]
    fn the_delete_of_a_fabric_primary_that_is_its_meshs_only_node_needs_another_ready_mesh_primary() {
        let mut t = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![
                Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready },
                Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::Ready },
            ],
            nodes: vec![node(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin", true, false), node(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", true, false), node(PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin", true, true)],
        };
        let fp: PathName = PathName::new("mesh2.node-admin", "1").unwrap();
        for action in ["delete"] {
            assert!(last_admin(&fp, action, &t, "mesh2").is_none(), "{action}: mesh1's Ready primary takes the fabric-primary role: {:?}", last_admin(&fp, action, &t, "mesh2"));
        }
        for n in t.nodes.iter_mut().filter(|n| n.name == PathName::new("mesh1.node-admin", "1").unwrap()) {
            n.status = NodeStatus::Pending;
        }
        for action in ["delete"] {
            match last_admin(&fp, action, &t, "mesh2") {
                Some(BuildReject::LastAdmin { refusal, .. }) => assert_eq!(refusal, "cannot transfer fabric-primary: no other node-admin Ready in mesh mesh2, no other mesh-primary Ready", "{action}"),
                other => panic!("{action}: no other mesh-primary is Ready: refused last-admin: {other:?}"),
            }
        }
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
        let current = crate::accepted::FabricTopology { section_of: None, pinned: Vec::new(), topology_storage_provider: None, fabric: "fabric1".into(), meshes: [("mesh1".to_string(), crate::accepted::MeshTopology::of(&crate::build::MeshDesired::of("mesh1", [("node-admin", 1), ("node", 1)])))].into() };
        let with = |p: PathName, r#type: &str, b: rdm_core::binding::Binding| {
            let mut next = current.clone();
            let m = next.meshes.get_mut("mesh1").unwrap();
            m.nodes.insert(p.clone());
            let mut spec = current.meshes["mesh1"].spec_by_path.get(&p).cloned().unwrap_or(rdm_core::meta::NodeSpec { mesh: "mesh1".into(), r#type: r#type.to_string(), path: p.clone(), storage_persistence: rdm_core::meta::StoragePersistence::Ephemeral, executable: None, tags: Default::default(), config: Default::default() });
            spec.executable = Some(b);
            m.spec_by_path.insert(p, spec);
            next
        };
        let rows = crate::accepted::Recorded::new();
        let good = bind("node");
        assert_eq!(named_executable_refusal(&current, &with(PathName::new("mesh1.node", "2").unwrap(), "node", good.clone()), &rows, ProviderKind::Process, &[]).await, None, "a hash-true file of the path's kind");
        let refused = |r: Option<BuildReject>| match r {
            Some(BuildReject::ExecutableRefused { node, detail }) => (node, detail),
            other => panic!("refused executable-refused: {other:?}"),
        };
        let missing = rdm_core::binding::Binding { executable: dir.join("absent"), ..good.clone() };
        let wrong = rdm_core::binding::Binding { sha256: "0".repeat(64), ..good.clone() };
        let admin = bind("node-admin");
        for (b, what) in [(missing, "a missing file"), (wrong, "a sha its bytes do not hash to"), (admin.clone(), "a binding of another kind")] {
            let (node, detail) = refused(named_executable_refusal(&current, &with(PathName::new("mesh1.node", "2").unwrap(), "node", b), &rows, ProviderKind::Process, &[]).await);
            assert_eq!(node, PathName::new("mesh1.node", "2").unwrap(), "{what}: {detail}");
        }
        // A held path whose row already records the executable is not hashed again: its file may be gone.
        let gone = rdm_core::binding::Binding { executable: dir.join("gone"), ..admin.clone() };
        let recorded: crate::accepted::Recorded = [(PathName::new("mesh1.node-admin", "1").unwrap(), crate::accepted::RecordedRow { mesh: "mesh1".into(), r#type: "node-admin".into(), config: None, tags: None, storage_persistence: None, node_id: crate::model::NodeId::mint(), sha256: Some(gone.sha256.clone()) })].into();
        assert_eq!(named_executable_refusal(&current, &with(PathName::new("mesh1.node-admin", "1").unwrap(), "node-admin", gone), &recorded, ProviderKind::Process, &[]).await, None);
        assert_eq!(BuildReject::ExecutableRefused { node: PathName::new("root", "x").unwrap(), detail: "y".into() }.reason(), "executable-refused");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A topology of mesh1 holding one node-admin, and that topology with each `(path, binding)` created.
    fn creating(created: &[(PathName, &str, rdm_core::binding::Binding)]) -> (crate::accepted::FabricTopology, crate::accepted::FabricTopology) {
        let current = crate::accepted::FabricTopology { section_of: None, pinned: Vec::new(), topology_storage_provider: None, fabric: "fabric1".into(), meshes: [("mesh1".to_string(), crate::accepted::MeshTopology::of(&crate::build::MeshDesired::of("mesh1", [("node-admin", 1)])))].into() };
        let mut next = current.clone();
        let m = next.meshes.get_mut("mesh1").unwrap();
        for (p, r#type, b) in created {
            m.nodes.insert(p.clone());
            m.spec_by_path.insert(p.clone(), rdm_core::meta::NodeSpec { mesh: "mesh1".into(), r#type: r#type.to_string(), path: p.clone(), storage_persistence: rdm_core::meta::StoragePersistence::Ephemeral, executable: Some(b.clone()), tags: Default::default(), config: Default::default() });
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
    /// type's launch id is still checked. Must NOT happen: the shared file read once per type.
    #[tokio::test]
    async fn a_file_several_kinds_share_is_hashed_once_at_acceptance() {
        let dir = crate::test_support::tempdir("hash-once-shared");
        let node = bound_file(&dir, "rdm-node", "broker", 4 << 20);
        let as_kind = |id: &str| rdm_core::binding::Binding { launch_id: id.into(), ..node.clone() };
        let (current, next) = creating(&[(PathName::new("mesh1.broker", "1").unwrap(), &("broker".to_string()), as_kind("broker")), (PathName::new("mesh1.gateway", "1").unwrap(), &("gateway".to_string()), as_kind("gateway")), (PathName::new("mesh1.compute", "1").unwrap(), &("compute".to_string()), as_kind("compute"))]);
        let capture = crate::span_capture::Capture::new("rdm148-hash-once-shared");
        let refused = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            named_executable_refusal(&current, &next, &crate::accepted::Recorded::new(), ProviderKind::Process, &[]).await
        };
        assert_eq!(refused, None, "the shared file is hash-true for every type");
        let hashes: Vec<_> = capture.named("rdm.node_admin.deployment.resolve.via-executable-hash").into_iter().filter(|s| s.attributes.get("from").map(String::as_str) == Some("request")).collect();
        assert_eq!(hashes.len(), 1, "one hash of the one file the three kinds share: {hashes:#?}");
        assert_eq!(hashes[0].attributes.get("outcome").map(String::as_str), Some("hashed"), "{hashes:#?}");
        // Each type's launch id is still checked: the shared file bound for the node-admin at a broker path.
        let (current, next) = creating(&[(PathName::new("mesh1.broker", "1").unwrap(), &("broker".to_string()), as_kind("broker")), (PathName::new("mesh1.gateway", "1").unwrap(), &("gateway".to_string()), as_kind("node-admin"))]);
        match named_executable_refusal(&current, &next, &crate::accepted::Recorded::new(), ProviderKind::Process, &[]).await {
            Some(BuildReject::ExecutableRefused { node, detail }) => assert_eq!(node, PathName::new("mesh1.gateway", "1").unwrap(), "{detail}"),
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
        let (current, next) = creating(&[(PathName::new("mesh1.node", "1").unwrap(), &("node".to_string()), bound_file(&dir, "rpc", "node", 64 << 20)), (PathName::new("mesh1.broker", "1").unwrap(), &("broker".to_string()), bound_file(&dir, "broker", "broker", 64 << 20)), (PathName::new("mesh1.gateway", "1").unwrap(), &("gateway".to_string()), bound_file(&dir, "gateway", "gateway", 64 << 20))]);
        let capture = crate::span_capture::Capture::new("rdm148-hash-once-distinct");
        let refused = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            named_executable_refusal(&current, &next, &crate::accepted::Recorded::new(), ProviderKind::Process, &[]).await
        };
        assert_eq!(refused, None, "every file is hash-true for its type");
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
            ([(PathName::new("mesh1.broker", "1").unwrap(), "broker", wrong_sha.clone()), (PathName::new("mesh1.gateway", "1").unwrap(), "gateway", wrong_kind.clone())], PathName::new("mesh1.broker", "1").unwrap(), "a wrong sha before a wrong launch id"),
            ([(PathName::new("mesh1.broker", "1").unwrap(), "broker", rdm_core::binding::Binding { launch_id: "compute".into(), ..b.clone() }), (PathName::new("mesh1.gateway", "1").unwrap(), "gateway", missing.clone())], PathName::new("mesh1.broker", "1").unwrap(), "a wrong launch id before a missing file"),
            ([(PathName::new("mesh1.broker", "1").unwrap(), "broker", b.clone()), (PathName::new("mesh1.gateway", "1").unwrap(), "gateway", missing.clone())], PathName::new("mesh1.gateway", "1").unwrap(), "a later missing file once the earlier path passes"),
        ] {
            let (current, next) = creating(&created);
            assert_eq!(next.meshes["mesh1"].spec_by_path.iter().find(|(_, spec)| !rdm_core::is_node_admin(&spec.r#type)).map(|(location, _)| location.clone()), Some(PathName::new("mesh1.broker", "1").unwrap()), "the topology's order");
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
            let p = dir.join("node");
            std::fs::write(&p, "#!/bin/sh\n# rpc_node\n").unwrap();
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
            rdm_core::binding::Binding { launch_id: "node".into(), sha256: rdm_core::binding::sha256_file(&p).unwrap(), executable: p, image: image.map(str::to_string) }
        };
        let image = Some(crate::deployment::container::RUNTIME_IMAGE);
        let current = crate::accepted::FabricTopology { section_of: None, pinned: Vec::new(), topology_storage_provider: None, fabric: "fabric1".into(), meshes: [("mesh1".to_string(), crate::accepted::MeshTopology::of(&crate::build::MeshDesired::of("mesh1", [("node-admin", 1), ("node", 0)])))].into() };
        let with = |b: rdm_core::binding::Binding| {
            let mut next = current.clone();
            let p: PathName = PathName::new("mesh1.node", "1").unwrap();
            let m = next.meshes.get_mut("mesh1").unwrap();
            m.nodes.insert(p.clone());
            m.spec_by_path.insert(p.clone(), rdm_core::meta::NodeSpec { mesh: "mesh1".into(), r#type: "node".into(), path: p, storage_persistence: rdm_core::meta::StoragePersistence::Ephemeral, executable: Some(b), tags: Default::default(), config: Default::default() });
            next
        };
        let rows = crate::accepted::Recorded::new();
        let dirs = vec![inside.clone()];
        assert_eq!(named_executable_refusal(&current, &with(bind(&inside, image)), &rows, ProviderKind::Container, &dirs).await, None, "inside a configured directory");
        let out = bind(&outside, image);
        for (configured, what) in [(dirs.clone(), "outside every configured directory"), (Vec::new(), "none configured")] {
            match named_executable_refusal(&current, &with(out.clone()), &rows, ProviderKind::Container, &configured).await {
                Some(BuildReject::ExecutableRefused { node, detail }) => {
                    assert_eq!(node, PathName::new("mesh1.node", "1").unwrap(), "{what}: {detail}");
                    assert!(detail.contains("RDM_EXECUTABLE_DIRS") && detail.contains(&out.executable.display().to_string()), "{what}: {detail}");
                }
                other => panic!("{what}: refused executable-refused: {other:?}"),
            }
        }
        // Outside, and absent: refused for its place, never misreported as missing.
        let absent = rdm_core::binding::Binding { executable: root.join("elsewhere").join("node"), ..out.clone() };
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
        let current = crate::accepted::FabricTopology { section_of: None, pinned: Vec::new(), topology_storage_provider: None, fabric: "fabric1".into(), meshes: [("mesh1".to_string(), crate::accepted::MeshTopology::of(&crate::build::MeshDesired::of("mesh1", [("node-admin", 1), ("node", 1)])))].into() };
        let observed = crate::topology::Topology { fabric: crate::model::Fabric { id: crate::model::FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: ProviderKind::Process }, meshes: vec![], nodes: vec![] };
        let grown = crate::accepted::compile(&current, &crate::accepted::TopologyChange::AddNode { mesh: "mesh1".into(), r#type: "node".into() }, &observed).unwrap();
        assert_eq!(created_without_executable(&current, &grown), Some(BuildReject::ExecutableMissing { node: PathName::new("mesh1.node", "2").unwrap() }));
        assert_eq!(created_without_executable(&current, &current), None, "held paths naming none are history");
        let mut named = grown.clone();
        named.meshes.get_mut("mesh1").unwrap().spec_by_path.get_mut(&PathName::new("mesh1.node", "2").unwrap()).unwrap().executable = Some(rdm_core::binding::Binding { launch_id: "node".into(), executable: "/x/rpc".into(), sha256: "0".repeat(64), image: None });
        assert_eq!(created_without_executable(&current, &named), None);
        assert_eq!(BuildReject::ExecutableMissing { node: PathName::new("root", "x").unwrap() }.reason(), "executable-missing");
    }
}
