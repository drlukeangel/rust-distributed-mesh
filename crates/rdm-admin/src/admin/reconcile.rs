//! PrimaryRole gate and the reconcile loop's wake.

use super::*;

/// The gates of the fabric-primary's drive: the Build log's target, read from the fabric-primary
/// record this admin holds.
pub(crate) struct RoleGate {
    pub(crate) builds: Arc<FabricBuildStateAdapter>,
    /// The control plane whose R-D1 reader (how long a peer mesh has gone unheard past the staleness
    /// floor) the admin installs once its digest view runs: read when asked, as the `BuildService` reads it.
    pub(crate) control: Arc<ControlPlane>,
    /// node.primary.update at this admin (op 0x21), commanded and run through it, which holds what each update
    /// was answered and said (`PrimaryUpdate::memo`).
    pub(crate) primary_update: Arc<crate::primary_update::PrimaryUpdate>,
    /// Projects the digest view into the view now, so a Mesh record read deleted is in the view the plan
    /// reads next.
    pub(crate) project: Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>,
    /// The Mesh records (mesh.storage on the fabric's shared root): which meshes are deleted, for the plan.
    pub(crate) meshes: Arc<dyn crate::storage::MeshStorage>,
    /// The fabric's change, told when a Build starts: it names the accepted Build once it runs.
    pub(crate) fabric_states: Arc<std::sync::OnceLock<Arc<crate::fabric_status::FabricStates>>>,
}

#[async_trait::async_trait]
impl crate::build_drive::Gate for RoleGate {
    async fn target(&self) -> Result<Option<rdm_core::NodeId>, crate::build_state::BuildStateError> {
        self.builds.target_check().await
    }
    async fn target_set(&self) {
        self.builds.target_set().await
    }
    fn mesh_unheard_ms(&self, mesh: &str) -> Option<u64> {
        self.control.mesh_unheard.get().and_then(|f| f(mesh))
    }
    /// node.primary.update before an operation on a primary (lifecycles/node-primary-update.md step 1:
    /// "The primary is commanded from above"): a mesh-primary is commanded from here (the
    /// fabric-primary commands a mesh-primary); any other type's primary through its mesh-primary, its
    /// commander; the fabric-primary's own primaries it moves itself, having nothing above it, both in
    /// one call when it is both ("Both primaries": the successor becomes both).
    async fn update_primaries(&self, m: &crate::executor::RoleMove, build_id: &crate::build::BuildId, operation: &rdm_core::OperationId) -> Result<(), crate::build_drive::RoleMoveEnd> {
        use crate::election::Moving;
        use crate::primary_update::update_operation;
        use rdm_core::PrimaryRole;
        let primary_update = &self.primary_update;
        if m.primary.node_id != primary_update.node_id {
            // A mesh-primary, commanded directly; any other type's primary, commanded through its
            // mesh-primary (this admin itself when it is that mesh-primary).
            if m.mesh_role || m.node_type_role {
                let role = if m.node_type_role { PrimaryRole::NodePrimary(m.primary.r#type.clone()) } else { PrimaryRole::MeshPrimary };
                let update = update_operation(role, operation);
                primary_update.command_update_primary(&m.primary, build_id.as_str(), &update).await?;
            }
            return Ok(());
        }
        // The fabric-primary role alone moves out of this admin's mesh (C2 search step 2: a mesh delete of its
        // mesh, mesh-delete.md step 1), by node.fabric-primary.update; with the mesh-primary role, both move in
        // node.primary.update's one row (Q1).
        let (moving, role) = match (m.mesh_role, m.fabric_role) {
            (true, true) => (Moving::BothRoles, PrimaryRole::MeshPrimary),
            (true, false) => (Moving::MeshRole, PrimaryRole::MeshPrimary),
            (false, true) => (Moving::FabricRoleOutside, PrimaryRole::FabricPrimary),
            (false, false) => return Ok(()),
        };
        let update = update_operation(role, operation);
        primary_update.update_own_primaries(moving, build_id.as_str(), &update, m.successor.as_ref()).await?;
        Ok(())
    }

    /// `t` with its primaries as this admin's atlas holds them (Q355): the answers to the updates it
    /// sent, and those its mesh-primaries streamed back to it.
    fn primaries(&self, t: &mut crate::topology::Topology) {
        self.control.atlas.primaries(t)
    }

    async fn primaries_updated(&self, updates: &[crate::build_drive::PrecedingUpdate], build_id: &crate::build::BuildId) -> Result<(), String> {
        for u in updates {
            if u.primary.is_none() && u.mesh == self.primary_update.topology_view.mesh_name() {
                continue;
            }
            self.primary_update.row_acknowledged(&u.mesh, &u.operation, u.primary.as_ref(), build_id.as_str()).await?;
        }
        Ok(())
    }

    /// What the node rows record of each member's executable (build-deployment.md §3), read from this
    /// admin's nodes.storage, every mesh's rows on the fabric's shared root.
    async fn mesh_rows(&self) -> Result<Vec<crate::storage::MeshRecord>, String> {
        self.meshes.meshes().await.map_err(|e| format!("{}: the Mesh rows could not be read: {e}", self.primary_update.node_id))
    }

    /// The node records of `ids`, read through the reader (query.md §1): the births a fabric.delete's
    /// begun node.deletes act on once the view has let them go.
    async fn node_rows(&self, ids: &[rdm_core::NodeId]) -> Result<Vec<crate::storage::NodeRecord>, String> {
        let records = self.control.current_records().ok_or_else(|| format!("{}: this admin holds no node records to read", self.primary_update.node_id))?;
        crate::atlas::subject_rows(&records, ids.iter()).await
    }

    /// The paths this Build's own creates hold in the atlas (fabric-atlas.md R-FA1).
    async fn atlas_created(&self, build: &crate::build_state::BuildProjection) -> std::collections::BTreeSet<rdm_core::PathName> {
        self.control.atlas.created_by(build)
    }

    /// The meshes of `observed` whose Mesh record is deleted: one point read of each record the view does
    /// not already hold deleted. Each is held as the fabric-primary that accepted its mesh.deleted holds it
    /// (`Records::hold_mesh_deleted`), so this admin's view shows it deleted from its next projection: a
    /// fabric-primary that took over after the mesh.delete reads it from the record, never from gossip.
    async fn deleted_meshes(&self, observed: &crate::topology::Topology) -> Result<std::collections::HashSet<crate::model::MeshId>, String> {
        let mut deleted = std::collections::HashSet::new();
        for id in observed.meshes.iter().filter(|m| m.status != crate::model::ScopeStatus::Dead).filter_map(|m| m.id.as_ref()) {
            if self.meshes.mesh(id).await.map_err(|e| format!("the Mesh record of {id} could not be read: {e}"))?.is_some_and(|r| r.is_deleted()) {
                self.primary_update.records.hold_mesh_deleted(id);
                deleted.insert(id.clone());
            }
        }
        if !deleted.is_empty() {
            (self.project)().await;
        }
        Ok(deleted)
    }

    /// A pending fabric's change waits on the accepted Build's start (`build-not-started`): its next
    /// turn is due at once, never a gossip interval later (states.md: a status change is sent at once).
    async fn build_started(&self, _build_id: &crate::build::BuildId) {
        if let Some(states) = self.fabric_states.get() {
            states.poke_change();
        }
    }

    fn fabric_ready(&self) -> bool {
        self.primary_update.records.fabric_ready()
    }

    /// The completed lifecycle steps of the Build, taken into the fabric-primary's atlas before the
    /// caller is answered `Complete` (fabric-atlas.md R-FA2): the atlas, never the gossip view, is what
    /// Build acceptance reads them from (Q197). The steps' subjects are read through the reader
    /// (query.md §1), what names a step's birth its Build pinned none of.
    async fn build_completed(&self, build_id: &crate::build::BuildId) {
        if let Ok(p) = self.builds.read_build(build_id).await {
            let rows = match self.control.current_records() {
                Some(records) => {
                    let subjects: Vec<rdm_core::NodeId> = p.steps.iter().filter(|s| s.step == "Complete" && s.outcome == crate::build_state::StepOutcome::Complete).filter_map(|s| s.subject.clone()).collect();
                    match crate::atlas::subject_rows(&records, subjects.iter()).await {
                        Ok(rows) => rows,
                        Err(e) => {
                            tracing::info_span!("rdm.node_admin.fabric.update.via-atlas-receipt", build_id = %build_id.log(), outcome = "subjects-unread", detail = %e, "otel.kind" = "internal")
                                .in_scope(|| tracing::info!("the steps' subjects could not be read: a step its Build pinned no birth of takes no effect"));
                            Vec::new()
                        }
                    }
                }
                None => Vec::new(),
            };
            self.control.atlas.receipts(&p, &rows);
        }
    }

    async fn step_streamed(&self, mesh: &str, build_id: &crate::build::BuildId, operation: &str) {
        self.control.take_streamed_receipt(&*self.builds, build_id, mesh, operation).await;
    }

    fn now_ms(&self) -> u64 {
        self.control.time.now_ms()
    }
}

/// The path and name the lost birth lived at, where fabric recovery re-creates the node-admin
/// (fabric-recovery.md step 3): its digest's, else its departure's. `None` while the view holds neither.
pub(crate) fn lost_birth_name(digest_view: &DigestView, lost: &rdm_core::handover::HandoverBirth) -> Option<PathName> {
    let exact = |d: &rdm_core::digest::MeshDigest| d.node.node_id == lost.node_id && d.node.incarnation == lost.incarnation;
    digest_view.get(lost.node_id.as_str())
        .map(|(d, _)| d)
        .filter(exact)
        .or_else(|| digest_view.stopped_digests().into_iter().find(exact))
        .map(|d| d.node.location())
        .or_else(|| digest_view.deleted().into_iter().find(|op| op.node_id == lost.node_id && op.incarnation == lost.incarnation).map(|op| op.location()))
}

/// What woke the reconcile loop for its next pass ([`ReconcileWake::next`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Woken {
    /// The loop's first pass, as it starts.
    First,
    /// A Build accepted on this admin (`ControlPlane::build_submitted`).
    Submitted,
    /// The installed view changed (`Records::subscribe_view`): births, status, primaries,
    /// verdicts, deleted meshes, the fabric-primary.
    ViewInstalled,
    /// The fabric state changed (`Records::subscribe_fabric_state`).
    FabricState,
    /// A drive ended its Build (`Drives::subscribe_ended`).
    DriveEnded,
    /// A runtime this admin's provider started exited (`DeploymentProvider::exits`).
    ProviderExit,
}

impl Woken {
    /// The name the pass's span carries.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::First => "first",
            Self::Submitted => "submitted",
            Self::ViewInstalled => "view-installed",
            Self::FabricState => "fabric-state",
            Self::DriveEnded => "drive-ended",
            Self::ProviderExit => "provider-exit",
        }
    }
}

/// The reconcile loop's wait between two passes (Q253, amended by Luke 2026-10-28: "I hate timers;
/// make this event driven"): the loop is `loop { wake.next().await; <pass> }`, and a pass runs on an
/// event, never on a tick. Each event the pass decides on wakes it: a Build submitted, a view installed,
/// the fabric state changed, a drive ended its Build, a runtime the
/// admin's provider started exited (Q254). An event during
/// a pass (the pass's own install among them) is held for the next: every source is marked seen as a
/// pass begins, and the submit's notification is armed then, so a `notify_waiters` while the pass runs
/// is not lost. The wait touches no lock.
pub struct ReconcileWake {
    submitted: Arc<tokio::sync::Notify>,
    /// The submit's notification armed as the last pass began; `None` before the first pass.
    armed: Option<std::pin::Pin<Box<tokio::sync::futures::OwnedNotified>>>,
    view: tokio::sync::watch::Receiver<u64>,
    fabric_state: tokio::sync::watch::Receiver<crate::fabric_state::FabricState>,
    drive_ended: tokio::sync::watch::Receiver<u64>,
    provider_exit: tokio::sync::watch::Receiver<u64>,
}

impl ReconcileWake {
    /// The wait over a Build submitted (`submitted`), a view installed (`view`), a fabric state change
    /// (`fabric_state`), a drive's end
    /// (`drive_ended`) and an exit the provider saw (`provider_exit`).
    pub fn new(
        submitted: Arc<tokio::sync::Notify>,
        view: tokio::sync::watch::Receiver<u64>,
        fabric_state: tokio::sync::watch::Receiver<crate::fabric_state::FabricState>,
        drive_ended: tokio::sync::watch::Receiver<u64>,
        provider_exit: tokio::sync::watch::Receiver<u64>,
    ) -> Self {
        Self { submitted, armed: None, view, fabric_state, drive_ended, provider_exit }
    }

    /// Wait for the next pass's event: the first pass begins at once. Every source is marked seen and
    /// the submit armed before this returns, so the pass that follows reads everything that happened
    /// before it and the next wait wakes on anything after. A source whose sender is gone wakes nothing.
    pub async fn next(&mut self) -> Woken {
        let woken = match self.armed.take() {
            None => Woken::First,
            Some(mut armed) => tokio::select! {
                _ = armed.as_mut() => Woken::Submitted,
                Ok(()) = self.view.changed() => Woken::ViewInstalled,
                Ok(()) = self.fabric_state.changed() => Woken::FabricState,
                Ok(()) = self.drive_ended.changed() => Woken::DriveEnded,
                Ok(()) = self.provider_exit.changed() => Woken::ProviderExit,
            },
        };
        let mut armed = Box::pin(self.submitted.clone().notified_owned());
        armed.as_mut().enable();
        self.armed = Some(armed);
        self.view.mark_unchanged();
        self.fabric_state.mark_unchanged();
        self.drive_ended.mark_unchanged();
        self.provider_exit.mark_unchanged();
        woken
    }
}


/// What an entry answer hands this admin of Fabric control: the Fabric identity, the answering
/// admin's current Build (the floor this admin's copy of that Build must reach), and
/// its own mesh's held lifecycle state ([`adopt_held_mesh_state`]). The first entry and every repeat of
/// it take them through here.
pub(crate) async fn take_entry_control(answer: &crate::wire::JoinAnswer, accepted: &AcceptedStore, entry_floor: &Mutex<Option<crate::build::BuildId>>, records: &Records, mesh: &str, mesh_id: &MeshId) {
    if let Some(r) = answer.control.fabric.clone() {
        accepted.learn(r, &answer.served_by.to_string()).await;
    }
    if let Some(floor) = answer.control.build.clone() {
        *entry_floor.lock().unwrap() = Some(floor.build_id);
    }
    adopt_held_mesh_state(answer, records, mesh, mesh_id);
}

/// This admin's own mesh's held lifecycle state, as its maker's entry answer holds it, adopted exactly
/// into its applied state (node-create.md "A node-admin created into a live mesh" step 2: it
/// "obtains its control/status context"; fabric-authority.md §7: the successor adopts the state that
/// already exists, with no durable write and no transition). An admin that holds a state for its mesh
/// keeps it. A new mesh's first admin is answered no state for its mesh: the fabric-primary applies
/// its Pending (states.md). A status word this build does not read as a Mesh lifecycle state is
/// refused by name and adopts nothing.
pub(crate) fn adopt_held_mesh_state(answer: &crate::wire::JoinAnswer, records: &Records, mesh: &str, mesh_id: &MeshId) {
    let held = answer.statuses.iter().find_map(|f| match f.data() {
        rdm_transport::topology::Frame::MeshStatus { mesh: m, status, publisher, .. } if m == mesh => Some((status.clone(), publisher.clone())),
        _ => None,
    });
    let Some((status, publisher)) = held else { return };
    let state = match mesh_state_of(&status) {
        Ok(state) => state,
        Err(why) => {
            tracing::info_span!("rdm.node_admin.mesh.reject.via-held-state-unread", mesh, mesh_id = %mesh_id, status = %status, served_by = %answer.served_by, reason = %why)
                .in_scope(|| tracing::info!("the entry answer's held state for this admin's mesh is not one it adopts"));
            return;
        }
    };
    {
        let mut declared = records.declared.lock().unwrap();
        if declared.meshes.contains_key(mesh_id) {
            return;
        }
        declared.hold_mesh(mesh_id.clone(), state);
    }
    // The adopting admin is named by the name its join was admitted under, as every admin span names its node.
    let (node_path, node_name) = answer.control.name.as_ref().map(|n| (n.path.as_str(), n.name.as_str())).unwrap_or_default();
    tracing::info_span!("rdm.node_admin.mesh.update.via-held-state-adopted", node_path, node_name, mesh, mesh_id = %mesh_id, state = ?state, served_by = %answer.served_by, publisher = %publisher)
        .in_scope(|| tracing::info!("the mesh's held lifecycle state is adopted as it exists: no transition, no durable write"));
}

/// A held Mesh status word as the Mesh lifecycle state it names (states.md: pending ->
/// ready -> leaving -> dead); any other word is refused by name.
pub(crate) fn mesh_state_of(status: &str) -> Result<rdm_core::status::MeshState, String> {
    use rdm_core::status::MeshState;
    match status {
        "pending" => Ok(MeshState::Pending),
        "ready" => Ok(MeshState::Ready),
        "leaving" => Ok(MeshState::Deleting),
        "dead" => Ok(MeshState::Deleted),
        other => Err(format!("mesh status {other:?} is no Mesh lifecycle state (pending, ready, leaving, dead)")),
    }
}

/// Never Ready while a Build's facts are missing: an admin that holds no current Build (R-BI3: no
/// Build in its builds store is in progress or completed) names itself blocked. Only Day 0 accepts
/// the first Build itself; every other admin fetches the Build facts from an existing authority
/// (its entry pull, or the fabric control topic).
///
/// Ready also needs the facts of the Build the entry named, the answering admin's current Build: it
/// (`floor`) must be readable from this admin's own log. An admin that was not served an entry (Day 0) owes no floor. The
/// reason is typed ([`crate::fetch_build::NodeAdminNotReady`]): the Ready check recovers each one with
/// the request it names.
pub async fn node_admin_not_ready(me: &PathName, builds: &dyn BuildStateAdapter, floor: Option<crate::build::BuildId>) -> Option<crate::fetch_build::NodeAdminNotReady> {
    not_ready_in(me, &FromStore(builds), floor).await
}

/// [`node_admin_not_ready`] over the facts `log` holds now: a launched node-admin's Build floor at its
/// start, the Build's committed records read from storage (Q292) and merged whole
/// ([`crate::build_state::LocalBuildLog::absorb_build`]).
pub(crate) async fn node_admin_not_ready_held(me: &PathName, log: &dyn crate::build_state::LocalBuildLog, floor: Option<crate::build::BuildId>) -> Option<crate::fetch_build::NodeAdminNotReady> {
    not_ready_in(me, &Held(log), floor).await
}

/// Where a node-admin's Ready check reads its Build holdings.
#[async_trait::async_trait]
trait Holdings: Send + Sync {
    async fn build(&self, id: &crate::build::BuildId) -> Result<crate::build_state::BuildProjection, crate::build_state::BuildStateError>;
    async fn current(&self) -> Result<crate::build_state::CurrentBuild, crate::build_state::BuildStateError>;
}

/// The Build log's reads, each reading its store.
struct FromStore<'a>(&'a dyn BuildStateAdapter);

#[async_trait::async_trait]
impl Holdings for FromStore<'_> {
    async fn build(&self, id: &crate::build::BuildId) -> Result<crate::build_state::BuildProjection, crate::build_state::BuildStateError> {
        self.0.read_build(id).await
    }
    async fn current(&self) -> Result<crate::build_state::CurrentBuild, crate::build_state::BuildStateError> {
        self.0.current_build().await
    }
}

/// What the Build log holds now, reading no store.
struct Held<'a>(&'a dyn crate::build_state::LocalBuildLog);

#[async_trait::async_trait]
impl Holdings for Held<'_> {
    async fn build(&self, id: &crate::build::BuildId) -> Result<crate::build_state::BuildProjection, crate::build_state::BuildStateError> {
        self.0.held_build(id).await
    }
    async fn current(&self) -> Result<crate::build_state::CurrentBuild, crate::build_state::BuildStateError> {
        self.0.held_current_build().await
    }
}

async fn not_ready_in(me: &PathName, builds: &dyn Holdings, floor: Option<crate::build::BuildId>) -> Option<crate::fetch_build::NodeAdminNotReady> {
    use crate::fetch_build::NodeAdminNotReady as B;
    if let Some(id) = floor {
        match builds.build(&id).await {
            Ok(_) => {}
            Err(crate::build_state::BuildStateError::UnknownBuild(_)) => return Some(B::NoCurrentBuild { me_path: me.path.clone(), me_name: me.name.clone(), wanted: Some(id) }),
            Err(e) => return Some(B::UnreadableBuild { me_path: me.path.clone(), me_name: me.name.clone(), build_id: id, error: e.to_string() }),
        }
    }
    match builds.current().await {
        Ok(crate::build_state::CurrentBuild::None) => Some(B::NoCurrentBuild { me_path: me.path.clone(), me_name: me.name.clone(), wanted: None }),
        Ok(_) => None,
        Err(e) => Some(B::BuildStoreUnreadable { me_path: me.path.clone(), me_name: me.name.clone(), error: e.to_string() }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accepted::FabricTopology;
    use crate::build::BuildId;
    use crate::build_state::{BuildAccepted, MemoryBuildStateAdapter};
    use crate::fabric_storage::{FabricIdentity, MemoryFabricStorage};
    use crate::fetch_build::NodeAdminNotReady;
    use crate::storage::{MemoryNodesStorage, NodesStorage, RuntimeRow};
    use crate::wire::{BuildFloor, JoinAnswer, JoinControl};
    use rdm_core::handover::HandoverBirth;
    use rdm_core::status::MeshState;
    use rdm_core::{DeploymentId, RuntimeFact};

    fn sample_digest(fabric_id: FabricId, mesh_id: MeshId, name: &PathName, mesh: &str, r#type: &str, node_id: &NodeId, incarnation: &IncarnationId) -> MeshDigest {
        MeshDigest {
            fabric_id,
            node: MeshNode { mesh: mesh.to_owned(), r#type: r#type.to_string(),
                node_id: node_id.clone(),
                path: name.path.clone(), name: name.name.clone(),
                endpoint_id: EndpointId("e".repeat(64)),
                transport_addr: "127.0.0.1:41001".parse().unwrap(),
                incarnation: incarnation.clone(),
                supersedes: None,
                runtime: None,
            },
            status: MemberStatus::Ready,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 1,
            emitted_at_rafka_ms: 1_000,
            data_dir: None,
            mesh_id: Some(mesh_id),
            in_flight: None,
            load: None,
            gossip: None,
            is_primary: None,
            is_fabric_primary: None,
        }
    }

    /// CONTRACT: docs/architecture/lifecycles/fabric-recovery.md:62
    /// "Any other verdict, node recovery re-creates it at the same path and name with a new node.id"
    #[tokio::test]
    async fn lost_birth_name_resolves_the_lost_births_path_and_name_once_its_digest_is_held() {
        let fabric_id = FabricId::mint();
        let mesh1_id = MeshId::mint();
        let dv = DigestView::default();

        let lost_id = NodeId::mint();
        let lost_inc = IncarnationId::mint();
        let lost_path: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
        let hb = HandoverBirth {
            mesh: "mesh1".to_string(),
            node_id: lost_id.clone(),
            incarnation: lost_inc.clone(),
        };

        // 1. `lost_birth_name` returns None when unknown, and resolves the birth's `(path, name)` once present in `DigestView`.
        assert_eq!(lost_birth_name(&dv, &hb), None);
        dv.record(sample_digest(fabric_id.clone(), mesh1_id.clone(), &lost_path, "mesh1", "node-admin", &lost_id, &lost_inc));
        assert_eq!(lost_birth_name(&dv, &hb), Some(lost_path.clone()));

    }

    /// CONTRACT: docs/architecture/build.md:308
    /// "first connection / JoinNode (op `0x1D`) -> Fabric identity + the Build to hold"
    /// CONTRACT: docs/architecture/lifecycles/node-create.md:133
    /// "as the mesh's successor primary it adopts the mesh's held state"
    #[tokio::test]
    async fn take_entry_control_and_node_admin_not_ready_held_track_the_entry_floor() {
        let me: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
        let mesh_id = MeshId::mint();
        let fabric_id = FabricId::mint();


        // 2. `take_entry_control` learns Fabric identity, records the Build floor, and adopts the mesh's held MeshState.
        let fab_store = Arc::new(MemoryFabricStorage::default());
        let accepted = AcceptedStore::new(fab_store, NodeId::mint().to_string());
        let entry_floor = Mutex::new(None);
        let records = Records::default();
        let build_id = BuildId::mint();
        let identity = FabricIdentity {
            fabric_id: fabric_id.clone(),
            name: "fab".to_string(),
        };
        let answer = JoinAnswer {
            served_by: NodeId::mint(),
            control: JoinControl {
                provider: ProviderKind::Process,
                fabric: Some(identity.clone()),
                build: Some(BuildFloor {
                    build_id: build_id.clone(),
                }),
                rafka_time_ms: 5_000,
                member_cert: Vec::new(),
                member_cert_dates: None,
                signing_cert_dates: None,
                name: Some(me.clone()),
                cert_policy: None,
            },
            statuses: vec![rdm_transport::topology::Frame::MeshStatus {
                mesh: "mesh1".to_string(),
                status: "ready".to_string(),
                publisher: "mesh1.node-admin.2".to_string(),
                forwarded_by: None,
                changed_at_rafka_ms: 4_000,
            }],
        };
        take_entry_control(&answer, &accepted, &entry_floor, &records, "mesh1", &mesh_id).await;
        assert_eq!(accepted.record().await.unwrap(), Some(identity));
        assert_eq!(*entry_floor.lock().unwrap(), Some(build_id.clone()));
        assert_eq!(
            records.declared.lock().unwrap().meshes.get(&mesh_id).copied(),
            Some(MeshState::Ready)
        );

        // 3. `node_admin_not_ready_held` checks the in-memory `LocalBuildLog` against the entry floor.
        let log = MemoryBuildStateAdapter::new();
        assert_eq!(
            node_admin_not_ready_held(&me, &log, Some(build_id.clone())).await,
            Some(NodeAdminNotReady::NoCurrentBuild {
                me_path: me.path.clone(), me_name: me.name.clone(),
                wanted: Some(build_id.clone()),
            })
        );

        log.publish_accepted(&BuildAccepted {
            build_id: build_id.clone(),
            topology: FabricTopology::root("fab", "mesh1", mesh_id),
            change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() },
            source: None, time: 1_000,
        })
        .await
        .unwrap();
        assert_eq!(node_admin_not_ready_held(&me, &log, Some(build_id)).await, None, "it holds the entry's Build");
    }
}
