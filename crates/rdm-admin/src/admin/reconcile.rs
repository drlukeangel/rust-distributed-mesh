//! Seat gate, provider departure proofs, and the drift reconcile loop.

use super::*;

/// The gates of the fabric-primary's drive: the Build log's fence, read from the fabric-primary
/// record this admin holds.
pub(crate) struct SeatGate {
    pub(crate) topology_view: TopologyView,
    pub(crate) builds: Arc<FabricBuildStateAdapter>,
    /// The control plane whose peer-mesh reader the investigation installs once its ladder runs
    /// (after the drive is built): read when asked, as the Build door reads it.
    pub(crate) control: Arc<ControlPlane>,
    /// The 0x21 door node.primary.update is commanded and run through, which holds what each update
    /// was answered and said (`HandoverDoor::memo`).
    pub(crate) handover: Arc<crate::fabric_handover::HandoverDoor>,
    /// Projects the digest view into the view now, so a completed stop-node lists the stopped birth stopped
    /// before `BuildReply::Complete` goes out on the caller's stream.
    pub(crate) project: Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>,
    /// The deployment provider and the durable runtime rows: a completed stop's exact birth is
    /// inspected through them before its stop is taken into the view.
    pub(crate) provider: Arc<dyn DeploymentProvider>,
    pub(crate) nodes: Arc<dyn crate::storage::NodesStorage>,
    /// The Mesh records (mesh.storage on the fabric's shared root): which meshes are Dead, for the plan.
    pub(crate) meshes: Arc<dyn crate::storage::MeshStorage>,
}

/// Whether a completed stop's receipt still describes its exact birth: node-stop.md's Status is
/// "stopped with a live process", and a later attempt "validates those receipts against the accepted
/// operation and exact births before it reuses them" (node-rpc-envelope.md, Workflow). A birth whose
/// runtime the provider reads exited is not stopped: it is node recovery's (node-recovery.md step 3),
/// and its stop is not taken into the view. Any other reading keeps the receipt.
pub(crate) async fn stop_receipt_holds(provider: &dyn DeploymentProvider, digest_view: &DigestView, nodes: &dyn crate::storage::NodesStorage, op: &rdm_core::LifecycleOp) -> bool {
    // A deleted birth's stop is refused by the view itself (`DigestView::stopped`): nothing to inspect.
    if digest_view.is_deleted(op.node_id.as_str()) {
        return true;
    }
    let mut birth = Node::allocated(op.name.clone());
    birth.node_id = op.node_id.clone();
    birth.incarnation_id = Some(op.incarnation.clone());
    let (inspected, _) = crate::node_investigation::inspect_birth_with_handle(provider, digest_view, Some(nodes), &birth).await;
    if let crate::deployment::provider::Inspected::Exited(code) = inspected {
        tracing::info_span!("rdm.node_admin.node.update.via-node-stopped", node = %op.name, node_id = %op.node_id, incarnation_id = %op.incarnation.as_str(), build_id = %op.build_id, attempt = op.attempt, operation = %op.operation, outcome = "runtime-exited", exit_code = code.map(|c| c.to_string()).unwrap_or_default())
            .in_scope(|| tracing::info!("the stopped birth's runtime exited: its stop receipt is not taken into the view, and the birth is node recovery's"));
        return false;
    }
    true
}

#[async_trait::async_trait]
impl crate::build_drive::Gate for SeatGate {
    async fn fence(&self) -> Result<Option<String>, crate::build_state::BuildStateError> {
        self.builds.fence_check().await
    }
    async fn fence_set(&self) {
        self.builds.fence_set().await
    }
    fn peer_mesh(&self, mesh: &str) -> Option<crate::investigate::PeerMesh> {
        self.control.peer_mesh.get().and_then(|f| f(mesh))
    }
    /// node.primary.update before an operation on a primary (lifecycles/node-primary-update.md step 1:
    /// "The primary is commanded from above"): a mesh-primary is commanded from here (the
    /// fabric-primary commands a mesh-primary); any other type's primary through its mesh-primary, its
    /// commander; the fabric-primary's own primaries it moves itself, having nothing above it, both in
    /// one call when it is both ("Both primaries": the successor becomes both).
    async fn update_primaries(&self, m: &crate::executor::SeatMove, build_id: &crate::build::BuildId, attempt: u32, run: u32, operation: &str) -> Result<(), crate::build_drive::SeatMoveEnd> {
        use crate::election::Moving;
        use crate::fabric_handover::update_operation;
        use rdm_core::Seat;
        let door = &self.handover;
        if m.holder.node_id != door.node_id {
            // A mesh-primary, commanded directly; any other type's primary, commanded through its
            // mesh-primary (this admin itself when it is that mesh-primary).
            if m.mesh_seat || m.cohort_seat {
                let seat = if m.cohort_seat { Seat::Cohort(m.holder.kind) } else { Seat::MeshPrimary };
                let update = update_operation(seat, build_id.as_str(), run, operation);
                door.command_update_primary(&m.holder, build_id.as_str(), attempt, &update).await?;
            }
            return Ok(());
        }
        // The fabric seat alone moves out of this admin's mesh (C2 search step 2: a mesh retire of its
        // mesh, mesh-delete.md step 1), by node.fabric-primary.update; with the mesh seat, both move in
        // node.primary.update's one row (Q1).
        let (moving, seat) = match (m.mesh_seat, m.fabric_seat) {
            (true, true) => (Moving::BothSeats, Seat::MeshPrimary),
            (true, false) => (Moving::MeshSeat, Seat::MeshPrimary),
            (false, true) => (Moving::FabricSeatOutside, Seat::FabricPrimary),
            (false, false) => return Ok(()),
        };
        let update = update_operation(seat, build_id.as_str(), run, operation);
        door.update_own_primaries(moving, build_id.as_str(), attempt, &update, m.successor.as_ref()).await?;
        Ok(())
    }

    async fn primaries_updated(&self, updates: &[crate::build_drive::PrecedingUpdate], build_id: &crate::build::BuildId, attempt: u32) -> Result<(), String> {
        for u in updates {
            if u.holder.is_none() && u.mesh == self.handover.me.mesh {
                continue;
            }
            self.handover.row_acknowledged(&u.mesh, &u.operation, u.holder.as_ref(), build_id.as_str(), attempt).await?;
        }
        Ok(())
    }

    /// What the node rows record of each member's executable (build-deployment.md §3), read from this
    /// admin's nodes.storage, every mesh's rows on the fabric's shared root.
    async fn mesh_rows(&self) -> Result<Vec<crate::storage::MeshRecord>, String> {
        self.control.claims.meshes().await.map_err(|e| format!("{}: the Mesh rows could not be read: {e}", self.handover.me))
    }

    async fn recorded_executables(&self, topology: &crate::accepted::FabricTopology) -> Result<crate::accepted::Recorded, String> {
        let Some(rows) = self.handover.records.contacts.get().cloned() else {
            return Err(format!("{}: no nodes.storage is open yet to read the node rows from", self.handover.me));
        };
        crate::accepted::recorded_executables(&*rows, topology).await
    }

    /// The meshes of `observed` whose Mesh record is Dead: one point read of each record the view does
    /// not already hold Dead. Each is held as the fabric-primary that accepted its mesh.deleted holds it
    /// (`Records::hold_mesh_deleted`), so this admin's view shows it Dead from its next projection: a
    /// fabric-primary that took over after the mesh.delete reads it from the record, never from gossip.
    async fn dead_meshes(&self, observed: &crate::topology::Topology) -> Result<std::collections::HashSet<crate::model::MeshId>, String> {
        let mut dead = std::collections::HashSet::new();
        for id in observed.meshes.iter().filter(|m| m.status != crate::model::ScopeStatus::Dead).filter_map(|m| m.id.as_ref()) {
            if self.meshes.mesh(id).await.map_err(|e| format!("the Mesh record of {id} could not be read: {e}"))?.is_some_and(|r| r.dead) {
                self.handover.records.hold_mesh_deleted(id);
                dead.insert(id.clone());
            }
        }
        if !dead.is_empty() {
            (self.project)().await;
        }
        Ok(dead)
    }

    fn fabric_ready(&self) -> bool {
        self.handover.records.fabric_state() == crate::fabric_state::FabricState::ReadyForTraffic
    }

    fn state_sync_failed(&self, build_id: &crate::build::BuildId, attempt: u32) -> Option<(String, String)> {
        self.handover.records.state_sync_failed(build_id.as_str(), attempt).map(|f| (f.operation, f.reason))
    }

    async fn attempt_completed(&self, build_id: &crate::build::BuildId, _attempt: u32) {
        if let Ok(p) = self.builds.read_build(build_id).await {
            let mut stopped = false;
            for s in &p.steps {
                if s.operation.starts_with("node.stop:") && s.step == "Complete" && s.outcome == crate::build_state::StepOutcome::Complete {
                    stopped = true;
                    if let Some(op) = s.output.as_ref().and_then(|v| serde_json::from_value::<rdm_core::LifecycleOp>(v.clone()).ok()) {
                        if stop_receipt_holds(&*self.provider, &self.topology_view.digest_view, &*self.nodes, &op).await {
                            let _ = self.topology_view.digest_view.stopped(op);
                        }
                    }
                }
            }
            if stopped {
                (self.project)().await;
            }
        }
    }
}

/// Proof that an executor's birth departed (build.md: "after exact departure proof"): the provider
/// inspects the exact runtime the digest view or the durable runtime rows name for it, and only an
/// exited runtime counts.
pub(crate) struct ProviderDeparture {
    /// The admin proving the departure: the reader its refusals name.
    pub(crate) me: PathName,
    pub(crate) topology: Arc<RwLock<Topology>>,
    /// Projects this admin's view now and installs it, so the next attempt is planned from the view
    /// as it is now.
    pub(crate) refresh: Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>,
    pub(crate) digest_view: DigestView,
    pub(crate) provider: Arc<dyn DeploymentProvider>,
    pub(crate) nodes: Arc<dyn crate::storage::NodesStorage>,
    pub(crate) records: Arc<Records>,
}

impl ProviderDeparture {
    pub(crate) fn held_name(&self) -> PathName {
        self.digest_view.held_path_name().unwrap_or_else(|| self.me.clone())
    }
}

#[async_trait::async_trait]
impl crate::build_drive::DepartureProof for ProviderDeparture {
    async fn proven(&self, executor: &PathName) -> Result<(), String> {
        let me = self.held_name();
        let Some(n) = self.topology.read().await.node(executor).cloned() else {
            return Err(format!("{executor} is not a node of this admin's view, so no exact birth is named to inspect"));
        };
        let Some(incarnation) = n.incarnation_id.clone() else {
            return Err(format!("{executor} holds no incarnation id to inspect"));
        };
        let held_row = self.digest_view.get(n.node_id.as_str()).filter(|(dg, _)| dg.node.incarnation == incarnation);
        // The birth's digest names its runtime; else its own durable runtime row does (a point read).
        let row = if held_row.as_ref().is_some_and(|(dg, _)| dg.node.runtime.is_some()) {
            None
        } else {
            Some(birth_runtime_row(&*self.nodes, &me, &n.name, &n.node_id, &incarnation).await?.ok_or_else(|| {
                format!("the view holds no runtime for {executor} ({}) at {} and no durable runtime row of that birth was ever written at {me}", n.node_id, incarnation.as_str())
            })?)
        };
        match crate::admin::inspection_of(&*self.provider, &held_row, row.as_ref()).await {
            Inspected::Exited(_) => {
                (self.refresh)().await;
                Ok(())
            }
            other => Err(format!("its exact runtime is not exited: {}", crate::admin::verdict_name(&other))),
        }
    }

    /// The fabric-primary claimed the create of the fully crashed `mesh`'s first node-admin: it gives the
    /// mesh a new node-admin and authors the fabric `degraded` ([`Records::open_mesh_recovery`]), naming
    /// the node-admin births of the mesh its view holds.
    async fn mesh_given_node_admin(&self, mesh: &str, _build_id: &crate::build::BuildId, _attempt: u32, from: std::time::Instant) {
        give_mesh_node_admin(&self.records, &self.held_name(), &self.topology, mesh, from).await;
    }

}

/// The path.name the lost birth lived at, where fabric recovery re-creates the node-admin
/// (fabric-recovery.md step 3): its digest's, else its departure's. `None` while the view holds neither.
pub(crate) fn lost_birth_name(digest_view: &DigestView, lost: &rdm_core::handover::HandoverBirth) -> Option<PathName> {
    let exact = |d: &rdm_core::digest::MeshDigest| d.node.node_id == lost.node_id && d.node.incarnation == lost.incarnation;
    digest_view.get(lost.node_id.as_str())
        .map(|(d, _)| d)
        .filter(exact)
        .or_else(|| digest_view.stopped_digests().into_iter().find(exact))
        .map(|d| d.node.name)
        .or_else(|| digest_view.deleted().into_iter().find(|op| op.node_id == lost.node_id && op.incarnation == lost.incarnation).map(|op| op.name))
}

/// The fabric primary `me` claimed the create of the fully crashed `mesh`'s first node-admin on the
/// provider's proof of every node: it gives the mesh a new node-admin and authors the fabric
/// `degraded` ([`Records::open_mesh_recovery`], verdict `fully-crashed`), naming the node-admin births
/// of the mesh `view` holds. `from` is the instant before the claim: the mesh's new primary reports
/// after it.
pub async fn give_mesh_node_admin(records: &Records, me: &PathName, view: &RwLock<Topology>, mesh: &str, from: std::time::Instant) -> bool {
    let lost = view.read().await.nodes_of_type(mesh, NodeKind::NodeAdmin).filter_map(|n| n.incarnation_id.clone()).collect();
    records.open_mesh_recovery(me, PeerRecovery { mesh: mesh.to_string(), verdict_rafka_ms: None, opened_at: from, lost }, "fully-crashed", false)
}

/// The mesh no member of which answers (mesh-investigation.md step 5, "No member answers"): a peer mesh
/// whose investigation decided `unreachable` (no member answered either probe, no carrier reported its
/// edge to a node-admin lost) is owed node.create of its first node-admin in process here, on the
/// investigation's verdict (Q111): the probes reached none of its node-admins, so they are re-created;
/// the create's own fence asks the provider about the birth at the path and decides its data dir
/// (`R-FM2`). Answers the mesh and the node-admin owed (the mesh's first in path order, or the one the
/// Build already carried across the fence).
pub(crate) fn unanswered_peer_mesh(me: &PathName, current: &crate::build_state::BuildProjection, unreachable: &(dyn Fn(&str) -> bool + Sync)) -> Option<(String, PathName)> {
    current.topology.meshes.values().filter(|m| m.name != me.mesh && unreachable(&m.name)).find_map(|m| {
        let first = m.nodes_of_type(NodeKind::NodeAdmin).min()?;
        crate::build_drive::first_admin_owed(current, &m.name, first).map(|owed| (m.name.clone(), owed))
    })
}

/// What a drift opening decides on, as one comparable string: for a recovery (the carrier-edge-lost
/// verdict, or the fully crashed mesh), the decided mesh's node-admins by birth and status and the
/// completed receipts of the create; for the
/// mesh that lost its mesh-primary, the exited birth and the completed receipts of its create and delete
/// (`node.delete:`, `node.create:`, `node.update:<path>`); for surplus, each live birth by
/// incarnation. A completed receipt is an operation's own `Complete` step: the steps a failed attempt
/// completed before its failure are no progress, so they move nothing and open no next attempt.
pub(crate) fn drift_inputs(t: &Topology, steps: &[crate::build_state::BuildStepReceipt], repair: Option<(&PathName, &IncarnationId)>, verdict: Option<(&str, &PathName)>, surplus: &[String]) -> String {
    let complete = |op: &str| {
        let member = format!("{op}/");
        steps.iter().filter(|s| s.step == "Complete" && s.outcome == crate::build_state::StepOutcome::Complete && (s.operation == op || s.operation.starts_with(&member))).count()
    };
    let birth = |n: &Node| format!("{}:{}", n.name, n.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or("-"));
    if let Some((label, admin)) = verdict {
        // The node-admins the view holds of the decided mesh, as they are now: a created admin, or one
        // heard again, moves them.
        let mut admins: Vec<String> = t.nodes_of_type(&admin.mesh, NodeKind::NodeAdmin).map(|n| format!("{}:{:?}", birth(n), n.status)).collect();
        admins.sort();
        return format!("{label} {} create {admin} admins=[{}] receipts={}", admin.mesh, admins.join(","), complete(&format!("node.create:{admin}")));
    }
    if let Some((path, incarnation)) = repair {
        let receipts: usize = ["node.delete", "node.create", "node.update"].iter().map(|op| complete(&format!("{op}:{path}"))).sum();
        return format!("repair {path}:{} receipts={receipts}", incarnation.as_str());
    }
    surplus.iter().map(|item| t.members().find(|n| n.name.to_string() == *item).map(birth).unwrap_or_else(|| item.clone())).collect::<Vec<_>>().join("; ")
}

/// The fabric authority's pass over the settled accepted Build (the current Build unchanged: the
/// topology did not change). When this admin is the fabric primary it opens the next attempt for one
/// of four things, in this order:
/// - a peer mesh whose investigation decided `carrier-edge-lost` (`decided`): the verdict runs
///   node.create of the mesh's first node-admin into the live mesh, on this same Build and the mesh's
///   own MeshId (mesh-investigation.md step 6). The attempt is the one-step recipe of that create
///   (`AttemptAction::Recipe`, reason Recipe); the create's own fence asks the provider about the lost
///   admin and deletes it only on its exact exit, so nothing is proven or recorded here;
/// - a peer mesh whose investigation decided `unreachable` (`unreachable`: mesh-investigation.md step
///   5, "No member answers"): the same recipe, for the mesh's first node-admin
///   ([`unanswered_peer_mesh`]); the drive that claims that attempt authors the fabric `degraded`
///   ([`Records::open_mesh_recovery`], R-DG1);
/// - a mesh this authority hears (it is not held unheard) that lost its mesh-primary, whose unheard
///   node-admin the provider was asked about: the same recipe, for that admin, on whatever verdict
///   (node-recovery.md step 3, Q111). A peer mesh held unheard is otherwise evidence only (R-D1);
/// - surplus: a live birth of a kept mesh the topology does not name, or an incomplete mesh.delete.
///
/// A member of a mesh with a live mesh-primary, a node-admin included, is not repaired here: its
/// mesh-primary investigates it and submits its recovery (node-recovery.md, mesh-recovery.md:8-9).
/// `started` holds, per opening, the inputs it was opened on (`drift_inputs`), so an attempt that
/// fails is opened again only once one of them moves, never once per pass; the adapter's
/// insert-and-fail on the attempt keeps two authorities from opening it twice.
pub async fn reconcile_drift(
    time: &rdm_transport::clock::RafkaTime,
    _me: &PathName,
    me_id: &NodeId,
    t: &Topology,
    digest_view: &DigestView,
    provider: &dyn crate::deployment::provider::DeploymentProvider,
    builds: &dyn BuildStateAdapter,
    contexts: &crate::build_claim::AttemptContexts,
    admission: &tokio::sync::Mutex<()>,
    durable: &[crate::storage::RuntimeRow],
    started: &mut HashSet<(crate::build::BuildId, u32, Vec<String>)>,
    held: &mut HashSet<(NodeId, IncarnationId)>,
    hold: &(dyn Fn(&str) -> Option<u64> + Sync),
    defers: &(dyn Fn(&str) -> bool + Sync),
    decided: &(dyn Fn(&str) -> bool + Sync),
    unreachable: &(dyn Fn(&str) -> bool + Sync),
) -> Option<(crate::build::BuildId, u32)> {
    let authority = t.fabric_primary().filter(|n| n.node_id == *me_id)?;
    let me = &authority.name;
    let current = match builds.current().await {
        Ok(current) => current?,
        Err(e) => {
            tracing::info_span!("rdm.node_admin.build.reject.via-build-store-unreadable", route = "proven-drift", node = %me, error = %e)
                .in_scope(|| tracing::info!("the current Build could not be read: no drift attempt is opened on a Build this pass cannot read"));
            return None;
        }
    };
    if matches!(current.state, crate::build_state::BuildState::Pending | crate::build_state::BuildState::Running) {
        return None;
    }
    // A fabric.delete in force, its attempt failed, is never current (R-BI3), yet its intent is retained:
    // nothing reconciles toward the Build before it, and no deleted runtime is recovered for being absent
    // (build.md §1; fabric-delete.md "Failure", Q155).
    match crate::build_state::fabric_delete_in_force(builds).await {
        Ok(None) => {}
        Ok(Some((delete, _))) => {
            let key = (delete.build_id.clone(), 0u32, vec!["fabric-delete-in-force".to_string()]);
            if started.insert(key) {
                tracing::info_span!("rdm.node_admin.build.reject.via-fabric-delete-in-force", route = "proven-drift", node = %me, build_id = %delete.build_id.log())
                    .in_scope(|| tracing::info!("a fabric.delete is in force: no drift attempt is opened"));
            }
            return None;
        }
        Err(e) => {
            tracing::info_span!("rdm.node_admin.build.reject.via-build-store-unreadable", route = "proven-drift", node = %me, error = %e)
                .in_scope(|| tracing::info!("the Build log could not be read for a fabric.delete in force: no drift attempt is opened"));
            return None;
        }
    }
    // Every unheard birth the provider was asked about, with its verdict (node-recovery.md step 3: any
    // verdict but `reached` opens the recovery, Q111).
    let mut exited = HashSet::new();
    let mut proven: std::collections::BTreeMap<PathName, crate::drift::ExitedBirth> = Default::default();
    // The map this authority reads from: its view, plus every birth the durable runtime rows name
    // at a path its view holds nothing at (a sibling this admin never heard). Such a birth is
    // unheard: it is inspected like any unheard birth.
    let mut map = t.clone();
    map.nodes.extend(durable_births(t, durable, &|p| current.topology.contains(p), &|id| digest_view.is_deleted(id)));
    let t = &map;
    // The carrier-edge-lost verdict (mesh-investigation.md step 6): a peer mesh whose investigation
    // decided its recovery is owed node.create of a node-admin. A mesh this authority never heard on the
    // backbone is held `ready` in its view from topology alone (topology is not liveness), so the
    // ladder's decision, not a status, names the mesh. The admin is the one the Build already carried
    // across the fence, else the mesh's first in path order.
    let verdict: Option<(String, PathName)> = current.topology.meshes.values().filter(|m| m.name != me.mesh && decided(&m.name)).find_map(|m| {
        let first = m.nodes_of_type(NodeKind::NodeAdmin).min()?;
        crate::build_drive::first_admin_owed(&current, &m.name, first).map(|admin| (m.name.clone(), admin))
    });
    // No member answers (mesh-investigation.md step 5): a peer mesh the investigation found unreachable
    // is owed the same create.
    let crashed = if verdict.is_none() { unanswered_peer_mesh(me, &current, unreachable) } else { None };
    // The births this authority may have to recover: the ones it no longer hears.
    let candidates = crate::drift::unheard(t);
    let mut held_now: HashSet<(NodeId, IncarnationId)> = HashSet::new();
    for n in candidates {
        // The node-admin the verdict opens a node.create for is that attempt's: its exit is neither held
        // as evidence nor proven here.
        if verdict.as_ref().is_some_and(|(_, admin)| *admin == n.name) {
            continue;
        }
        // A birth the Build stopped on request is not drift: its `node.stop:<path>` operation completed
        // for this exact birth (node-stop.md: no bind, no JoinNode, no replacement), and the exit it
        // proves is the one that was asked for.
        // The stop's `Complete` is that birth's when its attempt carried the stop fenced to it.
        let operation = format!("node.stop:{}", n.name);
        let stop = n.incarnation_id.clone().map(|from_incarnation| crate::accepted::AttemptAction::Stop { path: n.name.clone(), from_incarnation });
        if stop.as_ref().is_some_and(|stop| current.steps.iter().any(|s| s.operation == operation && s.step == "Complete" && s.outcome == crate::build_state::StepOutcome::Complete && current.carried(s.attempt, stop))) {
            let key = (current.build_id.clone(), 0u32, vec![n.name.to_string(), n.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default(), "stopped-on-request".to_string()]);
            if started.insert(key) {
                tracing::info_span!("rdm.node_admin.build.reject.via-stopped-on-request", node = %me, birth = %n.name, node_id = %n.node_id, operation = %operation)
                    .in_scope(|| tracing::info!("the birth's exit was asked for by a completed node.stop: no repair attempt"));
            }
            continue;
        }
        // A peer mesh with no live node-admin is recovered on the investigation's decision
        // (`crate::investigate`), not on the first verdict: every one of its births waits for it
        // (R-D1: provider evidence is not authority, and a node this authority replaces while the
        // mesh's first admin is still to be recovered is a node that admin then replaces again from
        // its own map). A node of a mesh that still has a live node-admin is recovered here as ever.
        // An ordinary birth of such a mesh is held as evidence once the ladder tracks the mesh as
        // unheard (`hold`); until then it waits with the admins.
        if n.mesh != me.mesh && defers(&n.mesh) && t.nodes_of_type(&n.mesh, NodeKind::NodeAdmin).all(|a| !a.status.is_live()) && (n.kind == NodeKind::NodeAdmin || hold(&n.mesh).is_none()) {
            // The hold is named once per mesh and Build: an unnamed deferral is a recovery nobody is
            // waiting on.
            let unheard = digest_view.mesh_unheard(&n.mesh, std::time::Instant::now());
            let key = (current.build_id.clone(), 0u32, vec!["deferred".to_string(), n.mesh.clone()]);
            if started.insert(key) {
                tracing::info_span!("rdm.node_admin.build.reject.via-drift-deferred", node = %me, mesh = %n.mesh, birth = %n.name, unheard_ms = unheard.map(|d| d.as_millis().to_string()).unwrap_or_else(|| "none".into()))
                    .in_scope(|| tracing::info!("the peer mesh's node-admins wait for the investigation's recovery decision"));
            }
            continue;
        }
        // A peer mesh held unheard is held as a mesh (R-D1): none of its nodes is restarted, replaced or
        // deleted from here, whatever the provider proves of its runtime. The ladder's own decision
        // (above) is the one release, for the mesh's first recovering node-admin; once that admin stands,
        // the mesh reconciles its ordinary nodes itself.
        let unheard_ms = if n.mesh != me.mesh { hold(&n.mesh) } else { None };
        // An ordinary member is recovered by its own mesh-primary (node-recovery.md: "The
        // mesh-primary is the reconciler for its own members"), never restarted or replaced from
        // here; only a held mesh's ordinary birth is still inspected, as evidence (R-D1).
        if n.kind != NodeKind::NodeAdmin && unheard_ms.is_none() {
            continue;
        }
        // So is a node-admin of a mesh with a live mesh-primary (mesh-recovery.md:8-9, "A lost member
        // of a mesh with a live mesh-primary is node recovery, driven by that mesh-primary"); a mesh
        // primary's own loss by the admin its mesh seats. So is every node-admin of a peer mesh heard
        // again whose recovery the ladder still holds decided: the decision released the mesh's first
        // recovering node-admin, which the verdict above opens, and nothing else of the mesh from here
        // (R-D1: "once mesh authority is restored, that mesh performs its own ordinary-node
        // reconciliation"), however long the ladder takes to step the decision off. What stays here is
        // the mesh that lost its mesh-primary (mesh recovery).
        if n.kind == NodeKind::NodeAdmin && unheard_ms.is_none() && (decided(&n.mesh) || crate::executor::node_admin_recoverer(&n.name, t).is_some()) {
            continue;
        }
        let held_row = digest_view.get(n.node_id.as_str()).filter(|(dg, _)| Some(&dg.node.incarnation) == n.incarnation_id.as_ref());
        let row = durable.iter().find(|r| r.node_id == n.node_id && Some(&r.incarnation_id) == n.incarnation_id.as_ref());
        let Some(incarnation) = n.incarnation_id.clone() else { continue };
        let found = crate::admin::inspection_of(provider, &held_row, row).await;
        let code = if let Inspected::Exited(code) = &found { *code } else { None };
        let verdict_name = crate::admin::verdict_name(&found);
        if let Some(unheard_ms) = unheard_ms {
            // The evidence is kept and named once per birth for as long as the hold lasts; it opens
            // nothing and is not the view's departure.
            held_now.insert((n.node_id.clone(), incarnation.clone()));
            if held.insert((n.node_id.clone(), incarnation.clone())) {
                tracing::info_span!(
                    "rdm.node_admin.build.reject.via-unheard-mesh",
                    node = %n.name,
                    node_id = %n.node_id,
                    incarnation_id = %incarnation.as_str(),
                    mesh = %n.mesh,
                    unheard_ms,
                    authority = %authority.name,
                    verdict = verdict_name,
                    exit_code = code.map(|c| c.to_string()).unwrap_or_default(),
                )
                .in_scope(|| tracing::info!("the provider was asked about the birth, but its mesh is unheard: no attempt, no NodeDeleted, no birth from here"));
            }
            continue;
        }
        exited.insert(incarnation.clone());
        proven.insert(n.name.clone(), crate::drift::ExitedBirth { node_id: n.node_id.clone(), incarnation, code, verdict: verdict_name });
    }
    // A hold ends with the silence: a mesh heard again holds no birth, and the next silence is a new hold.
    held.retain(|k| held_now.contains(k));
    let short = crate::drift::shortfall(&current.topology, t, &exited);
    // One unheard birth per attempt, the ones whose runtime does not run first, then in path.name
    // order; the next pass takes the next remaining one. A runtime that runs may still answer: the
    // create's fence asks it before anything is created over it ([`crate::seat_revision::fence`]).
    let first_exited: Option<(&PathName, &crate::drift::ExitedBirth)> =
        proven.iter().filter(|(p, _)| short.iter().any(|s| s.exited.iter().any(|e| *e == p.to_string()))).min_by_key(|(p, b)| (b.verdict == "running", p.to_string()));
    // Surplus: a live birth of a kept Mesh the accepted topology does not name (a removal the
    // executing view had not heard yet when the Build closed). Its retirement is its own attempt,
    // taken only once no exited birth is left to repair.
    let mut surplus: Vec<String> = t
        .members()
        .filter(|n| n.status.is_live() && current.topology.meshes.contains_key(&n.mesh) && !current.topology.contains(&n.name))
        .filter(|n| n.mesh == me.mesh || hold(&n.mesh).is_none())
        // A birth whose retirement the Build already receipted (`NodeDeleting` of this exact birth) is
        // not surplus: that operation retires it, and this view is only late in hearing it go.
        .filter(|n| {
            let (node_id, incarnation) = (serde_json::to_value(&n.node_id).unwrap_or_default(), serde_json::to_value(&n.incarnation_id).unwrap_or_default());
            !current.steps.iter().any(|st| st.step == "NodeDeleting" && st.output.as_ref().is_some_and(|o| o.get("node_id") == Some(&node_id) && o.get("incarnation") == Some(&incarnation)))
        })
        .map(|n| n.name.to_string())
        .collect();
    surplus.sort();
    if verdict.is_none() && crashed.is_none() && first_exited.is_none() && surplus.is_empty() {
        return None;
    }
    // The node-admin a recovery owes a node.create: the carrier-edge-lost verdict's, or the fully crashed mesh's.
    let recover_admin: Option<&PathName> = verdict.as_ref().map(|(_, admin)| admin).or(crashed.as_ref().map(|(_, admin)| admin));
    let attempt = current.attempt + 1;
    // The create of a node-admin into a live mesh is a one-step recipe on the existing carrier
    // (`BuildChange::Recipe`; no new wire variant): the node the create names pins the birth the view
    // holds there, so a later loss of the same path is a different action.
    let create_of = |admin: &PathName| crate::accepted::recipe_action(vec![rdm_core::build::RecipeStep::Create { node: admin.clone(), executable: None }], t);
    let (key, scope, action) = if let Some(admin) = recover_admin {
        match create_of(admin) {
            Ok(action) => (vec![admin.to_string()], admin.to_string(), Some(action)),
            Err(e) => {
                tracing::info_span!("rdm.node_admin.build.reject.via-recipe-unplannable", build_id = %current.build_id.log(), node = %me, create = %admin, detail = %e).in_scope(|| tracing::info!("the verdict's node.create is no recipe the planner accepts: no attempt"));
                return None;
            }
        }
    } else {
        match first_exited {
            Some((path, _)) => {
                // Only a node-admin of a mesh that lost its mesh-primary reaches here (every other member is
                // its mesh-primary's to recover): its mesh gets node.create of a node-admin, run by this
                // fabric-primary (node-recovery.md step 3); the create's fence deletes the unheard birth it
                // finds there, `TerminateRuntime` first when it runs.
                let scope = short.iter().filter(|s| s.exited.iter().any(|e| *e == path.to_string())).map(ToString::to_string).collect::<Vec<_>>().join("; ");
                match create_of(path) {
                    Ok(action) => (vec![path.to_string()], scope, Some(action)),
                    Err(e) => {
                        tracing::info_span!("rdm.node_admin.build.reject.via-recipe-unplannable", build_id = %current.build_id.log(), node = %me, create = %path, detail = %e).in_scope(|| tracing::info!("the node.create of the unheard admin is no recipe the planner accepts: no attempt"));
                        return None;
                    }
                }
            }
            None if surplus.is_empty() => return None,
            None => (surplus.clone(), format!("surplus: {}", surplus.join(" ")), None),
        }
    };
    let reason = if action.is_some() { crate::build_state::AttemptReason::Recipe } else { crate::build_state::AttemptReason::ProvenDrift };
    // An opening is keyed on what the attempt decides on, never on its number: a failed attempt
    // whose inputs are all as they were when it opened gets no next attempt from here, and one
    // input moving opens exactly one (lifecycles/node-status.md: "a failed Build ... gets its next
    // attempt", once per failure; R-S2: event-driven, never per pass). A key carrying the attempt
    // number never matches the next pass (every failed attempt raises it), which reopened a resume
    // that failed fast on every pass.
    let inputs = drift_inputs(t, &current.steps, first_exited.filter(|_| recover_admin.is_none()).map(|(p, b)| (p, &b.incarnation)), recover_admin.map(|admin| (if crashed.is_some() { "fully-crashed" } else { "carrier-edge-lost" }, admin)), &surplus);
    let ident = key.join(" ");
    let opened_at = (current.build_id.clone(), 0u32, vec!["opened".to_string(), ident.clone(), inputs.clone()]);
    if started.contains(&opened_at) {
        let unchanged = (current.build_id.clone(), 0u32, vec!["unchanged".to_string(), ident.clone(), inputs.clone(), current.attempt.to_string()]);
        if started.insert(unchanged) {
            tracing::info_span!("rdm.node_admin.build.reject.via-drift-unchanged", node = %me, build_id = %current.build_id.log(), failed_attempt = current.attempt, scope = %scope, inputs = %inputs)
                .in_scope(|| tracing::info!("the last attempt opened for these inputs ended and none of them moved since: no next attempt until one does"));
        }
        return None;
    }
    let opening = (current.build_id.clone(), attempt, key);
    // The same admission a submitted change takes, held to the attempt's durable write: the attempt
    // opens on the Build that is still the accepted one, still settled, at the same attempt, or
    // nothing opens, nothing of it is written and nothing is spent.
    let _admitted = admission.lock().await;
    match builds.current().await {
        Ok(Some(now)) if now.build_id == current.build_id && now.attempt == current.attempt && !matches!(now.state, crate::build_state::BuildState::Pending | crate::build_state::BuildState::Running) => {}
        Ok(other) => {
            tracing::info_span!("rdm.node_admin.build.reject.via-build-in-progress", route = "proven-drift", build_id = %current.build_id.log(), attempt, scope = %scope, accepted_now = %other.map(|b| format!("{} {:?}", b.build_id, b.state)).unwrap_or_default())
                .in_scope(|| tracing::info!("another Build or attempt was admitted first: no drift attempt"));
            return None;
        }
        Err(e) => {
            tracing::info_span!("rdm.node_admin.build.reject.via-build-store-unreadable", route = "proven-drift", node = %me, build_id = %current.build_id.log(), attempt, scope = %scope, error = %e)
                .in_scope(|| tracing::info!("the current Build could not be read again under the admission: no drift attempt"));
            return None;
        }
    }
    started.retain(|k| !(k.0 == current.build_id && k.1 == 0 && k.2.len() == 3 && k.2[0] == "opened" && k.2[1] == ident));
    started.insert(opened_at);
    started.insert(opening);
    let action_name = action.as_ref().map(|a| match a { crate::accepted::AttemptAction::Restart { .. } => "restart", crate::accepted::AttemptAction::DeleteConnections { .. } => "connections-delete", crate::accepted::AttemptAction::Stop { .. } => "stop", crate::accepted::AttemptAction::Start { .. } => "start", crate::accepted::AttemptAction::Recover { .. } => "recover", crate::accepted::AttemptAction::Delete { .. } => "delete", crate::accepted::AttemptAction::Recipe { .. } => "recipe", crate::accepted::AttemptAction::Terminate { .. } => "terminate" }).unwrap_or("none");
    let span = match &verdict {
        Some((mesh, admin)) => tracing::info_span!(
            "rdm.node_admin.build.update.via-carrier-edge-lost",
            build_id = %current.build_id.log(),
            attempt,
            mesh = %mesh,
            node = %admin,
            scope = %scope,
            authority = %authority.name,
            authority_node_id = %authority.node_id,
            reason = reason.as_str(),
            action = action_name,
        ),
        None => tracing::info_span!(
            "rdm.node_admin.build.update.via-proven-drift",
            build_id = %current.build_id.log(),
            attempt,
            scope = %scope,
            authority = %authority.name,
            authority_node_id = %authority.node_id,
            reason = reason.as_str(),
            action = action_name,
            exit_code = first_exited.and_then(|(_, b)| b.code).map(|c| c.to_string()).unwrap_or_default(),
            verdict = first_exited.map(|(_, b)| b.verdict).unwrap_or(""),
        ),
    };
    let opened = crate::build_state::AttemptOpened {
        mesh: None,
        build_id: current.build_id.clone(),
        attempt,
        reason,
        action,
        opened_by: me.to_string(),
        opened_at_ms: time.now_ms(),
    };
    // The attempt's context is this span, on this fabric-primary, before the attempt is open to be claimed.
    if let Err(e) = contexts.put(&current.build_id, attempt, &span.in_scope(crate::build_claim::current_context)).await {
        span.in_scope(|| tracing::info!(error = %e, "the attempt's context could not be recorded; the attempt is not opened"));
        return None;
    }
    match builds.open_attempt(&opened).await {
        Ok(()) => {
            span.in_scope(|| {
                if verdict.is_some() {
                    tracing::info!("the investigation decided the mesh's recovery: the next attempt of the same Build, node.create of its node-admin, is open")
                } else if crashed.is_some() {
                    tracing::info!("no member of the mesh answers: the next attempt of the same Build, node.create of its node-admin, is open")
                } else {
                    tracing::info!("a birth the accepted topology names is unheard: the next attempt of the same Build is open")
                }
            });
            Some((current.build_id.clone(), attempt))
        }
        Err(e @ crate::build_state::BuildStateError::AttemptTaken { .. }) => {
            // Another action holds the number: nothing was opened, and there is no attempt to execute.
            tracing::info_span!("rdm.node_admin.build.reject.via-attempt-taken", route = "proven-drift", build_id = %current.build_id.log(), attempt, scope = %scope, detail = %e)
                .in_scope(|| tracing::info!(detail = %e, "attempt refused: another action holds the number"));
            None
        }
        Err(e) => {
            span.in_scope(|| tracing::info!(error = %e, "the attempt could not be opened"));
            None
        }
    }
}

/// What an entry answer hands this admin of Fabric control: the Fabric identity, the answering
/// admin's current Build with its attempt (the floor this admin's copy of that Build must reach), and
/// its own mesh's held lifecycle state ([`adopt_held_mesh_state`]). The first entry and every repeat of
/// it take them through here.
pub(crate) async fn take_entry_control(answer: &crate::wire::JoinAnswer, accepted: &AcceptedStore, entry_floor: &Mutex<Option<(crate::build::BuildId, u32)>>, records: &Records, mesh: &str, mesh_id: &MeshId) {
    if let Some(r) = answer.control.fabric.clone() {
        accepted.learn(r, &answer.served_by).await;
    }
    if let Some(floor) = answer.control.build.clone() {
        *entry_floor.lock().unwrap() = Some((floor.build_id, floor.attempt));
    }
    adopt_held_mesh_state(answer, records, mesh, mesh_id);
}

/// This admin's own mesh's held lifecycle state, as its maker's entry answer holds it, adopted exactly
/// into its applied state (mesh-investigation.md "A node-admin joins the live mesh" step 2: it
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
        declared.meshes.insert(mesh_id.clone(), state);
    }
    tracing::info_span!("rdm.node_admin.mesh.update.via-held-state-adopted", mesh, mesh_id = %mesh_id, state = ?state, served_by = %answer.served_by, publisher = %publisher)
        .in_scope(|| tracing::info!("the mesh's held lifecycle state is adopted as it exists: no transition, no durable write"));
}

/// A held Mesh status word as the Mesh lifecycle state it names (states.md: pending ->
/// ready-for-traffic -> leaving -> dead); any other word is refused by name.
pub(crate) fn mesh_state_of(status: &str) -> Result<rdm_core::status::MeshState, String> {
    use rdm_core::status::MeshState;
    match status {
        "pending" => Ok(MeshState::Pending),
        "ready-for-traffic" => Ok(MeshState::ReadyForTraffic),
        "leaving" => Ok(MeshState::Leaving),
        "dead" => Ok(MeshState::Dead),
        other => Err(format!("mesh status {other:?} is no Mesh lifecycle state (pending, ready-for-traffic, leaving, dead)")),
    }
}

/// Never Ready while a Build's facts are missing: an admin that holds no current Build (R-BI3: no
/// Build in its builds store is in progress or completed) names itself blocked. Only Day 0 accepts
/// the first Build itself; every other admin fetches the Build facts from an existing authority
/// (its entry pull, or the fabric control topic).
///
/// Ready also needs the facts of the Build the entry named, the answering admin's current Build: it
/// must be readable from this admin's own log, and its folded attempt must reach the attempt that
/// admin held of it (`floor`). An admin that was not served an entry (Day 0) owes no floor. The
/// reason is typed ([`crate::fetch_build::NodeAdminNotReady`]): the Ready check recovers each one with
/// the request it names.
pub async fn node_admin_not_ready(me: &PathName, builds: &dyn BuildStateAdapter, floor: Option<(crate::build::BuildId, u32)>) -> Option<crate::fetch_build::NodeAdminNotReady> {
    use crate::fetch_build::NodeAdminNotReady as B;
    if let Some((id, attempt)) = floor {
        match builds.read_build(&id).await {
            Ok(held) if held.attempt < attempt => return Some(B::BehindFloor { me: me.clone(), build_id: id, held: held.attempt, floor: attempt }),
            Ok(_) => {}
            Err(crate::build_state::BuildStateError::UnknownBuild(_)) => return Some(B::NoCurrentBuild { me: me.clone(), wanted: Some(id) }),
            Err(e) => return Some(B::UnreadableBuild { me: me.clone(), build_id: id, error: e.to_string() }),
        }
    }
    match builds.current_build().await {
        Ok(crate::build_state::CurrentBuild::None) => Some(B::NoCurrentBuild { me: me.clone(), wanted: None }),
        Ok(_) => None,
        Err(e) => Some(B::BuildStoreUnreadable { me: me.clone(), error: e.to_string() }),
    }
}
