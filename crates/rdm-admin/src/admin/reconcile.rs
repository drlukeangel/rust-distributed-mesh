//! Seat gate, provider departure proofs, and the drift reconcile loop.

use super::*;

/// The gates of the fabric-primary's drive: the membership's cut-off rule and the Build log's
/// authority, read from the committed fabric seat record.
pub(crate) struct SeatGate {
    pub(crate) membership: Membership,
    pub(crate) builds: Arc<FabricBuildStateAdapter>,
    /// The control plane whose peer-mesh reader the investigation installs once its ladder runs
    /// (after the drive is built): read when asked, as the Build door reads it.
    pub(crate) control: Arc<ControlPlane>,
    /// The 0x21 door node.primary.update is commanded and run through, which holds what each update
    /// was answered and said (`HandoverDoor::memo`).
    pub(crate) handover: Arc<crate::fabric_handover::HandoverDoor>,
    /// Projects the book into the view now, so a completed stop-node lists the stopped birth parked
    /// before `BuildReply::Complete` goes out on the caller's stream.
    pub(crate) project: Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>,
}

#[async_trait::async_trait]
impl crate::build_drive::Gate for SeatGate {
    fn authorizes(&self) -> bool {
        self.membership.authorizes()
    }
    async fn fence(&self) -> Result<Option<String>, crate::build_state::BuildStateError> {
        self.builds.fence_check().await
    }
    async fn fence_set(&self) {
        self.builds.fence_set().await
    }
    fn peer_mesh(&self, mesh: &str) -> Option<crate::investigate::PeerMesh> {
        self.control.peer_mesh.get().and_then(|f| f(mesh))
    }
    /// node.primary.update before an operation on a seat holder (the approved proposal
    /// node-primary-update-fabric-mesh-and-node-type.md, Part C1 step 1: "The holder is commanded from
    /// above"): a mesh-primary's seat is commanded from here (the fabric-primary commands a
    /// mesh-primary); any other cohort's seat through its mesh-primary, its commander and writer (Q3); the fabric-primary's own seats it moves itself, having nothing above it, both in
    /// ONE row when it holds both (Q1: "the one row of C1 commits both seats to NA2 at their next
    /// epochs").
    async fn move_seats(&self, m: &crate::executor::SeatMove, build_id: &crate::build::BuildId, attempt: u32, run: u32, operation: &str) -> Result<(), crate::build_drive::SeatMoveEnd> {
        use crate::election::Moving;
        use crate::fabric_handover::update_operation;
        use rdm_core::Seat;
        let door = &self.handover;
        if m.holder.node_id != door.node_id {
            // A mesh-primary's seat, commanded on its holder; any other cohort's seat, commanded through
            // its mesh-primary, which writes the row (this admin itself when it is that mesh-primary).
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
        door.move_own_seats(moving, build_id.as_str(), attempt, &update).await?;
        Ok(())
    }

    async fn seats_acknowledged(&self, updates: &[crate::build_drive::PrecedingUpdate], build_id: &crate::build::BuildId, attempt: u32) -> Result<(), String> {
        for u in updates {
            if u.holder.is_none() && u.mesh == self.handover.me.mesh {
                continue;
            }
            self.handover.row_acknowledged(&u.mesh, &u.operation, u.holder.as_ref(), build_id.as_str(), attempt).await?;
        }
        Ok(())
    }

    async fn attempt_completed(&self, build_id: &crate::build::BuildId, _attempt: u32) {
        if let Ok(p) = self.builds.read_build(build_id).await {
            let mut stopped = false;
            for s in &p.steps {
                if s.operation.starts_with("node.stop:") && s.step == "Complete" && s.outcome == crate::build_state::StepOutcome::Complete {
                    stopped = true;
                    if let Some(op) = s.output.as_ref().and_then(|v| serde_json::from_value::<rdm_core::LifecycleOp>(v.clone()).ok()) {
                        let _ = self.membership.book.stopped(op);
                    }
                }
            }
            if stopped {
                (self.project)().await;
            }
        }
    }
}

/// Proof that an executor's birth departed: the provider inspects the exact runtime the digest book
/// or the durable runtime rows name for it, and only an exited runtime counts.
pub(crate) struct ProviderDeparture {
    /// The admin proving the departure: the reader its refusals name.
    pub(crate) me: PathName,
    pub(crate) topology: Arc<RwLock<Topology>>,
    /// Projects this admin's view now and installs it, so a birth just proven exited holds no seat
    /// in the view the next attempt is planned from.
    pub(crate) refresh: Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>,
    pub(crate) book: DigestBook,
    pub(crate) provider: Arc<dyn DeploymentProvider>,
    pub(crate) nodes: Arc<dyn crate::storage::NodesStorage>,
    pub(crate) records: Arc<Records>,
    /// The fabric's seat moves, read for the takeover that moved the fabric seat.
    pub(crate) fabric: Arc<dyn crate::fabric_storage::FabricStorage>,
}

impl ProviderDeparture {
    pub(crate) fn held_name(&self) -> PathName {
        self.book.held_path_name().unwrap_or_else(|| self.me.clone())
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
        let held_row = self.book.get(n.node_id.as_str()).filter(|(dg, _)| dg.node.incarnation == incarnation);
        // The birth's digest names its runtime; else its own durable runtime row does (a point read).
        let row = if held_row.as_ref().is_some_and(|(dg, _)| dg.node.runtime.is_some()) {
            None
        } else {
            Some(birth_runtime_row(&*self.nodes, &me, &n.name, &n.node_id, &incarnation).await?.ok_or_else(|| {
                format!("the book holds no runtime for {executor} ({}) at {} and no durable runtime row of that birth was ever written at {me}", n.node_id, incarnation.as_str())
            })?)
        };
        let (code, source) = crate::admin::exit_proof_of(&*self.provider, &held_row, row.as_ref()).await?;
        let recorded = self.records.put_exit(&n, code, source).await;
        tracing::info_span!("rdm.node_admin.build.update.via-executor-exit-proof", node = %n.name, node_id = %n.node_id, incarnation_id = %incarnation.as_str(), exit_code = code.map(|c| c.to_string()).unwrap_or_default(), exit_proof = source, exit_proof_row = %recorded.as_ref().map(|_| "put".to_string()).unwrap_or_else(|e| e.clone()))
            .in_scope(|| tracing::info!("the executor's exact runtime is inspected and exited"));
        recorded.map_err(|e| format!("{executor}'s exit is proven but not recorded, so no projection holds it gone: {e}"))?;
        (self.refresh)().await;
        Ok(())
    }

    /// The provider's exit proof of every one of `nodes`, as the fabric authority's pass reads it
    /// ([`nodes_exit_proof`] over this admin's view and the durable runtime rows): what the drive
    /// admits a fully crashed mesh's create on. Durable rows that cannot be read prove nothing, and
    /// every node is named so.
    async fn exits_of(&self, nodes: &[PathName]) -> crate::build_drive::ExitProofs {
        let me = self.held_name();
        let durable = match self.nodes.runtimes().await {
            Ok(rows) => rows,
            Err(e) => return crate::build_drive::ExitProofs { proven: Vec::new(), unproven: nodes.iter().map(|a| format!("{a}: the durable runtime rows could not be read at {me}: {e}")).collect() },
        };
        let view = self.topology.read().await.clone();
        let mut map = view.clone();
        map.nodes.extend(durable_births(&view, &durable, &|p| nodes.contains(p), &|id| self.book.is_departed(id)));
        nodes_exit_proof(nodes, &map, &self.book, &durable, self.records.path_probe().as_deref(), &*self.provider).await
    }

    /// The fabric-primary claimed the create of the fully crashed `mesh`'s first node-admin: it gives the
    /// mesh a new node-admin and authors the fabric `degraded` ([`Records::open_mesh_recovery`]), naming
    /// the node-admin births of the mesh its view holds.
    async fn mesh_given_node_admin(&self, mesh: &str, _build_id: &crate::build::BuildId, _attempt: u32, from: std::time::Instant) {
        give_mesh_node_admin(&self.records, &self.held_name(), &self.topology, mesh, from).await;
    }

    /// The fabric seat's latest move read from the fabric's seat moves, when it is a takeover. Moves
    /// that cannot be read are named and answer none: nothing is re-created without the takeover read.
    async fn taken_from(&self) -> Option<(u64, rdm_core::handover::HandoverBirth)> {
        match self.fabric.seat_moves().await {
            Ok(moves) => crate::fabric_storage::fabric_seat_taken_from(&moves),
            Err(e) => {
                tracing::info_span!("rdm.node_admin.seat.reject.via-seat-moves-unreadable", node = %self.held_name(), store = "fabric.storage", error = %e)
                    .in_scope(|| tracing::info!("the fabric's seat moves could not be read: no lost mesh's node-admin is re-created from the fabric lane this pass"));
                None
            }
        }
    }

    /// Whether the exit of the exact birth the view names for `executor` is recorded: a point read
    /// of its exit-proof row (node-recovery.md: "Exited is the proof, recorded durably"). A row that
    /// cannot be read is named and answers not recorded: the exit is proven again, and its row put
    /// again, a blind put of the same key.
    async fn recorded(&self, executor: &PathName) -> bool {
        let Some(n) = self.topology.read().await.node(executor).cloned() else {
            return false;
        };
        match self.records.exit_row_of(&n).await {
            Ok(row) => row.is_some(),
            Err(e) => {
                tracing::info_span!("rdm.node_admin.node.reject.via-exit-row-unreadable", node = %self.held_name(), subject = %n.name, node_id = %n.node_id, rows = 1, detail = %e)
                    .in_scope(|| tracing::info!("the executor's exit-proof row could not be read: its exit is not taken as recorded"));
                false
            }
        }
    }
}

/// The fabric primary `me` claimed the create of the fully crashed `mesh`'s first node-admin on the
/// provider's proof of every node: it gives the mesh a new node-admin and authors the fabric
/// `degraded` ([`Records::open_mesh_recovery`], verdict `fully-crashed`), naming the node-admin births
/// of the mesh `view` holds. `from` is the instant before the claim: the mesh's new primary reports
/// after it.
pub async fn give_mesh_node_admin(records: &Records, me: &PathName, view: &RwLock<Topology>, mesh: &str, from: std::time::Instant) -> bool {
    let lost = view.read().await.cohort(mesh, NodeKind::NodeAdmin).filter_map(|n| n.incarnation_id.clone()).collect();
    records.open_mesh_recovery(me, PeerRecovery { mesh: mesh.to_string(), verdict_rafka_ms: None, opened_at: from, lost }, "fully-crashed", false)
}

/// The provider's exact exit proof of every one of `nodes`, the nodes the accepted topology names for
/// one mesh (mesh-investigation.md step 5, "Fully crashed": "Every node of mesh-x, its node-admins
/// included, has crashed ... The fabric-primary acts on the provider's exact exit proof, never on
/// silence"): for each, the birth `t` holds at its path (the view, plus the births durable runtime
/// rows name at a path the view holds nothing at, [`durable_births`]), inspected through the runtime
/// its digest or its durable row names. A path with no birth, a birth with no incarnation, and a
/// runtime not proven exited (it runs, or cannot be told) are each named as not proven. Nothing is recorded. The fabric authority's pass and the drive's fence
/// both read it here ([`read_node_exits`]).
pub async fn nodes_exit_proof(
    nodes: &[PathName],
    t: &Topology,
    book: &DigestBook,
    durable: &[crate::storage::RuntimeRow],
    probe: Option<&dyn crate::seat_revision::PathProbe>,
    provider: &dyn crate::deployment::provider::DeploymentProvider,
) -> crate::build_drive::ExitProofs {
    read_node_exits(nodes, t, book, durable, probe, provider).await.0
}

/// [`nodes_exit_proof`], with each proven node's exited birth (`None` for a node nothing was ever born
/// of): what the fabric authority's pass opens the create on.
pub(crate) async fn read_node_exits(
    nodes: &[PathName],
    t: &Topology,
    book: &DigestBook,
    durable: &[crate::storage::RuntimeRow],
    probe: Option<&dyn crate::seat_revision::PathProbe>,
    provider: &dyn crate::deployment::provider::DeploymentProvider,
) -> (crate::build_drive::ExitProofs, Vec<(PathName, Option<crate::drift::ExitedBirth>)>) {
    let (mut proven, mut unproven) = (Vec::new(), Vec::new());
    for node in nodes {
        let Some(n) = t.node(node) else {
            // A node the accepted topology names that no view and no durable runtime row holds a birth of
            // (a Build's growth, not yet created): the provider is asked what is recorded at its path.
            match unborn_node_exit(node, probe).await {
                Ok(()) => proven.push((node.clone(), None)),
                Err(why) => unproven.push(format!("{node}: {why}")),
            }
            continue;
        };
        let Some(incarnation) = n.incarnation_id.clone() else {
            unproven.push(format!("{node}: its birth {} has no incarnation", n.node_id));
            continue;
        };
        let held_row = book.get(n.node_id.as_str()).filter(|(dg, _)| dg.node.incarnation == incarnation);
        let row = durable.iter().find(|r| r.node_id == n.node_id && r.incarnation_id == incarnation);
        match exit_proof_of(provider, &held_row, row).await {
            Ok((code, source)) => proven.push((node.clone(), Some(crate::drift::ExitedBirth { node_id: n.node_id.clone(), incarnation, code, source }))),
            Err(why) => unproven.push(format!("{node}: {why}")),
        }
    }
    (crate::build_drive::ExitProofs { proven: proven.iter().map(|(p, _)| p.to_string()).collect(), unproven }, proven)
}

/// The provider's proof for a node the accepted topology names that neither the view nor a durable
/// runtime row holds a birth of: the question the create's own fence asks of a path the view holds
/// nothing at ([`crate::seat_revision::fence`]), "the world is asked, not assumed", read through
/// [`crate::seat_revision::PathProbe::recorded`] (node-create.md: "Runtime exit requires independent exact
/// provider proof, even for a deployment that never joined"). A runtime recorded at the path that is
/// not proven exited (it runs, or cannot be told), records that cannot be read, and no probe held,
/// each hold the mesh by name; nothing recorded that runs proves the path.
pub(crate) async fn unborn_node_exit(node: &PathName, probe: Option<&dyn crate::seat_revision::PathProbe>) -> Result<(), String> {
    use crate::deployment::provider::DeploymentStatus;
    let Some(probe) = probe else {
        return Err("neither the view nor a durable runtime row holds a birth there, and no provider probe of the runtimes recorded at its path is held here".into());
    };
    let recorded = probe.recorded(node).await.map_err(|reason| format!("neither the view nor a durable runtime row holds a birth there, and the runtimes recorded at its path cannot be read: {reason}"))?;
    match recorded.iter().find(|(_, status)| !matches!(status, DeploymentStatus::Exited { .. })) {
        None => Ok(()),
        Some((at, DeploymentStatus::Running)) => Err(format!("no birth is held for it, and a runtime recorded at {at} still runs")),
        Some((at, DeploymentStatus::OsStopped)) => Err(format!("no birth is held for it, and the operating system holds a runtime recorded at {at} suspended (os-stopped)")),
        Some((at, status @ (DeploymentStatus::Unknown | DeploymentStatus::Exited { .. }))) => Err(format!("no birth is held for it, and the runtime recorded at {at} is not proven exited: {status:?}")),
    }
}

/// The fully crashed mesh (mesh-investigation.md step 5, "Fully crashed"; R-ML3): a peer mesh whose
/// investigation decided `unreachable` (`unreachable`: no member answered, no carrier reported its edge
/// to a node-admin lost) is held as ever. It is owed node.create of its first node-admin only if the
/// provider proves the exact runtime of EVERY node the accepted topology names for it exited
/// ([`nodes_exit_proof`] over `t`): "Every node of mesh-x, its node-admins included, has crashed". A
/// node not proven (a member that still runs included) holds the mesh, and the hold names it. Silence
/// is never the proof. The proof read is one named span, once per distinct result. Answers the mesh,
/// the node-admin owed (the mesh's first in path order, or the one the Build already carried across the
/// fence) and that node-admin's proof; nothing is recorded, and the create's own fence deletes a birth
/// only on its exact exit.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn crashed_peer_mesh(
    me: &PathName,
    current: &crate::build_state::BuildProjection,
    t: &Topology,
    book: &DigestBook,
    durable: &[crate::storage::RuntimeRow],
    probe: Option<&dyn crate::seat_revision::PathProbe>,
    provider: &dyn crate::deployment::provider::DeploymentProvider,
    started: &mut HashSet<(crate::build::BuildId, u32, Vec<String>)>,
    unreachable: &(dyn Fn(&str) -> bool + Sync),
    taken: Option<&(u64, rdm_core::handover::HandoverBirth)>,
) -> Option<(String, PathName, Option<crate::drift::ExitedBirth>)> {
    for m in current.topology.meshes.values().filter(|m| m.name != me.mesh && unreachable(&m.name)) {
        let Some(first) = m.cohort(NodeKind::NodeAdmin).min() else { continue };
        // The mesh the fabric seat was taken from (elections §4.3): its first node-admin is re-created
        // with no exit proof, and the create's own fence decides its data dir (node-recovery.md `R-FM2`).
        if let Some((seat_move, lost)) = taken.filter(|(_, lost)| lost.mesh == m.name) {
            if t.cohort(&m.name, NodeKind::NodeAdmin).all(|a| !a.status.is_live()) {
                let Some(owed) = crate::build_drive::first_admin_owed(current, &m.name, first) else { continue };
                if started.insert((current.build_id.clone(), 0u32, vec!["takeover-recreate".to_string(), m.name.clone(), lost.node_id.to_string(), seat_move.to_string()])) {
                    crate::build_drive::takeover_recreate_span(me, &current.build_id, &m.name, *seat_move, lost);
                }
                return Some((m.name.clone(), owed, None));
            }
        }
        let nodes: Vec<PathName> = m.nodes.iter().cloned().collect();
        let (proofs, proven) = read_node_exits(&nodes, t, book, durable, probe, provider).await;
        if started.insert((current.build_id.clone(), 0u32, vec!["crash-proof".to_string(), m.name.clone(), proofs.proven.join(","), proofs.unproven.join("; ")])) {
            crate::build_drive::crash_proof_span(me, &current.build_id, &m.name, nodes.len(), &proofs);
        }
        if !proofs.unproven.is_empty() {
            continue;
        }
        let Some(owed) = crate::build_drive::first_admin_owed(current, &m.name, first) else { continue };
        let Some((_, proof)) = proven.into_iter().find(|(p, _)| *p == owed) else { continue };
        return Some((m.name.clone(), owed, proof));
    }
    None
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
        let mut admins: Vec<String> = t.cohort(&admin.mesh, NodeKind::NodeAdmin).map(|n| format!("{}:{:?}", birth(n), n.status)).collect();
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
/// - a peer mesh whose investigation decided `unreachable` (`unreachable`) and whose every node the
///   provider proves exited (mesh-investigation.md step 5, "Fully crashed": a mesh whose every node is gone is
///   unheard by construction and no member can answer): the same recipe, for the mesh's first node-admin
///   ([`crashed_peer_mesh`]); the drive that claims that attempt authors the fabric `degraded`
///   ([`Records::open_mesh_recovery`], R-DG1). A node whose exit is not proven holds the mesh, and the
///   hold names it;
/// - a mesh this authority hears (it is not held unheard) that lost its mesh-primary, whose node-admin the
///   provider proves exited (exact exit proof, never silence): the same recipe, for that admin. A peer mesh
///   held unheard is otherwise evidence only (R-D1);
/// - surplus: a live birth of a kept mesh the topology does not name, or an incomplete mesh.delete.
///
/// A member of a mesh with a live mesh-primary, a node-admin included, is not repaired here: its
/// mesh-primary proves its exit and submits its recovery (node-recovery.md, mesh-recovery.md:8-9).
/// `started` holds, per opening, the inputs it was opened on (`drift_inputs`), so an attempt that
/// fails is opened again only once one of them moves, never once per pass; the adapter's
/// insert-and-fail on the attempt keeps two authorities from opening it twice.
/// `verdicts` holds the exits this loop recorded ([`record_verdict_once`]), so a proven exit's row is
/// put once, by the pass that proved it.
pub async fn reconcile_drift(
    time: &rdm_transport::clock::RafkaTime,
    _me: &PathName,
    me_id: &NodeId,
    t: &Topology,
    book: &DigestBook,
    provider: &dyn crate::deployment::provider::DeploymentProvider,
    builds: &dyn BuildStateAdapter,
    contexts: &crate::build_claim::AttemptContexts,
    admission: &tokio::sync::Mutex<()>,
    durable: &[crate::storage::RuntimeRow],
    started: &mut HashSet<(crate::build::BuildId, u32, Vec<String>)>,
    held: &mut HashSet<(NodeId, IncarnationId)>,
    verdicts: &ExitVerdicts,
    hold: &(dyn Fn(&str) -> Option<u64> + Sync),
    defers: &(dyn Fn(&str) -> bool + Sync),
    decided: &(dyn Fn(&str) -> bool + Sync),
    unreachable: &(dyn Fn(&str) -> bool + Sync),
    exits: &Records,
    taken: Option<&(u64, rdm_core::handover::HandoverBirth)>,
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
    // Every unheard birth whose exact runtime the provider proves exited, with the exit code the
    // proof carries (the provider's own, or the process runtime's record in its data dir).
    let mut exited = HashSet::new();
    let mut proven: std::collections::BTreeMap<PathName, crate::drift::ExitedBirth> = Default::default();
    // The map this authority proves from: its view, plus every birth the durable runtime rows name
    // at a path its view holds nothing at (a sibling this admin never heard). Such a birth is not
    // reached, not dead: it is inspected like any unheard birth, and only an exited runtime counts.
    let mut map = t.clone();
    map.nodes.extend(durable_births(t, durable, &|p| current.topology.contains(p), &|id| book.is_departed(id)));
    let t = &map;
    // The carrier-edge-lost verdict (mesh-investigation.md step 6): a peer mesh whose investigation
    // decided its recovery is owed node.create of a node-admin. A mesh this authority never heard on the
    // backbone is held `ready` in its view from topology alone (topology is not liveness), so the
    // ladder's decision, not a status, names the mesh. The admin is the one the Build already carried
    // across the fence, else the mesh's first in path order.
    let verdict: Option<(String, PathName)> = current.topology.meshes.values().filter(|m| m.name != me.mesh && decided(&m.name)).find_map(|m| {
        let first = m.cohort(NodeKind::NodeAdmin).min()?;
        crate::build_drive::first_admin_owed(&current, &m.name, first).map(|admin| (m.name.clone(), admin))
    });
    // The fully crashed mesh (mesh-investigation.md step 5): a peer mesh the investigation found
    // unreachable, every node of which the provider proves exited, is owed the same create.
    let crashed = if verdict.is_none() { crashed_peer_mesh(me, &current, t, book, durable, exits.path_probe().as_deref(), provider, started, unreachable, taken).await } else { None };
    // The births this authority may have to prove: the ones it no longer hears.
    let candidates = crate::drift::unheard(t);
    let mut held_now: HashSet<(NodeId, IncarnationId)> = HashSet::new();
    let mut proven_now: HashSet<(NodeId, IncarnationId)> = HashSet::new();
    // Where each proven exit's row stands after this pass (`<path.name>=put|recorded|<refusal>`), named
    // on the pass's attempt span or its unchanged refusal.
    let mut exit_proof_rows: Vec<String> = Vec::new();
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
        // (`crate::investigate`), not on the first exit proof: every one of its births waits for it
        // (R-D1: provider evidence is not authority, and a node this authority replaces while the
        // mesh's first admin is still to be recovered is a node that admin then replaces again from
        // its own map). A node of a mesh that still has a live node-admin is proven here as ever.
        // An ordinary birth of such a mesh is held as evidence once the ladder tracks the mesh as
        // unheard (`hold`); until then it waits with the admins.
        if n.mesh != me.mesh && defers(&n.mesh) && t.cohort(&n.mesh, NodeKind::NodeAdmin).all(|a| !a.status.is_live()) && (n.kind == NodeKind::NodeAdmin || hold(&n.mesh).is_none()) {
            // The hold is named once per mesh and Build: an unnamed deferral is a recovery nobody is
            // waiting on.
            let unheard = book.mesh_unheard(&n.mesh, std::time::Instant::now());
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
        let held_row = book.get(n.node_id.as_str()).filter(|(dg, _)| Some(&dg.node.incarnation) == n.incarnation_id.as_ref());
        let row = durable.iter().find(|r| r.node_id == n.node_id && Some(&r.incarnation_id) == n.incarnation_id.as_ref());
        // A silent birth this authority cannot prove is named once, with what it lacks: it is not
        // drift until its exact runtime is inspected, and an authority that holds no fact for it
        // can never inspect it.
        let mut unprovable = |why: String| {
            let key = (current.build_id.clone(), 0u32, vec![n.name.to_string(), n.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default(), why.clone()]);
            if started.insert(key) {
                tracing::info_span!("rdm.node_admin.build.reject.via-drift-unprovable", node = %me, birth = %n.name, node_id = %n.node_id, status = ?n.status, reason = %why)
                    .in_scope(|| tracing::info!("a silent birth the accepted topology names cannot be proven exited by this authority"));
            }
        };
        let (code, source) = match exit_proof_of(provider, &held_row, row).await {
            Ok(p) => p,
            Err(why) => {
                unprovable(why);
                continue;
            }
        };
        {
            let incarnation = n.incarnation_id.clone().expect("filtered on it");
            if let Some(unheard_ms) = unheard_ms {
                // The evidence is kept and named once per birth for as long as the hold lasts; the exit
                // authorizes nothing and is not recorded as the view's departure.
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
                        exit_code = code.map(|c| c.to_string()).unwrap_or_default(),
                        exit_proof = source,
                    )
                    .in_scope(|| tracing::info!("the provider proves the birth exited, but its mesh is unheard: no attempt, no NodeDeleted, no birth from here"));
                }
                continue;
            }
            // The proof is recorded durably, once (`record_verdict_once`): the birth's exit-proof row is
            // what every projection reads as its proven loss, put by the pass that proved the exit and
            // not again by a later pass that finds it standing. A refused put leaves the birth
            // unproven to every reader, and the next pass that proves it puts the row.
            let birth = (n.node_id.clone(), incarnation.clone());
            proven_now.insert(birth.clone());
            match record_verdict_once(verdicts, birth, &Inspected::Exited(code, source), || async { exits.put_exit(n, code, source).await.map(|_| ()) }).await {
                Ok(stands) => exit_proof_rows.push(format!("{}={stands}", n.name)),
                Err(e) => {
                    exit_proof_rows.push(format!("{}={e}", n.name));
                    tracing::info_span!("rdm.node_admin.node.reject.via-exit-row-unwritten", node = %n.name, node_id = %n.node_id, incarnation_id = %incarnation.as_str(), authority = %authority.name, error = %e)
                        .in_scope(|| tracing::info!("the provider proves the birth exited, but its exit-proof row is not written: no projection holds it exited; the repair is decided from the proof in hand"));
                }
            }
            exited.insert(incarnation.clone());
            proven.insert(n.name.clone(), crate::drift::ExitedBirth { node_id: n.node_id.clone(), incarnation, code, source });
        }
    }
    // A hold ends with the silence: a mesh heard again holds no birth, and the next silence is a new hold.
    held.retain(|k| held_now.contains(k));
    // A recorded exit is held while this pass still proves it: a birth no longer proven here is
    // recorded again by the next pass that proves it.
    verdicts.lock().unwrap().retain(|k, _| proven_now.contains(k));
    let short = crate::drift::shortfall(&current.topology, t, &exited);
    // One exited birth per attempt, in path.name order; the next pass takes the next remaining one.
    let first_exited: Option<(&PathName, &crate::drift::ExitedBirth)> =
        proven.iter().find(|(p, _)| short.iter().any(|s| s.exited.iter().any(|e| *e == p.to_string())));
    // Surplus: a live birth of a kept Mesh the accepted topology does not name (a removal the
    // executing view had not heard yet when the Build closed). Its retirement is its own attempt,
    // taken only once no exited birth is left to repair.
    let mut surplus: Vec<String> = t
        .members()
        // A `<name>.new` successor is its recipe's, never surplus (lifecycles/node-replace.md "Rollback").
        .filter(|n| n.status.is_live() && !n.name.new && current.topology.meshes.contains_key(&n.mesh) && !current.topology.contains(&n.name))
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
    let recover_admin: Option<&PathName> = verdict.as_ref().map(|(_, admin)| admin).or(crashed.as_ref().map(|(_, admin, _)| admin));
    let attempt = current.attempt + 1;
    // The create of a node-admin into a live mesh is a one-step recipe on the existing carrier
    // (`BuildChange::Recipe`; no new wire variant): the node the create names pins the birth the view
    // holds there, so a later loss of the same path is a different action.
    let create_of = |admin: &PathName| crate::accepted::recipe_action(vec![rdm_core::build::RecipeStep::Create { node: admin.clone() }], t);
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
                // its mesh-primary's to recover), and a proven-exited birth has no process to restart
                // (node-restart.md): its mesh gets node.create of a node-admin, run by this fabric-primary
                // for a mesh with no node-admin left that is not proven gone (mesh-investigation.md step 5
                // "Fully crashed"); the create's fence deletes the exited birth it finds there.
                let scope = short.iter().filter(|s| s.exited.iter().any(|e| *e == path.to_string())).map(ToString::to_string).collect::<Vec<_>>().join("; ");
                match create_of(path) {
                    Ok(action) => (vec![path.to_string()], scope, Some(action)),
                    Err(e) => {
                        tracing::info_span!("rdm.node_admin.build.reject.via-recipe-unplannable", build_id = %current.build_id.log(), node = %me, create = %path, detail = %e).in_scope(|| tracing::info!("the node.create of the exited admin is no recipe the planner accepts: no attempt"));
                        return None;
                    }
                }
            }
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
            tracing::info_span!("rdm.node_admin.build.reject.via-drift-unchanged", node = %me, build_id = %current.build_id.log(), failed_attempt = current.attempt, scope = %scope, inputs = %inputs, exit_proof_rows = %exit_proof_rows.join(", "))
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
    let action_name = action.as_ref().map(|a| match a { crate::accepted::AttemptAction::Restart { .. } => "restart", crate::accepted::AttemptAction::DeleteConnections { .. } => "drain", crate::accepted::AttemptAction::Stop { .. } => "stop", crate::accepted::AttemptAction::Start { .. } => "start", crate::accepted::AttemptAction::Recover { .. } => "recover", crate::accepted::AttemptAction::Delete { .. } => "delete", crate::accepted::AttemptAction::Recipe { .. } => "recipe" }).unwrap_or("none");
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
            exit_code = crashed.as_ref().and_then(|(_, _, b)| b.as_ref()).or(first_exited.map(|(_, b)| b)).and_then(|b| b.code).map(|c| c.to_string()).unwrap_or_default(),
            exit_proof = crashed.as_ref().and_then(|(_, _, b)| b.as_ref()).or(first_exited.map(|(_, b)| b)).map(|b| b.source).unwrap_or("none"),
            exit_proof_rows = %exit_proof_rows.join(", "),
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
                    tracing::info!("every node of the fully crashed mesh is proven exited: the next attempt of the same Build, node.create of its node-admin, is open")
                } else {
                    tracing::info!("a birth the accepted topology names is proven gone: the next attempt of the same Build is open")
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
        rdm_transport::membership::Frame::MeshStatus { mesh: m, status, publisher, .. } if m == mesh => Some((status.clone(), publisher.clone())),
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

/// Never Ready around missing control hydration: an admin that holds no current Build (R-BI3: no
/// Build in its builds store is in progress or completed) names itself blocked. Only Day 0 accepts
/// the first Build itself; every other admin hydrates the Build facts from an existing authority
/// (its entry pull, or the fabric control topic).
///
/// Ready also needs the facts of the Build the entry named, the answering admin's current Build: it
/// must be readable from this admin's own log, and its folded attempt must reach the attempt that
/// admin held of it (`floor`). An admin that was not served an entry (Day 0) owes no floor. The
/// reason is typed ([`crate::hydrate::HydrationBlocker`]): the Ready check recovers each one with
/// the request it names.
pub async fn hydration_blocker(me: &PathName, builds: &dyn BuildStateAdapter, floor: Option<(crate::build::BuildId, u32)>) -> Option<crate::hydrate::HydrationBlocker> {
    use crate::hydrate::HydrationBlocker as B;
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
