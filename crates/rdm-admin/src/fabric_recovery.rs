//! Fabric recovery (fabric-recovery.md "The loss of the fabric-primary while a node-admin lives";
//! elections §3, §4.1, §4.3; build.md §5.3; Q122). Unheard past the staleness floor, the fabric-primary is
//! taken by nobody: the lowest Ready node-admin of its mesh, or when the mesh has none the
//! lowest Ready mesh-primary on the backbone from mesh investigation's decision step, runs one
//! workflow. It takes no primary role. Node investigation decides what becomes of the lost birth (Q111):
//! reached, it is commanded back through node.mesh.join; any other verdict, node recovery re-creates the
//! node-admin at its path.name, an attempt of the current Build that names the lost birth, except from the
//! backbone on `running`, where the report stands and nothing is written (Q153). When the node
//! at that path.name is Ready the workflow restores it as mesh-primary and fabric-primary
//! (node.primary.update's call on it): the node installs both roles, acknowledges, and records them on
//! its own row; the workflow completes on the acknowledgement.
//! Under an in-force fabric.delete it re-creates and restores nothing: the executor holds the
//! fabric-primary's roles to the end and continues the delete (step 4, Q155). It takes the fabric-primary
//! role and the lost mesh's mesh-primary role by one winning claim of the delete's next attempt and one
//! recorded transition at the next revision, keeping its own mesh's, from its own mesh or from the
//! backbone alike (Q160).
//!
//! Every fact the workflow acts on is read again from the view, the records and the Build on each pass:
//! an executor that replaces a lost one starts it over and resumes from what is recorded.

use crate::election::Incumbency;
use crate::model::{Node, NodeId, NodeKind, NodeStatus, PathName};
use crate::topology::Topology;
use rdm_core::Primary;
use rdm_transport::topology::DigestView;
use std::sync::{Arc, Mutex};
use tracing::Instrument as _;

/// What fabric recovery is, in one admin's view: the fabric-primary record it restores, the path.name the
/// node-admin is re-created at, and the admin that runs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricRecovery {
    /// The fabric-primary record whose birth is not a heard primary.
    pub target: Primary,
    /// The path.name that birth lived at: its node's in the view, else its digest's or its departure's.
    pub at: Option<PathName>,
    /// The admin that runs the workflow: the lowest Ready node-admin of the target's mesh, else
    /// the lowest Ready mesh-primary of another mesh.
    pub executor: Option<PathName>,
    /// Whether `executor` is of the target's own mesh (the own-mesh arm) rather than the backbone's.
    pub own_mesh: bool,
}

/// Fabric recovery in `view` with the records `inc` and the digest view `digest_view`; `None` while the fabric-primary
/// record names a heard primary, or no record is held.
pub fn of(view: &Topology, inc: &Incumbency, digest_view: &DigestView) -> Option<FabricRecovery> {
    let members: Vec<Node> = view.members().cloned().collect();
    let target = crate::election::fabric_recovery_target(&members, inc)?.clone();
    let lost = rdm_core::handover::HandoverBirth { mesh: target.mesh.clone(), node_id: target.node_id.clone(), incarnation: target.incarnation.clone() };
    let at = members.iter().find(|n| n.node_id == target.node_id).map(|n| n.name.clone()).or_else(|| crate::admin::lost_birth_name(digest_view, &lost));
    // Without the lost birth's path.name this admin cannot tell the node re-created there from the mesh's
    // other node-admins: it names no executor of the mesh's own (a node that recovery re-created learns of
    // the workflow from its maker).
    let own = at.as_ref().and_then(|at| crate::election::fabric_recovery_executor(&members, inc, &target, Some(at))).map(|n| n.name.clone());
    let backbone = || {
        members
            .iter()
            .filter(|n| n.kind == NodeKind::NodeAdmin && n.mesh != target.mesh && n.is_primary && n.status == NodeStatus::Ready && !n.incarnation_id.as_ref().is_some_and(|i| inc.lost.contains(i)))
            .min_by(|a, b| a.node_id.as_str().cmp(b.node_id.as_str()))
            .map(|n| n.name.clone())
    };
    let executor = if at.is_some() { own.clone().or_else(backbone) } else { None };
    Some(FabricRecovery { own_mesh: own.is_some(), executor, target, at })
}

/// What the claim of an in-force fabric.delete's next attempt came to (Q160).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteClaim {
    /// This admin holds the attempt: the one it claimed now, or held already.
    Won(u32),
    /// Another executor holds the delete's attempt, live or by a winning claim, and continues it: named.
    Lost(String),
    /// Nothing decided on this pass (a store not read, a claim not open yet): named.
    NotYet(String),
    /// The delete has no attempt claimed yet, so no executor of it was lost and there is nothing to
    /// supersede: its first attempt is the drive's claim, once the Builds it interrupts have ended
    /// (build.md §1).
    First,
}

/// The claim, by `me`, of the in-force fabric.delete `delete`'s next attempt (fabric-recovery.md step 4,
/// Q160: "one winning claim"). An attempt `me` holds already is its own. An attempt held by an executor
/// other than the lost fabric-primary at `lost_at` is that executor's, which continues it. Otherwise the
/// lost executor's in-flight row of its attempt, which the claim supersedes, is released (a removal that
/// takes it only while it names that attempt), and the next attempt is decided as every claim is
/// ([`crate::build_claim::decide_claim`]): the in-flight row of the lost fabric-primary's mesh taken first,
/// one row whichever mesh each contender is of, then the claim recorded, so of two executors one wins and
/// the other is told the winner. `me_birth` is `me`'s exact node id with the node rows it is read on: a
/// deleted birth claims nothing (Q181).
pub async fn claim_delete_attempt(me: &PathName, builds: &dyn crate::build_state::BuildStateAdapter, claims: &dyn crate::storage::MeshStorage, contexts: &crate::build_claim::AttemptContexts, delete: &crate::build_state::BuildProjection, lost_at: Option<&PathName>, me_birth: Option<(&dyn crate::storage::NodesStorage, &NodeId)>) -> DeleteClaim {
    use crate::build_state::BuildState;
    if delete.attempt == 0 {
        return DeleteClaim::First;
    }
    let executor = delete.executor.as_deref().and_then(|e| e.parse::<PathName>().ok());
    if delete.state == BuildState::Running {
        match &executor {
            Some(e) if e == me => return DeleteClaim::Won(delete.attempt),
            Some(e) if Some(e) != lost_at => return DeleteClaim::Lost(format!("attempt {} of fabric.delete {} is held by {e}", delete.attempt, delete.build_id)),
            Some(e) => {
                let row = crate::mesh_claims::InFlight { build_id: delete.build_id.clone(), lane: None, attempt: delete.attempt, claimant: e.to_string(), claimant_node: None };
                match claims.in_flight(&e.mesh).await {
                    Ok(Some(held)) if held.same_attempt(&row) && held.claimant == row.claimant => {
                        if let Err(err) = claims.release_in_flight(&e.mesh, &held).await {
                            return DeleteClaim::NotYet(format!("mesh {}'s in-flight row of the lost executor's attempt {} could not be released: {err}", e.mesh, delete.attempt));
                        }
                    }
                    Ok(_) => {}
                    Err(err) => return DeleteClaim::NotYet(format!("mesh {}'s in-flight row could not be read: {err}", e.mesh)),
                }
            }
            None => {}
        }
    }
    let attempt = delete.attempt + 1;
    let claim = crate::build_state::BuildAttemptClaim { build_id: delete.build_id.clone(), attempt, executor: me.to_string(), claimant: Some(me.to_string()), mesh: None };
    let decided_in = lost_at.map(|l| l.mesh.as_str()).unwrap_or(me.mesh.as_str());
    match crate::build_claim::decide_claim(me, builds, claims, contexts, &claim, decided_in, me_birth).await {
        rdm_core::build_claim::BuildClaimReply::Won { .. } => DeleteClaim::Won(attempt),
        rdm_core::build_claim::BuildClaimReply::Lost { holder } => DeleteClaim::Lost(format!("attempt {attempt} of fabric.delete {} was claimed by {holder}", delete.build_id)),
        other => DeleteClaim::NotYet(format!("the claim of attempt {attempt} of fabric.delete {} answered {other:?}", delete.build_id)),
    }
}

/// The recorded transition (fabric-recovery.md step 4, Q160: "one recorded transition at the next
/// revision"): in the attempt `attempt` of the delete `build_id` that `me` won, its `PrimariesTaken`
/// receipt, naming the lost fabric-primary `target`, the fabric-primary record's next revision, the lost
/// mesh's mesh-primary record's `mesh_revision`, and the receipts of the superseded attempt `me` holds
/// now. Recorded once: a receipt already in the attempt is the transition.
pub async fn record_primaries_taken(builds: &dyn crate::build_state::BuildStateAdapter, build_id: &crate::build::BuildId, attempt: u32, me: &Node, target: &Primary, mesh_revision: u64) -> Result<crate::build_state::PrimariesTaken, String> {
    use crate::build_state::{receipt_key, BuildStepReceipt, PrimariesTaken, StepOutcome, STEP_PRIMARIES_TAKEN};
    let p = builds.read_build(build_id).await.map_err(|e| format!("Build {build_id} could not be read: {e}"))?;
    if let Some(t) = p.steps.iter().filter(|r| r.mesh.is_none() && r.attempt == attempt && r.step == STEP_PRIMARIES_TAKEN).find_map(|r| r.output.clone().and_then(|v| serde_json::from_value::<PrimariesTaken>(v).ok())) {
        return Ok(t);
    }
    let incarnation = me.incarnation_id.clone().ok_or_else(|| format!("{} holds no incarnation", me.name))?;
    let superseded_attempt = attempt.saturating_sub(1);
    let prior_fabric = crate::build_state::primaries_taken(&p.steps).iter().map(|t| t.fabric_revision).max().unwrap_or(0);
    let taken = PrimariesTaken {
        lost_node_id: target.node_id.clone(),
        lost_incarnation: target.incarnation.clone(),
        executor: me.name.to_string(),
        executor_node_id: me.node_id.clone(),
        executor_incarnation: incarnation,
        fabric_revision: target.revision.max(prior_fabric) + 1,
        mesh: target.mesh.clone(),
        mesh_revision,
        superseded_attempt,
        held: p.steps.iter().filter(|r| r.mesh.is_none() && r.attempt == superseded_attempt).map(receipt_key).collect(),
    };
    let receipt = BuildStepReceipt {
        mesh: None,
        build_id: build_id.clone(),
        attempt,
        operation: crate::build_state::primaries_taken_operation(&target.node_id),
        step: STEP_PRIMARIES_TAKEN.into(),
        outcome: StepOutcome::Complete,
        output: serde_json::to_value(&taken).ok(),
        executor: Some(me.name.to_string()),
    };
    builds.append_step_receipt(&receipt).await.map_err(|e| format!("the {STEP_PRIMARIES_TAKEN} receipt could not be recorded: {e}"))?;
    tracing::info_span!(
        "rdm.node_admin.primary.update.via-fabric-delete-transition",
        node = %me.name,
        build_id = %build_id.log(),
        attempt,
        lost_node_id = %target.node_id,
        fabric_revision = taken.fabric_revision,
        mesh = %taken.mesh,
        mesh_revision,
        superseded_attempt,
        held = taken.held.len() as u64,
    )
    .in_scope(|| tracing::info!("the executor records the transition: the fabric-primary role and the lost mesh's mesh-primary role at the next revision"));
    Ok(taken)
}

/// Fold every recorded [`crate::build_state::PrimariesTaken`] transition of an in-force fabric.delete into
/// `digest_view` and `records` for the executor `me` taking over from `target` (fabric-recovery.md step 4,
/// Q155, Q160): each lost fabric-primary birth is marked gone and offline so a birth known only from
/// forwarded topology (`Heard::Topology`) is not read as a live primary of its mesh, and each mesh whose
/// mesh-primary role an earlier transition moved to the fabric-primary is held by `me` at the next
/// revision.
pub fn fold_primaries_taken(digest_view: &DigestView, records: &crate::admin::Records, me: &Node, target: Option<&Primary>, transitions: &[crate::build_state::PrimariesTaken]) {
    let mut offline = records.offline.lock().unwrap();
    for t in transitions {
        digest_view.primaries.mark_gone(t.lost_incarnation.clone());
        offline.insert((t.lost_node_id.clone(), t.lost_incarnation.clone()));
    }
    if let Some(target) = target {
        digest_view.primaries.mark_gone(target.incarnation.clone());
        offline.insert((target.node_id.clone(), target.incarnation.clone()));
    }
    drop(offline);
    let Some(me_inc) = me.incarnation_id.clone() else { return };
    let target_mesh = target.map(|t| t.mesh.as_str());
    let mut mesh_revs: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    for t in transitions {
        if t.mesh != me.mesh && Some(t.mesh.as_str()) != target_mesh {
            mesh_revs.entry(t.mesh.clone()).and_modify(|r| *r = (*r).max(t.mesh_revision)).or_insert(t.mesh_revision);
        }
    }
    for (mesh, recorded_rev) in mesh_revs {
        let rev = digest_view.primaries.mesh(&mesh).map(|m| m.revision).unwrap_or(0).max(recorded_rev) + 1;
        digest_view.primaries.take(rdm_core::PrimaryRole::of_node_type(NodeKind::NodeAdmin), &Primary { mesh, node_id: me.node_id.clone(), incarnation: me_inc.clone(), revision: rev });
    }
}

/// A hook that reads fabric recovery in a view (the records and the digest view are the admin's own).
pub type Reader = Arc<dyn Fn(&Topology) -> Option<FabricRecovery> + Send + Sync>;

/// A [`Reader`] set once its admin's view and records are held.
#[derive(Default)]
pub struct ReaderSlot(std::sync::OnceLock<Reader>);

impl std::fmt::Debug for ReaderSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.get().is_some() { "ReaderSlot(set)" } else { "ReaderSlot(unset)" })
    }
}

impl ReaderSlot {
    /// Hold `read`; a second set is ignored.
    pub fn set(&self, read: Reader) {
        let _ = self.0.set(read);
    }

    /// The reader, once set.
    pub fn get(&self) -> Option<&Reader> {
        self.0.get()
    }
}

/// The reader over `digest_view`'s records.
pub fn reader(digest_view: DigestView, records: Arc<crate::admin::Records>) -> Reader {
    Arc::new(move |t: &Topology| of(t, &crate::admin::incumbency_of(&digest_view, &records), &digest_view))
}

/// Whether `me` runs fabric recovery of `mesh` in `t` while the mesh has no mesh-primary: the writer of
/// the mesh and node records the attempt's own operations require (build.md §5.3, Q122).
pub fn executes_for(read: Option<&Reader>, t: &Topology, me: &PathName, mesh: &str) -> bool {
    read.and_then(|r| r(t)).is_some_and(|fr| fr.target.mesh == mesh && fr.executor.as_ref() == Some(me)) && t.node_type_primary(mesh, NodeKind::NodeAdmin).is_none()
}

/// The workflow's door on one admin.
pub struct FabricRecoveryDoor {
    /// This admin's path.name as born; its held name is the view's.
    pub me: PathName,
    /// This admin's node id.
    pub me_id: NodeId,
    /// Its view.
    pub topology: Arc<tokio::sync::RwLock<Topology>>,
    /// Its digest view.
    pub digest_view: DigestView,
    /// Its records.
    pub records: Arc<crate::admin::Records>,
    /// Node investigation on the lost birth, and its command back when reached.
    pub looker: crate::primary_watch::Looker,
    /// Node recovery's submit of the re-create.
    pub recovery: crate::admin::RecoverySubmitter,
    /// node.primary.update's door: the restore's call.
    pub handover: crate::fabric_handover::HandoverSlot,
    /// Whether a person launched this admin on the fabric-primary's data dir to recover the fabric
    /// (`RDM_FABRIC_PRIMARY`; fabric-recovery.md: "It starts when a person or a script launches one
    /// node-admin on the fabric-primary's data dir"): at the fabric-primary's path.name with no admin to
    /// run the workflow ("Nothing alive": "It is the only node, the fabric-primary"), it restores itself.
    launched_to_recover: bool,
    /// Whether that launch is the adoption under an in-force fabric.delete, at its own new path.name
    /// (Q159): it runs the workflow when no admin does, whatever the lost birth's path.name.
    launched_at_new_path: bool,
    /// The birth whose workflow runs here now.
    running: Mutex<Option<crate::model::IncarnationId>>,
    /// The admin's stop gate: the workflow is work a stop deletes with the birth
    /// ([`crate::stop_gate::StopGate::until_stopped`]), ended where it stands at this admin's stop.
    gate: crate::stop_gate::StopGate,
}

impl FabricRecoveryDoor {
    /// A door for the admin these name.
    #[allow(clippy::too_many_arguments)]
    pub fn new(me: PathName, me_id: NodeId, topology: Arc<tokio::sync::RwLock<Topology>>, digest_view: DigestView, records: Arc<crate::admin::Records>, looker: crate::primary_watch::Looker, recovery: crate::admin::RecoverySubmitter, handover: crate::fabric_handover::HandoverSlot, launched_to_recover: bool, gate: crate::stop_gate::StopGate) -> Self {
        Self { me, me_id, topology, digest_view, records, looker, recovery, handover, launched_to_recover, launched_at_new_path: false, running: Mutex::new(None), gate }
    }

    /// This door's admin is the adoption launch under an in-force fabric.delete, at its own new path.name
    /// (Q159), when `yes`.
    pub fn adopted_at_new_path(mut self, yes: bool) -> Self {
        self.launched_at_new_path = yes;
        self
    }

    /// Whether this admin runs the workflow `fr`: its executor, or the admin a person launched at the
    /// fabric-primary's path.name when no admin runs it.
    fn runs(&self, fr: &FabricRecovery) -> bool {
        let me = self.held_name();
        // Launched on the fabric-primary's own data dir, its path.name is the fabric-primary's, whether or
        // not this admin knows the lost birth's.
        fr.executor.as_ref() == Some(&me) || (self.launched_to_recover && fr.executor.is_none() && (self.launched_at_new_path || fr.at.as_ref().is_none_or(|at| *at == me)) && self.me_id != fr.target.node_id)
    }

    fn held_name(&self) -> PathName {
        self.digest_view.held_path_name().unwrap_or_else(|| self.me.clone())
    }

    /// Fabric recovery in this admin's view now.
    pub async fn read(&self) -> Option<FabricRecovery> {
        let t = self.topology.read().await.clone();
        of(&t, &crate::admin::incumbency_of(&self.digest_view, &self.records), &self.digest_view)
    }

    /// Start the workflow when this admin runs it: in its own mesh at the first tick after the mark, once
    /// the fabric record has named the unheard birth for a floor (`held_a_floor`); from the backbone at
    /// mesh investigation's decision step (`decided`). Once per lost birth while it runs here.
    pub async fn start_if_mine(self: &Arc<Self>, held_a_floor: bool, decided: bool) {
        let Some(fr) = self.read().await else { return };
        let launched = fr.executor.is_none() && self.runs(&fr);
        if !self.runs(&fr) || !(launched || if fr.own_mesh { held_a_floor } else { decided }) {
            return;
        }
        {
            let mut running = self.running.lock().unwrap();
            if running.as_ref() == Some(&fr.target.incarnation) {
                return;
            }
            *running = Some(fr.target.incarnation.clone());
        }
        let door = self.clone();
        tokio::spawn(async move {
            door.run(fr.target.clone()).await;
            let mut running = door.running.lock().unwrap();
            if running.as_ref() == Some(&fr.target.incarnation) {
                *running = None;
            }
        });
    }

    /// The workflow for the fabric-primary record `target`, pass by pass until the node at its path.name
    /// is restored, the record names a heard primary, or this admin no longer runs it. This admin's stop
    /// ends it where it stands, a look or a submit in flight included: a stopping node takes no new
    /// lifecycle step, and a signal's stop ends its process. After a start the primary watch's next turn decides
    /// again whether it runs here.
    async fn run(self: &Arc<Self>, target: Primary) {
        let me = self.held_name();
        // A turn across the arm and its span: a stop's quiesce resolves once the arm has ended and
        // written its end (stop_gate.rs: work a stop deletes with the birth ends at the stop).
        let _turn = self.gate.begun("fabric-recovery");
        let span = tracing::info_span!(
            "rdm.node_admin.fabric.update.via-fabric-recovery",
            node = %me,
            target = %target,
            at = tracing::field::Empty,
            arm = tracing::field::Empty,
            verdict = tracing::field::Empty,
            restored = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        let (door, of) = (self.clone(), span.clone());
        let outcome = self
            .gate
            .until_stopped(async move { door.run_in(&of, &target).instrument(of.clone()).await })
            .await
            .unwrap_or_else(|| format!("{me}'s stop ended it where it stood"));
        span.record("outcome", outcome.as_str());
        span.in_scope(|| tracing::info!("fabric recovery ended at this admin"));
    }

    async fn run_in(self: &Arc<Self>, span: &tracing::Span, target: &Primary) -> String {
        let mut investigated = false;
        // A re-create was submitted for the lost birth: only then is one owed again after a submit that
        // ended without the node. A reached birth is commanded back, never re-created (Q111); if it is not
        // heard again within a staleness floor, the next investigation decides again.
        let mut submitted = false;
        let mut next_look = std::time::Instant::now();
        // The re-create is owed until its attempt completes, under the one backoff rule: a submit that ends
        // without the node is made again after a delay that doubles, never on every pass.
        let mut done = false;
        let mut next_submit = std::time::Instant::now();
        let mut backoff = rdm_transport::topology::gossip_interval();
        let ceiling = rdm_transport::topology::staleness_floor() * 4;
        loop {
            let me = self.held_name();
            let Some(fr) = self.read().await.filter(|fr| fr.target == *target) else {
                return "the fabric-primary record names a heard primary".into();
            };
            if !self.runs(&fr) {
                return format!("{me} no longer runs it: {}", fr.executor.map(|e| e.to_string()).unwrap_or_else(|| "nobody does".into()));
            }
            let Some(at) = fr.at.clone().or_else(|| self.launched_to_recover.then(|| me.clone())) else {
                return "the lost birth's path.name is not known here".into();
            };
            span.record("at", at.to_string().as_str()).record("arm", if fr.own_mesh { "own-mesh" } else { "backbone" });
            // Under an in-force fabric.delete the workflow re-creates and restores nothing: the delete is the
            // current Build and names no node, so the executor holds the fabric-primary's roles to the end and
            // continues the delete; no hand-back follows (fabric-recovery.md step 4, Q155).
            match crate::build_state::fabric_delete_in_force(&*self.recovery.builds).await {
                Ok(Some((delete, _))) => {
                    span.record("arm", if fr.own_mesh { "own-mesh-under-delete" } else if fr.executor.is_none() { "launched-under-delete" } else { "backbone-under-delete" });
                    if let Some(end) = self.hold_under_delete(span, target, &fr, &delete).await {
                        return end;
                    }
                    tokio::time::sleep(rdm_transport::topology::gossip_interval()).await;
                    continue;
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::info!(error = %e, "the Build log could not be read for a fabric.delete in force: nothing is re-created on this pass");
                    tokio::time::sleep(rdm_transport::topology::gossip_interval()).await;
                    continue;
                }
            }
            let view = self.topology.read().await.clone();
            let lost = view.members().find(|n| n.node_id == target.node_id && n.incarnation_id.as_ref() == Some(&target.incarnation)).cloned();
            // Ready in the view, or re-created by this workflow's completed attempt, whose create
            // waited for it to be ready (an executor of another mesh holds no member status of it).
            let restored = view.node(&at).filter(|n| n.kind == NodeKind::NodeAdmin && (n.status == NodeStatus::Ready || (done && n.node_id != target.node_id))).cloned();
            match (lost, restored) {
                // The node at the path.name is Ready: a new birth, or the lost birth returned.
                (_, Some(node)) if node.node_id != target.node_id || node.incarnation_id.as_ref() != Some(&target.incarnation) => {
                    return match self.restore(target, &node).await {
                        Ok(entries) => {
                            span.record("restored", node.name.to_string().as_str());
                            format!("restored: {}", rdm_node::member_handover::entries_named(&entries))
                        }
                        Err(reply) => format!("the restore of {} was refused: {reply:?}", node.name),
                    };
                }
                (_, Some(_)) => return "the lost birth is heard again: its record names it".into(),
                // The lost birth is still in the view: node investigation decides (Q111).
                (Some(node), None) if !investigated && std::time::Instant::now() >= next_look => {
                    investigated = true;
                    let finding = self.looker.look(&view, &node).await;
                    span.record("verdict", finding.name());
                    match (&finding, finding.inspected()) {
                        (crate::node_investigation::Finding::Reached { .. }, _) => {
                            if let (Some(heard_again), Some(inc)) = (self.looker.heard_again.get(), node.incarnation_id.as_ref()) {
                                let held = Node { status: NodeStatus::PendingReconnect, ..node.clone() };
                                heard_again.command(&held, inc).await;
                            }
                            investigated = false;
                            next_look = std::time::Instant::now() + rdm_transport::topology::staleness_floor();
                        }
                        // From the backbone, `running`: the investigation's report stands and the executor
                        // writes nothing, no row, no name release, no mark against the birth in its own view,
                        // no create (fabric-recovery.md step 3, Q153). A runtime this executor inspects running
                        // but cannot reach is unreachable from it, not lost. The next investigation decides
                        // again a staleness floor on, while the record still names the unheard birth.
                        (crate::node_investigation::Finding::Running, _) if !fr.own_mesh && fr.executor.is_some() => {
                            tracing::info!(subject = %node.name, verdict = finding.name(), "the backbone arm read the fabric-primary's runtime running: the report stands and nothing is written");
                            investigated = false;
                            next_look = std::time::Instant::now() + rdm_transport::topology::staleness_floor();
                        }
                        // From its own mesh, `running` with no eligible peer of the mesh to tickle through:
                        // only this executor failed to reach the fabric-primary, so it is unreachable from
                        // this executor, not lost, and nothing is written; a running fabric-primary its own
                        // mesh cannot reach, its peers' tickles unanswered too, is re-created below
                        // (fabric-recovery.md step 3, Q153). The next investigation decides again a
                        // staleness floor on.
                        (crate::node_investigation::Finding::Running, _) if fr.own_mesh && crate::offline::eligible_carriers(&view, &node, Some(&self.me_id)).next().is_none() => {
                            tracing::info!(subject = %node.name, verdict = finding.name(), "the own-mesh arm read the fabric-primary's runtime running and reached it through no peer: unreachable from this executor, not lost; nothing is written");
                            investigated = false;
                            next_look = std::time::Instant::now() + rdm_transport::topology::staleness_floor();
                        }
                        (_, Some(inspected)) => {
                            submitted = true;
                            done = self.submit(&node, &inspected, fr.own_mesh, &mut next_submit, &mut backoff, ceiling).await;
                        }
                        (_, None) => {}
                    }
                }
                // Gone from the view and not re-created yet (a replacement executor, or a re-create that
                // ended without the node): the attempt resumes from its receipts.
                (Some(node), None) if submitted && !done && std::time::Instant::now() >= next_submit => {
                    // Investigated already: its re-create is owed again after a submit that ended without it.
                    let inspected = crate::deployment::provider::Inspected::Unavailable("re-create owed after a submit that ended without the node".into());
                    done = self.submit(&node, &inspected, fr.own_mesh, &mut next_submit, &mut backoff, ceiling).await;
                }
                (None, None) if !done && std::time::Instant::now() >= next_submit => {
                    let mut node = Node::allocated(at.clone());
                    node.node_id = target.node_id.clone();
                    node.incarnation_id = Some(target.incarnation.clone());
                    let inspected = crate::deployment::provider::Inspected::Unavailable("gone from the view".into());
                    submitted = true;
                    done = self.submit(&node, &inspected, fr.own_mesh, &mut next_submit, &mut backoff, ceiling).await;
                }
                _ => {}
            }
            tokio::time::sleep(rdm_transport::topology::gossip_interval()).await;
        }
    }

    /// Under the in-force fabric.delete `delete` (fabric-recovery.md step 4, Q155, Q160): the executor,
    /// of the lost birth's own mesh, from the backbone, or the admin a person launched ("Nothing alive"),
    /// claims the delete's next attempt ([`claim_delete_attempt`]: one claim wins), records the transition
    /// ([`record_primaries_taken`]) and holds, at the next revision, the fabric-primary role and the lost
    /// mesh's mesh-primary role, keeping its own mesh's, installed on its own node with no call; its drive
    /// then continues the delete as fabric-primary. Nothing is re-created and no hand-back follows. `Some`
    /// ends the workflow (the roles held, or another executor's claim won); `None` holds nothing on this
    /// pass (the next pass reads again).
    async fn hold_under_delete(&self, span: &tracing::Span, target: &Primary, fr: &FabricRecovery, delete: &crate::build_state::BuildProjection) -> Option<String> {
        let me = self.topology.read().await.own(&self.me_id).cloned()?;
        let Some(door) = self.recovery.door.get().cloned() else {
            tracing::info!(build_id = %delete.build_id.log(), "this admin's Build door is not built yet: the delete's next attempt is claimed on a later pass");
            return None;
        };
        let control = &door.control;
        let me_birth = control.node_rows.get().map(|rows| (&**rows, &me.node_id));
        let attempt = match claim_delete_attempt(&me.name, &*control.builds, &*control.claims, &control.contexts, delete, fr.at.as_ref(), me_birth).await {
            DeleteClaim::Won(attempt) => Some(attempt),
            DeleteClaim::First => None,
            DeleteClaim::Lost(why) => {
                tracing::info!(build_id = %delete.build_id.log(), lost = %why, "another executor's claim of the delete's attempt won: it continues the delete");
                return Some(format!("under fabric.delete {}: {why}; another executor continues it", delete.build_id));
            }
            DeleteClaim::NotYet(why) => {
                tracing::info!(build_id = %delete.build_id.log(), reason = %why, "the delete's next attempt is not claimed on this pass");
                return None;
            }
        };
        let prior = crate::build_state::primaries_taken(&delete.steps);
        let prior_mesh = prior.iter().filter(|t| t.mesh == target.mesh).map(|t| t.mesh_revision).max().unwrap_or(0);
        let mesh_revision = self.digest_view.primaries.mesh(&target.mesh).map(|m| m.revision).unwrap_or(0).max(prior_mesh) + 1;
        // With an attempt won, the transition is recorded in it; with none claimed yet there is no attempt
        // to supersede, and the drive's claim of the first is the one claim (build.md §1).
        if let Some(attempt) = attempt {
            if let Err(e) = record_primaries_taken(&*control.builds, &delete.build_id, attempt, &me, target, mesh_revision).await {
                tracing::info!(build_id = %delete.build_id.log(), error = %e, "the transition could not be recorded on this pass");
                return None;
            }
        }
        let transitions = match control.builds.read_build(&delete.build_id).await {
            Ok(p) => crate::build_state::primaries_taken(&p.steps),
            Err(_) => prior,
        };
        fold_primaries_taken(&self.digest_view, &self.records, &me, Some(target), &transitions);
        let installed = if me.mesh == target.mesh {
            self.restore(target, &me).await
        } else {
            match self.handover.get().cloned() {
                Some(h) => h.take_under_delete(target, mesh_revision, &me).await,
                None => Err(rdm_core::reply::handover::not_ready("this admin's node.primary.update door is not built yet".into())),
            }
        };
        match installed {
            Ok(entries) => {
                span.record("restored", me.name.to_string().as_str());
                tracing::info!(build_id = %delete.build_id.log(), attempt = ?attempt, "under an in-force fabric.delete the executor holds the fabric-primary's roles by its claim and the recorded transition, and continues the delete; nothing is re-created");
                Some(format!("holds the fabric-primary's roles under fabric.delete {} at attempt {}: {}", delete.build_id, attempt.map(|a| a.to_string()).unwrap_or_else(|| "none yet".into()), rdm_node::member_handover::entries_named(&entries)))
            }
            Err(reply) => {
                tracing::info!(build_id = %delete.build_id.log(), refused = ?reply, "the executor could not hold the fabric-primary's roles on this pass");
                None
            }
        }
    }

    /// One submit of the re-create of `node`: `true` once its attempt completed; else the next is due after
    /// `backoff`, which doubles up to `ceiling`.
    /// From the backbone (`own_mesh` false) the re-create terminates nothing in the mesh (fabric-recovery.md
    /// step 3, Q148): its node.delete of the exact lost birth writes that birth's row deleted with no
    /// `TerminateRuntime` and releases the name, then node.create at the path.name; it is never submitted on `running` (Q153).
    async fn submit(&self, node: &Node, inspected: &crate::deployment::provider::Inspected, own_mesh: bool, next: &mut std::time::Instant, backoff: &mut std::time::Duration, ceiling: std::time::Duration) -> bool {
        let route = if own_mesh { "fabric-recovery" } else { "fabric-recovery-backbone" };
        match self.recovery.submit_route(node, inspected, route).await {
            Ok(()) => true,
            Err(e) => {
                tracing::info!(node = %node.name, error = %e, retry_ms = backoff.as_millis() as u64, "fabric recovery's re-create ended without the node: owed again after the backoff");
                *next = std::time::Instant::now() + *backoff;
                *backoff = (*backoff * 2).min(ceiling);
                false
            }
        }
    }

    /// Restore `node`, Ready at the target's path.name, as mesh-primary and fabric-primary
    /// (fabric-recovery.md step 4): node.primary.update's call on it, each record at its next revision.
    async fn restore(&self, target: &Primary, node: &Node) -> Result<Vec<rdm_node::member_handover::Entry>, rdm_core::handover::NodePrimaryUpdateReply> {
        let door = self.handover.get().cloned().ok_or_else(|| rdm_core::reply::handover::not_ready("this admin's node.primary.update door is not built yet".into()))?;
        let primaries = self.digest_view.primaries.clone();
        let mesh_record = primaries.mesh(&target.mesh);
        door.restore(target, mesh_record.as_ref(), node).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_state::{BuildAccepted, BuildAttemptClaim, BuildStateAdapter, MemoryBuildStateAdapter};
    use crate::storage::MeshStorage;

    /// CONTRACT (fabric-recovery.md step 4, Q160: "The executor takes the fabric-primary role and the lost
    /// mesh's mesh-primary role by one winning claim"): under a fabric.delete whose attempt 1 the lost
    /// fabric-primary mesh1.admin.1 claimed, holding mesh1's in-flight row, two backbone executors of two
    /// other meshes each read the delete at attempt 1 and claim its next attempt. One wins attempt 2; the
    /// other is told the winner, and so is a third admin that reads the delete held by the winner; the
    /// winner's claim again is its own. Must NOT happen: two executors holding attempt 2, the lost
    /// executor's row left holding mesh1, or an executor claiming past a live executor's attempt.
    #[tokio::test]
    async fn competing_backbone_executors_one_claim_of_the_deletes_next_attempt_wins() {
        let builds = MemoryBuildStateAdapter::new();
        let claims = crate::storage::MemoryMeshStorage::default();
        let contexts = crate::build_claim::AttemptContexts::in_memory();
        let id = crate::build::BuildId::mint();
        builds.publish_accepted(&BuildAccepted { build_id: id.clone(), topology: crate::accepted::FabricTopology { section_of: None, pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: Default::default() }, submitted_change: Some(crate::accepted::TopologyChange::DeleteFabric), submitted_at_ms: 1 }).await.unwrap();
        builds.claim_attempt(&BuildAttemptClaim { build_id: id.clone(), attempt: 1, executor: "mesh1.admin.1".into(), claimant: Some("mesh1.admin.1".into()), mesh: None }).await.unwrap();
        let lost_row = crate::mesh_claims::InFlight { build_id: id.clone(), lane: None, attempt: 1, claimant: "mesh1.admin.1".into(), claimant_node: None };
        assert_eq!(claims.take_in_flight("mesh1", &lost_row).await.unwrap(), crate::mesh_claims::Taken::Won);
        let lost_at: PathName = "mesh1.admin.1".parse().unwrap();
        let (a, b, c): (PathName, PathName, PathName) = ("mesh2.admin.1".parse().unwrap(), "mesh3.admin.1".parse().unwrap(), "mesh4.admin.1".parse().unwrap());
        let read = builds.read_build(&id).await.unwrap();
        let first = claim_delete_attempt(&a, &builds, &claims, &contexts, &read, Some(&lost_at), None).await;
        let second = claim_delete_attempt(&b, &builds, &claims, &contexts, &read, Some(&lost_at), None).await;
        assert_eq!(first, DeleteClaim::Won(2), "the first claim of attempt 2 wins");
        assert!(matches!(&second, DeleteClaim::Lost(why) if why.contains("mesh2.admin.1")), "the second claimant is told the winner: {second:?}");
        let now = builds.read_build(&id).await.unwrap();
        assert_eq!((now.attempt, now.executor.as_deref()), (2, Some("mesh2.admin.1")), "one executor holds attempt 2");
        assert_eq!(claim_delete_attempt(&a, &builds, &claims, &contexts, &now, Some(&lost_at), None).await, DeleteClaim::Won(2), "the winner's claim again is its own");
        assert!(matches!(claim_delete_attempt(&c, &builds, &claims, &contexts, &now, Some(&lost_at), None).await, DeleteClaim::Lost(why) if why.contains("mesh2.admin.1")), "an executor never claims past a live executor's attempt");
        assert_eq!(claims.in_flight("mesh1").await.unwrap().map(|r| (r.attempt, r.claimant)), Some((2, "mesh2.admin.1".to_string())), "the lost executor's row is superseded by the winner's");
    }

    /// CONTRACT (docs/architecture/lifecycles/fabric-recovery.md:45-47: "1. **Unheard past the staleness
    /// floor.** Every admin marks the fabric-primary `PendingReconnect` at the floor (node recovery step 1).
    /// Silence is the trigger; no provider verdict stands between the mark and the row."
    /// docs/architecture/lifecycles/fabric-recovery.md:48-50: "2. **Fabric recovery starts.** The executor
    /// names itself by the four questions on its own view, opens the attempt and broadcasts it. Any other
    /// admin that notices tells it; it acts only when its own view agrees; repeats answer `AlreadyApplied`."):
    /// while the fabric-primary record names a heard Ready primary, `of` returns `None`. Once silence marks
    /// the fabric-primary `PendingReconnect` (no provider verdict required), `of` names the lowest Ready
    /// node-admin in its own mesh (`own_mesh = true`), or the lowest Ready mesh-primary on the backbone when
    /// its own mesh has none (`own_mesh = false`), taking no primary role, and opening the attempt on the
    /// Build log is idempotent on repeats.
    #[tokio::test]
    async fn unheard_fabric_primary_is_marked_pending_reconnect_and_executor_names_itself_by_four_questions() {
        let mut fp = Node::allocated("mesh1.admin.1".parse().unwrap());
        fp.kind = NodeKind::NodeAdmin;
        fp.mesh = "mesh1".into();
        fp.node_id = NodeId::mint();
        fp.incarnation_id = Some(crate::model::IncarnationId::mint());
        fp.status = NodeStatus::Ready;
        fp.is_primary = true;
        fp.is_fabric_primary = true;

        let mut own_low = Node::allocated("mesh1.admin.2".parse().unwrap());
        own_low.kind = NodeKind::NodeAdmin;
        own_low.mesh = "mesh1".into();
        own_low.node_id = NodeId::parse("000000000002").unwrap();
        own_low.incarnation_id = Some(crate::model::IncarnationId::mint());
        own_low.status = NodeStatus::Ready;

        let mut own_high = Node::allocated("mesh1.admin.3".parse().unwrap());
        own_high.kind = NodeKind::NodeAdmin;
        own_high.mesh = "mesh1".into();
        own_high.node_id = NodeId::parse("000000000003").unwrap();
        own_high.incarnation_id = Some(crate::model::IncarnationId::mint());
        own_high.status = NodeStatus::Ready;

        let mut bb_low = Node::allocated("mesh2.admin.1".parse().unwrap());
        bb_low.kind = NodeKind::NodeAdmin;
        bb_low.mesh = "mesh2".into();
        bb_low.node_id = NodeId::parse("000000000010").unwrap();
        bb_low.incarnation_id = Some(crate::model::IncarnationId::mint());
        bb_low.status = NodeStatus::Ready;
        bb_low.is_primary = true;

        let mut bb_high = Node::allocated("mesh3.admin.1".parse().unwrap());
        bb_high.kind = NodeKind::NodeAdmin;
        bb_high.mesh = "mesh3".into();
        bb_high.node_id = NodeId::parse("000000000020").unwrap();
        bb_high.incarnation_id = Some(crate::model::IncarnationId::mint());
        bb_high.status = NodeStatus::Ready;
        bb_high.is_primary = true;

        let target = Primary {
            mesh: "mesh1".into(),
            node_id: fp.node_id.clone(),
            incarnation: fp.incarnation_id.clone().unwrap(),
            revision: 1,
        };
        let mut inc = Incumbency::default();
        inc.fabric = Some(target.clone());
        inc.meshes.insert("mesh1".into(), target.clone());
        let digest_view = DigestView::default();

        // Heard fabric-primary -> no fabric recovery.
        let heard_view = Topology {
            fabric: crate::model::Fabric {
                id: rdm_core::FabricId::mint(),
                name: "fabric1".into(),
                status: crate::model::ScopeStatus::Ready,
                provider: crate::model::ProviderKind::Process,
            },
            meshes: Vec::new(),
            nodes: vec![fp.clone(), own_low.clone(), own_high.clone(), bb_low.clone(), bb_high.clone()],
        };
        assert_eq!(of(&heard_view, &inc, &digest_view), None);

        // Step 1: Unheard past the staleness floor -> marked PendingReconnect (silence is the trigger).
        let mut fp_unheard = fp.clone();
        fp_unheard.status = NodeStatus::PendingReconnect;
        fp_unheard.is_primary = false;
        fp_unheard.is_fabric_primary = false;
        let own_view = Topology {
            nodes: vec![fp_unheard.clone(), own_low.clone(), own_high.clone(), bb_low.clone(), bb_high.clone()],
            ..heard_view.clone()
        };

        // Step 2 (own-mesh arm): lowest Ready node-admin in mesh1 names itself executor, takes no primary role.
        let fr_own = of(&own_view, &inc, &digest_view).expect("fabric recovery is active in own mesh");
        assert!(fr_own.own_mesh);
        assert_eq!(fr_own.at, Some("mesh1.admin.1".parse().unwrap()));
        assert_eq!(fr_own.executor, Some("mesh1.admin.2".parse().unwrap()));
        let read: Reader = Arc::new({
            let inc = inc.clone();
            let dv = digest_view.clone();
            move |t: &Topology| of(t, &inc, &dv)
        });
        assert!(executes_for(Some(&read), &own_view, &"mesh1.admin.2".parse().unwrap(), "mesh1"));
        assert!(!executes_for(Some(&read), &own_view, &"mesh1.admin.3".parse().unwrap(), "mesh1"));
        assert!(!executes_for(Some(&read), &own_view, &"mesh2.admin.1".parse().unwrap(), "mesh1"));

        // Step 2 (backbone arm): when mesh1 has no Ready node-admin, the lowest Ready mesh-primary on the backbone runs it.
        let bb_view = Topology {
            nodes: vec![fp_unheard, bb_high, bb_low],
            ..heard_view
        };
        let fr_bb = of(&bb_view, &inc, &digest_view).expect("fabric recovery is active from backbone");
        assert!(!fr_bb.own_mesh);
        assert_eq!(fr_bb.at, Some("mesh1.admin.1".parse().unwrap()));
        assert_eq!(fr_bb.executor, Some("mesh2.admin.1".parse().unwrap()));

        // Opening the recovery attempt is idempotent on repeats (AlreadyApplied).
        let builds = MemoryBuildStateAdapter::new();
        let build_id = crate::build::BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted {
                build_id: build_id.clone(),
                topology: crate::accepted::FabricTopology::root("fabric1", "mesh1", crate::model::MeshId::mint()),
                submitted_change: None,
                submitted_at_ms: 1,
            })
            .await
            .unwrap();
        builds
            .claim_attempt(&BuildAttemptClaim {
                build_id: build_id.clone(),
                attempt: 1,
                executor: "mesh1.admin.1".into(),
                claimant: Some("mesh1.admin.1".into()),
                mesh: None,
            })
            .await
            .unwrap();
        builds
            .append_attempt_receipt(&crate::build_state::BuildAttemptReceipt {
                mesh: None,
                build_id: build_id.clone(),
                attempt: 1,
                outcome: crate::build_state::AttemptOutcome::Converged,
            })
            .await
            .unwrap();
        let opened = crate::build_state::AttemptOpened {
            mesh: None,
            build_id: build_id.clone(),
            attempt: 2,
            reason: crate::build_state::AttemptReason::Recovery,
            action: None,
            opened_by: "mesh1.admin.2".into(),
            opened_at_ms: 2,
            context: None,
        };
        builds.open_attempt(&opened).await.expect("first open succeeds");
        builds.open_attempt(&opened).await.expect("repeat open is idempotent (AlreadyApplied)");
        let opened_count = builds
            .facts()
            .await
            .unwrap()
            .iter()
            .filter(|f| matches!(f, crate::build_state::BuildFact::Opened(_)))
            .count();
        assert_eq!(opened_count, 1, "repeat open records only one Opened fact");
    }

    /// CONTRACT (fabric-recovery.md step 4, Q155, Q160; mesh-delete.md "Resume after the primary's loss"
    /// step 2): when a backbone executor (mesh2.admin.1) takes the fabric-primary role and mesh1's
    /// mesh-primary role in attempt 2 of fabric.delete and is then lost midway, the next executor
    /// (mesh2.admin.2) in attempt 3 folds the recorded transitions so mesh1.admin.1—even when held in
    /// mesh2.admin.2's digest view only from entry topology (`Heard::Topology`)—is neither live nor
    /// mesh1's primary, and mesh2.admin.2 holds mesh1's mesh-primary record at the next revision.
    #[tokio::test]
    async fn the_next_executor_under_fabric_delete_folds_earlier_transitions_so_the_lost_mesh_primary_holds_no_role_or_liveness() {
        let builds = MemoryBuildStateAdapter::new();
        let id = crate::build::BuildId::mint();
        builds.publish_accepted(&BuildAccepted { build_id: id.clone(), topology: crate::accepted::FabricTopology { section_of: None, pinned: Vec::new(), storage_change: None, topology_storage_provider: None, fabric: "fabric1".into(), meshes: Default::default() }, submitted_change: Some(crate::accepted::TopologyChange::DeleteFabric), submitted_at_ms: 1 }).await.unwrap();
        let make_admin = |name: &str| {
            let mut n = Node::allocated(name.parse().unwrap());
            n.incarnation_id = Some(crate::model::IncarnationId::mint());
            n.status = NodeStatus::Ready;
            n
        };
        let fp1 = make_admin("mesh1.admin.1");
        let ex1 = make_admin("mesh2.admin.1");
        let ex2 = make_admin("mesh2.admin.2");
        let target1 = Primary { mesh: "mesh1".into(), node_id: fp1.node_id.clone(), incarnation: fp1.incarnation_id.clone().unwrap(), revision: 1 };
        let t1 = record_primaries_taken(&builds, &id, 2, &ex1, &target1, 2).await.expect("attempt 2 transition");
        let target2 = Primary { mesh: "mesh2".into(), node_id: ex1.node_id.clone(), incarnation: ex1.incarnation_id.clone().unwrap(), revision: t1.fabric_revision };
        let t2 = record_primaries_taken(&builds, &id, 3, &ex2, &target2, 2).await.expect("attempt 3 transition");
        assert_eq!(t2.fabric_revision, 3);

        let fabric_id = crate::model::FabricId::mint();
        let digest_view = DigestView::default();
        let records = crate::admin::Records::default();
        let digest_of = |n: &Node, is_primary: Option<u64>, is_fabric_primary: Option<u64>| rdm_core::MeshDigest {
            held_hash: Default::default(),
            fabric_id: fabric_id.clone(),
            node: rdm_core::MeshNode { node_id: n.node_id.clone(), name: n.name.clone(), endpoint_id: crate::model::EndpointId(String::new()), transport_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 1)), incarnation: n.incarnation_id.clone().unwrap(), supersedes: None, runtime: None },
            status: rdm_core::MemberStatus::Ready,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 1,
            emitted_at_rafka_ms: 0,
            data_dir: None,
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            is_primary,
            is_fabric_primary,
        };
        // mesh2.admin.2 learned mesh1.admin.1 only via entry topology (`record_topology`, `Heard::Topology`),
        // which never ages on its own, and holds mesh1's initial primary record at revision 1.
        digest_view.record(digest_of(&fp1, Some(1), Some(1)));
        digest_view.record(digest_of(&ex2, None, None));

        let steps = builds.read_build(&id).await.unwrap().steps;
        let transitions = crate::build_state::primaries_taken(&steps);
        fold_primaries_taken(&digest_view, &records, &ex2, Some(&target2), &transitions);
        digest_view.primaries.take(rdm_core::PrimaryRole::of_node_type(NodeKind::NodeAdmin), &Primary { mesh: "mesh2".into(), node_id: ex2.node_id.clone(), incarnation: ex2.incarnation_id.clone().unwrap(), revision: 2 });
        digest_view.primaries.take(rdm_core::PrimaryRole::FabricPrimary, &Primary { mesh: "mesh2".into(), node_id: ex2.node_id.clone(), incarnation: ex2.incarnation_id.clone().unwrap(), revision: 3 });

        let view = crate::admin::project("fabric1", &fabric_id, crate::model::ProviderKind::Process, &digest_view, &records);
        assert_eq!(view.node_type_primary("mesh1", NodeKind::NodeAdmin), None, "mesh1 has no live node-admin primary in mesh2.admin.2's view");
        assert_eq!(view.nodes_of_type("mesh1", NodeKind::NodeAdmin).find(|n| n.status.is_live()), None, "the lost mesh1.admin.1 is not live in mesh2.admin.2's view");
        assert_eq!(digest_view.primaries.mesh("mesh1").map(|p| (p.node_id, p.revision)), Some((ex2.node_id.clone(), 3)), "mesh2.admin.2 holds mesh1's mesh-primary record at the next revision");
    }
}
