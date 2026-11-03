//! Fabric recovery (fabric-recovery.md "The loss of the fabric-primary while a node-admin lives";
//! elections §3, §4.1, §4.3; build.md §5.3; Q122). Unheard past the staleness floor, the fabric-primary is
//! taken by nobody: the lowest Ready node-admin of its mesh, or when the mesh has none the
//! lowest Ready mesh-primary on the backbone, runs one workflow, started at the mark on either arm. It takes no primary role. The primary watch's node investigation decides what becomes of the lost birth (Q111, Q337):
//! reached, it is commanded back through node.mesh.join; any other verdict, node recovery re-creates the
//! node-admin at its path and name, an attempt of the current Build that names the lost birth, except from the
//! backbone on `running`, where the report stands and nothing is written (Q153). When the node
//! at that path and name is Ready the workflow restores it as mesh-primary and fabric-primary
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
use crate::model::{Node, NodeId, NodeStatus, PathName};
use crate::topology::Topology;
use rdm_core::Primary;
use rdm_transport::topology::DigestView;
use std::sync::{Arc, Mutex};
use tracing::Instrument as _;

/// What fabric recovery is, in one admin's view: the fabric-primary record it restores, the path and name the
/// node-admin is re-created at, and the admin that runs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricRecovery {
    /// The fabric-primary record whose birth is not a heard primary.
    pub target: Primary,
    /// The path and name that birth lived at: its node's in the view, else its digest's or its departure's.
    pub at: Option<PathName>,
    /// The admin that runs the workflow: the lowest Ready node-admin of the target's mesh, else
    /// the lowest Ready mesh-primary of another mesh.
    pub executor: Option<rdm_core::framing::Birth>,
    /// Whether `executor` is of the target's own mesh (the own-mesh arm) rather than the backbone's.
    pub own_mesh: bool,
}

/// Fabric recovery in `view` with the records `inc` and the digest view `digest_view`; `None` while the fabric-primary
/// record names a heard primary, or no record is held.
pub fn of(view: &Topology, inc: &Incumbency, digest_view: &DigestView) -> Option<FabricRecovery> {
    let members: Vec<Node> = view.births().cloned().collect();
    let target = crate::election::fabric_recovery_target(&members, inc)?.clone();
    let lost = rdm_core::handover::HandoverBirth { mesh: target.mesh.clone(), node_id: target.node_id.clone(), incarnation: target.incarnation.clone() };
    let at = members.iter().find(|n| n.node_id == target.node_id).map(|n| n.location()).or_else(|| crate::admin::lost_birth_name(digest_view, &lost));
    // Without the lost birth's path and name this admin cannot tell the node re-created there from the mesh's
    // other node-admins: it names no executor of the mesh's own (a node that recovery re-created learns of
    // the workflow from its maker).
    let own = at.as_ref().and_then(|at| crate::election::fabric_recovery_executor(&members, inc, &target, Some(at))).and_then(Node::birth);
    let backbone = || {
        members
            .iter()
            .filter(|n| rdm_core::is_node_admin(&n.r#type) && n.mesh != target.mesh && n.is_primary && n.status == NodeStatus::Ready && !n.incarnation_id.as_ref().is_some_and(|i| inc.lost.contains(i)))
            .min_by(|a, b| a.node_id.as_str().cmp(b.node_id.as_str()))
            .and_then(Node::birth)
    };
    let executor = if at.is_some() { own.clone().or_else(backbone) } else { None };
    Some(FabricRecovery { own_mesh: own.is_some(), executor, target, at })
}

/// Fold the lost fabric-primary `target` into `digest_view` and `records` for the executor `me` taking
/// over from it under an in-force fabric.delete (fabric-recovery.md step 4, Q155): the lost birth is
/// marked gone and offline, so a birth known only from forwarded topology (`Heard::Topology`) is not read
/// as a live primary of its mesh.
pub fn fold_lost_fabric_primary(digest_view: &DigestView, records: &crate::admin::Records, target: &Primary) {
    digest_view.primaries.mark_gone(target.incarnation.clone());
    records.offline.lock().unwrap().insert((target.node_id.clone(), target.incarnation.clone()));
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
pub fn executes_for(read: Option<&Reader>, t: &Topology, me: &crate::storage::BirthRef, mesh: &str) -> bool {
    read.and_then(|r| r(t)).is_some_and(|fr| fr.target.mesh == mesh && fr.executor.as_ref().is_some_and(|e| e.node_id == me.node_id && e.incarnation == me.incarnation_id)) && t.node_type_primary(mesh, "node-admin").is_none()
}

/// The workflow's door on one admin.
pub struct FabricRecoveryDoor {
    /// This admin's path as born; its held name is the view's.
    pub me_path: String,
    /// This node's name, beside its path.
    pub me_name: String,
    /// This admin's node id.
    pub me_id: NodeId,
    /// The incarnation established when this process starts.
    pub me_incarnation: crate::model::IncarnationId,
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
    /// node.primary.update at this admin (`PrimaryUpdate`): the restore's call.
    pub primary_update: crate::primary_update::PrimaryUpdateSlot,
    /// Whether a person launched this admin on the fabric-primary's data dir to recover the fabric
    /// (`RDM_FABRIC_PRIMARY`; fabric-recovery.md: "It starts when a person or a script launches one
    /// node-admin on the fabric-primary's data dir"): at the fabric-primary's path and name with no admin to
    /// run the workflow ("Nothing alive": "It is the only node, the fabric-primary"), it restores itself.
    launched_to_recover: bool,
    /// Whether that launch is the adoption under an in-force fabric.delete, at its own new path and name
    /// (Q159): it runs the workflow when no admin does, whatever the lost birth's path and name.
    launched_at_new_path: bool,
    /// The birth whose workflow runs here now.
    running: Mutex<Option<rdm_core::framing::Birth>>,
    /// The birth the workflow running here asks the primary watch's verdict on (Q337), until the watch
    /// hands it over.
    asked: Mutex<Option<rdm_core::framing::Birth>>,
    /// The primary watch's verdict on that birth: node investigation's finding, which the workflow acts
    /// on; it takes no look of its own.
    verdict: Mutex<Option<(rdm_core::framing::Birth, crate::node_investigation::Finding)>>,
    /// The admin's stop gate: the workflow is work a stop deletes with the birth
    /// ([`crate::stop_gate::StopGate::until_stopped`]), ended where it stands at this admin's stop.
    gate: crate::stop_gate::StopGate,
}

impl FabricRecoveryDoor {
    /// The `(path, name)` lookup index key of me_path and me_name.
    pub fn me(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.me_path.clone(), name: self.me_name.clone() } }
}

impl FabricRecoveryDoor {
    /// A door for the admin these name.
    #[allow(clippy::too_many_arguments)]
    pub fn new(me: PathName, me_id: NodeId, me_incarnation: crate::model::IncarnationId, topology: Arc<tokio::sync::RwLock<Topology>>, digest_view: DigestView, records: Arc<crate::admin::Records>, looker: crate::primary_watch::Looker, recovery: crate::admin::RecoverySubmitter, primary_update: crate::primary_update::PrimaryUpdateSlot, launched_to_recover: bool, gate: crate::stop_gate::StopGate) -> Self {
        Self { me_path: me.path.clone(), me_name: me.name.clone(), me_id, me_incarnation, topology, digest_view, records, looker, recovery, primary_update, launched_to_recover, launched_at_new_path: false, running: Mutex::new(None), asked: Mutex::new(None), verdict: Mutex::new(None), gate }
    }

    /// This door's admin is the adoption launch under an in-force fabric.delete, at its own new path and name
    /// (Q159), when `yes`.
    pub fn adopted_at_new_path(mut self, yes: bool) -> Self {
        self.launched_at_new_path = yes;
        self
    }

    /// Whether this admin runs the workflow `fr`: its executor, or the admin a person launched at the
    /// fabric-primary's path and name when no admin runs it.
    fn runs(&self, fr: &FabricRecovery) -> bool {
        let me = self.held_name();
        // Launched on the fabric-primary's own data dir, its path and name is the fabric-primary's, whether or
        // not this admin knows the lost birth's.
        fr.executor.as_ref().is_some_and(|e| e.node_id == self.me_id && e.incarnation == self.me_incarnation) || (self.launched_to_recover && fr.executor.is_none() && (self.launched_at_new_path || fr.at.as_ref().is_none_or(|at| *at == me)) && self.me_id != fr.target.node_id)
    }

    fn held_name(&self) -> PathName {
        self.digest_view.held_path_name().unwrap_or_else(|| self.me())
    }

    /// The primary watch's verdict `finding` on `node`'s exact birth (Q337): held for the workflow that
    /// asked for it, which acts on it; a verdict on any other birth is no input of it.
    pub fn take_verdict(&self, node: &Node, finding: &crate::node_investigation::Finding) {
        let Some(incarnation) = node.incarnation_id.clone() else { return };
        let birth = rdm_core::framing::Birth { node_id: node.node_id.clone(), incarnation };
        let mut asked = self.asked.lock().unwrap();
        if asked.as_ref() == Some(&birth) {
            *asked = None;
            *self.verdict.lock().unwrap() = Some((birth, finding.clone()));
        }
    }

    /// The birth the workflow running here asks the primary watch to investigate, if it asks (Q337).
    pub fn asked(&self) -> Option<rdm_core::framing::Birth> {
        self.asked.lock().unwrap().clone()
    }

    /// The watch's verdict on `node`'s exact birth, taken once.
    fn verdict_on(&self, node: &Node) -> Option<crate::node_investigation::Finding> {
        self.verdict.lock().unwrap().take().filter(|(birth, _)| birth.node_id == node.node_id && node.incarnation_id.as_ref() == Some(&birth.incarnation)).map(|(_, f)| f)
    }

    /// Fabric recovery in this admin's view now.
    pub async fn read(&self) -> Option<FabricRecovery> {
        let t = self.topology.read().await.clone();
        of(&t, &crate::admin::incumbency_of(&self.digest_view, &self.records), &self.digest_view)
    }

    /// Start the workflow when this admin runs it: from its own mesh at the first tick after the mark, once
    /// the fabric record has named the unheard birth for a floor (`held_a_floor`); from the backbone, once
    /// the fabric-primary has also been unheard here for twice the floor (Q349). The primary watch's node investigation of the lost birth is the verdict that
    /// decides (fabric-recovery.md step 3, Q335, Q337). Once per lost birth while it runs here.
    pub async fn start_if_mine(self: &Arc<Self>, held_a_floor: bool) {
        let Some(fr) = self.read().await else { return };
        let launched = fr.executor.is_none() && self.runs(&fr);
        if !self.runs(&fr) || !(launched || held_a_floor) {
            return;
        }
        // From the backbone the arm starts only once the fabric-primary has been unheard here for twice the
        // floor (fabric-recovery.md step 2, Q349): the floor after the election is its own mesh's node-admin's
        // to act in as the workflow's executor.
        if !fr.own_mesh && fr.executor.is_some() {
            let floor = self.digest_view.staleness_floor();
            let unheard = self.digest_view.get(fr.target.node_id.as_str()).filter(|(d, _)| d.node.incarnation == fr.target.incarnation).map(|(_, unheard)| unheard);
            if unheard.is_some_and(|unheard| unheard < floor * 2) {
                return;
            }
        }
        let target_birth = rdm_core::framing::Birth { node_id: fr.target.node_id.clone(), incarnation: fr.target.incarnation.clone() };
        {
            let mut running = self.running.lock().unwrap();
            if running.as_ref() == Some(&target_birth) {
                return;
            }
            *running = Some(target_birth.clone());
        }
        let door = self.clone();
        tokio::spawn(async move {
            // From the backbone the arm acts only "When the mesh has no node-admin left" (fabric-recovery.md
            // step 2, Q335): node investigation of each other node-admin birth of the fabric-primary's mesh,
            // once for this lost birth (`Looker::node_admin_left`); one left leaves the workflow to that
            // mesh's own arm, and this admin starts nothing for the birth (it stays the one `running` here).
            if !fr.own_mesh && fr.executor.is_some() {
                let view = door.topology.read().await.clone();
                if door.looker.node_admin_left(&view, &fr.target.mesh, &fr.target.node_id).await.is_some() {
                    return;
                }
            }
            door.run(fr.target.clone()).await;
            let mut running = door.running.lock().unwrap();
            if running.as_ref() == Some(&target_birth) {
                *running = None;
            }
        });
    }

    /// The workflow for the fabric-primary record `target`, pass by pass until the node at its path and name
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
            node_path = %me.path, node_name = %me.name,
            target_node_id = %target.node_id, target_incarnation = %target.incarnation,
            at_path = tracing::field::Empty,
            at_name = tracing::field::Empty,
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
            .unwrap_or_else(|| format!("node_id={} stop ended it where it stood", self.me_id));
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
                return format!("node_id={} no longer runs it: {}", self.me_id, fr.executor.map(|e| format!("{e:?}")).unwrap_or_else(|| "nobody does".into()));
            }
            let Some(at) = fr.at.clone().or_else(|| self.launched_to_recover.then(|| me.clone())) else {
                return "the lost birth's path and name are not known here".into();
            };
            span.record("at_path", at.path.as_str()).record("at_name", at.name.as_str()).record("arm", if fr.own_mesh { "own-mesh" } else { "backbone" });
            // Under an in-force fabric.delete the workflow re-creates and restores nothing: the delete is the
            // current Build and names no node, so the executor holds the fabric-primary's roles to the end and
            // continues the delete; no hand-back follows (fabric-recovery.md step 4, Q155).
            match crate::build_state::fabric_delete_in_force(&*self.recovery.builds).await {
                Ok(Some((delete, _))) => {
                    span.record("arm", if fr.own_mesh { "own-mesh-under-delete" } else if fr.executor.is_none() { "launched-under-delete" } else { "backbone-under-delete" });
                    if let Some(end) = self.hold_under_delete(span, target, &delete).await {
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
            let lost = view.births().find(|n| n.node_id == target.node_id && n.incarnation_id.as_ref() == Some(&target.incarnation)).cloned();
            // Ready in the view, or re-created by this workflow's completed attempt, whose create
            // waited for it to be ready (an executor of another mesh holds no member status of it).
            let restored = view.node(&at).filter(|n| rdm_core::is_node_admin(&n.r#type) && (n.status == NodeStatus::Ready || (done && n.node_id != target.node_id))).cloned();
            match (lost, restored) {
                // The node at the path and name is Ready: a new birth, or the lost birth returned.
                (_, Some(node)) if node.node_id != target.node_id || node.incarnation_id.as_ref() != Some(&target.incarnation) => {
                    return match self.restore(target, &node).await {
                        Ok(entries) => {
                            span.record("restored", node.node_id.as_str());
                            format!("restored: {}", rdm_node::member_handover::entries_named(&entries))
                        }
                        Err(reply) => format!("the restore of node_id={} was refused: {reply:?}", node.node_id),
                    };
                }
                (_, Some(_)) => return "the lost birth is heard again: its record names it".into(),
                // The lost birth is still in the view: the primary watch's node investigation decides
                // (Q111), its verdict taken as it is: the workflow takes no look of its own (Q337).
                (Some(node), None) if !investigated && std::time::Instant::now() >= next_look => {
                    let Some(finding) = self.verdict_on(&node) else {
                        // Asked of the watch, whose next pass investigates the birth and hands the verdict here.
                        if let Some(incarnation) = node.incarnation_id.clone() {
                            *self.asked.lock().unwrap() = Some(rdm_core::framing::Birth { node_id: node.node_id.clone(), incarnation });
                        }
                        tokio::time::sleep(rdm_transport::topology::gossip_interval()).await;
                        continue;
                    };
                    investigated = true;
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
                            tracing::info!(subject_path = %node.path, subject_name = %node.name, verdict = finding.name(), "the backbone arm read the fabric-primary's runtime running: the report stands and nothing is written");
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
                            tracing::info!(subject_path = %node.path, subject_name = %node.name, verdict = finding.name(), "the own-mesh arm read the fabric-primary's runtime running and reached it through no peer: unreachable from this executor, not lost; nothing is written");
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
                    let mut node = Node::allocated(at.clone(), target.mesh.clone(), "node-admin");
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

    /// Under the in-force fabric.delete `delete` (fabric-recovery.md step 4, Q155): the executor, of the
    /// lost birth's own mesh, from the backbone, or the admin a person launched ("Nothing alive"), takes
    /// by one recorded transition, at the next revision, the fabric-primary role and the lost mesh's
    /// mesh-primary role, keeping its own mesh's, installed on its own node with no call; its drive then
    /// continues the delete as fabric-primary from the delete's events. Nothing is re-created and no
    /// hand-back follows. `Some` ends the workflow (the roles held); `None` holds nothing on this pass (the
    /// next pass reads again).
    async fn hold_under_delete(&self, span: &tracing::Span, target: &Primary, delete: &crate::build_state::BuildProjection) -> Option<String> {
        let me = self.topology.read().await.own(&self.me_id).filter(|node| node.incarnation_id.as_ref() == Some(&self.me_incarnation)).cloned()?;
        let mesh_revision = self.digest_view.primaries.mesh(&target.mesh).map(|m| m.revision).unwrap_or(0) + 1;
        fold_lost_fabric_primary(&self.digest_view, &self.records, target);
        // One recorded transition at the next revision (fabric-recovery.md step 4): the fabric-primary role
        // and the lost mesh's mesh-primary role, on the primary records.
        let installed = if me.mesh == target.mesh {
            self.restore(target, &me).await
        } else {
            match self.primary_update.get().cloned() {
                Some(h) => h.take_under_delete(target, mesh_revision, &me).await,
                None => Err(rdm_core::reply::handover::not_ready("this admin's node.primary.update is not served yet".into())),
            }
        };
        match installed {
            Ok(entries) => {
                span.record("restored", me.node_id.as_str());
                tracing::info!(build_id = %delete.build_id.log(), "under an in-force fabric.delete the executor holds the fabric-primary's roles by the recorded transition, and continues the delete; nothing is re-created");
                Some(format!("holds the fabric-primary's roles under fabric.delete {}: {}", delete.build_id, rdm_node::member_handover::entries_named(&entries)))
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
    /// `TerminateRuntime` and releases the name, then node.create at the path and name; it is never submitted on `running` (Q153).
    async fn submit(&self, node: &Node, inspected: &crate::deployment::provider::Inspected, own_mesh: bool, next: &mut std::time::Instant, backoff: &mut std::time::Duration, ceiling: std::time::Duration) -> bool {
        let route = if own_mesh { "fabric-recovery" } else { "fabric-recovery-backbone" };
        match self.recovery.submit_route(node, inspected, route).await {
            Ok(()) => true,
            Err(e) => {
                tracing::info!(node_path = %node.path, node_name = %node.name, error = %e, retry_ms = backoff.as_millis() as u64, "fabric recovery's re-create ended without the node: owed again after the backoff");
                *next = std::time::Instant::now() + *backoff;
                *backoff = (*backoff * 2).min(ceiling);
                false
            }
        }
    }

    /// Restore `node`, Ready at the target's path and name, as mesh-primary and fabric-primary
    /// (fabric-recovery.md step 4): node.primary.update's call on it, each record at its next revision.
    async fn restore(&self, target: &Primary, node: &Node) -> Result<Vec<rdm_node::member_handover::Entry>, rdm_core::handover::NodePrimaryUpdateReply> {
        let primary_update = self.primary_update.get().cloned().ok_or_else(|| rdm_core::reply::handover::not_ready("this admin's node.primary.update is not served yet".into()))?;
        let primaries = self.digest_view.primaries.clone();
        let mesh_record = primaries.mesh(&target.mesh);
        primary_update.restore(target, mesh_record.as_ref(), node).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_state::{BuildStateAdapter, MemoryBuildStateAdapter};
    use crate::storage::MeshStorage;

    /// CONTRACT (docs/architecture/lifecycles/fabric-recovery.md:45-47: "1. **Unheard past the staleness
    /// floor.** Every admin marks the fabric-primary `PendingReconnect` at the floor (node recovery step 1).
    /// Silence is the trigger; no provider verdict stands between the mark and the row."
    /// docs/architecture/lifecycles/fabric-recovery.md:48-50: "2. **Fabric recovery starts.** The executor
    /// names itself by the four questions on its own view, opens the attempt and broadcasts it. Any other
    /// admin that notices tells it; it acts only when its own view agrees; repeats answer `AlreadyApplied`."):
    /// while the fabric-primary record names a heard Ready primary, `of` returns `None`. Once silence marks
    /// the fabric-primary `PendingReconnect` (no provider verdict required), `of` names the lowest Ready
    /// node-admin in its own mesh (`own_mesh = true`), or the lowest Ready mesh-primary on the backbone when
    /// its own mesh has none (`own_mesh = false`), taking no primary role.
    #[tokio::test]
    async fn unheard_fabric_primary_is_marked_pending_reconnect_and_executor_names_itself_by_four_questions() {
        let mut fp = Node::allocated(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin");


        fp.node_id = NodeId::mint();
        fp.incarnation_id = Some(crate::model::IncarnationId::mint());
        fp.status = NodeStatus::Ready;
        fp.is_primary = true;
        fp.is_fabric_primary = true;

        let mut own_low = Node::allocated(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin");


        own_low.node_id = NodeId::parse("000000000002").unwrap();
        own_low.incarnation_id = Some(crate::model::IncarnationId::mint());
        own_low.status = NodeStatus::Ready;

        let mut own_high = Node::allocated(rdm_core::PathName::new("mesh1.node-admin", "3").unwrap(), "mesh1", "node-admin");


        own_high.node_id = NodeId::parse("000000000003").unwrap();
        own_high.incarnation_id = Some(crate::model::IncarnationId::mint());
        own_high.status = NodeStatus::Ready;

        let mut bb_low = Node::allocated(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin");


        bb_low.node_id = NodeId::parse("000000000010").unwrap();
        bb_low.incarnation_id = Some(crate::model::IncarnationId::mint());
        bb_low.status = NodeStatus::Ready;
        bb_low.is_primary = true;

        let mut bb_high = Node::allocated(rdm_core::PathName::new("mesh3.node-admin", "1").unwrap(), "mesh3", "node-admin");


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
        assert_eq!(fr_own.at, Some(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()));
        assert_eq!(fr_own.executor, own_low.birth());
        let read: Reader = Arc::new({
            let inc = inc.clone();
            let dv = digest_view.clone();
            move |t: &Topology| of(t, &inc, &dv)
        });
        assert!(executes_for(Some(&read), &own_view, &crate::storage::BirthRef { node_id: own_low.node_id.clone(), incarnation_id: own_low.incarnation_id.clone().unwrap() }, "mesh1"));
        assert!(!executes_for(Some(&read), &own_view, &crate::storage::BirthRef { node_id: own_high.node_id.clone(), incarnation_id: own_high.incarnation_id.clone().unwrap() }, "mesh1"));
        assert!(!executes_for(Some(&read), &own_view, &crate::storage::BirthRef { node_id: bb_low.node_id.clone(), incarnation_id: bb_low.incarnation_id.clone().unwrap() }, "mesh1"));

        // Step 2 (backbone arm): when mesh1 has no Ready node-admin, the lowest Ready mesh-primary on the backbone runs it.
        let bb_view = Topology {
            nodes: vec![fp_unheard, bb_high, bb_low.clone()],
            ..heard_view
        };
        let fr_bb = of(&bb_view, &inc, &digest_view).expect("fabric recovery is active from backbone");
        assert!(!fr_bb.own_mesh);
        assert_eq!(fr_bb.at, Some(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()));
        assert_eq!(fr_bb.executor, bb_low.birth());

    }
}
