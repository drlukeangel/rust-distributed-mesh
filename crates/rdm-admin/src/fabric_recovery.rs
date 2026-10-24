//! Fabric recovery (fabric-recovery.md "The loss of the fabric-primary while a node-admin lives";
//! elections §3, §4.1, §4.3; build.md §5.3; Q122). Unheard past the staleness floor, the fabric-primary is
//! taken by nobody: the lowest ReadyForTraffic node-admin of its mesh, or when the mesh has none the
//! lowest ReadyForTraffic mesh-primary on the backbone from mesh investigation's decision step, runs one
//! workflow. It takes no primary role. Node investigation decides what becomes of the lost birth (Q111):
//! reached, it is commanded back through node.mesh.join; any other verdict, node recovery re-creates the
//! node-admin at its path.name, an attempt of the current Build that names the lost birth. When the node
//! at that path.name is ReadyForTraffic the workflow restores it as mesh-primary and fabric-primary
//! (node.primary.update's call on it): the node installs both roles, acknowledges, and records them on
//! its own row; the workflow completes on the acknowledgement.
//!
//! Every fact the workflow acts on is read again from the view, the records and the Build on each pass:
//! an executor that replaces a lost one starts it over and resumes from what is recorded.

use crate::election::Incumbency;
use crate::model::{Node, NodeId, NodeKind, NodeStatus, PathName};
use crate::topology::Topology;
use rdm_core::Primary;
use rdm_transport::membership::DigestBook;
use std::sync::{Arc, Mutex};
use tracing::Instrument as _;

/// What fabric recovery is, in one admin's view: the fabric-primary record it restores, the path.name the
/// node-admin is re-created at, and the admin that runs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricRecovery {
    /// The fabric-primary record whose birth is not a heard holder.
    pub target: Primary,
    /// The path.name that birth lived at: its node's in the view, else its digest's or its departure's.
    pub at: Option<PathName>,
    /// The admin that runs the workflow: the lowest ReadyForTraffic node-admin of the target's mesh, else
    /// the lowest ReadyForTraffic mesh-primary of another mesh.
    pub executor: Option<PathName>,
    /// Whether `executor` is of the target's own mesh (the own-mesh arm) rather than the backbone's.
    pub own_mesh: bool,
}

/// Fabric recovery in `view` with the records `inc` and the book `book`; `None` while the fabric-primary
/// record names a heard holder, or no record is held.
pub fn of(view: &Topology, inc: &Incumbency, book: &DigestBook) -> Option<FabricRecovery> {
    let members: Vec<Node> = view.members().cloned().collect();
    let target = crate::election::fabric_recovery_target(&members, inc)?.clone();
    let lost = rdm_core::handover::HandoverBirth { mesh: target.mesh.clone(), node_id: target.node_id.clone(), incarnation: target.incarnation.clone() };
    let at = members.iter().find(|n| n.node_id == target.node_id).map(|n| n.name.clone()).or_else(|| crate::admin::lost_birth_name(book, &lost));
    // Without the lost birth's path.name this admin cannot tell the node re-created there from the mesh's
    // other node-admins: it names no executor of the mesh's own (a node that recovery re-created learns of
    // the workflow from its maker).
    let own = at.as_ref().and_then(|at| crate::election::fabric_recovery_executor(&members, inc, &target, Some(at))).map(|n| n.name.clone());
    let backbone = || {
        members
            .iter()
            .filter(|n| n.kind == NodeKind::NodeAdmin && n.mesh != target.mesh && n.is_primary && n.status == NodeStatus::ReadyForTraffic && !n.incarnation_id.as_ref().is_some_and(|i| inc.lost.contains(i)))
            .min_by(|a, b| a.node_id.as_str().cmp(b.node_id.as_str()))
            .map(|n| n.name.clone())
    };
    let executor = if at.is_some() { own.clone().or_else(backbone) } else { None };
    Some(FabricRecovery { own_mesh: own.is_some(), executor, target, at })
}

/// A hook that reads fabric recovery in a view (the records and the book are the admin's own).
pub type Reader = Arc<dyn Fn(&Topology) -> Option<FabricRecovery> + Send + Sync>;

/// A [`Reader`] set once its admin's book and records are held.
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

/// The reader over `book`'s records.
pub fn reader(book: DigestBook, records: Arc<crate::admin::Records>) -> Reader {
    Arc::new(move |t: &Topology| of(t, &crate::admin::incumbency_of(&book, &records), &book))
}

/// Whether `me` runs fabric recovery of `mesh` in `t` while the mesh has no mesh-primary: the writer of
/// the mesh and node records the attempt's own operations require (build.md §5.3, Q122).
pub fn executes_for(read: Option<&Reader>, t: &Topology, me: &PathName, mesh: &str) -> bool {
    read.and_then(|r| r(t)).is_some_and(|fr| fr.target.mesh == mesh && fr.executor.as_ref() == Some(me)) && t.node_type_primary(mesh, NodeKind::NodeAdmin).is_none()
}

/// The workflow's door on one admin.
pub struct FabricRecoveryDoor {
    /// This admin's path.name as born; its held name is the book's.
    pub me: PathName,
    /// This admin's node id.
    pub me_id: NodeId,
    /// Its view.
    pub topology: Arc<tokio::sync::RwLock<Topology>>,
    /// Its digest book.
    pub book: DigestBook,
    /// Its records.
    pub records: Arc<crate::admin::Records>,
    /// Node investigation on the lost birth, and its command back when reached.
    pub looker: crate::seat_watch::Looker,
    /// Node recovery's submit of the re-create.
    pub recovery: crate::admin::RecoverySubmitter,
    /// node.primary.update's door: the restore's call.
    pub handover: crate::fabric_handover::HandoverSlot,
    /// Whether a person launched this admin on the fabric-primary's data dir to recover the fabric
    /// (`RDM_FABRIC_PRIMARY`; fabric-recovery.md: "It starts when a person or a script launches one
    /// node-admin on the fabric-primary's data dir"): at the fabric-primary's path.name with no admin to
    /// run the workflow ("Nothing alive": "It is the only node, the fabric-primary"), it restores itself.
    launched_to_recover: bool,
    /// The birth whose workflow runs here now.
    running: Mutex<Option<crate::model::IncarnationId>>,
}

impl FabricRecoveryDoor {
    /// A door for the admin these name.
    #[allow(clippy::too_many_arguments)]
    pub fn new(me: PathName, me_id: NodeId, topology: Arc<tokio::sync::RwLock<Topology>>, book: DigestBook, records: Arc<crate::admin::Records>, looker: crate::seat_watch::Looker, recovery: crate::admin::RecoverySubmitter, handover: crate::fabric_handover::HandoverSlot, launched_to_recover: bool) -> Self {
        Self { me, me_id, topology, book, records, looker, recovery, handover, launched_to_recover, running: Mutex::new(None) }
    }

    /// Whether this admin runs the workflow `fr`: its executor, or the admin a person launched at the
    /// fabric-primary's path.name when no admin runs it.
    fn runs(&self, fr: &FabricRecovery) -> bool {
        let me = self.held_name();
        // Launched on the fabric-primary's own data dir, its path.name is the fabric-primary's, whether or
        // not this admin knows the lost birth's.
        fr.executor.as_ref() == Some(&me) || (self.launched_to_recover && fr.executor.is_none() && fr.at.as_ref().is_none_or(|at| *at == me) && self.me_id != fr.target.node_id)
    }

    fn held_name(&self) -> PathName {
        self.book.held_path_name().unwrap_or_else(|| self.me.clone())
    }

    /// Fabric recovery in this admin's view now.
    pub async fn read(&self) -> Option<FabricRecovery> {
        let t = self.topology.read().await.clone();
        of(&t, &crate::admin::incumbency_of(&self.book, &self.records), &self.book)
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
    /// is restored, the record names a heard holder, or this admin no longer runs it.
    async fn run(self: &Arc<Self>, target: Primary) {
        let me = self.held_name();
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
        let outcome = self.run_in(&span, &target).instrument(span.clone()).await;
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
        let mut backoff = rdm_transport::membership::gossip_interval();
        let ceiling = rdm_transport::membership::staleness_floor() * 4;
        loop {
            let me = self.held_name();
            let Some(fr) = self.read().await.filter(|fr| fr.target == *target) else {
                return "the fabric-primary record names a heard holder".into();
            };
            if !self.runs(&fr) {
                return format!("{me} no longer runs it: {}", fr.executor.map(|e| e.to_string()).unwrap_or_else(|| "nobody does".into()));
            }
            let Some(at) = fr.at.clone().or_else(|| self.launched_to_recover.then(|| me.clone())) else {
                return "the lost birth's path.name is not known here".into();
            };
            span.record("at", at.to_string().as_str()).record("arm", if fr.own_mesh { "own-mesh" } else { "backbone" });
            let view = self.topology.read().await.clone();
            let lost = view.members().find(|n| n.node_id == target.node_id && n.incarnation_id.as_ref() == Some(&target.incarnation)).cloned();
            // ReadyForTraffic in the view, or re-created by this workflow's completed attempt, whose create
            // waited for it to be ready (an executor of another mesh holds no member status of it).
            let restored = view.node(&at).filter(|n| n.kind == NodeKind::NodeAdmin && (n.status == NodeStatus::ReadyForTraffic || (done && n.node_id != target.node_id))).cloned();
            match (lost, restored) {
                // The node at the path.name is ReadyForTraffic: a new birth, or the lost birth returned.
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
                            next_look = std::time::Instant::now() + rdm_transport::membership::staleness_floor();
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
            tokio::time::sleep(rdm_transport::membership::gossip_interval()).await;
        }
    }

    /// One submit of the re-create of `node`: `true` once its attempt completed; else the next is due after
    /// `backoff`, which doubles up to `ceiling`.
    /// From the backbone (`own_mesh` false) the re-create terminates nothing in the mesh (fabric-recovery.md
    /// step 3, Q148): its node.delete of the exact lost birth writes that birth's row deleted with no
    /// `TerminateRuntime` and releases the name, then node.create at the path.name.
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

    /// Restore `node`, ReadyForTraffic at the target's path.name, as mesh-primary and fabric-primary
    /// (fabric-recovery.md step 4): node.primary.update's call on it, each record at its next revision.
    async fn restore(&self, target: &Primary, node: &Node) -> Result<Vec<rdm_node::member_handover::Entry>, rdm_core::handover::NodePrimaryUpdateReply> {
        let door = self.handover.get().cloned().ok_or_else(|| rdm_core::reply::handover::not_ready("this admin's node.primary.update door is not built yet".into()))?;
        let seats = self.book.seats.clone();
        let mesh_record = seats.mesh(&target.mesh);
        door.restore(target, mesh_record.as_ref(), node).await
    }
}
