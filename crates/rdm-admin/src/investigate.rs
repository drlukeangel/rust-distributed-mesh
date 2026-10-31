//! The peer-mesh investigation (`docs/architecture/lifecycles/mesh-investigation.md`;
//! `docs/architecture/node-identity-and-state.md` §7.3; ruling R-CK1).
//!
//! One investigation per peer mesh, owned by the fabric primary. t is the elapsed monotonic time
//! since the fabric primary's last valid direct backbone receipt from the mesh; only that receipt
//! starts or resets it, forwarded topology never does, and neither the backbone interval nor a
//! delayed tick moves a rung. The silent mark is the shared staleness mark, so the rungs are taken
//! from the staleness floor (`Rungs::at`):
//!
//! ```text
//! unheard t   20 s   probe 1    the exact mesh-primary first, then the other node-admins,
//!                               each the one tickle ladder (node-tickle.md): direct, then
//!                               Forward{ProbeNodeState} through up to two nodes at once
//!             30 s   mark       the mesh is silent (nothing is sent)
//!             40 s   probe 2    the same, another carrier preferred
//!             60 s   decision   opens once the probes finished; recovery only if the latest
//!                               probe answered connection-lost
//! ```
//!
//! An answer from a node-admin stops it (`admin-alive`); a backbone receipt cancels it
//! (`heard-again`); no node answering holds. The state machine here is pure over the elapsed
//! time and the probe results; [`run`] drives it from the fabric primary's view.

use crate::admin::Records;
use crate::model::{Node, NodeId, NodeKind, NodeStatus, PathName};
use crate::topology::Topology;
use rdm_transport::topology::TopologyView;
use crate::offline::{tickle, PeerProbe, ProbeEnd, Tickle, TickleCalls, Unreached, VIA_PEER_TICKLE_FANOUT};
use rdm_core::status::{Status, StatusReply, StatusRequest};
use rdm_transport::{Budget, CallOptions, NodeRpcClient, NodeTarget};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The ruled rungs (`docs/architecture/lifecycles/mesh-investigation.md`): how long the mesh has
/// gone unheard, elapsed monotonic time since the fabric primary's last valid direct backbone
/// receipt from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rungs {
    /// Unheard this long, the mesh is tracked (a publication missed beyond its jitter).
    pub track: Duration,
    /// Unheard this long, the first probe is made.
    pub probe1: Duration,
    /// Unheard this long, the mesh is marked silent and nothing is sent.
    pub mark: Duration,
    /// Unheard this long, the second probe is made.
    pub probe2: Duration,
    /// Unheard this long, the decision step opens.
    pub decide: Duration,
}

impl Rungs {
    /// The rungs at the staleness floor `floor` (`RDM_STALENESS_MS`) and the backbone gossip interval
    /// `backbone`: the silent mark is the shared staleness mark (mesh-investigation.md step 2), and
    /// probe 1, probe 2 and the decision stand at two thirds, four thirds and twice it: 20 s, 30 s,
    /// 40 s and 60 s at the 30 s default floor. The backbone interval sets only when tracking begins
    /// (one publication missed beyond its jitter, never after probe 1), a span and no threshold.
    pub fn at(floor: Duration, backbone: Duration) -> Rungs {
        let probe1 = floor * 2 / 3;
        Rungs { track: (backbone * 2).min(probe1), probe1, mark: floor, probe2: floor * 4 / 3, decide: floor * 2 }
    }

    /// The rungs at the configured cadences.
    pub fn configured() -> Rungs {
        Self::at(rdm_core::cadence::staleness_floor(), rdm_core::cadence::backbone_gossip_interval())
    }
}

/// What a probe found (`docs/architecture/lifecycles/mesh-investigation.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProbeOutcome {
    /// The mesh's node-admin answered the carried probe (it re-asserted its presence).
    AdminAlive,
    /// A node answered the Ping but reports it cannot reach its own node-admin.
    ConnectionLost,
    /// No node answered, or the carried call ended any other way.
    #[default]
    Unreachable,
}

impl ProbeOutcome {
    /// The outcome's name as it appears in spans and evidence.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AdminAlive => "admin-alive",
            Self::ConnectionLost => "connection-lost",
            Self::Unreachable => "unreachable",
        }
    }
}

/// What the ladder asks of its driver (`docs/architecture/lifecycles/mesh-investigation.md`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// The mesh went unheard: tracking begins.
    Track {
        /// How long the mesh has gone unheard, in milliseconds.
        unheard_ms: u64,
    },
    /// The shared silent mark: nothing is sent.
    Mark {
        /// How long the mesh has gone unheard, in milliseconds.
        unheard_ms: u64,
    },
    /// Make probe `n` (1 or 2).
    Probe {
        /// The probe's number.
        n: u8,
        /// How long the mesh has gone unheard, in milliseconds.
        unheard_ms: u64,
        /// The investigation the probe belongs to: its result is recorded with it
        /// ([`Ladder::probed`]).
        ticket: u64,
    },
    /// The node-admin answered: the investigation stops.
    Stop {
        /// How long the mesh has gone unheard, in milliseconds.
        unheard_ms: u64,
    },
    /// The decision: `recover` only when the latest probe answered `connection-lost`.
    Decide {
        /// Whether the mesh's node-admins are recovered.
        recover: bool,
        /// What the latest probe found.
        latest: ProbeOutcome,
        /// How long the mesh has gone unheard, in milliseconds.
        unheard_ms: u64,
    },
    /// The mesh was heard on the backbone again: the investigation is cancelled.
    Cancel {
        /// How long the mesh had gone unheard, in milliseconds.
        unheard_ms: u64,
        /// The probes made.
        probes: u8,
    },
}

#[derive(Debug, Default)]
struct State {
    /// How long the mesh has gone unheard, as of the latest step.
    unheard: Duration,
    tracked: bool,
    marked: bool,
    probes: [Option<ProbeOutcome>; 2],
    /// The probe made and not yet ended: one probe of a mesh is in flight at a time, so the latest
    /// completed probe is always the later one, and the decision waits for it.
    in_flight: Option<u8>,
    /// This investigation, issued when tracking began: a probe's result is recorded only in the
    /// investigation it was made for, never in a fresh one after a cancel.
    ticket: u64,
    stopped: bool,
    /// The decision, once taken: whether it was a recovery.
    verdict: Option<bool>,
}

impl State {
    fn probes_made(&self) -> u8 {
        self.probes.iter().flatten().count() as u8 + u8::from(self.in_flight.is_some())
    }
}

/// A peer mesh as the ladder holds it (`docs/architecture/lifecycles/mesh-investigation.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerMesh {
    /// `Some(ms unheard)` while the ladder tracks the mesh as unheard: loss of its control-plane
    /// visibility targets lifecycle mutation for every node of the mesh (R-D1).
    pub unheard_ms: Option<u64>,
    /// The ladder has decided the recovery of the mesh's node-admins.
    pub recovery_decided: bool,
    /// The ladder's 60 s decision found the latest probe `unreachable`: no node answered and no
    /// carrier reported its connection to a node-admin lost. The mesh is held; its node-admin is created
    /// only on the provider's exact exited verdict of every node of the mesh
    /// (mesh-investigation.md step 5, "Fully crashed"), which the fabric-primary reads, never the ladder.
    pub unreachable_decided: bool,
}

/// One investigation per peer mesh the fabric primary has heard on the backbone
/// (`docs/architecture/lifecycles/mesh-investigation.md`).
#[derive(Debug)]
pub struct Ladder {
    rungs: Rungs,
    meshes: BTreeMap<String, State>,
    /// The last investigation ticket issued; never reused, across a cleared or forgotten mesh too.
    issued: u64,
    /// Moves each time what this ladder holds of a mesh moves (tracked, its decision, forgotten): what
    /// the reconcile loop waits on, whose drift check and drive read it.
    moved: tokio::sync::watch::Sender<u64>,
}

impl Ladder {
    /// A ladder over `rungs` with no mesh tracked.
    pub fn new(rungs: Rungs) -> Self {
        Self { rungs, meshes: BTreeMap::new(), issued: 0, moved: tokio::sync::watch::Sender::new(0) }
    }

    /// What this ladder holds as it moves: a waiter arms on it to see a mesh tracked, decided or forgotten.
    pub fn subscribe_moved(&self) -> tokio::sync::watch::Receiver<u64> {
        self.moved.subscribe()
    }

    /// The meshes held now.
    pub fn meshes(&self) -> BTreeSet<String> {
        self.meshes.keys().cloned().collect()
    }

    /// Forget everything: this node is not the fabric primary.
    pub fn clear(&mut self) {
        let held = self.meshes.keys().any(|m| self.held(m) != (false, None));
        self.meshes.clear();
        if held {
            self.moved.send_modify(|v| *v += 1);
        }
    }

    /// Forget `mesh`: it has no backbone receipt held at all.
    pub fn forget(&mut self, mesh: &str) {
        let before = self.held(mesh);
        self.meshes.remove(mesh);
        self.moved_since(mesh, before);
    }

    /// What the R-D1 target, the drift check and the drive read of `mesh` from this ladder: whether it
    /// is tracked and its decision, `(false, None)` for a mesh it holds nothing of.
    fn held(&self, mesh: &str) -> (bool, Option<bool>) {
        self.meshes.get(mesh).map(|st| (st.tracked, st.verdict)).unwrap_or((false, None))
    }

    /// Move the watch when what this ladder holds of `mesh` differs from `before`.
    fn moved_since(&self, mesh: &str, before: (bool, Option<bool>)) {
        if self.held(mesh) != before {
            self.moved.send_modify(|v| *v += 1);
        }
    }

    /// Has this ladder decided the recovery of `mesh`'s node-admins? A fabric primary holds that
    /// recovery back for every peer mesh it hears on the backbone until this is true: a node-admin
    /// whose runtime was proven exited is replaced when the ladder decides, not before, and a
    /// fabric primary that took the role a moment ago has decided nothing yet.
    pub(crate) fn recovery_decided(&self, mesh: &str) -> bool {
        self.meshes.get(mesh).is_some_and(|s| s.verdict == Some(true))
    }

    /// Has this ladder's decision on `mesh` come out `unreachable` (a hold)? True from the 60 s
    /// decision until the mesh is heard again or the ladder forgets it.
    pub(crate) fn unreachable_decided(&self, mesh: &str) -> bool {
        self.meshes.get(mesh).is_some_and(|s| s.verdict == Some(false))
    }

    /// What this ladder holds of `mesh`: `unheard_ms` is set while the ladder tracks the mesh as unheard
    /// (from the rung [`Rungs::track`] on, until it is heard again), the same clock [`Self::step`] counts;
    /// `None` when the ladder holds no investigation of the mesh at all.
    pub fn peer_mesh(&self, mesh: &str) -> Option<PeerMesh> {
        let st = self.meshes.get(mesh)?;
        Some(PeerMesh { unheard_ms: st.tracked.then(|| st.unheard.as_millis() as u64), recovery_decided: st.verdict == Some(true), unreachable_decided: st.verdict == Some(false) })
    }

    /// What the R-D1 target reads of `mesh`, for the Build door, the drive and the drift check alike:
    /// `unheard` is the elapsed time since this admin's last valid direct backbone receipt from the
    /// mesh (`DigestView::mesh_unheard`, mesh-investigation.md: "Let t be the elapsed monotonic time
    /// since the fabric-primary's last valid direct backbone receipt from mesh-x"), `None` for a mesh
    /// the backbone never carried here.
    /// A mesh heard on the backbone is targeted from the rung the ladder tracks it from
    /// ([`Rungs::track`]), whether or not the ladder has stepped it yet (a fabric-primary that took the
    /// role a moment ago has stepped nothing), and its decided recovery is the ladder's; a mesh the
    /// backbone never carried is what the ladder holds of it (watched from the role).
    pub fn peer_mesh_unheard(&self, mesh: &str, unheard: Option<Duration>) -> Option<PeerMesh> {
        match unheard {
            Some(t) => (t >= self.rungs.track).then(|| PeerMesh { unheard_ms: Some(t.as_millis() as u64), recovery_decided: self.recovery_decided(mesh), unreachable_decided: self.unreachable_decided(mesh) }),
            None => self.peer_mesh(mesh),
        }
    }

    /// How long after `unheard` the next rung of `mesh` falls due: the driver wakes then, so a tick
    /// never moves a rung. `None` once nothing of the mesh is left to step.
    pub fn next_due(&self, mesh: &str, unheard: Duration) -> Option<Duration> {
        let (r, st) = (self.rungs, self.meshes.get(mesh)?);
        if st.stopped || st.verdict.is_some() {
            return (!st.marked).then(|| r.mark.saturating_sub(unheard));
        }
        [r.track, r.probe1, r.mark, r.probe2, r.decide].into_iter().find(|rung| *rung > unheard).map(|rung| rung - unheard)
    }

    /// What `mesh` being unheard for `unheard` asks next. One probe or the decision per call; the
    /// driver makes the probe, records it with [`Self::probed`] when it ends, and asks again. While
    /// a probe is in flight the ladder still tracks and marks, and asks for no other probe and no
    /// decision.
    pub fn step(&mut self, mesh: &str, unheard: Duration) -> Vec<Step> {
        let before = self.held(mesh);
        let out = self.stepped(mesh, unheard);
        self.moved_since(mesh, before);
        out
    }

    /// [`Self::step`]'s ladder, without the watch.
    fn stepped(&mut self, mesh: &str, unheard: Duration) -> Vec<Step> {
        let Ladder { rungs: r, meshes, issued, .. } = self;
        let r = *r;
        let st = meshes.entry(mesh.to_string()).or_default();
        let mut out = Vec::new();
        st.unheard = unheard;
        let unheard_ms = unheard.as_millis() as u64;
        if unheard < r.track {
            if st.tracked && (st.probes_made() > 0 || st.marked || st.verdict.is_some()) {
                out.push(Step::Cancel { unheard_ms, probes: st.probes_made() });
            }
            *st = State::default();
            return out;
        }
        if !st.tracked {
            st.tracked = true;
            *issued += 1;
            st.ticket = *issued;
            out.push(Step::Track { unheard_ms });
        }
        if unheard >= r.mark && !st.marked {
            st.marked = true;
            out.push(Step::Mark { unheard_ms });
        }
        if st.stopped || st.verdict.is_some() || st.in_flight.is_some() {
            return out;
        }
        if unheard >= r.probe1 && st.probes[0].is_none() {
            st.in_flight = Some(1);
            out.push(Step::Probe { n: 1, unheard_ms, ticket: st.ticket });
        } else if unheard >= r.probe2 && st.probes[1].is_none() {
            st.in_flight = Some(2);
            out.push(Step::Probe { n: 2, unheard_ms, ticket: st.ticket });
        } else if unheard >= r.decide {
            // The latest completed probe decides; an older connection-lost never overrides a
            // newer unreachable.
            let latest = st.probes[1].or(st.probes[0]).unwrap_or(ProbeOutcome::Unreachable);
            let recover = latest == ProbeOutcome::ConnectionLost;
            st.verdict = Some(recover);
            out.push(Step::Decide { recover, latest, unheard_ms });
        }
        out
    }

    /// Probe `n` of `mesh`'s investigation `ticket` found `outcome`. An answering node-admin stops
    /// the investigation.
    /// A probe of an investigation since cancelled, forgotten or dropped with the role, or one this
    /// investigation is not waiting on, records nothing.
    pub fn probed(&mut self, mesh: &str, ticket: u64, n: u8, outcome: ProbeOutcome, unheard_ms: u64) -> Option<Step> {
        let st = self.meshes.get_mut(mesh).filter(|st| st.ticket == ticket && st.in_flight == Some(n))?;
        st.in_flight = None;
        st.probes[usize::from(n.clamp(1, 2)) - 1] = Some(outcome);
        (outcome == ProbeOutcome::AdminAlive).then(|| {
            st.stopped = true;
            Step::Stop { unheard_ms }
        })
    }
}

/// A direct probe is answered within two intervals or the node-admin did not answer.
pub(crate) fn direct_budget(interval: Duration) -> Duration {
    interval * 2
}

/// The carried probe outlasts the carrier's one inner call (made with `CallOptions::default`) by two
/// intervals, so the carrier's answer, connection-lost included, reaches the origin before it gives
/// up: an inner dial to a dead node-admin runs to the inner call's whole budget.
pub(crate) fn carried_budget(interval: Duration) -> Budget {
    match CallOptions::default().budget {
        Budget::Overall(inner) => Budget::Overall(inner + interval * 2),
        Budget::Split { send, reply } => Budget::Split { send: send + interval * 2, reply },
        Budget::Stream { send } => Budget::Stream { send: send + interval * 2 },
        // Owed is never a default (node-rpc.md §34): it has no bound to outlast.
        Budget::Owed => Budget::Owed,
    }
}

/// What the driver needs of its admin.
pub struct Watch {
    /// The admin's stop gate: this watch is held at its own wait while the admin is stopped, never
    /// where it reads the view.
    pub gate: crate::stop_gate::StopGate,
    /// This admin's `path.name`.
    pub me: PathName,
    /// The node id this admin was born with: whether it holds the fabric-primary role is decided by it.
    pub me_id: crate::model::NodeId,
    /// The length of a backbone interval.
    pub interval: Duration,
    /// The admin's topology view.
    pub topology_view: TopologyView,
    /// The admin's observed topology.
    pub topology: Arc<tokio::sync::RwLock<Topology>>,
    /// The records this admin holds.
    pub records: Arc<Records>,
    /// Is the current Build settled (not pending, not running)? A mesh the Build is still birthing
    /// is not unheard. `Err` names why the builds store could not be read: never read as either.
    pub settled: Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bool, String>> + Send>> + Send + Sync>,
    /// The client probes are made through.
    pub client: Arc<NodeRpcClient>,
    /// Does this node hold an Active Direct connection to the exact node? A member it is connected
    /// to is asked first as a carrier.
    pub connected: Arc<dyn Fn(&NodeId) -> bool + Send + Sync>,
    /// The ladder.
    pub ladder: Arc<Mutex<Ladder>>,
    /// The carrier each mesh's latest probe went through.
    pub carriers: Mutex<BTreeMap<String, PathName>>,
    /// Fabric recovery (Q122): no node-admin of the fabric-primary's mesh answered by the decision step, so
    /// the lowest Ready mesh-primary on the backbone starts it.
    pub fabric_recovery: Arc<crate::fabric_recovery::FabricRecoveryDoor>,
}

impl Watch {
    fn held_name_in(&self, view: &Topology) -> PathName {
        self.topology_view.digest_view.held_path_name().unwrap_or_else(|| view.held_name_of(&self.me, Some(&self.me_id)))
    }
}

/// The local OS clock: the silence of a mesh is measured from a local event, and the read is only
/// named in spans. No node reads it as a stamp.
fn local_now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// Run the investigation for as long as the task lives: once per interval, while this admin is the
/// fabric primary and its view authorizes, every peer mesh it has heard on the backbone is stepped
/// down its ladder.
pub async fn run(w: Watch) {
    let w = Arc::new(w);
    let inv = Investigations::new(w.me.to_string(), w.ladder.clone(), w.gate.clone());
    // When this admin took the fabric-primary role: a peer mesh it has never heard is unheard from then.
    let mut never_since: BTreeMap<String, Instant> = BTreeMap::new();
    let mut was_fabric_primary = false;
    // The role the ladder's state was built in: a decision taken as the fabric-primary (on a peer mesh) is
    // not the backbone rule's decision on the fabric-primary role's mesh once the role has moved elsewhere.
    let mut ladder_role_fabric_primary: Option<bool> = None;
    let mut wait = w.interval;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = inv.landed() => {}
            _ = w.records.wake.report_notify.notified() => {}
            _ = w.records.view_changed.notified(), if !was_fabric_primary => {}
        }
        wait = w.interval;
        // Held here while the admin is stopped: its own wait, outside every lock. The turn ends with
        // the pass.
        let _turn = w.gate.turn("peer-mesh-investigation").await;
        let view = Arc::new(w.topology.read().await.clone());
        let me = w.held_name_in(&view);
        inv.set_me(me.to_string());
        let fabric_primary = view.is_fabric_primary(&w.me_id);
        was_fabric_primary = fabric_primary;
        if ladder_role_fabric_primary != Some(fabric_primary) {
            if ladder_role_fabric_primary.is_some() {
                w.ladder.lock().unwrap().clear();
            }
            ladder_role_fabric_primary = Some(fabric_primary);
        }
        if !was_fabric_primary {
            never_since.clear();
            // A mesh-primary outside the fabric-primary's mesh runs the ladder on that mesh alone
            // while the fabric-primary is unheard (fabric-recovery.md "The loss of the fabric-primary
            // while a node-admin lives" step 2; elections §4.3): the fabric-primary first, then its
            // other node-admins; no answer from any of them by the decision step is the backbone rule.
            // An admin whose view is cut off or healing acts on nothing (gossip.md §6).
            match fabric_primary_mesh_to_investigate(&w, &view, &me) {
                Some(mesh) => {
                    forget_unheard_meshes(&w.ladder, &BTreeSet::from([mesh.clone()]));
                    if let Some(unheard) = w.topology_view.digest_view.mesh_unheard(&mesh, Instant::now()) {
                        investigate(&w, &inv, &view, mesh.clone(), unheard).await;
                        if let Some(due) = w.topology_view.digest_view.mesh_unheard(&mesh, Instant::now()).and_then(|u| w.ladder.lock().unwrap().next_due(&mesh, u)) {
                            wait = wait.min(due);
                        }
                        // No node-admin of that mesh answered by the decision step: fabric recovery starts at the
                        // lowest Ready mesh-primary on the backbone, exactly as in the own-mesh arm (Q122).
                        let decided = { let l = w.ladder.lock().unwrap(); l.unreachable_decided(&mesh) || l.recovery_decided(&mesh) };
                        w.fabric_recovery.start_if_mine(false, decided).await;
                    }
                }
                None => w.ladder.lock().unwrap().clear(),
            }
            continue;
        }
        let digest_view = &w.topology_view.digest_view;
        let heard = digest_view.backbone_meshes();
        let current_never = never_heard(&view, &me.mesh, &heard);
        never_since.retain(|m, _| current_never.contains(m));
        // The clock of a mesh never heard starts once the accepted Build is settled: from the role,
        // and from each settling, because a mesh the Build is still birthing has not had its chance
        // to be heard; once started for a settled mesh, it runs across a subsequent Build targeted on it.
        match (w.settled)().await {
            Ok(true) => {
                let track = w.ladder.lock().unwrap().rungs.track;
                let start = Instant::now().checked_sub(track).unwrap_or_else(Instant::now);
                for m in &current_never {
                    never_since.entry(m.clone()).or_insert(start);
                }
            }
            Ok(false) => {}
            // A store that cannot be read starts no new clock: named.
            Err(e) => {
                tracing::info_span!("rdm.node_admin.build.reject.via-build-store-unreadable", route = "mesh-investigation", node = %me, error = %e)
                    .in_scope(|| tracing::info!("the current Build could not be read: the never-heard clock does not run on a read that failed"));
            }
        }
        let never: BTreeSet<String> = never_since.keys().cloned().collect();
        forget_unheard_meshes(&w.ladder, &heard.union(&never).cloned().collect());
        let mut tasks = Vec::new();
        let mut unheard_now = Vec::new();
        for mesh in heard.into_iter().filter(|m| *m != me.mesh) {
            let Some(unheard) = digest_view.mesh_unheard(&mesh, Instant::now()) else { continue };
            unheard_now.push(mesh.clone());
            tasks.push(investigate(&w, &inv, &view, mesh, unheard));
        }
        for (mesh, since) in &never_since {
            let unheard = since.elapsed();
            tasks.push(investigate(&w, &inv, &view, mesh.clone(), unheard));
        }
        futures_util::future::join_all(tasks).await;
        // The next wake: a receipt is looked for every interval, and a rung is stepped when it falls
        // due, not on the interval after it.
        for mesh in unheard_now {
            if let Some(due) = digest_view.mesh_unheard(&mesh, Instant::now()).and_then(|u| w.ladder.lock().unwrap().next_due(&mesh, u)) {
                wait = wait.min(due);
            }
        }
        for (mesh, since) in &never_since {
            if let Some(due) = w.ladder.lock().unwrap().next_due(mesh, since.elapsed()) {
                wait = wait.min(due);
            }
        }
        restore_peer_recovery(&me, &w.records, &view);
    }
}

/// The mesh this admin, a mesh-primary that does not hold the fabric-primary role, investigates while the
/// fabric-primary is unheard: the fabric primary record's mesh, when it is not this admin's own (its own
/// mesh's standby is the primary watch's arm, node-recovery.md step 2) and the record's birth is held off
/// ready in this admin's view. `None` otherwise.
fn fabric_primary_mesh_to_investigate(w: &Watch, view: &Topology, me: &PathName) -> Option<String> {
    if !view.is_mesh_primary(&me.mesh, &w.me_id) {
        return None;
    }
    let record = w.topology_view.primaries().fabric()?;
    if record.mesh == me.mesh {
        return None;
    }
    let held = view.members().any(|n| n.node_id == record.node_id && n.incarnation_id.as_ref() == Some(&record.incarnation) && matches!(n.status, NodeStatus::PendingReconnect | NodeStatus::Dead));
    held.then_some(record.mesh)
}

/// The peer meshes the view holds nodes of that the backbone has never carried to this admin: it
/// learned them from topology alone, which is not liveness (R-G2), so nothing says whether they are
/// alive. An admin that took the fabric-primary role watches them from the moment it took it.
pub(crate) fn never_heard(view: &Topology, me_mesh: &str, heard: &BTreeSet<String>) -> BTreeSet<String> {
    view.members().map(|n| n.mesh.clone()).filter(|m| m != me_mesh && !heard.contains(m)).collect()
}

/// Forget every investigation of a mesh the backbone no longer carries (a deleted mesh).
fn forget_unheard_meshes(ladder: &Mutex<Ladder>, heard: &BTreeSet<String>) {
    let gone: Vec<String> = ladder.lock().unwrap().meshes().difference(heard).cloned().collect();
    let mut held = ladder.lock().unwrap();
    for mesh in gone {
        held.forget(&mesh);
    }
}

/// The fabric returns to ready when the mesh the fabric primary gave a new node-admin (the
/// connection-lost verdict's, or the fully crashed mesh's) has reported for its checklist: its current
/// node-admin primary, a birth the decision did not find, declared the Mesh ready to this fabric
/// primary after the decision (mesh-investigation.md step 7: "Only on that report, the fabric-primary
/// clears the degraded health observation"). A ready primary of a newer birth in the view, with no
/// report, is not this (`crate::checklist::recovered`).
pub fn restore_peer_recovery(me: &PathName, records: &Records, view: &Topology) {
    for recovery in records.peer_recoveries() {
        let primary = view.node_type_primary(&recovery.mesh, NodeKind::NodeAdmin).and_then(|p| p.incarnation_id.clone());
        let report = records.declared.lock().unwrap().report_of(&recovery.mesh, view.node_type_primary(&recovery.mesh, NodeKind::NodeAdmin));
        if crate::checklist::recovered(&recovery, primary.as_ref(), report.as_ref()) {
            records.close_peer_recovery(&recovery.mesh);
            tracing::info_span!("rdm.node_admin.mesh.update.via-recovered", node = %me, mesh = %recovery.mesh, primary = ?primary, verdict_rafka_ms = ?recovery.verdict_rafka_ms)
                .in_scope(|| tracing::info!("the recovered mesh's primary reported its checklist complete: the fabric is ready again"));
        }
    }
}

/// The driver of the ladder for every mesh: it steps the ladder on elapsed time and starts each
/// probe the ladder asks for, recording the probe's result when the probe ends.
pub(crate) struct Investigations {
    /// This admin's `path.name`, named on the probe and verdict spans.
    me: Mutex<String>,
    ladder: Arc<Mutex<Ladder>>,
    /// The probes in flight, each its own task, held by the admin's stop gate while it is stopped;
    /// dropped with the driver, they end with it.
    probes: Mutex<tokio::task::JoinSet<()>>,
    /// Woken when a probe ends, so the step it opens (probe 2, the decision) is taken then.
    landed: Arc<tokio::sync::Notify>,
    /// The admin's stop gate: a probe in flight while the admin is stopped is held where it stands.
    gate: crate::stop_gate::StopGate,
}

impl Investigations {
    pub(crate) fn new(me: String, ladder: Arc<Mutex<Ladder>>, gate: crate::stop_gate::StopGate) -> Self {
        Self { me: Mutex::new(me), ladder, probes: Mutex::new(tokio::task::JoinSet::new()), landed: Arc::new(tokio::sync::Notify::new()), gate }
    }

    pub(crate) fn set_me(&self, me: String) {
        *self.me.lock().unwrap() = me;
    }

    /// Resolves when a probe has ended since the last wake.
    pub(crate) async fn landed(&self) {
        self.landed.notified().await
    }

    /// Step `mesh` at `unheard` and start each probe the ladder asks for with `start`, as its own
    /// task: a running probe never holds the ladder (R-CK1: a delayed tick never moves a threshold).
    /// Answers the steps the ladder took; a probe's result is recorded with its ticket and its span
    /// emitted when the probe ends.
    pub(crate) async fn tick<F, Fut>(&self, mesh: &str, unheard: Duration, start: F) -> Vec<Step>
    where
        F: Fn(u8) -> Fut,
        Fut: std::future::Future<Output = Found> + Send + 'static,
    {
        let steps = self.ladder.lock().unwrap().step(mesh, unheard);
        let mut probes = self.probes.lock().unwrap();
        while probes.try_join_next().is_some() {}
        let me = self.me.lock().unwrap().clone();
        for s in &steps {
            if let Step::Probe { n, unheard_ms, ticket } = *s {
                // The probe's span opens when it is asked and its calls are its children; what it
                // found is recorded on it when it ends.
                let empty = tracing::field::Empty;
                let span = tracing::info_span!(
                    "rdm.node_admin.mesh.update.via-probe",
                    node = %me,
                    mesh = %mesh,
                    probe = n,
                    primary_record = empty,
                    target = empty,
                    member = empty,
                    carrier = empty,
                    outcome = empty,
                    detail = empty,
                    iroh_known_addrs = empty,
                    iroh_active_addrs = empty,
                    unheard_ms,
                );
                let (me, ladder, landed, mesh) = (me.clone(), self.ladder.clone(), self.landed.clone(), mesh.to_string());
                let probe = tracing::Instrument::instrument(start(n), span.clone());
                // A probe takes no lock a handler needs: while the admin is stopped it is held where it
                // stands, so a stopped admin's probe makes no further call.
                probes.spawn(self.gate.hold(async move {
                    let found = probe.await;
                    land(&me, &span, &ladder, &mesh, ticket, n, unheard_ms, found);
                    landed.notify_one();
                }));
            }
        }
        steps
    }
}

/// Probe `n` of `mesh`'s investigation `ticket` ended with `found`: record it, and say what it
/// found on its span (`span`, opened when it was asked).
fn land(me: &str, span: &tracing::Span, ladder: &Mutex<Ladder>, mesh: &str, ticket: u64, n: u8, unheard_ms: u64, found: Found) {
    // The ladder is held until the verdict's span is out: a backbone receipt's cancel, stepped after
    // this stop, emits its `heard-again` after this verdict, never before it.
    let mut held = ladder.lock().unwrap();
    let stop = held.probed(mesh, ticket, n, found.outcome, unheard_ms);
    span.record("primary_record", found.primary_record.as_str());
    span.record("target", found.target.as_str());
    span.record("member", found.members.as_str());
    span.record("carrier", found.carrier.as_str());
    span.record("outcome", found.outcome.as_str());
    span.record("detail", found.detail.as_str());
    span.record("iroh_known_addrs", found.iroh_known.as_str());
    span.record("iroh_active_addrs", found.iroh_active.as_str());
    span.in_scope(|| tracing::info!("one probe of the unheard mesh"));
    if let Some(Step::Stop { unheard_ms }) = stop {
        verdict(me, mesh, found.outcome.as_str(), unheard_ms, "the node-admin answered: the investigation stops");
    }
    drop(held);
}

async fn investigate(w: &Arc<Watch>, inv: &Investigations, view: &Arc<Topology>, mesh: String, unheard: Duration) {
    let me = w.held_name_in(view);
    let at = local_now_ms().saturating_sub(unheard.as_millis() as u64);
    let start = |n: u8| {
        let (w, view, mesh) = (w.clone(), view.clone(), mesh.clone());
        async move { probe(&WatchCalls(&w), &w.carriers, &view, w.topology_view.digest_view.primaries.mesh(&mesh), &mesh, n).await }
    };
    let steps = inv.tick(&mesh, unheard, start).await;
    for step in steps {
        match step {
            Step::Track { unheard_ms } => {
                tracing::info_span!("rdm.node_admin.mesh.update.via-unheard", node = %me, mesh = %mesh, at, unheard_ms, mark = false)
                    .in_scope(|| tracing::info!("no word of the mesh on the backbone: the investigation tracks it"));
            }
            Step::Mark { unheard_ms } => {
                tracing::info_span!("rdm.node_admin.mesh.update.via-unheard", node = %me, mesh = %mesh, at, unheard_ms, mark = true)
                    .in_scope(|| tracing::info!("the mesh is marked silent; nothing is sent"));
            }
            Step::Probe { .. } | Step::Stop { .. } => {}
            Step::Decide { recover, latest, unheard_ms } => {
                // The fabric-primary's mesh, decided at a mesh-primary that is not the fabric-primary: no
                // node-admin of that mesh answered either probe, so the backbone rule applies and the
                // lowest Ready mesh-primary is fabric-primary (fabric-recovery.md "Outside its
                // mesh"; elections §4.3). The re-creation of the lost mesh's node-admin follows it.
                let record = w.topology_view.primaries().fabric().filter(|r| r.mesh == mesh && r.node_id != w.me_id);
                if let (Some(_), false) = (record, view.is_fabric_primary(&w.me_id)) {
                    verdict(&me.to_string(), &mesh, latest.as_str(), unheard_ms, "no node-admin of the fabric-primary's mesh answered by the decision step: the backbone rule applies");
                    continue;
                }
                verdict(&me.to_string(), &mesh, latest.as_str(), unheard_ms, if recover { "the latest probe says a member cannot reach its own node-admin: the mesh is recovered" } else { "no member could say: the mesh is held" });
                if recover {
                    let verdict_ms = w.topology_view.clock().now_rafka_ms();
                    let lost = view.nodes_of_type(&mesh, NodeKind::NodeAdmin).filter_map(|a| a.incarnation_id.clone()).collect();
                    w.records.open_mesh_recovery(&me, crate::admin::PeerRecovery { mesh: mesh.clone(), verdict_rafka_ms: Some(verdict_ms), opened_at: Instant::now(), lost }, "connection-lost", true);
                }
            }
            Step::Cancel { unheard_ms, probes } => {
                tracing::info_span!("rdm.node_admin.mesh.update.via-probe-verdict", node = %me, mesh = %mesh, outcome = "heard-again", at = local_now_ms(), unheard_ms, probes)
                    .in_scope(|| tracing::info!("the mesh is heard on the backbone again: the investigation is cancelled"));
            }
        }
    }
}

fn verdict(me: &str, mesh: &str, outcome: &str, unheard_ms: u64, why: &str) {
    tracing::info_span!("rdm.node_admin.mesh.update.via-probe-verdict", node = %me, mesh = %mesh, outcome, at = local_now_ms(), unheard_ms).in_scope(|| tracing::info!("{why}"));
}

/// What one node-admin's `unreached` says of the mesh (mesh-investigation.md step 5). Only a carrier
/// saying its own connection to the node-admin is lost (`InnerNotSent`, the tickle's `connection-lost`)
/// is `connection-lost`, and that carrier is named; a carrier that cannot be reached, refuses, or
/// loses the call says nothing of the node-admin, so `unreachable`, naming the first carrier that
/// answered anything at all (a reply that was not the exact birth's, or an `Indeterminate`), or none.
pub(crate) fn classify_unreached(unreached: &Unreached) -> (ProbeOutcome, Option<String>) {
    if let Some(PeerProbe { carrier, .. }) = unreached.peers.iter().find(|p| matches!(p.end, ProbeEnd::ConnectionLost(_))) {
        return (ProbeOutcome::ConnectionLost, Some(carrier.clone()));
    }
    (ProbeOutcome::Unreachable, unreached.peers.iter().find(|p| p.end.reached_a_process()).map(|p| p.carrier.clone()))
}

/// What one probe found, with what it asked.
#[derive(Debug, Clone, Default)]
pub(crate) struct Found {
    pub(crate) outcome: ProbeOutcome,
    pub(crate) target: String,
    pub(crate) members: String,
    pub(crate) carrier: String,
    pub(crate) detail: String,
    /// iroh's local view of each node-admin asked, `<path.name>=<known addrs>` joined by `;`: a
    /// diagnostic beside the probe, nothing decides on it (`rdm_transport::iroh_obs`).
    pub(crate) iroh_known: String,
    /// The addresses iroh used for each node-admin asked, as `iroh_known`.
    pub(crate) iroh_active: String,
    /// The mesh-primary record the probe ordered its node-admins by, or `none` held.
    pub(crate) primary_record: String,
}

/// The node-admins of `mesh` a probe carries `ProbeNodeState` to, in the order it asks them: the
/// exact mesh-primary the held primary record `primary` names first (its birth, the record's, R-EL1a),
/// then the mesh's other node-admins in path order (mesh-investigation.md step 1). With no record
/// held, path order. Only exact births are asked: a probe names `(node.id, incarnation)`.
pub(crate) fn admins_in_probe_order(view: &Topology, mesh: &str, primary: Option<&rdm_core::role::Primary>) -> Vec<Node> {
    let mut admins: Vec<Node> = view.nodes_of_type(mesh, NodeKind::NodeAdmin).filter(|a| a.incarnation_id.is_some()).cloned().collect();
    admins.sort_by_key(|a| a.name.to_string());
    if let Some(record) = primary.filter(|r| r.mesh == mesh) {
        if let Some(i) = admins.iter().position(|a| a.node_id == record.node_id) {
            let mut primary = admins.remove(i);
            primary.incarnation_id = Some(record.incarnation.clone());
            admins.insert(0, primary);
        }
    }
    admins
}

/// What one probe found over every node-admin it asked, in the order asked: any node-admin
/// answering is `admin-alive` (the mesh has a node-admin; mesh-investigation.md step 4); with none
/// answering it is `connection-lost` only when the carrier reported its connection lost to every one of
/// them (step 5: the recovery recovers every node-admin of the mesh, so each must be reported
/// unreachable from inside it); any other end says nothing of that node-admin, so `unreachable`.
pub(crate) fn fold_admin_outcomes(outcomes: &[ProbeOutcome]) -> ProbeOutcome {
    if outcomes.contains(&ProbeOutcome::AdminAlive) {
        ProbeOutcome::AdminAlive
    } else if !outcomes.is_empty() && outcomes.iter().all(|o| *o == ProbeOutcome::ConnectionLost) {
        ProbeOutcome::ConnectionLost
    } else {
        ProbeOutcome::Unreachable
    }
}

/// The calls one probe makes: the tickle's own ([`TickleCalls`]: the direct probe, the carried
/// probe and the carriers, each a primitive) and the diagnostic read beside them. [`probe`] is the
/// workflow that calls them in mesh-investigation.md's order, and the admin plugs in its own Node RPC
/// client ([`WatchCalls`]).
#[async_trait::async_trait]
pub(crate) trait ProbeCalls: TickleCalls {
    /// iroh's local view of `admin`, read beside its probe: its known and its active addresses. A
    /// diagnostic beside the probe; nothing decides on it.
    async fn observe(&self, admin: &Node) -> (String, String);
}

/// The admin's own calls for a probe: its Node RPC client under the investigation's budgets, its
/// connections and its endpoint.
pub(crate) struct WatchCalls<'a>(pub(crate) &'a Watch);

#[async_trait::async_trait]
impl TickleCalls for WatchCalls<'_> {
    fn caller(&self) -> String {
        self.0.me.to_string()
    }

    fn deadline(&self) -> Duration {
        direct_budget(self.0.interval)
    }

    async fn direct(&self, admin: &Node, req: &StatusRequest) -> ProbeEnd {
        let opts = CallOptions { budget: Budget::Overall(direct_budget(self.0.interval)), ..CallOptions::default() };
        let (out, _) = self.0.client.call::<Status>(&NodeTarget::ExactNode(admin.node_id.clone()), req, &opts).await;
        ProbeEnd::of(out)
    }

    async fn via_peer(&self, carrier: &Node, admin: &Node, req: &StatusRequest) -> ProbeEnd {
        // A carrier this admin cannot reach ends the kick within one direct probe's budget (the
        // connect and send), as the probe's member Ping did; a carrier that took the kick has the whole
        // carried budget for its answer, connection-lost included (mesh-investigation.md steps 1, 3).
        let budget = match carried_budget(self.0.interval) {
            Budget::Overall(reply) => Budget::Split { send: direct_budget(self.0.interval), reply },
            other => other,
        };
        let opts = CallOptions { budget, ..CallOptions::default() };
        let (out, _) = self.0.client.call_via::<Status>(&NodeTarget::ExactNode(carrier.node_id.clone()), &admin.node_id, req, &opts).await;
        ProbeEnd::of(out)
    }

    fn carriers(&self, view: &Topology, admin: &Node) -> Vec<Node> {
        let avoid = self.0.carriers.lock().unwrap().get(&admin.mesh).cloned();
        investigation_carriers(view, &admin.mesh, |id| (self.0.connected)(id), avoid.as_ref())
    }

    /// No leads: the probe's kick goes through mesh-x's own members, active connections first
    /// (mesh-investigation.md step 1), and step 5 decides on what a mesh-x member reports of its
    /// node-admin, so no node of another mesh carries it.
    async fn leads(&self, _view: &Topology, _admin: &Node) -> Result<Vec<Node>, String> {
        Ok(Vec::new())
    }
}

#[async_trait::async_trait]
impl ProbeCalls for WatchCalls<'_> {
    async fn observe(&self, admin: &Node) -> (String, String) {
        let key = admin.endpoint_id.as_ref().and_then(|k| k.0.parse::<iroh::PublicKey>().ok());
        let seen = rdm_transport::iroh_obs::observe_remote(&self.0.topology_view.endpoint(), key).await;
        (seen.known_addrs, seen.active_addrs)
    }
}

/// The carriers of a probe of `mesh`'s node-admins, in the order asked (mesh-investigation.md steps
/// 1 and 3): the mesh's ordinary members, active connections first, then path order, the member
/// `avoid` (the one that carried the latest probe, when this is probe 2) last; the tickle takes the
/// first [`VIA_PEER_TICKLE_FANOUT`] of them, so probe 2 prefers members probe 1 did not carry through.
pub(crate) fn investigation_carriers(view: &Topology, mesh: &str, connected: impl Fn(&NodeId) -> bool, avoid: Option<&PathName>) -> Vec<Node> {
    let mut members: Vec<Node> = view.members().filter(|m| m.mesh == mesh && m.kind != NodeKind::NodeAdmin && m.status != NodeStatus::Leaving).cloned().collect();
    members.sort_by_key(|m| (!connected(&m.node_id), m.name.to_string()));
    members.sort_by_key(|m| avoid == Some(&m.name));
    members.into_iter().take(VIA_PEER_TICKLE_FANOUT).collect()
}

/// Probe `mesh`'s node-admins (mesh-investigation.md steps 1 and 3) through `calls`: the one tickle
/// ladder (node-tickle.md) over the exact mesh-primary the held primary record `record` names first,
/// then over each other node-admin (`admins_in_probe_order`): `ProbeNodeState` direct and, when that
/// fails, carried by up to two members of the mesh at once ([`investigation_carriers`]). The first
/// node-admin that answers ends the probe. `carriers` holds the carrier each mesh's latest probe went
/// through; probe 2 prefers other members.
pub(crate) async fn probe<C: ProbeCalls + ?Sized>(calls: &C, carriers: &Mutex<BTreeMap<String, PathName>>, view: &Topology, record: Option<rdm_core::role::Primary>, mesh: &str, _n: u8) -> Found {
    let primary_record = record.as_ref().map_or_else(|| "none".to_string(), ToString::to_string);
    let admins = admins_in_probe_order(view, mesh, record.as_ref());
    if admins.is_empty() {
        return Found { detail: format!("the view holds no node-admin of {mesh}"), primary_record, ..Found::default() };
    }
    // The carrier probe 2 prefers to avoid is the one that carried probe 1 (`calls.carriers` reads it
    // from `carriers`); the carrier this probe went through lands there once it ends, never while it runs.
    let mut carrier: Option<String> = None;
    let mut members_asked: Vec<String> = Vec::new();
    let (mut outcomes, mut asked_admins, mut details) = (Vec::new(), Vec::new(), Vec::new());
    let (mut iroh_known, mut iroh_active) = (Vec::new(), Vec::new());
    for admin in &admins {
        let ladder = tickle(admin, view, calls).await;
        // One read of iroh's local view of the node-admin, beside its probe; the outcome is the
        // Node RPC's, never this.
        let (known, active) = calls.observe(admin).await;
        iroh_known.push(format!("{}={known}", admin.name));
        iroh_active.push(format!("{}={active}", admin.name));
        match ladder {
            Err(reason) => {
                outcomes.push(ProbeOutcome::Unreachable);
                asked_admins.push(format!("{}=unreachable", admin.name));
                details.push(reason);
            }
            Ok(Tickle::Reached { answer, carrier: via }) => {
                outcomes.push(ProbeOutcome::AdminAlive);
                let said = match &answer {
                    StatusReply::Current { state, .. } => format!("{} answered: {state:?}", admin.name),
                    other => format!("{} answered: {}", admin.name, other.name()),
                };
                match via {
                    None => asked_admins.push(format!("{}=admin-alive(direct)", admin.name)),
                    Some(c) => {
                        asked_admins.push(format!("{}=admin-alive(via {c})", admin.name));
                        if !members_asked.contains(&c) {
                            members_asked.push(c.clone());
                        }
                        carrier = Some(c);
                    }
                }
                details.push(said);
                break;
            }
            Ok(Tickle::Unreached(unreached)) => {
                unreached.next("decide");
                let (outcome, via) = classify_unreached(&unreached);
                for p in &unreached.peers {
                    if !members_asked.contains(&p.carrier) {
                        members_asked.push(p.carrier.clone());
                    }
                }
                outcomes.push(outcome);
                let carried = unreached.peers.iter().map(|p| format!("via {}: {}", p.carrier, p.end.name())).collect::<Vec<_>>().join("; ");
                details.push(if carried.is_empty() { format!("direct: {}; no member to carry it", unreached.direct.name()) } else { format!("direct: {}; {carried}", unreached.direct.name()) });
                match via {
                    Some(c) => {
                        asked_admins.push(format!("{}={}(via {c})", admin.name, outcome.as_str()));
                        carrier = Some(c);
                    }
                    None => asked_admins.push(format!("{}={}", admin.name, outcome.as_str())),
                }
            }
        }
    }
    if let Some(c) = &carrier {
        if let Ok(c) = c.parse::<PathName>() {
            carriers.lock().unwrap().insert(mesh.to_string(), c);
        }
    }
    let carrier = carrier.unwrap_or_default();
    Found { outcome: fold_admin_outcomes(&outcomes), target: asked_admins.join(","), members: members_asked.join(","), carrier, detail: details.join("; "), iroh_known: iroh_known.join(";"), iroh_active: iroh_active.join(";"), primary_record }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ProbeOutcome::*;
    use std::time::Duration;

    /// The ladder at the production cadences: a 30 s staleness floor, 2 s backbone intervals.
    fn ladder() -> Ladder {
        Ladder::new(Rungs::at(Duration::from_secs(30), Duration::from_secs(2)))
    }

    /// `k` ticks of 2 s unheard.
    fn tick(k: u64) -> Duration {
        Duration::from_secs(2 * k)
    }

    fn ms(k: u64) -> u64 {
        tick(k).as_millis() as u64
    }

    /// Walk the ruled timeline: `probes` are what probe 1 and probe 2 find.
    fn walk(l: &mut Ladder, probes: [ProbeOutcome; 2]) -> Vec<(u64, Step)> {
        let mut log = Vec::new();
        for intervals in 0..=40 {
            let mut asked = l.step("mesh2", tick(intervals));
            // The driver asks again after a probe is recorded, as it does on the next interval.
            let mut guard = 0;
            while !asked.is_empty() {
                for s in asked.drain(..) {
                    if let Step::Probe { n, ticket, .. } = &s {
                        let found = probes[usize::from(*n) - 1];
                        if let Some(stop) = l.probed("mesh2", *ticket, *n, found, ms(intervals)) {
                            log.push((intervals, stop));
                        }
                    }
                    log.push((intervals, s));
                }
                guard += 1;
                if guard > 1 {
                    break;
                }
            }
        }
        log
    }

    fn at(log: &[(u64, Step)], f: impl Fn(&Step) -> bool) -> Vec<u64> {
        log.iter().filter(|(_, s)| f(s)).map(|(r, _)| *r).collect()
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:5
    /// "Mesh investigation is the fabric-primary's probe of an unheard peer mesh and its decision
    /// whether to run node.create of a node-admin into it."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:17
    /// "One investigation per peer mesh, owned by the fabric-primary. Let t be the elapsed monotonic time
    /// since the fabric-primary's last valid direct backbone receipt from mesh-x."
    /// CONTRACT (#2803 detection): two connection-lost probes decide a recovery at 30 intervals, with
    /// the probes at 10 and 20 and the silent mark between them at 15; nothing before.
    #[test]
    fn a_peer_mesh_known_from_topology_alone_is_watched_from_the_fabric_primary() {
        use crate::model::*;
        let node = |n: &str| Node::allocated(n.parse().unwrap());
        let t = Topology {
            fabric: Fabric { id: rdm_core::FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![node("mesh1.admin.1"), node("mesh2.admin.1"), node("mesh2.rpc.1"), node("mesh3.admin.1")],
        };
        let heard: BTreeSet<String> = ["mesh3".to_string()].into();
        assert_eq!(never_heard(&t, "mesh1", &heard), BTreeSet::from(["mesh2".to_string()]), "mesh2 only: mesh1 is this admin's own, mesh3 was heard on the backbone");
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:5
    /// "Mesh investigation is the fabric-primary's probe of an unheard peer mesh and its decision
    /// whether to run node.create of a node-admin into it."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:17
    /// "One investigation per peer mesh, owned by the fabric-primary. Let t be the elapsed monotonic time
    /// since the fabric-primary's last valid direct backbone receipt from mesh-x."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:43
    /// "5. At 60 s the decision step opens; reaching 60 s proves nothing by itself. Probes still outstanding
    /// finish first; no silence-only path marks the mesh deleted."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:46
    /// "- **Members live.** The latest probe answers `connection-lost`: a mesh-x member reports that
    /// it cannot reach its own node-admin. Record the verdict before opening the node.create attempt."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:48
    /// "- **No member answers.** Every node of mesh-x, its node-admins included, is unheard while the
    /// fabric is alive, so connection-lost cannot arise."
    /// CONTRACT (R-D1: loss of peer-mesh control-plane visibility targets lifecycle mutation for every
    /// node in that mesh; mesh-investigation.md: "Let t be the elapsed monotonic time since the
    /// fabric-primary's last valid direct backbone receipt from mesh-x"; step 5: "No answer from any
    /// member in either probe means hold"): the target reads t, not whether the ladder has stepped the
    /// mesh yet. An admin that took the fabric-primary role a moment ago holds a ladder that has
    /// stepped nothing, and a peer mesh its view last heard on the backbone 7 s ago is targeted from that
    /// moment, unheard 7000 ms, no recovery decided; a mesh heard within the tracking rung is not
    /// targeted; a decided recovery is read from the ladder; a mesh the backbone never carried here is
    /// what the ladder holds of it. Must NOT happen: a node of a mesh unheard past the rung claimed or
    /// dispatched for because the ladder has not stepped it yet.
    #[test]
    fn the_investigation_reads_t_from_the_last_direct_backbone_receipt_before_the_ladder_steps_the_mesh() {
        let mut l = Ladder::new(Rungs::at(Duration::from_secs(3), Duration::from_millis(500)));
        assert_eq!(
            l.peer_mesh_unheard("mesh1", Some(Duration::from_secs(7))),
            Some(PeerMesh { unheard_ms: Some(7000), recovery_decided: false, unreachable_decided: false }),
            "a role taken a moment ago: the ladder stepped nothing, t is 7 s, the mesh is targeted"
        );
        assert_eq!(l.peer_mesh_unheard("mesh1", Some(Duration::from_millis(200))), None, "heard within the tracking rung: no peer_mesh_unheard");
        // The ladder steps the mesh: probe 1, probe 2, both connection-lost, then the decision.
        for _ in 0..3 {
            for s in l.step("mesh1", Duration::from_secs(7)) {
                if let Step::Probe { n, ticket, unheard_ms } = s {
                    l.probed("mesh1", ticket, n, ConnectionLost, unheard_ms);
                }
            }
        }
        assert!(l.recovery_decided("mesh1"), "two connection-lost probes decide the recovery");
        assert_eq!(l.peer_mesh_unheard("mesh1", Some(Duration::from_secs(8))), Some(PeerMesh { unheard_ms: Some(8000), recovery_decided: true, unreachable_decided: false }), "the decided recovery is the ladder's");
        assert_eq!(l.peer_mesh_unheard("mesh9", None), None, "a mesh never carried on the backbone and never stepped: the ladder holds nothing");
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:27
    /// "1. At 20 s, probe 1 contacts mesh-x's current exact mesh-primary first, then mesh-x's other
    /// node-admins, with [ProbeNodeState](../node-rpc-envelope.md#status-op-0x1b-rdm)."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:34
    /// "2. At 30 s, mark mesh-x silent using the shared staleness mark. Send nothing at this mark."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:36
    /// "3. At 40 s, probe 2 repeats probe 1 (exact mesh-primary first, then the other node-admins,
    /// each direct then via a peer), preferring a different carrier member."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:43
    /// "5. At 60 s the decision step opens; reaching 60 s proves nothing by itself. Probes still outstanding
    /// finish first; no silence-only path marks the mesh deleted."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:46
    /// "- **Members live.** The latest probe answers `connection-lost`: a mesh-x member reports that
    /// it cannot reach its own node-admin. Record the verdict before opening the node.create attempt."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:86
    /// "| unheard interval | Fabric-primary | direct backbone receipt absent | track mesh and last-heard time | `rdm.node_admin.mesh.update.via-unheard` (`mesh`, `at`) |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:87
    /// "| 20 s | Fabric-primary → mesh-x's exact mesh-primary, then its other node-admins | ProbeNodeState direct; else Ping a member and, on pong, Forward carrying ProbeNodeState | probe 1: admin answered (ends the investigation), connection-lost or unreachable | `rdm.node_admin.mesh.update.via-probe` (`mesh`, `probe=1`, `member`, `carrier`, `outcome`); RPC call/serve chain below |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:88
    /// "| 30 s | Fabric-primary | no frame | shared mesh silent mark only | `rdm.node_admin.mesh.update.via-unheard` (`mesh`, `at`) |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:89
    /// "| 40 s | Fabric-primary → mesh-x's exact mesh-primary, then its other node-admins | as probe 1 | probe 2, prefer a different carrier member | `rdm.node_admin.mesh.update.via-probe` (`mesh`, `probe=2`, `member`, `carrier`, `outcome`); RPC call/serve chain below |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:91
    /// "| 60 s | Fabric-primary | the decision step opens; outstanding probes finish first | connection-lost: node.create of a node-admin; no member answers: node investigation of mesh-x's node-admins, then node.mesh.join or node.create in process on its verdict (step 5) | `rdm.node_admin.mesh.update.via-probe-verdict` (`mesh`, `outcome=connection-lost or unreachable`, `at`) |"
    #[test]
    fn two_connection_lost_probes_decide_a_recovery_at_thirty_intervals() {
        let log = walk(&mut ladder(), [ConnectionLost, ConnectionLost]);
        assert_eq!(at(&log, |s| matches!(s, Step::Track { .. })), vec![2]);
        assert_eq!(at(&log, |s| matches!(s, Step::Probe { n: 1, .. })), vec![10]);
        assert_eq!(at(&log, |s| matches!(s, Step::Mark { .. })), vec![15]);
        assert_eq!(at(&log, |s| matches!(s, Step::Probe { n: 2, .. })), vec![20]);
        assert_eq!(at(&log, |s| matches!(s, Step::Decide { recover: true, latest: ConnectionLost, .. })), vec![30]);
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:36
    /// "3. At 40 s, probe 2 repeats probe 1 (exact mesh-primary first, then the other node-admins,
    /// each direct then via a peer), preferring a different carrier member. Record the latest completed probe's outcome; an older
    /// `connection-lost` cannot override a newer unreachable outcome."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:43
    /// "5. At 60 s the decision step opens; reaching 60 s proves nothing by itself. Probes still outstanding
    /// finish first; no silence-only path marks the mesh deleted."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:46
    /// "- **Members live.** The latest probe answers `connection-lost`: a mesh-x member reports that
    /// it cannot reach its own node-admin. Record the verdict before opening the node.create attempt."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:48
    /// "- **No member answers.** Every node of mesh-x, its node-admins included, is unheard while the
    /// fabric is alive, so connection-lost cannot arise."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:91
    /// "| 60 s | Fabric-primary | the decision step opens; outstanding probes finish first | connection-lost: node.create of a node-admin; no member answers: node investigation of mesh-x's node-admins, then node.mesh.join or node.create in process on its verdict (step 5) | `rdm.node_admin.mesh.update.via-probe-verdict` (`mesh`, `outcome=connection-lost or unreachable`, `at`) |"
    /// CONTRACT: the latest completed probe decides. An older connection-lost never overrides a
    /// newer unreachable, and no answer at all holds.
    #[test]
    fn the_latest_probe_decides_and_unreachable_holds() {
        for probes in [[ConnectionLost, Unreachable], [Unreachable, Unreachable]] {
            let log = walk(&mut ladder(), probes);
            assert_eq!(at(&log, |s| matches!(s, Step::Decide { recover: false, latest: Unreachable, .. })), vec![30], "{probes:?}");
        }
        let log = walk(&mut ladder(), [Unreachable, ConnectionLost]);
        assert_eq!(at(&log, |s| matches!(s, Step::Decide { recover: true, .. })), vec![30]);
    }

    fn view_of(nodes: Vec<Node>) -> Topology {
        use crate::model::{Fabric, FabricId, ProviderKind, ScopeStatus};
        Topology { fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process }, meshes: vec![], nodes }
    }

    fn admin(name: &str, primary: bool) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.node_id = NodeId::mint();
        n.incarnation_id = Some(crate::model::IncarnationId::mint());
        n.status = NodeStatus::Ready;
        n.is_primary = primary;
        n
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:17
    /// "One investigation per peer mesh, owned by the fabric-primary. Let t be the elapsed monotonic time
    /// since the fabric-primary's last valid direct backbone receipt from mesh-x. Only that receipt starts
    /// or resets t; forwarded snapshots and forwarded topology never reset or extend it, and the backbone
    /// interval or a delayed tick never moves a threshold: the thresholds below are elapsed time, not
    /// counts of intervals."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:27
    /// "1. At 20 s, probe 1 contacts mesh-x's current exact mesh-primary first, then mesh-x's other
    /// node-admins, with [ProbeNodeState](../node-rpc-envelope.md#status-op-0x1b-rdm)."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:34
    /// "2. At 30 s, mark mesh-x silent using the shared staleness mark. Send nothing at this mark."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:36
    /// "3. At 40 s, probe 2 repeats probe 1 (exact mesh-primary first, then the other node-admins,
    /// each direct then via a peer), preferring a different carrier member."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:43
    /// "5. At 60 s the decision step opens; reaching 60 s proves nothing by itself."
    /// CONTRACT (R-CK1, mesh-investigation.md: "the thresholds below are elapsed time, not
    /// counts of intervals"; step 2 "the shared staleness mark"): the rungs are 20 s, 30 s, 40 s and 60 s of
    /// elapsed time unheard at the 30 s staleness floor, the silent mark being the staleness floor
    /// itself, and a changed backbone interval moves none of them. At the gate's 3 s floor they are
    /// 2 s, 3 s, 4 s and 6 s.
    #[test]
    fn the_rungs_are_elapsed_time_from_the_staleness_floor_never_backbone_intervals() {
        let (twenty, thirty, forty, sixty) = (Duration::from_secs(20), Duration::from_secs(30), Duration::from_secs(40), Duration::from_secs(60));
        for backbone in [Duration::from_millis(500), Duration::from_secs(2), Duration::from_secs(5)] {
            let r = Rungs::at(thirty, backbone);
            assert_eq!((r.probe1, r.mark, r.probe2, r.decide), (twenty, thirty, forty, sixty), "backbone interval {backbone:?}");
            assert!(r.track <= r.probe1, "tracking begins no later than probe 1: {r:?}");
        }
        let gate = Rungs::at(Duration::from_secs(3), Duration::from_millis(500));
        assert_eq!((gate.probe1, gate.mark, gate.probe2, gate.decide), (Duration::from_secs(2), Duration::from_secs(3), Duration::from_secs(4), Duration::from_secs(6)));
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:5
    /// "Remote liveness comes only from direct backbone receipt and a node's own call and connection facts,
    /// never forwarded topology"
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:17
    /// "One investigation per peer mesh, owned by the fabric-primary. Let t be the elapsed monotonic time
    /// since the fabric-primary's last valid direct backbone receipt from mesh-x. Only that receipt starts
    /// or resets t; forwarded snapshots and forwarded topology never reset or extend it, and the backbone
    /// interval or a delayed tick never moves a threshold: the thresholds below are elapsed time, not
    /// counts of intervals."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:27
    /// "1. At 20 s, probe 1 contacts mesh-x's current exact mesh-primary first, then mesh-x's other
    /// node-admins, with [ProbeNodeState](../node-rpc-envelope.md#status-op-0x1b-rdm)."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:34
    /// "2. At 30 s, mark mesh-x silent using the shared staleness mark. Send nothing at this mark."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:36
    /// "3. At 40 s, probe 2 repeats probe 1 (exact mesh-primary first, then the other node-admins,
    /// each direct then via a peer), preferring a different carrier member."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:39
    /// "Direct backbone receipt from mesh-x at any point cancels the investigation and resets the
    /// unheard interval."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:43
    /// "5. At 60 s the decision step opens; reaching 60 s proves nothing by itself. Probes still outstanding
    /// finish first; no silence-only path marks the mesh deleted."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:48
    /// "- **No member answers.** Every node of mesh-x, its node-admins included, is unheard while the
    /// fabric is alive, so connection-lost cannot arise."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:90
    /// "| any time | Fabric-primary / answering admin | admin answer or direct backbone receipt | stop / cancel | `rdm.node_admin.mesh.update.via-probe-verdict` (`mesh`, `outcome`, `at`) |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:166
    /// "No status or lifecycle gossip is sent at the 20 s/40 s probes or the 30 s silent mark."
    /// CONTRACT (R-CK1): one investigation chain for a silent peer mesh. Only a direct backbone
    /// receipt starts or resets t: forwarded topology of the mesh arriving every interval neither resets
    /// nor extends it, so the ladder walks probe 1, the mark, probe 2 and the decision at the same
    /// elapsed times whether it is stepped every 0.5 s or every 2 s; a fresh backbone receipt cancels
    /// it. With no probe answered the decision is a hold: nothing marks the mesh dead on silence.
    #[test]
    fn one_investigation_runs_on_elapsed_time_from_the_last_direct_backbone_receipt() {
        use rdm_core::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshNode};
        let digest = |name: &str| MeshDigest { held_hash: Default::default(),
            fabric_id: FabricId::parse("fab000000001").unwrap(),
            node: MeshNode { node_id: NodeId::mint(), name: name.parse().unwrap(), endpoint_id: EndpointId("key".into()), transport_addr: "127.0.0.1:41000".parse().unwrap(), incarnation: IncarnationId::mint(), supersedes: None, runtime: None },
            status: MemberStatus::Ready,
            node_state: Default::default(),
            admin_api_base: None,
            emitted_at_rafka_ms: 1,
            digest_seq: 1,
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            data_dir: None, is_primary: None, is_fabric_primary: None
        };
        let floor = Duration::from_secs(30);
        for every in [Duration::from_millis(500), Duration::from_secs(2)] {
            let digest_view = rdm_transport::topology::DigestView::default();
            let (admin, rpc) = (digest("mesh2.admin.1"), digest("mesh2.rpc.1"));
            let t0 = Instant::now();
            assert!(digest_view.record_forwarded_at(admin.clone(), t0), "the direct backbone receipt");
            let mut l = Ladder::new(Rungs::at(floor, every));
            let mut log: Vec<(Duration, Step)> = Vec::new();
            let mut k = 1u32;
            while every * k <= Duration::from_secs(70) {
                let now = t0 + every * k;
                // The peer mesh's topology forwarded onto this node's mesh channel, every interval.
                let mut fwd = rpc.clone();
                fwd.digest_seq = u64::from(k) + 1;
                digest_view.record_topology_at(fwd, now);
                let unheard = digest_view.mesh_unheard("mesh2", now).expect("the mesh was heard on the backbone");
                assert_eq!(unheard, every * k, "forwarded topology neither resets nor extends t");
                for s in l.step("mesh2", unheard) {
                    if let Step::Probe { n, unheard_ms, ticket } = s {
                        l.probed("mesh2", ticket, n, Unreachable, unheard_ms);
                    }
                    log.push((unheard, s));
                }
                k += 1;
            }
            let first = |f: &dyn Fn(&Step) -> bool| log.iter().find(|(_, s)| f(s)).map(|(t, _)| *t);
            assert_eq!(first(&|s| matches!(s, Step::Probe { n: 1, .. })), Some(Duration::from_secs(20)), "every {every:?}: {log:?}");
            assert_eq!(first(&|s| matches!(s, Step::Mark { .. })), Some(Duration::from_secs(30)), "every {every:?}");
            assert_eq!(first(&|s| matches!(s, Step::Probe { n: 2, .. })), Some(Duration::from_secs(40)), "every {every:?}");
            assert_eq!(first(&|s| matches!(s, Step::Decide { .. })), Some(Duration::from_secs(60)), "every {every:?}");
            assert!(log.iter().all(|(_, s)| !matches!(s, Step::Decide { recover: true, .. })), "silence and unreachable probes decide no recovery: {log:?}");
            assert!(!l.recovery_decided("mesh2"));
            // A direct backbone receipt resets t and cancels the investigation.
            let later = t0 + every * k;
            assert!(digest_view.record_forwarded_at(admin.clone(), later));
            let reset = digest_view.mesh_unheard("mesh2", later).unwrap();
            assert_eq!(reset, Duration::ZERO, "a direct backbone receipt resets t");
            assert!(matches!(l.step("mesh2", reset).as_slice(), [Step::Cancel { .. }]), "a receipt cancels the investigation");
        }
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:27
    /// "1. At 20 s, probe 1 contacts mesh-x's current exact mesh-primary first, then mesh-x's other
    /// node-admins, with [ProbeNodeState](../node-rpc-envelope.md#status-op-0x1b-rdm)."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:36
    /// "3. At 40 s, probe 2 repeats probe 1 (exact mesh-primary first, then the other node-admins,
    /// each direct then via a peer), preferring a different carrier member."
    /// CONTRACT (mesh-investigation.md step 1: "probe 1 contacts mesh-x's current exact
    /// mesh-primary first, then mesh-x's other node-admins"): a probe carries `ProbeNodeState` first
    /// to the exact birth the mesh-primary record this admin holds names, then to each other node-admin in path order. The
    /// record leads even where the view marks another admin primary or another path sorts first, and
    /// it is the record's birth that is asked when the view holds an older one of that node. With no
    /// record held, the node-admins are asked in path order. Must NOT happen: the view's `is_primary`
    /// choosing the first target, a probe that asks only one node-admin, or another mesh's admin.
    #[test]
    fn a_probe_asks_the_recorded_mesh_primary_first_then_the_other_admins() {
        use rdm_core::role::Primary;
        let v = view_of(vec![admin("mesh2.admin.1", false), admin("mesh2.admin.3", false), admin("mesh2.admin.2", true), admin("mesh1.admin.1", true)]);
        let a3 = v.nodes.iter().find(|n| n.name.to_string() == "mesh2.admin.3").unwrap().clone();
        let names = |order: Vec<Node>| order.iter().map(|a| a.name.to_string()).collect::<Vec<_>>();
        let record = Primary { mesh: "mesh2".into(), node_id: a3.node_id.clone(), incarnation: a3.incarnation_id.clone().unwrap(), revision: 4 };
        assert_eq!(names(admins_in_probe_order(&v, "mesh2", Some(&record))), vec!["mesh2.admin.3", "mesh2.admin.1", "mesh2.admin.2"], "the primary record's primary first, then the others in path order, and nothing of another mesh; never the view's is_primary");
        assert_eq!(names(admins_in_probe_order(&v, "mesh2", None)), vec!["mesh2.admin.1", "mesh2.admin.2", "mesh2.admin.3"], "no record held: path order, the view's is_primary choosing nothing");
        let newer = Primary { incarnation: crate::model::IncarnationId::mint(), revision: 5, ..record };
        let order = admins_in_probe_order(&v, "mesh2", Some(&newer));
        assert_eq!((order[0].name.to_string(), order[0].incarnation_id.clone()), ("mesh2.admin.3".to_string(), Some(newer.incarnation.clone())), "the record's exact birth is asked, not the older birth the view holds");
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:34
    /// "2. At 30 s, mark mesh-x silent using the shared staleness mark. Send nothing at this mark."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:43
    /// "5. At 60 s the decision step opens; reaching 60 s proves nothing by itself. Probes still outstanding
    /// finish first; no silence-only path marks the mesh deleted."
    /// CONTRACT (R-CK1: "a delayed tick never moves a threshold"; mesh-investigation.md step 5:
    /// "Probes still outstanding finish first"): a probe in flight never holds the ladder. At the
    /// gate's rungs (2 s, 3 s, 4 s, 6 s at its 3 s floor) with every probe running 5 s, the silent
    /// mark is stepped at 3 s while probe 1 is still running. One probe of a mesh is in flight at a
    /// time ("the latest completed probe" is then always the later one), so probe 2 is made once
    /// probe 1 ended, its rung having passed meanwhile, and the decision waits for probe 2 to end.
    /// Must NOT happen: the mark stepped only when probe 1 ends (7 s), a probe asked again while it
    /// runs, or a decision taken with a probe outstanding.
    #[tokio::test(start_paused = true)]
    async fn a_running_probe_never_holds_the_ladder() {
        let inv = Investigations::new("mesh1.admin.1".into(), Arc::new(Mutex::new(Ladder::new(Rungs::at(Duration::from_secs(3), Duration::from_millis(500))))), crate::stop_gate::StopGate::new());
        let ended = Arc::new(Mutex::new(Vec::<(u8, u64)>::new()));
        let t0 = tokio::time::Instant::now();
        let mut log: Vec<Step> = Vec::new();
        while t0.elapsed() <= Duration::from_secs(15) {
            let ended = ended.clone();
            let start = move |n: u8| {
                let ended = ended.clone();
                async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    ended.lock().unwrap().push((n, t0.elapsed().as_millis() as u64));
                    Found { outcome: ConnectionLost, ..Found::default() }
                }
            };
            log.extend(inv.tick("mesh2", t0.elapsed(), start).await);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let ended = ended.lock().unwrap().clone();
        let marks: Vec<u64> = log.iter().filter_map(|s| if let Step::Mark { unheard_ms } = s { Some(*unheard_ms) } else { None }).collect();
        assert!(marks.len() == 1 && (3000..3100).contains(&marks[0]), "the mark is stepped at its 3 s rung while probe 1 runs, not when it ends: {marks:?} (probes ended {ended:?})");
        let asked = |k: u8| log.iter().filter_map(|s| if let Step::Probe { n, unheard_ms, .. } = s { (*n == k).then_some(*unheard_ms) } else { None }).collect::<Vec<_>>();
        assert_eq!(asked(1), vec![2000], "probe 1 is asked once, at its rung: {log:?}");
        let end = |k: u8| ended.iter().find(|(n, _)| *n == k).map(|(_, at)| *at).unwrap_or_else(|| panic!("probe {k} ended: {ended:?}"));
        assert!(asked(2).len() == 1 && asked(2)[0] >= end(1), "probe 2 is asked once, after probe 1 ended at {} ms: {log:?}", end(1));
        let decided: Vec<u64> = log.iter().filter_map(|s| if let Step::Decide { recover: true, unheard_ms, .. } = s { Some(*unheard_ms) } else { None }).collect();
        assert!(decided.len() == 1 && decided[0] >= end(2), "the decision is taken once, after probe 2 ended at {} ms: {decided:?}", end(2));
    }

    /// CONTRACT (node-stop: a stopped admin's tasks are held, and it says nothing once cut;
    /// stop_gate.rs: a task that takes no lock a handler needs is held where it stands while the gate
    /// is stopped): a probe the investigation started before its admin stopped makes no step while the
    /// admin is stopped, and finishes, from where it was, once the gate opens. Must NOT happen: a
    /// stopped admin's probe going on to its next call (a ping, a status kick through a peer) after
    /// the stop cut its connections.
    #[tokio::test(start_paused = true)]
    async fn a_probe_in_flight_when_the_admin_stops_makes_no_step_until_the_gate_opens() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let gate = crate::stop_gate::StopGate::new();
        let inv = Investigations::new("mesh1.admin.1".into(), Arc::new(Mutex::new(Ladder::new(Rungs::at(Duration::from_secs(3), Duration::from_millis(500))))), gate.clone());
        let calls = Arc::new(AtomicU32::new(0));
        let start = |_n: u8| {
            let calls = calls.clone();
            async move {
                // Its first call, then its next one a second later.
                calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(1)).await;
                calls.fetch_add(1, Ordering::SeqCst);
                Found { outcome: ConnectionLost, ..Found::default() }
            }
        };
        let mut asked = false;
        for unheard_ms in (0..=2_000u64).step_by(100) {
            asked |= inv.tick("mesh2", Duration::from_millis(unheard_ms), start).await.iter().any(|s| matches!(s, Step::Probe { n: 1, .. }));
        }
        assert!(asked, "probe 1 was asked at its 2 s rung");
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1, "the probe made its first call");
        gate.stop();
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1, "the admin is stopped: its probe made no step");
        gate.resume();
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2, "the gate opened: the probe finished from where it was");
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:39
    /// "4. If the node-admin answers either probe (Current or Blocked, under the
    /// [Status contract](../node-rpc-envelope.md#status-op-0x1b-rdm)), stop the investigation.
    /// Direct backbone receipt from mesh-x at any point cancels the investigation and resets the
    /// unheard interval."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:90
    /// "| any time | Fabric-primary / answering admin | admin answer or direct backbone receipt | stop / cancel | `rdm.node_admin.mesh.update.via-probe-verdict` (`mesh`, `outcome`, `at`) |"
    /// CONTRACT (mesh-investigation.md step 4: a direct backbone receipt "cancels the investigation
    /// and resets the unheard interval"): a probe still running when its investigation is cancelled
    /// lands on nothing. The fresh interval makes its own probe 1, the cancelled interval's
    /// connection-lost never stands in for it, and the cancel counts the probe that was in flight.
    /// Must NOT happen: the fresh interval skipping probe 1 because the stale probe landed in it.
    #[test]
    fn a_probe_that_lands_after_its_investigation_was_cancelled_records_nothing() {
        let mut l = ladder();
        let stale = l.step("mesh2", tick(10)).into_iter().find_map(|s| if let Step::Probe { n: 1, ticket, .. } = s { Some(ticket) } else { None }).expect("probe 1 at 20 s");
        let cancel = l.step("mesh2", tick(0));
        // The fresh interval is tracked when the stale probe ends.
        let mut again: Vec<Step> = (1..=3).flat_map(|r| l.step("mesh2", tick(r))).collect();
        assert!(again.iter().any(|s| matches!(s, Step::Track { .. })), "the fresh interval is tracked: {again:?}");
        assert_eq!(l.probed("mesh2", stale, 1, ConnectionLost, ms(12)), None);
        again.extend((4..=10).flat_map(|r| l.step("mesh2", tick(r))));
        assert!(again.iter().any(|s| matches!(s, Step::Probe { n: 1, .. })), "the fresh interval makes its own probe 1: {again:?}");
        assert_eq!(cancel, vec![Step::Cancel { unheard_ms: 0, probes: 1 }], "a receipt cancels the investigation with its probe in flight");
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:39
    /// "4. If the node-admin answers either probe (Current or Blocked, under the
    /// [Status contract](../node-rpc-envelope.md#status-op-0x1b-rdm)), stop the investigation."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:43
    /// "5. At 60 s the decision step opens; reaching 60 s proves nothing by itself. Probes still outstanding
    /// finish first; no silence-only path marks the mesh deleted."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:46
    /// "- **Members live.** The latest probe answers `connection-lost`: a mesh-x member reports that
    /// it cannot reach its own node-admin. Record the verdict before opening the node.create attempt."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:48
    /// "- **No member answers.** Every node of mesh-x, its node-admins included, is unheard while the
    /// fabric is alive, so connection-lost cannot arise."
    /// CONTRACT (mesh-investigation.md steps 4-5): over the node-admins one probe asked, any answer is
    /// `admin-alive`; `connection-lost` only when every one of them was reported lost from inside
    /// the mesh; anything else, a timeout or an unreachable carrier included, is `unreachable`.
    #[test]
    fn a_probe_over_several_admins_is_connection_lost_only_when_every_admin_is() {
        assert_eq!(fold_admin_outcomes(&[ConnectionLost, AdminAlive]), AdminAlive);
        assert_eq!(fold_admin_outcomes(&[ConnectionLost, ConnectionLost]), ConnectionLost);
        assert_eq!(fold_admin_outcomes(&[ConnectionLost, Unreachable]), Unreachable);
        assert_eq!(fold_admin_outcomes(&[]), Unreachable);
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:27
    /// "A successful contact ends the failed-reachability investigation."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:39
    /// "4. If the node-admin answers either probe (Current or Blocked, under the
    /// [Status contract](../node-rpc-envelope.md#status-op-0x1b-rdm)), stop the investigation."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:90
    /// "| any time | Fabric-primary / answering admin | admin answer or direct backbone receipt | stop / cancel | `rdm.node_admin.mesh.update.via-probe-verdict` (`mesh`, `outcome`, `at`) |"
    /// CONTRACT: the node-admin answering either probe stops the investigation: no second probe
    /// after probe 1, no decision.
    #[test]
    fn an_answering_admin_stops_the_investigation() {
        let log = walk(&mut ladder(), [AdminAlive, ConnectionLost]);
        assert_eq!(at(&log, |s| matches!(s, Step::Stop { .. })), vec![10]);
        assert!(at(&log, |s| matches!(s, Step::Probe { n: 2, .. } | Step::Decide { .. })).is_empty(), "{log:?}");
        let log = walk(&mut ladder(), [ConnectionLost, AdminAlive]);
        assert_eq!(at(&log, |s| matches!(s, Step::Stop { .. })), vec![20]);
        assert!(at(&log, |s| matches!(s, Step::Decide { .. })).is_empty(), "{log:?}");
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:39
    /// "Direct backbone receipt from mesh-x at any point cancels the investigation and resets the
    /// unheard interval."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:86
    /// "| unheard interval | Fabric-primary | direct backbone receipt absent | track mesh and last-heard time | `rdm.node_admin.mesh.update.via-unheard` (`mesh`, `at`) |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:90
    /// "| any time | Fabric-primary / answering admin | admin answer or direct backbone receipt | stop / cancel | `rdm.node_admin.mesh.update.via-probe-verdict` (`mesh`, `outcome`, `at`) |"
    /// CONTRACT: a backbone receipt cancels the investigation at any point, resets the interval, and
    /// the mesh is then tracked and probed afresh; a mesh heard before it was ever probed cancels
    /// silently.
    #[test]
    fn a_backbone_receipt_cancels_and_resets_the_interval() {
        let mut l = ladder();
        assert!(l.step("mesh2", tick(3)).iter().any(|s| matches!(s, Step::Track { .. })));
        assert!(l.step("mesh2", tick(0)).is_empty(), "heard before any probe: nothing to cancel loudly");
        let ticket = (0..=12).flat_map(|r| l.step("mesh2", tick(r))).find_map(|s| if let Step::Probe { n: 1, ticket, .. } = s { Some(ticket) } else { None }).expect("probe 1 at 10");
        l.probed("mesh2", ticket, 1, ConnectionLost, ms(12));
        assert_eq!(l.step("mesh2", tick(0)), vec![Step::Cancel { unheard_ms: 0, probes: 1 }]);
        assert!(l.step("mesh2", tick(1)).is_empty());
        let again: Vec<Step> = (2..=10).flat_map(|r| l.step("mesh2", tick(r))).collect();
        assert!(again.iter().any(|s| matches!(s, Step::Probe { n: 1, unheard_ms, .. } if *unheard_ms == ms(10))), "a fresh interval probes again at 10: {again:?}");
    }

    /// CONTRACT: the recovery of a peer mesh's node-admins is decided only by a `recovery` decision: not
    /// by a hold, not by a mesh the ladder has not stepped yet, and not once the ladder forgot it.
    #[test]
    fn a_peer_meshs_recovery_is_decided_only_by_a_recovery_decision() {
        let mut l = ladder();
        assert!(!l.recovery_decided("mesh2"), "not stepped yet");
        walk(&mut l, [ConnectionLost, ConnectionLost]);
        assert!(l.recovery_decided("mesh2"));
        let mut held = ladder();
        assert!(!held.unreachable_decided("mesh2"), "not stepped yet: nothing is decided either way");
        walk(&mut held, [Unreachable, Unreachable]);
        assert!(!held.recovery_decided("mesh2"), "a hold decides no recovery");
        l.forget("mesh2");
        assert!(!l.recovery_decided("mesh2"));
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:43
    /// "5. At 60 s the decision step opens; reaching 60 s proves nothing by itself. Probes still outstanding
    /// finish first; no silence-only path marks the mesh deleted."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:46
    /// "- **Members live.** The latest probe answers `connection-lost`: a mesh-x member reports that
    /// it cannot reach its own node-admin. Record the verdict before opening the node.create attempt."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:48
    /// "- **No member answers.** Every node of mesh-x, its node-admins included, is unheard while the
    /// fabric is alive, so connection-lost cannot arise."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:91
    /// "| 60 s | Fabric-primary | the decision step opens; outstanding probes finish first | connection-lost: node.create of a node-admin; no member answers: node investigation of mesh-x's node-admins, then node.mesh.join or node.create in process on its verdict (step 5) | `rdm.node_admin.mesh.update.via-probe-verdict` (`mesh`, `outcome=connection-lost or unreachable`, `at`) |"
    /// CONTRACT (mesh-investigation.md step 5): the 60 s decision on `unreachable` evidence is a hold the
    /// ladder exposes as its own fact, `unreachable_decided`, beside `recovery_decided` and never
    /// with it: the fabric-primary reads the provider's exited verdict of the mesh's node-admins only for
    /// a mesh whose decision opened this way. A connection-lost decision is the recovery, not the hold.
    /// A mesh heard again is decided neither way. Must NOT happen: the hold read as a recovery, or the
    /// fact surviving the silence.
    #[test]
    fn an_unreachable_decision_is_exposed_as_a_hold_and_never_as_connection_lost() {
        let mut l = ladder();
        walk(&mut l, [Unreachable, Unreachable]);
        assert!(l.unreachable_decided("mesh2") && !l.recovery_decided("mesh2"));
        let targeted = l.peer_mesh_unheard("mesh2", Some(Duration::from_secs(61))).expect("an unheard mesh is targeted");
        assert_eq!((targeted.recovery_decided, targeted.unreachable_decided), (false, true), "the target reads the hold: {targeted:?}");
        assert_eq!(l.peer_mesh("mesh2").map(|p| (p.recovery_decided, p.unreachable_decided)), Some((false, true)));
        let mut recovered = ladder();
        walk(&mut recovered, [ConnectionLost, ConnectionLost]);
        assert!(recovered.recovery_decided("mesh2") && !recovered.unreachable_decided("mesh2"), "a connection-lost decision is the recovery");
        assert_eq!(recovered.peer_mesh("mesh2").map(|p| (p.recovery_decided, p.unreachable_decided)), Some((true, false)));
        // A direct backbone receipt resets t: the mesh is decided neither way.
        assert!(matches!(l.step("mesh2", Duration::ZERO).as_slice(), [Step::Cancel { .. }]));
        assert!(!l.unreachable_decided("mesh2") && !l.recovery_decided("mesh2"), "heard again: the hold is over");
    }

    /// CONTRACT (#2803 detection): a fabric primary that loses its role drops every investigation;
    /// the next one starts from its own view, so nothing it decided is decided again, and a mesh it
    /// never heard on the backbone is not held back at all.
    #[test]
    fn a_ladder_dropped_with_the_fabric_primary_starts_over_for_the_next_fabric_primary() {
        let mut first = ladder();
        walk(&mut first, [ConnectionLost, ConnectionLost]);
        assert!(first.recovery_decided("mesh2"));
        first.clear();
        assert!(first.meshes().is_empty(), "nothing survives the role");
        let mut next = ladder();
        assert!(!next.recovery_decided("mesh2"), "the next primary has decided nothing yet");
        let log = walk(&mut next, [ConnectionLost, ConnectionLost]);
        assert_eq!(at(&log, |s| matches!(s, Step::Decide { recover: true, .. })), vec![30], "it decides once, from its own 30 intervals");
    }

    /// CONTRACT: two peer meshes unheard at once are two ladders; one mesh's probes, hold or
    /// decision never move the other's.
    #[test]
    fn two_unheard_meshes_run_independent_ladders() {
        let mut l = ladder();
        for r in 0..=40 {
            let a = l.step("mesh2", tick(r));
            for s in a {
                if let Step::Probe { n, unheard_ms, ticket } = s {
                    l.probed("mesh2", ticket, n, ConnectionLost, unheard_ms);
                }
            }
            // mesh3 is heard throughout.
            assert!(l.step("mesh3", tick(0)).is_empty());
        }
        assert!(l.recovery_decided("mesh2"));
        assert!(!l.recovery_decided("mesh3"), "mesh3 was never decided");
    }

    /// CONTRACT (#2803 detection): forgetting the investigation of a mesh the backbone no longer
    /// carries (a deleted mesh) returns; it never holds the ladder's lock while it takes it again,
    /// which would hold the fabric primary's watch and its drift check (both read the ladder) forever.
    #[test]
    fn forgetting_a_deleted_mesh_does_not_deadlock_on_the_ladder_lock() {
        let ladder = Arc::new(Mutex::new(ladder()));
        ladder.lock().unwrap().step("mesh2", Duration::ZERO);
        let (tx, rx) = std::sync::mpsc::channel();
        let l = ladder.clone();
        std::thread::spawn(move || {
            forget_unheard_meshes(&l, &BTreeSet::new());
            let _ = tx.send(());
        });
        assert!(rx.recv_timeout(std::time::Duration::from_secs(5)).is_ok(), "forgetting a deleted mesh never returned: the ladder lock is held by the loop that takes it");
        assert!(ladder.lock().unwrap().meshes().is_empty());
    }

    /// A probe's calls, recorded in the order made and answered as the cell says: a node-admin in
    /// `answers_direct` answers its direct `ProbeNodeState`, a node-admin in `answers_carried` answers
    /// the carried `ProbeNodeState` through the carrier `answering_carrier`; with `members_carry`,
    /// every other carried call is the carrier's `connection-lost`, without it no member can be
    /// reached at all; `connected` are the members this node holds an Active Direct connection to.
    struct Recorder {
        made: Mutex<Vec<String>>,
        answers_direct: BTreeSet<String>,
        answers_carried: BTreeSet<String>,
        answering_carrier: &'static str,
        members_carry: bool,
        connected: BTreeSet<NodeId>,
        carriers: Mutex<BTreeMap<String, PathName>>,
    }

    impl Default for Recorder {
        fn default() -> Self {
            Self { made: Mutex::default(), answers_direct: BTreeSet::new(), answers_carried: BTreeSet::new(), answering_carrier: "mesh2.rpc.1", members_carry: true, connected: BTreeSet::new(), carriers: Mutex::default() }
        }
    }

    impl Recorder {
        fn made(&self) -> Vec<String> {
            self.made.lock().unwrap().clone()
        }

        fn note(&self, call: String, admin: Option<(&Node, &StatusRequest)>) {
            // A ProbeNodeState names the node-admin's exact birth; anything else is recorded as such.
            let exact = admin.is_none_or(|(a, req)| matches!(req, StatusRequest::ProbeNodeState { node_id, incarnation } if *node_id == a.node_id && Some(incarnation) == a.incarnation_id.as_ref()));
            self.made.lock().unwrap().push(if exact { call } else { format!("{call} (not its exact birth)") });
        }

        fn current(admin: &Node) -> ProbeEnd {
            ProbeEnd::Reply(StatusReply::Current { node_id: admin.node_id.clone(), incarnation: admin.incarnation_id.clone().unwrap(), state: rdm_core::status::NodeState::Ready })
        }
    }

    #[async_trait::async_trait]
    impl TickleCalls for Recorder {
        fn caller(&self) -> String {
            "mesh1.admin.1".into()
        }

        fn deadline(&self) -> Duration {
            direct_budget(Duration::from_secs(2))
        }

        async fn direct(&self, admin: &Node, req: &StatusRequest) -> ProbeEnd {
            self.note(format!("direct {}", admin.name), Some((admin, req)));
            if self.answers_direct.contains(&admin.name.to_string()) {
                Self::current(admin)
            } else {
                ProbeEnd::Other { end: "NotSent", detail: "not sent: dial deadline".into() }
            }
        }

        async fn via_peer(&self, carrier: &Node, admin: &Node, req: &StatusRequest) -> ProbeEnd {
            self.note(format!("via {} > {}", carrier.name, admin.name), Some((admin, req)));
            // Both carried probes are in flight before either ends.
            tokio::task::yield_now().await;
            if !self.members_carry {
                ProbeEnd::Other { end: "NotSent", detail: "not sent: NoActiveRoute".into() }
            } else if self.answers_carried.contains(&admin.name.to_string()) && carrier.name.to_string() == self.answering_carrier {
                Self::current(admin)
            } else {
                ProbeEnd::ConnectionLost(format!("{} cannot reach {}", carrier.name, admin.name))
            }
        }

        fn carriers(&self, view: &Topology, admin: &Node) -> Vec<Node> {
            let avoid = self.carriers.lock().unwrap().get(&admin.mesh).cloned();
            investigation_carriers(view, &admin.mesh, |id| self.connected.contains(id), avoid.as_ref())
        }

        async fn leads(&self, _view: &Topology, _admin: &Node) -> Result<Vec<Node>, String> {
            Ok(Vec::new())
        }
    }

    #[async_trait::async_trait]
    impl ProbeCalls for Recorder {
        async fn observe(&self, _admin: &Node) -> (String, String) {
            ("unavailable".into(), "unavailable".into())
        }
    }

    /// A ready member of its mesh: its kind is its path's.
    fn member(name: &str) -> Node {
        admin(name, false)
    }

    /// The peer mesh mesh2 of the probe cells: node-admins 1 and 2, the held mesh-primary record
    /// naming node-admin 2 (so the exact mesh-primary is not the first in path order), rpc members 1
    /// to 3, and this admin's own mesh1, never asked.
    fn mesh2() -> (Arc<Topology>, rdm_core::role::Primary) {
        let v = view_of(vec![admin("mesh1.admin.1", true), member("mesh1.rpc.1"), admin("mesh2.admin.1", true), admin("mesh2.admin.2", false), member("mesh2.rpc.1"), member("mesh2.rpc.2"), member("mesh2.rpc.3")]);
        let a2 = v.nodes.iter().find(|n| n.name.to_string() == "mesh2.admin.2").unwrap().clone();
        let record = rdm_core::role::Primary { mesh: "mesh2".into(), node_id: a2.node_id.clone(), incarnation: a2.incarnation_id.clone().unwrap(), revision: 3 };
        (Arc::new(v), record)
    }

    fn node_id_of(v: &Topology, name: &str) -> NodeId {
        v.nodes.iter().find(|n| n.name.to_string() == name).unwrap().node_id.clone()
    }

    fn calls(lines: &[&str]) -> Vec<String> {
        lines.iter().map(|l| l.to_string()).collect()
    }

    /// CONTRACT: docs/architecture/lifecycles/node-tickle.md:12
    /// "- the fabric-primary in [mesh investigation](mesh-investigation.md), on a mesh's primary and then
    ///  its other node-admins (probe 1 at 20 s, probe 2 at 40 s);"
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:27
    /// "1. At 20 s, probe 1 contacts mesh-x's current exact mesh-primary first, then mesh-x's other
    /// node-admins, with [ProbeNodeState](../node-rpc-envelope.md#status-op-0x1b-rdm). Each target
    /// follows the established direct-then-via-peer policy: direct first; when that fails,
    /// [Ping](../node-rpc-envelope.md#ping-op-0x01-core) a few mesh-x members in parallel (active
    /// connections in the connections table first, otherwise members dialled from held topology) and,
    /// on a pong, use that member as the carrier for [Forward](../node-rpc-envelope.md#forward-op-0x1a-core)."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:36
    /// "3. At 40 s, probe 2 repeats probe 1 (exact mesh-primary first, then the other node-admins,
    /// each direct then via a peer), preferring a different carrier member."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:87
    /// "| 20 s | Fabric-primary → mesh-x's exact mesh-primary, then its other node-admins | ProbeNodeState direct; else Ping a member and, on pong, Forward carrying ProbeNodeState | probe 1: admin answered (ends the investigation), connection-lost or unreachable | `rdm.node_admin.mesh.update.via-probe` (`mesh`, `probe=1`, `member`, `carrier`, `outcome`); RPC call/serve chain below |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:89
    /// "| 40 s | Fabric-primary → mesh-x's exact mesh-primary, then its other node-admins | as probe 1 | probe 2, prefer a different carrier member | `rdm.node_admin.mesh.update.via-probe` (`mesh`, `probe=2`, `member`, `carrier`, `outcome`); RPC call/serve chain below |"
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:150
    /// "A probe of one node-admin uses the [Ping](../node-rpc-envelope.md#ping-op-0x01-core), [Forward](../node-rpc-envelope.md#forward-op-0x1a-core) and [Status](../node-rpc-envelope.md#status-op-0x1b-rdm) schemas:"
    /// CONTRACT (mesh-investigation.md step 1: "probe 1 contacts mesh-x's current exact mesh-primary
    /// first, then mesh-x's other node-admins, with ProbeNodeState. Each target follows the
    /// established direct-then-via-peer policy"; node-tickle.md step 3: "the same probe through up to
    /// two eligible live peers of the node's mesh, both sent at once"; step 3: "probe 2 repeats probe
    /// 1 (exact mesh-primary first, then the other node-admins, each direct then via a peer),
    /// preferring a different carrier member"). The driver makes probes 1 and 2 of one investigation
    /// at their rungs through the probe's calls, recorded in order. Probe 1: the exact birth the role
    /// record names (node-admin 2) direct; it fails, so the carried ProbeNodeState goes through rpc 3
    /// (the one member held Active Direct) and rpc 1 at once; then node-admin 1 direct, and its own
    /// carried probes through the same two. Probe 2 repeats it preferring members probe 1 did not
    /// carry through: rpc 1 and rpc 2. Every carrier answers connection-lost, so both probes find
    /// it, and each probe's span names every node-admin asked in that order, the leg and the carrier
    /// (the first asked that said its connection was lost). Must NOT happen: the first node-admin asked
    /// being the path-first one, a node-admin's carried probe before its direct one, one carrier
    /// asked after the other ended, more than two carriers, probe 2 carried by probe 1's carrier
    /// while other members stand, or a node of mesh1 asked.
    #[tokio::test]
    async fn a_probe_asks_each_node_admin_direct_then_through_two_carriers_at_once_the_exact_mesh_primary_first() {
        let (view, record) = mesh2();
        let carriers = Arc::new(Mutex::new(BTreeMap::new()));
        let rig = Arc::new(Recorder { connected: [node_id_of(&view, "mesh2.rpc.3")].into(), ..Recorder::default() });
        let capture = crate::span_capture::Capture::new("i143-3097-probe-order");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let inv = Investigations::new("mesh1.admin.1".into(), Arc::new(Mutex::new(Ladder::new(Rungs::at(Duration::from_secs(3), Duration::from_millis(500))))), crate::stop_gate::StopGate::new());
        let start = |n: u8| {
            let (rig, carriers, view, record) = (rig.clone(), carriers.clone(), view.clone(), record.clone());
            async move {
                // The rig reads the carrier to avoid where the admin's calls do: the carriers table.
                *rig.carriers.lock().unwrap() = carriers.lock().unwrap().clone();
                probe(&*rig, &carriers, &view, Some(record), "mesh2", n).await
            }
        };
        // Probe 1 at its 2 s rung, probe 2 at its 4 s rung once probe 1 landed.
        let mut made = Vec::new();
        for (n, at) in [(1u8, Duration::from_secs(2)), (2, Duration::from_secs(4))] {
            let steps = inv.tick("mesh2", at, start).await;
            assert!(steps.iter().any(|s| matches!(s, Step::Probe { n: k, .. } if *k == n)), "probe {n} is asked at {at:?}: {steps:?}");
            tokio::time::timeout(Duration::from_secs(10), inv.landed()).await.unwrap_or_else(|_| panic!("probe {n} landed"));
            made.push(rig.made()[made.iter().map(Vec::len).sum::<usize>()..].to_vec());
        }
        assert_eq!(
            made[0],
            calls(&["direct mesh2.admin.2", "via mesh2.rpc.3 > mesh2.admin.2", "via mesh2.rpc.1 > mesh2.admin.2", "direct mesh2.admin.1", "via mesh2.rpc.3 > mesh2.admin.1", "via mesh2.rpc.1 > mesh2.admin.1"]),
            "probe 1: the primary record's exact mesh-primary first, then the other node-admin, each direct, then carried through two members at once (the Active Direct member first)"
        );
        assert_eq!(
            made[1],
            calls(&["direct mesh2.admin.2", "via mesh2.rpc.1 > mesh2.admin.2", "via mesh2.rpc.2 > mesh2.admin.2", "direct mesh2.admin.1", "via mesh2.rpc.1 > mesh2.admin.1", "via mesh2.rpc.2 > mesh2.admin.1"]),
            "probe 2 repeats probe 1, preferring members probe 1 did not carry through (rpc 3 is asked last and left out)"
        );
        let spans = capture.named("rdm.node_admin.mesh.update.via-probe");
        assert_eq!(spans.len(), 2, "one span per probe: {spans:?}");
        let attr = |i: usize, k: &str| spans[i].attributes.get(k).cloned().unwrap_or_default();
        for (i, carrier) in [(0, "mesh2.rpc.3"), (1, "mesh2.rpc.1")] {
            assert_eq!(attr(i, "probe"), (i + 1).to_string());
            assert_eq!(attr(i, "target"), format!("mesh2.admin.2=connection-lost(via {carrier}),mesh2.admin.1=connection-lost(via {carrier})"), "probe {}: every node-admin asked, in order, with its leg", i + 1);
            assert_eq!(attr(i, "carrier"), carrier, "probe {}", i + 1);
            assert_eq!(attr(i, "outcome"), "connection-lost", "probe {}", i + 1);
            assert_eq!(attr(i, "primary_record"), record.to_string(), "probe {}: ordered by the held primary record", i + 1);
            eprintln!("span rdm.node_admin.mesh.update.via-probe trace_id={} {:?}", spans[i].trace_id, spans[i].attributes);
        }
        assert_eq!(attr(0, "member"), "mesh2.rpc.3,mesh2.rpc.1", "probe 1 asked the Active Direct member first");
        assert_eq!(attr(1, "member"), "mesh2.rpc.1,mesh2.rpc.2", "probe 2 asked members other than probe 1's carrier");
        let tickles = capture.named("rdm.node.tickle.returns-unknown.via-unreached");
        assert_eq!(tickles.len(), 4, "one unreached ladder per node-admin per probe, each saying the investigation decides next: {tickles:?}");
        assert!(tickles.iter().all(|t| t.attributes.get("next").map(String::as_str) == Some("decide") && t.attributes.get("caller").map(String::as_str) == Some("mesh1.admin.1")), "{tickles:?}");
    }

    /// CONTRACT (mesh-investigation.md step 1: each target follows the direct-then-via-peer policy;
    /// node-tickle.md step 3: a peer that cannot carry the probe "counts as no answer"): a node-admin
    /// whose direct probe fails is probed through two members at once even when no member carried the
    /// probe of the node-admin before it. With no member reachable at all, the probe is `unreachable`,
    /// names no carrier, and says, for each node-admin, how each carried probe ended. Must NOT happen:
    /// the second node-admin recorded unreachable with no carried probe made for it, or a carrier
    /// named when none answered anything.
    #[tokio::test]
    async fn a_node_admin_whose_direct_probe_fails_is_probed_through_two_members_at_once_even_after_none_carried_the_one_before() {
        let (view, record) = mesh2();
        let rig = Recorder { members_carry: false, ..Recorder::default() };
        let found = probe(&rig, &Mutex::new(BTreeMap::new()), &view, Some(record), "mesh2", 1).await;
        assert_eq!(
            rig.made(),
            calls(&["direct mesh2.admin.2", "via mesh2.rpc.1 > mesh2.admin.2", "via mesh2.rpc.2 > mesh2.admin.2", "direct mesh2.admin.1", "via mesh2.rpc.1 > mesh2.admin.1", "via mesh2.rpc.2 > mesh2.admin.1"]),
            "each node-admin whose direct probe failed is carried through two members at once"
        );
        assert_eq!(found.outcome, Unreachable, "no member reached: {found:?}");
        assert_eq!(found.target, "mesh2.admin.2=unreachable,mesh2.admin.1=unreachable");
        assert_eq!(found.carrier, "", "no member carried the probe");
        assert_eq!(found.members, "mesh2.rpc.1,mesh2.rpc.2", "the members asked");
        assert_eq!(found.detail.matches("direct: NotSent: not sent: dial deadline; via mesh2.rpc.1: NotSent: not sent: NoActiveRoute; via mesh2.rpc.2: NotSent: not sent: NoActiveRoute").count(), 2, "each node-admin names how each of its probes ended: {}", found.detail);
    }

    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:27
    /// "A successful contact ends the failed-reachability investigation."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:36
    /// "3. At 40 s, probe 2 repeats probe 1 (exact mesh-primary first, then the other node-admins,
    /// each direct then via a peer), preferring a different carrier member."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:39
    /// "4. If the node-admin answers either probe (Current or Blocked, under the
    /// [Status contract](../node-rpc-envelope.md#status-op-0x1b-rdm)), stop the investigation."
    /// CONTRACT: docs/architecture/lifecycles/mesh-investigation.md:90
    /// "| any time | Fabric-primary / answering admin | admin answer or direct backbone receipt | stop / cancel | `rdm.node_admin.mesh.update.via-probe-verdict` (`mesh`, `outcome`, `at`) |"
    /// CONTRACT (mesh-investigation.md step 1: "A successful contact ends the failed-reachability
    /// investigation"; step 3: probe 2 "preferring a different carrier member"): the first node-admin
    /// that answers ends the probe, direct or carried, and no node-admin after it is asked; probe 2
    /// is carried by members other than probe 1's carrier while any stand. Must NOT happen: a carried
    /// probe after a direct answer, a call to the next node-admin after a carried answer, or probe 2
    /// through probe 1's carrier while two other members stand.
    #[tokio::test]
    async fn the_first_answering_node_admin_ends_the_probe_and_probe_two_prefers_other_carriers() {
        let (view, record) = mesh2();
        let capture = crate::span_capture::Capture::new("answering_admin_stops_probe");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);

        let rig = Arc::new(Recorder { answers_direct: ["mesh2.admin.2".to_string()].into(), ..Recorder::default() });
        let inv = Investigations::new("mesh1.admin.1".into(), Arc::new(Mutex::new(Ladder::new(Rungs::at(Duration::from_secs(3), Duration::from_millis(500))))), crate::stop_gate::StopGate::new());
        let start = |n: u8| {
            let (rig, view, record) = (rig.clone(), view.clone(), record.clone());
            async move { probe(&*rig, &Mutex::new(BTreeMap::new()), &view, Some(record), "mesh2", n).await }
        };
        let steps = inv.tick("mesh2", Duration::from_secs(2), start).await;
        assert!(steps.iter().any(|s| matches!(s, Step::Probe { n: 1, .. })));
        tokio::time::timeout(Duration::from_secs(10), inv.landed()).await.unwrap();
        assert_eq!(rig.made(), calls(&["direct mesh2.admin.2"]), "the exact mesh-primary answered direct: nothing else is asked");
        let verdicts = capture.named("rdm.node_admin.mesh.update.via-probe-verdict");
        assert_eq!(verdicts.len(), 1, "answering probe emits via-probe-verdict: {verdicts:?}");
        assert_eq!(verdicts[0].attributes.get("mesh").map(String::as_str), Some("mesh2"));
        assert_eq!(verdicts[0].attributes.get("outcome").map(String::as_str), Some("admin-alive"));

        let rig = Recorder { answers_direct: ["mesh2.admin.2".to_string()].into(), ..Recorder::default() };
        let found = probe(&rig, &Mutex::new(BTreeMap::new()), &view, Some(record.clone()), "mesh2", 1).await;
        assert_eq!(rig.made(), calls(&["direct mesh2.admin.2"]), "the exact mesh-primary answered direct: nothing else is asked");
        assert_eq!((found.outcome, found.target.as_str(), found.carrier.as_str()), (AdminAlive, "mesh2.admin.2=admin-alive(direct)", ""));

        let rig = Recorder { answers_carried: ["mesh2.admin.2".to_string()].into(), ..Recorder::default() };
        let found = probe(&rig, &Mutex::new(BTreeMap::new()), &view, Some(record.clone()), "mesh2", 1).await;
        assert_eq!(rig.made(), calls(&["direct mesh2.admin.2", "via mesh2.rpc.1 > mesh2.admin.2", "via mesh2.rpc.2 > mesh2.admin.2"]), "the exact mesh-primary answered through a carrier: node-admin 1 is never asked");
        assert_eq!((found.outcome, found.target.as_str(), found.carrier.as_str()), (AdminAlive, "mesh2.admin.2=admin-alive(via mesh2.rpc.1)", "mesh2.rpc.1"));

        // Probe 1 was carried by rpc 3: probe 2 goes through the two other members.
        let rig = Recorder { carriers: Mutex::new(BTreeMap::from([("mesh2".to_string(), "mesh2.rpc.3".parse::<PathName>().unwrap())])), ..Recorder::default() };
        let carriers = Mutex::new(BTreeMap::from([("mesh2".to_string(), "mesh2.rpc.3".parse::<PathName>().unwrap())]));
        let found = probe(&rig, &carriers, &view, Some(record), "mesh2", 2).await;
        assert_eq!(
            rig.made(),
            calls(&["direct mesh2.admin.2", "via mesh2.rpc.1 > mesh2.admin.2", "via mesh2.rpc.2 > mesh2.admin.2", "direct mesh2.admin.1", "via mesh2.rpc.1 > mesh2.admin.1", "via mesh2.rpc.2 > mesh2.admin.1"]),
            "probe 1's carrier is asked last and left out while two other members stand"
        );
        assert_eq!((found.outcome, found.carrier.as_str()), (ConnectionLost, "mesh2.rpc.1"));
        assert_eq!(carriers.lock().unwrap().get("mesh2").map(ToString::to_string).as_deref(), Some("mesh2.rpc.1"), "the carrier this probe went through is held for the next");
    }

    /// CONTRACT (Q253 amended, rdm#164: the reconcile loop's drift check and drive read what this ladder
    /// holds of a peer mesh and wait on its moves, never on a tick): the ladder's watch moves when a mesh
    /// is tracked, decided, heard again or forgotten, and never on a step that moves none of those (a mark,
    /// a probe asked or recorded, a step below the track rung, a forget of a mesh it holds nothing of).
    /// Must NOT happen: a decision the reconcile loop never hears of, or a move per interval.
    #[test]
    fn the_ladder_moves_its_watch_when_a_mesh_is_tracked_decided_heard_again_or_forgotten_and_never_otherwise() {
        let mut l = ladder();
        let rx = l.subscribe_moved();
        let probe = |steps: Vec<Step>| steps.into_iter().find_map(|s| if let Step::Probe { n, ticket, .. } = s { Some((n, ticket)) } else { None }).expect("a probe is asked");
        l.step("mesh2", tick(1));
        assert_eq!(*rx.borrow(), 0, "below the track rung nothing moves");
        l.step("mesh2", tick(2));
        assert_eq!(*rx.borrow(), 1, "tracked");
        let (n, ticket) = probe(l.step("mesh2", tick(10)));
        l.probed("mesh2", ticket, n, Unreachable, ms(10));
        l.step("mesh2", tick(15));
        let (n, ticket) = probe(l.step("mesh2", tick(20)));
        l.probed("mesh2", ticket, n, ConnectionLost, ms(20));
        assert_eq!(*rx.borrow(), 1, "probes asked and recorded and the mark move nothing the reconcile loop reads");
        l.step("mesh2", tick(30));
        assert!(l.recovery_decided("mesh2"));
        assert_eq!(*rx.borrow(), 2, "decided");
        l.step("mesh2", tick(31));
        assert_eq!(*rx.borrow(), 2, "a step after the decision moves nothing");
        l.step("mesh2", tick(1));
        assert_eq!(*rx.borrow(), 3, "heard again: the decision and the tracking are gone");
        l.step("mesh2", tick(2));
        l.forget("mesh2");
        assert_eq!(*rx.borrow(), 5, "tracked again, then forgotten");
        l.forget("mesh2");
        l.step("mesh3", tick(1));
        l.clear();
        assert_eq!(*rx.borrow(), 5, "forgetting or clearing what holds nothing tracked or decided moves nothing");
        l.step("mesh3", tick(2));
        l.clear();
        assert_eq!(*rx.borrow(), 7, "tracked, then cleared");
    }
}
