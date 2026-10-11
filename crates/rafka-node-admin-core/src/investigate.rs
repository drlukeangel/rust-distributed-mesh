//! The peer-mesh investigation (rafka-v2 fabric-node-lifecycle-events-ops-gossip.md, "Mesh
//! Recovery" -> "Detection: the peer-mesh investigation"; Luke 2026-10-08).
//!
//! The fabric primary hears a peer mesh on the backbone. When it stops hearing it, it asks the
//! mesh's own members whether the mesh's node-admin is there, and rebirths the mesh's node-admins
//! only when a member says it cannot reach its own node-admin (`carrier-edge-lost`). Time is
//! counted in backbone rounds, so a cell at 500 ms rounds runs the same ladder as production at
//! 2 s:
//!
//! ```text
//! unheard rounds   10   probe 1   Ping members, Forward{ProbeNodeState} to the node-admin
//!                  15   mark      the mesh is silent (nothing is sent)
//!                  20   probe 2   the same, another carrier preferred
//!                  30   decision  rebirth only if the latest probe answered carrier-edge-lost
//! ```
//!
//! An answer from the node-admin stops it (`admin-alive`); a backbone receipt cancels it
//! (`heard-again`); no member answering holds. The state machine here is pure over the unheard
//! rounds and the probe results; [`run`] drives it from the fabric primary's view.

use crate::admin::Records;
use crate::model::{Node, NodeId, NodeKind, NodeStatus, PathName};
use crate::topology::Topology;
use rafka_mesh_transport::membership::Membership;
use rafka_node_rpc::{Budget, CallOptions, NodeRpcClient, NodeTarget};
use rafka_node_rpc_contract::outcome::NotSentReason;
use rafka_node_rpc_contract::ping::{Ping, PingRequest};
use rafka_node_rpc_contract::status::{Status, StatusReply, StatusRequest};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The ruled rungs: how long the mesh has gone unheard, elapsed monotonic time since the fabric
/// primary's last valid direct backbone receipt from it.
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
    /// The rungs at the staleness floor `floor` and the backbone gossip interval `backbone`.
    pub fn at(floor: Duration, backbone: Duration) -> Rungs {
        let _ = floor;
        Rungs { track: backbone * 2, probe1: backbone * 10, mark: backbone * 15, probe2: backbone * 20, decide: backbone * 30 }
    }

    /// The rungs at the configured cadences.
    pub fn configured() -> Rungs {
        Self::at(rafka_mesh_entity::cadence::staleness_floor(), rafka_mesh_entity::cadence::backbone_gossip_interval())
    }
}

/// Every node's pooled connection idles out after the staleness floor; that closes a connection
/// to a dead peer before the second probe only if the floor is shorter than the probe's wait
/// (`probe2` unheard). A floor at or past it refuses the start by name.
pub fn check_idle_below_probe2(staleness_floor: std::time::Duration, backbone_round: std::time::Duration) -> Result<(), String> {
    let probe2 = Rungs::at(staleness_floor, backbone_round).probe2;
    if staleness_floor >= probe2 {
        return Err(format!(
            "the staleness floor RDM_STALENESS_MS ({} ms) is not below the second probe (RDM_BACKBONE_INTERVAL_MS = {} ms, {} ms): a pooled connection to a dead peer would still be open when the second probe dials",
            staleness_floor.as_millis(),
            backbone_round.as_millis(),
            probe2.as_millis()
        ));
    }
    Ok(())
}

/// What a probe found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeOutcome {
    /// The mesh's node-admin answered the carried probe (it re-asserted its presence).
    AdminAlive,
    /// A member answered the Ping but reports it cannot reach its own node-admin.
    CarrierEdgeLost,
    /// No member answered, or the carried call ended any other way.
    Unreachable,
}

impl ProbeOutcome {
    /// The outcome's name as it appears in spans and evidence.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AdminAlive => "admin-alive",
            Self::CarrierEdgeLost => "carrier-edge-lost",
            Self::Unreachable => "unreachable",
        }
    }
}

/// What the ladder asks of its driver.
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
    },
    /// The node-admin answered: the investigation stops.
    Stop {
        /// How long the mesh has gone unheard, in milliseconds.
        unheard_ms: u64,
    },
    /// The decision: `rebirth` only when the latest probe answered `carrier-edge-lost`.
    Decide {
        /// Whether the mesh's node-admins are reborn.
        rebirth: bool,
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
    stopped: bool,
    /// The decision, once taken: whether it was a rebirth.
    verdict: Option<bool>,
}

impl State {
    fn probes_made(&self) -> u8 {
        self.probes.iter().flatten().count() as u8
    }
}

/// A peer mesh as the ladder holds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerMesh {
    /// `Some(ms unheard)` while the ladder tracks the mesh as unheard: loss of its control-plane
    /// visibility fences lifecycle mutation for every node of the mesh (R-D1).
    pub unheard_ms: Option<u64>,
    /// The ladder has decided the rebirth of the mesh's node-admins.
    pub rebirth_decided: bool,
}

/// One investigation per peer mesh the fabric primary has heard on the backbone.
#[derive(Debug)]
pub struct Ladder {
    rungs: Rungs,
    meshes: BTreeMap<String, State>,
}

impl Ladder {
    /// A ladder over `rungs` with no mesh tracked.
    pub fn new(rungs: Rungs) -> Self {
        Self { rungs, meshes: BTreeMap::new() }
    }

    /// The meshes held now.
    pub fn meshes(&self) -> BTreeSet<String> {
        self.meshes.keys().cloned().collect()
    }

    /// Forget everything: this node is not the fabric primary.
    pub fn clear(&mut self) {
        self.meshes.clear();
    }

    /// Forget `mesh`: it has no backbone receipt held at all.
    pub fn forget(&mut self, mesh: &str) {
        self.meshes.remove(mesh);
    }

    /// Has this ladder decided the rebirth of `mesh`'s node-admins? A fabric primary holds that
    /// rebirth back for every peer mesh it hears on the backbone until this is true: a node-admin
    /// whose runtime was proven exited is replaced when the ladder decides, not before, and a
    /// fabric primary that took the seat a moment ago has decided nothing yet.
    pub(crate) fn rebirth_decided(&self, mesh: &str) -> bool {
        self.meshes.get(mesh).is_some_and(|s| s.verdict == Some(true))
    }

    /// What this ladder holds of `mesh`: `unheard_ms` is set while the ladder tracks the mesh as unheard
    /// (from the rung [`Rungs::track`] on, until it is heard again), the same clock [`Self::step`] counts;
    /// `None` when the ladder holds no investigation of the mesh at all.
    pub fn peer_mesh(&self, mesh: &str) -> Option<PeerMesh> {
        let st = self.meshes.get(mesh)?;
        Some(PeerMesh { unheard_ms: st.tracked.then(|| st.unheard.as_millis() as u64), rebirth_decided: st.verdict == Some(true) })
    }

    /// What `mesh` being unheard for `unheard` asks next. One probe or the decision per call; the
    /// driver makes the probe, records it with [`Self::probed`], and asks again.
    pub fn step(&mut self, mesh: &str, unheard: Duration) -> Vec<Step> {
        let r = self.rungs;
        let st = self.meshes.entry(mesh.to_string()).or_default();
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
            out.push(Step::Track { unheard_ms });
        }
        if unheard >= r.mark && !st.marked {
            st.marked = true;
            out.push(Step::Mark { unheard_ms });
        }
        if st.stopped || st.verdict.is_some() {
            return out;
        }
        if unheard >= r.probe1 && st.probes[0].is_none() {
            out.push(Step::Probe { n: 1, unheard_ms });
        } else if unheard >= r.probe2 && st.probes[1].is_none() {
            out.push(Step::Probe { n: 2, unheard_ms });
        } else if unheard >= r.decide {
            // The latest completed probe decides; an older carrier-edge-lost never overrides a
            // newer unreachable.
            let latest = st.probes[1].or(st.probes[0]).unwrap_or(ProbeOutcome::Unreachable);
            let rebirth = latest == ProbeOutcome::CarrierEdgeLost;
            st.verdict = Some(rebirth);
            out.push(Step::Decide { rebirth, latest, unheard_ms });
        }
        out
    }

    /// Probe `n` of `mesh` found `outcome`. An answering node-admin stops the investigation.
    pub fn probed(&mut self, mesh: &str, n: u8, outcome: ProbeOutcome, unheard_ms: u64) -> Option<Step> {
        let st = self.meshes.get_mut(mesh)?;
        st.probes[usize::from(n.clamp(1, 2)) - 1] = Some(outcome);
        (outcome == ProbeOutcome::AdminAlive).then(|| {
            st.stopped = true;
            Step::Stop { unheard_ms }
        })
    }
}

/// A Ping is answered within two rounds or the member did not answer.
pub(crate) fn ping_budget(round: Duration) -> Duration {
    round * 2
}

/// The carried probe outlasts the carrier's one inner call (made with `CallOptions::default`) by two
/// rounds, so the carrier's answer, carrier-edge-lost included, reaches the origin before it gives
/// up: an inner dial to a dead node-admin runs to the inner call's whole budget.
pub(crate) fn carried_budget(round: Duration) -> Budget {
    match CallOptions::default().budget {
        Budget::Overall(inner) => Budget::Overall(inner + round * 2),
        Budget::Split { send, reply } => Budget::Split { send: send + round * 2, reply },
        Budget::Stream { send } => Budget::Stream { send: send + round * 2 },
    }
}

/// How many members are asked in parallel.
pub(crate) const PROBE_FANOUT: usize = 3;

/// What the driver needs of its admin.
pub struct Watch {
    /// This admin's `path.name`.
    pub me: PathName,
    /// The node id this admin was born with: whether it holds the fabric seat is decided by it.
    pub me_id: crate::model::NodeId,
    /// The length of a backbone round.
    pub round: Duration,
    /// The admin's membership.
    pub membership: Membership,
    /// The admin's observed topology.
    pub topology: Arc<tokio::sync::RwLock<Topology>>,
    /// The records this admin holds.
    pub records: Arc<Records>,
    /// Is the accepted Build settled (not pending, not running)? A mesh the Build is still birthing
    /// is not unheard.
    pub settled: Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send>> + Send + Sync>,
    /// The client probes are made through.
    pub client: Arc<NodeRpcClient>,
    /// Does this node hold an Active Direct connection to the exact node?
    pub connected: Arc<dyn Fn(&NodeId) -> bool + Send + Sync>,
    /// The ladder.
    pub ladder: Arc<Mutex<Ladder>>,
    /// The carrier each mesh's latest probe went through.
    pub carriers: Mutex<BTreeMap<String, PathName>>,
}

/// The local OS clock: the silence of a mesh is measured from a local event, and the read is only
/// named in spans. No node reads it as a stamp.
fn local_now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// Run the investigation for as long as the task lives: once per round, while this admin is the
/// fabric primary and its view authorizes, every peer mesh it has heard on the backbone is stepped
/// down its ladder.
pub async fn run(w: Watch) {
    // When this admin took the fabric-primary seat: a peer mesh it has never heard is unheard from then.
    let mut seat_since: Option<Instant> = None;
    loop {
        tokio::time::sleep(w.round).await;
        let view = w.topology.read().await.clone();
        let fabric_primary = view.holds_fabric_seat(&w.me_id);
        if !(fabric_primary && w.membership.authorizes()) {
            w.ladder.lock().unwrap().clear();
            seat_since = None;
            continue;
        }
        // The clock of a mesh never heard runs while the accepted Build is settled: from the seat, and
        // from each settling, because a mesh the Build is still birthing has not had its chance to be heard.
        if (w.settled)().await {
            seat_since.get_or_insert_with(Instant::now);
        } else {
            seat_since = None;
        }
        let book = &w.membership.book;
        let heard = book.backbone_meshes();
        let never = if seat_since.is_some() { never_heard(&view, &w.me.mesh, &heard) } else { BTreeSet::new() };
        forget_unheard_meshes(&w.ladder, &heard.union(&never).cloned().collect());
        let mut tasks = Vec::new();
        for mesh in heard.into_iter().filter(|m| *m != w.me.mesh) {
            let Some(unheard) = book.mesh_unheard(&mesh, Instant::now()) else { continue };
            tasks.push(investigate(&w, &view, mesh, unheard));
        }
        for mesh in never {
            let unheard = seat_since.expect("never is empty without it").elapsed();
            tasks.push(investigate(&w, &view, mesh, unheard));
        }
        futures_util::future::join_all(tasks).await;
        restore(&w, &view);
    }
}

/// The peer meshes the view holds nodes of that the backbone has never carried to this admin: it
/// learned them from topology alone, which is not liveness (R-G2), so nothing says whether they are
/// alive. An admin that took the fabric-primary seat watches them from the moment it took it.
pub(crate) fn never_heard(view: &Topology, me_mesh: &str, heard: &BTreeSet<String>) -> BTreeSet<String> {
    view.members().map(|n| n.mesh.clone()).filter(|m| m != me_mesh && !heard.contains(m)).collect()
}

/// Forget every investigation of a mesh the backbone no longer carries (a retired mesh).
fn forget_unheard_meshes(ladder: &Mutex<Ladder>, heard: &BTreeSet<String>) {
    let gone: Vec<String> = ladder.lock().unwrap().meshes().difference(heard).cloned().collect();
    let mut held = ladder.lock().unwrap();
    for mesh in gone {
        held.forget(&mesh);
    }
}

/// The fabric returns to ready when the mesh that was reborn has reported for its round: its
/// current node-admin primary, a birth the decision did not find, declared the Mesh ready to this
/// fabric primary after the decision. A ready primary of a newer birth in the view, with no report,
/// is not this (`crate::round::recovered`).
fn restore(w: &Watch, view: &Topology) {
    let Some(recovery) = w.records.peer_recovery() else { return };
    let primary = view.cohort_primary(&recovery.mesh, NodeKind::NodeAdmin).and_then(|p| p.incarnation_id.clone());
    let report = w.records.declared.lock().unwrap().reports.get(&recovery.mesh).cloned();
    if crate::round::recovered(&recovery, primary.as_ref(), report.as_ref()) {
        w.records.set_peer_recovery(None);
        tracing::info_span!("rdm.node_admin.mesh.update.via-recovered", node = %w.me, mesh = %recovery.mesh, primary = ?primary, verdict_rafka_ms = recovery.verdict_rafka_ms)
            .in_scope(|| tracing::info!("the reborn mesh's primary reported its round complete: the fabric is ready again"));
    }
}

async fn investigate(w: &Watch, view: &Topology, mesh: String, unheard: Duration) {
    let at = local_now_ms().saturating_sub(unheard.as_millis() as u64);
    let steps = w.ladder.lock().unwrap().step(&mesh, unheard);
    for step in steps {
        match step {
            Step::Track { unheard_ms } => {
                tracing::info_span!("rdm.node_admin.mesh.update.via-unheard", node = %w.me, mesh = %mesh, at, unheard_ms, mark = false)
                    .in_scope(|| tracing::info!("no word of the mesh on the backbone: the investigation tracks it"));
            }
            Step::Mark { unheard_ms } => {
                tracing::info_span!("rdm.node_admin.mesh.update.via-unheard", node = %w.me, mesh = %mesh, at, unheard_ms, mark = true)
                    .in_scope(|| tracing::info!("the mesh is marked silent; nothing is sent"));
            }
            Step::Probe { n, unheard_ms } => {
                let found = probe(w, view, &mesh, n).await;
                let stop = w.ladder.lock().unwrap().probed(&mesh, n, found.outcome, unheard_ms);
                tracing::info_span!(
                    "rdm.node_admin.mesh.update.via-probe",
                    node = %w.me,
                    mesh = %mesh,
                    probe = n,
                    target = %found.target,
                    member = %found.members,
                    carrier = %found.carrier,
                    outcome = found.outcome.as_str(),
                    detail = %found.detail,
                    unheard_ms,
                )
                .in_scope(|| tracing::info!("one probe of the unheard mesh"));
                if let Some(Step::Stop { unheard_ms }) = stop {
                    verdict(w, &mesh, found.outcome.as_str(), unheard_ms, at, "the node-admin answered: the investigation stops");
                }
            }
            Step::Stop { .. } => {}
            Step::Decide { rebirth, latest, unheard_ms } => {
                verdict(w, &mesh, latest.as_str(), unheard_ms, at, if rebirth { "the latest probe says a member cannot reach its own node-admin: the mesh is reborn" } else { "no member could say: the mesh is held" });
                if rebirth {
                    let verdict_ms = w.membership.clock().now_rafka_ms();
                    let lost = view.cohort(&mesh, NodeKind::NodeAdmin).filter_map(|a| a.incarnation_id.clone()).collect();
                    w.records.set_peer_recovery(Some(crate::admin::PeerRecovery { mesh: mesh.clone(), verdict_rafka_ms: verdict_ms, opened_at: Instant::now(), lost }));
                    tracing::info_span!("rdm.node_admin.mesh.create.via-rebirth", node = %w.me, mesh = %mesh, at = local_now_ms(), fabric_status = "degraded", verdict_rafka_ms = verdict_ms)
                        .in_scope(|| tracing::info!("the existing Mesh Recovery opens; the fabric primary authors degraded"));
                }
            }
            Step::Cancel { unheard_ms, probes } => {
                tracing::info_span!("rdm.node_admin.mesh.update.via-probe-verdict", node = %w.me, mesh = %mesh, outcome = "heard-again", at = local_now_ms(), unheard_ms, probes)
                    .in_scope(|| tracing::info!("the mesh is heard on the backbone again: the investigation is cancelled"));
            }
        }
    }
}

fn verdict(w: &Watch, mesh: &str, outcome: &str, unheard_ms: u64, _heard_at: u64, why: &str) {
    tracing::info_span!("rdm.node_admin.mesh.update.via-probe-verdict", node = %w.me, mesh = %mesh, outcome, at = local_now_ms(), unheard_ms).in_scope(|| tracing::info!("{why}"));
}

/// What the carried `ProbeNodeState` found. Any reply from the node-admin means it is there. Only the
/// carrier saying its own edge to the node-admin is lost is `carrier-edge-lost`; a carrier that
/// cannot be reached, refuses, or loses the call (it died after its Ping) says nothing of the
/// node-admin, so it is `unreachable`.
pub fn classify(out: &rafka_node_rpc_contract::outcome::RpcOutcome<StatusReply>, admin: &PathName) -> (ProbeOutcome, String) {
    use rafka_node_rpc_contract::outcome::RpcOutcome;
    match out {
        RpcOutcome::Reply(r) => match r.value() {
            StatusReply::Current { state, .. } => (ProbeOutcome::AdminAlive, format!("{admin} answered: {state:?}")),
            other => (ProbeOutcome::AdminAlive, format!("{admin} answered: {}", other.name())),
        },
        RpcOutcome::NotSent(ns) => match ns.reason() {
            NotSentReason::CarrierEdgeLost(edge) => (ProbeOutcome::CarrierEdgeLost, edge.clone()),
            other => (ProbeOutcome::Unreachable, format!("not sent: {other:?}")),
        },
        other => (ProbeOutcome::Unreachable, other.name().to_string()),
    }
}

/// What one probe found, with what it asked.
struct Found {
    outcome: ProbeOutcome,
    target: String,
    members: String,
    carrier: String,
    detail: String,
}

/// The node-admins of `mesh` a probe carries `ProbeNodeState` to, in the order it asks them: the
/// mesh's primary node-admin first, then its other node-admins in path order (Luke 2026-10-11: the
/// investigation's first step is the fabric-primary's tickle of the peer mesh's primary, then of its
/// other node-admins). Only exact births are asked: a probe names `(node.id, incarnation)`.
pub(crate) fn admins_in_probe_order(view: &Topology, mesh: &str) -> Vec<Node> {
    let mut admins: Vec<Node> = view.cohort(mesh, NodeKind::NodeAdmin).filter(|a| a.incarnation_id.is_some()).cloned().collect();
    admins.sort_by_key(|a| (!a.is_primary, a.name.to_string()));
    admins
}

/// What one probe found over every node-admin it asked, in the order asked: any node-admin
/// answering is `admin-alive` (the mesh has a node-admin; mesh-investigation.md step 4); with none
/// answering it is `carrier-edge-lost` only when the carrier reported its edge lost to every one of
/// them (step 5: the rebirth reborns every node-admin of the mesh, so each must be reported
/// unreachable from inside it); any other end says nothing of that node-admin, so `unreachable`.
pub(crate) fn fold_admin_outcomes(outcomes: &[ProbeOutcome]) -> ProbeOutcome {
    if outcomes.contains(&ProbeOutcome::AdminAlive) {
        ProbeOutcome::AdminAlive
    } else if !outcomes.is_empty() && outcomes.iter().all(|o| *o == ProbeOutcome::CarrierEdgeLost) {
        ProbeOutcome::CarrierEdgeLost
    } else {
        ProbeOutcome::Unreachable
    }
}

/// Probe `mesh`'s node-admins: Ping its members, then Forward `ProbeNodeState` through one that
/// answered, to the mesh's primary node-admin and then to each other one (`admins_in_probe_order`). Active connections are asked first, then path order; probe 2 prefers a member probe 1
/// did not use.
async fn probe(w: &Watch, view: &Topology, mesh: &str, n: u8) -> Found {
    let admins = admins_in_probe_order(view, mesh);
    let Some(primary) = admins.first() else {
        return Found { outcome: ProbeOutcome::Unreachable, target: String::new(), members: String::new(), carrier: String::new(), detail: format!("the view holds no node-admin of {mesh}") };
    };
    let mut members: Vec<Node> = view.members().filter(|m| m.mesh == mesh && m.kind != NodeKind::NodeAdmin && m.status != NodeStatus::Leaving && m.status != NodeStatus::Restarting).cloned().collect();
    members.sort_by_key(|m| (!(w.connected)(&m.node_id), m.name.to_string()));
    let used = w.ladder_last_carrier(mesh, n);
    if n > 1 {
        // Probe 2 prefers a member probe 1 did not carry through.
        members.sort_by_key(|m| used.as_ref() == Some(&m.name));
    }
    let asked: Vec<Node> = members.into_iter().take(PROBE_FANOUT).collect();
    let pings = futures_util::future::join_all(asked.iter().map(|m| async move {
        let req = PingRequest::Ping { payload: b"peer-mesh-probe".to_vec() };
        let opts = CallOptions { budget: Budget::Overall(ping_budget(w.round)), ..CallOptions::default() };
        let (out, _) = w.client.call::<Ping>(&NodeTarget::ExactNode(m.node_id.clone()), &req, &opts).await;
        (m, out.reply().is_some(), out.name().to_string())
    }))
    .await;
    let members_asked = asked.iter().map(|m| m.name.to_string()).collect::<Vec<_>>().join(",");
    let Some((carrier, _, _)) = pings.iter().find(|(_, ponged, _)| *ponged) else {
        let why = pings.iter().map(|(m, _, o)| format!("{}: {o}", m.name)).collect::<Vec<_>>().join("; ");
        return Found { outcome: ProbeOutcome::Unreachable, target: primary.name.to_string(), members: members_asked, carrier: String::new(), detail: format!("no member answered the Ping ({why})") };
    };
    w.note_carrier(mesh, carrier.name.clone());
    // The mesh's primary node-admin first, then each other node-admin, through the one carrier; the
    // first node-admin that answers ends the probe.
    let opts = CallOptions { budget: carried_budget(w.round), ..CallOptions::default() };
    let (mut outcomes, mut asked_admins, mut details) = (Vec::new(), Vec::new(), Vec::new());
    for admin in &admins {
        let req = StatusRequest::ProbeNodeState { node_id: admin.node_id.clone(), incarnation: admin.incarnation_id.clone().expect("filtered on it") };
        let (out, _) = w.client.call_via::<Status>(&NodeTarget::ExactNode(carrier.node_id.clone()), &admin.node_id, &req, &opts).await;
        let (outcome, detail) = classify(&out, &admin.name);
        outcomes.push(outcome);
        asked_admins.push(format!("{}={}", admin.name, outcome.as_str()));
        details.push(detail);
        if outcome == ProbeOutcome::AdminAlive {
            break;
        }
    }
    Found { outcome: fold_admin_outcomes(&outcomes), target: asked_admins.join(","), members: members_asked, carrier: carrier.name.to_string(), detail: details.join("; ") }
}

impl Watch {
    fn ladder_last_carrier(&self, mesh: &str, n: u8) -> Option<PathName> {
        (n > 1).then(|| self.carriers.lock().unwrap().get(mesh).cloned()).flatten()
    }

    fn note_carrier(&self, mesh: &str, carrier: PathName) {
        self.carriers.lock().unwrap().insert(mesh.to_string(), carrier);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ProbeOutcome::*;
    use std::time::Duration;

    #[test]
    fn a_staleness_floor_below_the_second_probe_starts() {
        // production: 30 s floor, 2 s round, probe 2 at 40 s; gate: 3 s floor, 0.5 s round, probe 2 at 10 s.
        assert!(check_idle_below_probe2(Duration::from_secs(30), Duration::from_secs(2)).is_ok());
        assert!(check_idle_below_probe2(Duration::from_secs(3), Duration::from_millis(500)).is_ok());
        assert!(check_idle_below_probe2(Duration::from_millis(39_999), Duration::from_secs(2)).is_ok());
    }

    #[test]
    fn a_staleness_floor_at_or_past_the_second_probe_refuses_by_name() {
        for floor_ms in [40_000, 40_001, 90_000] {
            let e = check_idle_below_probe2(Duration::from_millis(floor_ms), Duration::from_secs(2)).unwrap_err();
            assert!(e.contains("RDM_STALENESS_MS") && e.contains("RDM_BACKBONE_INTERVAL_MS") && e.contains("40000 ms"), "{e}");
        }
    }

    /// The ladder at the production cadences: a 30 s staleness floor, 2 s backbone rounds.
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
        for rounds in 0..=40 {
            let mut asked = l.step("mesh2", tick(rounds));
            // The driver asks again after a probe is recorded, as it does on the next round.
            let mut guard = 0;
            while !asked.is_empty() {
                for s in asked.drain(..) {
                    if let Step::Probe { n, .. } = &s {
                        let found = probes[usize::from(*n) - 1];
                        if let Some(stop) = l.probed("mesh2", *n, found, ms(rounds)) {
                            log.push((rounds, stop));
                        }
                    }
                    log.push((rounds, s));
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

    /// CONTRACT (#2803 detection): two carrier-edge-lost probes decide a rebirth at 30 rounds, with
    /// the probes at 10 and 20 and the silent mark between them at 15; nothing before.
    #[test]
    fn a_peer_mesh_known_from_topology_alone_is_watched_from_the_seat() {
        use crate::model::*;
        let node = |n: &str| Node::allocated(n.parse().unwrap());
        let t = Topology {
            fabric: Fabric { id: rafka_mesh_entity::FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![node("mesh1.admin.1"), node("mesh2.admin.1"), node("mesh2.rpc.1"), node("mesh3.admin.1")],
        };
        let heard: BTreeSet<String> = ["mesh3".to_string()].into();
        assert_eq!(never_heard(&t, "mesh1", &heard), BTreeSet::from(["mesh2".to_string()]), "mesh2 only: mesh1 is this admin's own, mesh3 was heard on the backbone");
    }

    #[test]
    fn two_carrier_edge_lost_probes_decide_a_rebirth_at_thirty_rounds() {
        let log = walk(&mut ladder(), [CarrierEdgeLost, CarrierEdgeLost]);
        assert_eq!(at(&log, |s| matches!(s, Step::Track { .. })), vec![2]);
        assert_eq!(at(&log, |s| matches!(s, Step::Probe { n: 1, .. })), vec![10]);
        assert_eq!(at(&log, |s| matches!(s, Step::Mark { .. })), vec![15]);
        assert_eq!(at(&log, |s| matches!(s, Step::Probe { n: 2, .. })), vec![20]);
        assert_eq!(at(&log, |s| matches!(s, Step::Decide { rebirth: true, latest: CarrierEdgeLost, .. })), vec![30]);
    }

    /// CONTRACT: the latest completed probe decides. An older carrier-edge-lost never overrides a
    /// newer unreachable, and no answer at all holds.
    #[test]
    fn the_latest_probe_decides_and_unreachable_holds() {
        for probes in [[CarrierEdgeLost, Unreachable], [Unreachable, Unreachable]] {
            let log = walk(&mut ladder(), probes);
            assert_eq!(at(&log, |s| matches!(s, Step::Decide { rebirth: false, latest: Unreachable, .. })), vec![30], "{probes:?}");
        }
        let log = walk(&mut ladder(), [Unreachable, CarrierEdgeLost]);
        assert_eq!(at(&log, |s| matches!(s, Step::Decide { rebirth: true, .. })), vec![30]);
    }

    fn view_of(nodes: Vec<Node>) -> Topology {
        use crate::model::{Fabric, FabricId, ProviderKind, ScopeStatus};
        Topology { fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process }, meshes: vec![], nodes }
    }

    fn admin(name: &str, primary: bool) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.node_id = NodeId::mint();
        n.incarnation_id = Some(crate::model::IncarnationId::mint());
        n.status = NodeStatus::ReadyForTraffic;
        n.is_primary = primary;
        n
    }

    /// CONTRACT (R-CK1, mesh-investigation.md: "the thresholds below are elapsed time, not round
    /// counts"; step 2 "the shared staleness mark"): the rungs are 20 s, 30 s, 40 s and 60 s of
    /// elapsed time unheard at the 30 s staleness floor, the silent mark being the staleness floor
    /// itself, and a changed backbone interval moves none of them. At the gate's 3 s floor they are
    /// 2 s, 3 s, 4 s and 6 s.
    #[test]
    fn the_rungs_are_elapsed_time_from_the_staleness_floor_never_backbone_rounds() {
        let (twenty, thirty, forty, sixty) = (Duration::from_secs(20), Duration::from_secs(30), Duration::from_secs(40), Duration::from_secs(60));
        for backbone in [Duration::from_millis(500), Duration::from_secs(2), Duration::from_secs(5)] {
            let r = Rungs::at(thirty, backbone);
            assert_eq!((r.probe1, r.mark, r.probe2, r.decide), (twenty, thirty, forty, sixty), "backbone interval {backbone:?}");
            assert!(r.track <= r.probe1, "tracking begins no later than probe 1: {r:?}");
        }
        let gate = Rungs::at(Duration::from_secs(3), Duration::from_millis(500));
        assert_eq!((gate.probe1, gate.mark, gate.probe2, gate.decide), (Duration::from_secs(2), Duration::from_secs(3), Duration::from_secs(4), Duration::from_secs(6)));
    }

    /// CONTRACT (R-CK1): one investigation chain for a silent peer mesh. Only a direct backbone
    /// receipt starts or resets t: forwarded topology of the mesh arriving every round neither resets
    /// nor extends it, so the ladder walks probe 1, the mark, probe 2 and the decision at the same
    /// elapsed times whether it is stepped every 0.5 s or every 2 s; a fresh backbone receipt cancels
    /// it. With no probe answered the decision is a hold: nothing marks the mesh dead on silence.
    #[test]
    fn one_investigation_runs_on_elapsed_time_from_the_last_direct_backbone_receipt() {
        use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshNode};
        let digest = |name: &str| MeshDigest {
            fabric_id: FabricId::parse("fab000000001").unwrap(),
            node: MeshNode { node_id: NodeId::mint(), name: name.parse().unwrap(), endpoint_id: EndpointId("key".into()), transport_addr: "127.0.0.1:41000".parse().unwrap(), incarnation: IncarnationId::mint(), supersedes: None, runtime: None },
            status: MemberStatus::ReadyForTraffic,
            admin_api_base: None,
            emitted_at_rafka_ms: 1,
            digest_seq: 1,
            mesh_id: None,
            in_flight: None,
            config: Default::default(),
            load: None,
            gossip: None,
            data_dir: None,
        };
        let floor = Duration::from_secs(30);
        for every in [Duration::from_millis(500), Duration::from_secs(2)] {
            let book = rafka_mesh_transport::membership::DigestBook::default();
            let (admin, rpc) = (digest("mesh2.admin.1"), digest("mesh2.rpc.1"));
            let t0 = Instant::now();
            assert!(book.record_forwarded_at(admin.clone(), t0), "the direct backbone receipt");
            let mut l = Ladder::new(Rungs::at(floor, every));
            let mut log: Vec<(Duration, Step)> = Vec::new();
            let mut k = 1u32;
            while every * k <= Duration::from_secs(70) {
                let now = t0 + every * k;
                // The peer mesh's topology forwarded onto this node's mesh channel, every round.
                let mut fwd = rpc.clone();
                fwd.digest_seq = u64::from(k) + 1;
                book.record_topology_at(fwd, now);
                let unheard = book.mesh_unheard("mesh2", now).expect("the mesh was heard on the backbone");
                assert_eq!(unheard, every * k, "forwarded topology neither resets nor extends t");
                for s in l.step("mesh2", unheard) {
                    if let Step::Probe { n, unheard_ms } = s {
                        l.probed("mesh2", n, Unreachable, unheard_ms);
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
            assert!(log.iter().all(|(_, s)| !matches!(s, Step::Decide { rebirth: true, .. })), "silence and unreachable probes decide no rebirth: {log:?}");
            assert!(!l.rebirth_decided("mesh2"));
            // A direct backbone receipt resets t and cancels the investigation.
            let later = t0 + every * k;
            assert!(book.record_forwarded_at(admin.clone(), later));
            let reset = book.mesh_unheard("mesh2", later).unwrap();
            assert_eq!(reset, Duration::ZERO, "a direct backbone receipt resets t");
            assert!(matches!(l.step("mesh2", reset).as_slice(), [Step::Cancel { .. }]), "a receipt cancels the investigation");
        }
    }

    /// CONTRACT (Luke 2026-10-11, the investigation's first step: "fabric-primary's tickle of
    /// peer-mesh-primary then its other node-admins"): a probe carries `ProbeNodeState` to the peer
    /// mesh's primary node-admin first, then to each of its other node-admins in path order. The
    /// primary leads even where another admin's path sorts first. Must NOT happen: a probe that asks
    /// only one node-admin, or a secondary before the primary.
    #[test]
    fn a_probe_asks_the_peer_meshs_primary_admin_first_then_its_other_admins() {
        let v = view_of(vec![admin("mesh2.admin.1", false), admin("mesh2.admin.3", false), admin("mesh2.admin.2", true), admin("mesh1.admin.1", true)]);
        let order: Vec<String> = admins_in_probe_order(&v, "mesh2").iter().map(|a| a.name.to_string()).collect();
        assert_eq!(order, vec!["mesh2.admin.2", "mesh2.admin.1", "mesh2.admin.3"], "the primary first, then the others in path order, and nothing of another mesh");
    }

    /// CONTRACT (mesh-investigation.md steps 4-5): over the node-admins one probe asked, any answer is
    /// `admin-alive`; `carrier-edge-lost` only when every one of them was reported lost from inside
    /// the mesh; anything else, a timeout or an unreachable carrier included, is `unreachable`.
    #[test]
    fn a_probe_over_several_admins_is_edge_lost_only_when_every_admin_is() {
        assert_eq!(fold_admin_outcomes(&[CarrierEdgeLost, AdminAlive]), AdminAlive);
        assert_eq!(fold_admin_outcomes(&[CarrierEdgeLost, CarrierEdgeLost]), CarrierEdgeLost);
        assert_eq!(fold_admin_outcomes(&[CarrierEdgeLost, Unreachable]), Unreachable);
        assert_eq!(fold_admin_outcomes(&[]), Unreachable);
    }

    /// CONTRACT: the node-admin answering either probe stops the investigation: no second probe
    /// after probe 1, no decision.
    #[test]
    fn an_answering_admin_stops_the_investigation() {
        let log = walk(&mut ladder(), [AdminAlive, CarrierEdgeLost]);
        assert_eq!(at(&log, |s| matches!(s, Step::Stop { .. })), vec![10]);
        assert!(at(&log, |s| matches!(s, Step::Probe { n: 2, .. } | Step::Decide { .. })).is_empty(), "{log:?}");
        let log = walk(&mut ladder(), [CarrierEdgeLost, AdminAlive]);
        assert_eq!(at(&log, |s| matches!(s, Step::Stop { .. })), vec![20]);
        assert!(at(&log, |s| matches!(s, Step::Decide { .. })).is_empty(), "{log:?}");
    }

    /// CONTRACT: a backbone receipt cancels the investigation at any point, resets the interval, and
    /// the mesh is then tracked and probed afresh; a mesh heard before it was ever probed cancels
    /// silently.
    #[test]
    fn a_backbone_receipt_cancels_and_resets_the_interval() {
        let mut l = ladder();
        assert!(l.step("mesh2", tick(3)).iter().any(|s| matches!(s, Step::Track { .. })));
        assert!(l.step("mesh2", tick(0)).is_empty(), "heard before any probe: nothing to cancel loudly");
        for r in 0..=12 {
            l.step("mesh2", tick(r));
        }
        l.probed("mesh2", 1, CarrierEdgeLost, ms(12));
        assert_eq!(l.step("mesh2", tick(0)), vec![Step::Cancel { unheard_ms: 0, probes: 1 }]);
        assert!(l.step("mesh2", tick(1)).is_empty());
        let again: Vec<Step> = (2..=10).flat_map(|r| l.step("mesh2", tick(r))).collect();
        assert!(again.contains(&Step::Probe { n: 1, unheard_ms: ms(10) }), "a fresh interval probes again at 10: {again:?}");
    }

    /// CONTRACT: the rebirth of a peer mesh's node-admins is decided only by a `rebirth` decision: not
    /// by a hold, not by a mesh the ladder has not stepped yet, and not once the ladder forgot it.
    #[test]
    fn a_peer_meshs_rebirth_is_decided_only_by_a_rebirth_decision() {
        let mut l = ladder();
        assert!(!l.rebirth_decided("mesh2"), "not stepped yet");
        walk(&mut l, [CarrierEdgeLost, CarrierEdgeLost]);
        assert!(l.rebirth_decided("mesh2"));
        let mut held = ladder();
        walk(&mut held, [Unreachable, Unreachable]);
        assert!(!held.rebirth_decided("mesh2"), "a hold decides no rebirth");
        l.forget("mesh2");
        assert!(!l.rebirth_decided("mesh2"));
    }

    /// CONTRACT (#2803 detection): a fabric primary that loses its seat drops every investigation;
    /// the next one starts from its own view, so nothing it decided is decided again, and a mesh it
    /// never heard on the backbone is not held back at all.
    #[test]
    fn a_ladder_dropped_with_the_seat_starts_over_for_the_next_fabric_primary() {
        let mut first = ladder();
        walk(&mut first, [CarrierEdgeLost, CarrierEdgeLost]);
        assert!(first.rebirth_decided("mesh2"));
        first.clear();
        assert!(first.meshes().is_empty(), "nothing survives the seat");
        let mut next = ladder();
        assert!(!next.rebirth_decided("mesh2"), "the next primary has decided nothing yet");
        let log = walk(&mut next, [CarrierEdgeLost, CarrierEdgeLost]);
        assert_eq!(at(&log, |s| matches!(s, Step::Decide { rebirth: true, .. })), vec![30], "it decides once, from its own 30 rounds");
    }

    /// CONTRACT: two peer meshes unheard at once are two ladders; one mesh's probes, hold or
    /// decision never move the other's.
    #[test]
    fn two_unheard_meshes_run_independent_ladders() {
        let mut l = ladder();
        for r in 0..=40 {
            let a = l.step("mesh2", tick(r));
            for s in a {
                if let Step::Probe { n, unheard_ms } = s {
                    l.probed("mesh2", n, CarrierEdgeLost, unheard_ms);
                }
            }
            // mesh3 is heard throughout.
            assert!(l.step("mesh3", tick(0)).is_empty());
        }
        assert!(l.rebirth_decided("mesh2"));
        assert!(!l.rebirth_decided("mesh3"), "mesh3 was never decided");
    }

    /// CONTRACT (#2803 detection): forgetting the investigation of a mesh the backbone no longer
    /// carries (a retired mesh) returns; it never holds the ladder's lock while it takes it again,
    /// which would hold the fabric primary's watch and its drift check (both read the ladder) forever.
    #[test]
    fn forgetting_a_retired_mesh_does_not_deadlock_on_the_ladder_lock() {
        let ladder = Arc::new(Mutex::new(ladder()));
        ladder.lock().unwrap().step("mesh2", Duration::ZERO);
        let (tx, rx) = std::sync::mpsc::channel();
        let l = ladder.clone();
        std::thread::spawn(move || {
            forget_unheard_meshes(&l, &BTreeSet::new());
            let _ = tx.send(());
        });
        assert!(rx.recv_timeout(std::time::Duration::from_secs(5)).is_ok(), "forgetting a retired mesh never returned: the ladder lock is held by the loop that takes it");
        assert!(ladder.lock().unwrap().meshes().is_empty());
    }
}
