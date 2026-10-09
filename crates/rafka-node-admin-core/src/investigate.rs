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

/// The ruled rungs, in backbone rounds unheard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rungs {
    /// Unheard this long, the mesh is tracked (a round missed beyond the publication jitter).
    pub track: u64,
    pub probe1: u64,
    pub mark: u64,
    pub probe2: u64,
    pub decide: u64,
}

impl Rungs {
    /// 20 s, 30 s, 40 s and 60 s at the 2 s default round.
    pub const RULED: Rungs = Rungs { track: 2, probe1: 10, mark: 15, probe2: 20, decide: 30 };
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
    Track { rounds: u64 },
    /// The shared silent mark: nothing is sent.
    Mark { rounds: u64 },
    /// Make probe `n` (1 or 2).
    Probe { n: u8, rounds: u64 },
    /// The node-admin answered: the investigation stops.
    Stop { rounds: u64 },
    /// The decision: `rebirth` only when the latest probe answered `carrier-edge-lost`.
    Decide { rebirth: bool, latest: ProbeOutcome, rounds: u64 },
    /// The mesh was heard on the backbone again: the investigation is cancelled.
    Cancel { rounds: u64, probes: u8 },
}

#[derive(Debug, Default)]
struct State {
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

/// One investigation per peer mesh the fabric primary has heard on the backbone.
#[derive(Debug)]
pub struct Ladder {
    rungs: Rungs,
    meshes: BTreeMap<String, State>,
}

impl Ladder {
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
    pub fn rebirth_decided(&self, mesh: &str) -> bool {
        self.meshes.get(mesh).is_some_and(|s| s.verdict == Some(true))
    }

    /// What `mesh` being unheard for `rounds` asks next. One probe or the decision per call; the
    /// driver makes the probe, records it with [`Self::probed`], and asks again.
    pub fn step(&mut self, mesh: &str, rounds: u64) -> Vec<Step> {
        let r = self.rungs;
        let st = self.meshes.entry(mesh.to_string()).or_default();
        let mut out = Vec::new();
        if rounds < r.track {
            if st.tracked && (st.probes_made() > 0 || st.marked || st.verdict.is_some()) {
                out.push(Step::Cancel { rounds, probes: st.probes_made() });
            }
            *st = State::default();
            return out;
        }
        if !st.tracked {
            st.tracked = true;
            out.push(Step::Track { rounds });
        }
        if rounds >= r.mark && !st.marked {
            st.marked = true;
            out.push(Step::Mark { rounds });
        }
        if st.stopped || st.verdict.is_some() {
            return out;
        }
        if rounds >= r.probe1 && st.probes[0].is_none() {
            out.push(Step::Probe { n: 1, rounds });
        } else if rounds >= r.probe2 && st.probes[1].is_none() {
            out.push(Step::Probe { n: 2, rounds });
        } else if rounds >= r.decide {
            // The latest completed probe decides; an older carrier-edge-lost never overrides a
            // newer unreachable.
            let latest = st.probes[1].or(st.probes[0]).unwrap_or(ProbeOutcome::Unreachable);
            let rebirth = latest == ProbeOutcome::CarrierEdgeLost;
            st.verdict = Some(rebirth);
            out.push(Step::Decide { rebirth, latest, rounds });
        }
        out
    }

    /// Probe `n` of `mesh` found `outcome`. An answering node-admin stops the investigation.
    pub fn probed(&mut self, mesh: &str, n: u8, outcome: ProbeOutcome, rounds: u64) -> Option<Step> {
        let st = self.meshes.get_mut(mesh)?;
        st.probes[usize::from(n.clamp(1, 2)) - 1] = Some(outcome);
        (outcome == ProbeOutcome::AdminAlive).then(|| {
            st.stopped = true;
            Step::Stop { rounds }
        })
    }
}

/// A Ping is answered within two rounds or the member did not answer.
pub fn ping_budget(round: Duration) -> Duration {
    round * 2
}

/// The carried probe outlasts the carrier's one inner call (made with `CallOptions::default`) by two
/// rounds, so the carrier's answer, carrier-edge-lost included, reaches the origin before it gives
/// up: an inner dial to a dead node-admin runs to the inner call's whole budget.
pub fn carried_budget(round: Duration) -> Budget {
    match CallOptions::default().budget {
        Budget::Overall(inner) => Budget::Overall(inner + round * 2),
        Budget::Split { send, reply } => Budget::Split { send: send + round * 2, reply },
    }
}

/// How many members are asked in parallel.
pub const PROBE_FANOUT: usize = 3;

/// What the driver needs of its admin.
pub struct Watch {
    pub me: PathName,
    pub round: Duration,
    pub membership: Membership,
    pub topology: Arc<tokio::sync::RwLock<Topology>>,
    pub records: Arc<Records>,
    pub client: Arc<NodeRpcClient>,
    /// Does this node hold an Active Direct connection to the exact node?
    pub connected: Arc<dyn Fn(&NodeId) -> bool + Send + Sync>,
    pub ladder: Arc<Mutex<Ladder>>,
    /// The carrier each mesh's latest probe went through.
    pub carriers: Mutex<BTreeMap<String, PathName>>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// Run the investigation for as long as the task lives: once per round, while this admin is the
/// fabric primary and its view authorizes, every peer mesh it has heard on the backbone is stepped
/// down its ladder.
pub async fn run(w: Watch) {
    loop {
        tokio::time::sleep(w.round).await;
        let view = w.topology.read().await.clone();
        let fabric_primary = view.fabric_primary().is_some_and(|n| n.name == w.me);
        if !(fabric_primary && w.membership.authorizes()) {
            w.ladder.lock().unwrap().clear();
            w.records.set_peer_recovery(None);
            continue;
        }
        let book = &w.membership.book;
        let heard = book.backbone_meshes();
        forget_unheard_meshes(&w.ladder, &heard);
        let round_ms = w.round.as_millis().max(1) as u64;
        let mut tasks = Vec::new();
        for mesh in heard.into_iter().filter(|m| *m != w.me.mesh) {
            let Some(unheard) = book.mesh_unheard(&mesh, Instant::now()) else { continue };
            let rounds = unheard.as_millis() as u64 / round_ms;
            tasks.push(investigate(&w, &view, mesh, rounds, unheard));
        }
        futures_util::future::join_all(tasks).await;
        restore(&w, &view);
    }
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

async fn investigate(w: &Watch, view: &Topology, mesh: String, rounds: u64, unheard: Duration) {
    let at = now_ms().saturating_sub(unheard.as_millis() as u64);
    let steps = w.ladder.lock().unwrap().step(&mesh, rounds);
    for step in steps {
        match step {
            Step::Track { rounds } => {
                tracing::info_span!("rdm.node_admin.mesh.update.via-unheard", node = %w.me, mesh = %mesh, at, rounds, mark = false)
                    .in_scope(|| tracing::info!("no word of the mesh on the backbone: the investigation tracks it"));
            }
            Step::Mark { rounds } => {
                tracing::info_span!("rdm.node_admin.mesh.update.via-unheard", node = %w.me, mesh = %mesh, at, rounds, mark = true)
                    .in_scope(|| tracing::info!("the mesh is marked silent; nothing is sent"));
            }
            Step::Probe { n, rounds } => {
                let found = probe(w, view, &mesh, n).await;
                let stop = w.ladder.lock().unwrap().probed(&mesh, n, found.outcome, rounds);
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
                    rounds,
                )
                .in_scope(|| tracing::info!("one probe of the unheard mesh"));
                if let Some(Step::Stop { rounds }) = stop {
                    verdict(w, &mesh, found.outcome.as_str(), rounds, at, "the node-admin answered: the investigation stops");
                }
            }
            Step::Stop { .. } => {}
            Step::Decide { rebirth, latest, rounds } => {
                verdict(w, &mesh, latest.as_str(), rounds, at, if rebirth { "the latest probe says a member cannot reach its own node-admin: the mesh is reborn" } else { "no member could say: the mesh is held" });
                if rebirth {
                    let verdict_ms = w.membership.clock().now_rafka_ms();
                    let lost = view.cohort(&mesh, NodeKind::NodeAdmin).filter_map(|a| a.incarnation_id.clone()).collect();
                    w.records.set_peer_recovery(Some(crate::admin::PeerRecovery { mesh: mesh.clone(), verdict_rafka_ms: verdict_ms, opened_at: Instant::now(), lost }));
                    tracing::info_span!("rdm.node_admin.mesh.create.via-rebirth", node = %w.me, mesh = %mesh, at = now_ms(), fabric_status = "degraded", verdict_rafka_ms = verdict_ms)
                        .in_scope(|| tracing::info!("the existing Mesh Recovery opens; the fabric primary authors degraded"));
                }
            }
            Step::Cancel { rounds, probes } => {
                tracing::info_span!("rdm.node_admin.mesh.update.via-probe-verdict", node = %w.me, mesh = %mesh, outcome = "heard-again", at = now_ms(), rounds, probes)
                    .in_scope(|| tracing::info!("the mesh is heard on the backbone again: the investigation is cancelled"));
            }
        }
    }
}

fn verdict(w: &Watch, mesh: &str, outcome: &str, rounds: u64, _heard_at: u64, why: &str) {
    tracing::info_span!("rdm.node_admin.mesh.update.via-probe-verdict", node = %w.me, mesh = %mesh, outcome, at = now_ms(), rounds).in_scope(|| tracing::info!("{why}"));
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

/// Probe `mesh`'s node-admin: Ping its members, then Forward `ProbeNodeState` through one that
/// answered. Active connections are asked first, then path order; probe 2 prefers a member probe 1
/// did not use.
async fn probe(w: &Watch, view: &Topology, mesh: &str, n: u8) -> Found {
    let admin = view.cohort(mesh, NodeKind::NodeAdmin).filter(|a| a.incarnation_id.is_some()).min_by_key(|a| (!a.is_primary, a.name.to_string())).cloned();
    let Some(admin) = admin else {
        return Found { outcome: ProbeOutcome::Unreachable, target: String::new(), members: String::new(), carrier: String::new(), detail: format!("the view holds no node-admin of {mesh}") };
    };
    let mut members: Vec<Node> = view.nodes.iter().filter(|m| m.mesh == mesh && m.kind != NodeKind::NodeAdmin && m.status != NodeStatus::Leaving && m.status != NodeStatus::Restarting).cloned().collect();
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
        return Found { outcome: ProbeOutcome::Unreachable, target: admin.name.to_string(), members: members_asked, carrier: String::new(), detail: format!("no member answered the Ping ({why})") };
    };
    w.note_carrier(mesh, carrier.name.clone());
    let req = StatusRequest::ProbeNodeState { node_id: admin.node_id.clone(), incarnation: admin.incarnation_id.clone().expect("filtered on it") };
    let opts = CallOptions { budget: carried_budget(w.round), ..CallOptions::default() };
    let (out, _) = w.client.call_via::<Status>(&NodeTarget::ExactNode(carrier.node_id.clone()), &admin.node_id, &req, &opts).await;
    let (outcome, detail) = classify(&out, &admin.name);
    Found { outcome, target: admin.name.to_string(), members: members_asked, carrier: carrier.name.to_string(), detail }
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

    fn ladder() -> Ladder {
        Ladder::new(Rungs::RULED)
    }

    /// Walk the ruled timeline: `probes` are what probe 1 and probe 2 find.
    fn walk(l: &mut Ladder, probes: [ProbeOutcome; 2]) -> Vec<(u64, Step)> {
        let mut log = Vec::new();
        for rounds in 0..=40 {
            let mut asked = l.step("mesh2", rounds);
            // The driver asks again after a probe is recorded, as it does on the next round.
            let mut guard = 0;
            while !asked.is_empty() {
                for s in asked.drain(..) {
                    if let Step::Probe { n, .. } = &s {
                        let found = probes[usize::from(*n) - 1];
                        if let Some(stop) = l.probed("mesh2", *n, found, rounds) {
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
        assert!(l.step("mesh2", 3).iter().any(|s| matches!(s, Step::Track { .. })));
        assert!(l.step("mesh2", 0).is_empty(), "heard before any probe: nothing to cancel loudly");
        for r in 0..=12 {
            l.step("mesh2", r);
        }
        l.probed("mesh2", 1, CarrierEdgeLost, 12);
        assert_eq!(l.step("mesh2", 0), vec![Step::Cancel { rounds: 0, probes: 1 }]);
        assert!(l.step("mesh2", 1).is_empty());
        let again: Vec<Step> = (2..=10).flat_map(|r| l.step("mesh2", r)).collect();
        assert!(again.contains(&Step::Probe { n: 1, rounds: 10 }), "a fresh interval probes again at 10: {again:?}");
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
            let a = l.step("mesh2", r);
            for s in a {
                if let Step::Probe { n, rounds } = s {
                    l.probed("mesh2", n, CarrierEdgeLost, rounds);
                }
            }
            // mesh3 is heard throughout.
            assert!(l.step("mesh3", 0).is_empty());
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
        ladder.lock().unwrap().step("mesh2", 0);
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
