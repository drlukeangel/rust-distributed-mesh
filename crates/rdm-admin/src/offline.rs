//! The offline tickle (node-identity-and-state.md §7.3, i77 PRD row 18), ported from rafka-v2's
//! `node-admin/src/lib.rs::OfflineTickle`.
//!
//! A silent node is never restarted or deleted for being silent. Every node's own pruner marks it
//! `PendingReconnect` at the staleness floor. Every node-admin keeps the silence bookkeeping
//! (`first_seen`), so a node-admin that takes the seat mid-window tickles on the window already
//! running; only the node-admin holding its mesh's primary seat tickles. Half a floor after the
//! mark (floor + half a floor in all) it makes one QUIC connect, direct then via a peer. Each round
//! runs beside the loop, one per exact birth, and the first tick after it ends folds what it found
//! (node-recovery.md: one loop, on every gossip interval). An answer is `reachable-silent`: recorded,
//! no action. No path on round 1 opens the hold-down at the instant that no path is known, when
//! round 1's probes ended; no path again at least one staleness floor after that instant is TRUE
//! OFFLINE. A node that leaves the silent set has
//! returned. The `Dead` status is observer-inferred, never sent, never a reason to restart or delete.
//! The admin writes the node's status on its row in nodes.storage and shows it in the view.

use std::collections::{HashMap, HashSet};

/// One silent node of this tick's silent set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SilentNode {
    pub node_id: String,
    pub path: String,
    /// The exact birth's incarnation, as the watched record holds it (`None` for a record that holds
    /// none yet).
    pub incarnation: Option<crate::model::IncarnationId>,
    /// The mark, on this admin's local clock (ms): the instant this admin's digest book holds the
    /// birth went unheard past the staleness floor ([`silent_set`]). `None` for a birth the book holds
    /// no receipt of (a birth only a durable runtime row names), whose silence is first seen at the
    /// tick that lists it.
    pub marked_at_ms: Option<u64>,
}

/// The silent set of one tick: every watched birth, each with its mark read from this admin's digest
/// book at `now` (`now_ms` on the local clock): the birth's last receipt plus the staleness floor
/// `floor`, the instant its silence passed the floor (node-recovery.md "Detection and proof" step 1:
/// "Each node's pruner marks a member `PendingReconnect` once it is unheard past the staleness
/// floor"). A birth the book holds only from its mesh-primary's projection is unheard since the book
/// first held it. A birth the book holds nothing of, a receipt of another incarnation, or one heard
/// within the floor carries no mark.
pub(crate) fn silent_set<'a>(watched: impl IntoIterator<Item = &'a crate::model::Node>, book: &rdm_transport::membership::DigestBook, now: std::time::Instant, now_ms: u64, floor: std::time::Duration) -> Vec<SilentNode> {
    watched
        .into_iter()
        .map(|n| {
            let marked_at_ms = book
                .get_at(n.node_id.as_str(), now)
                .or_else(|| book.projected_at(n.node_id.as_str(), now))
                .filter(|(d, unheard)| Some(&d.node.incarnation) == n.incarnation_id.as_ref() && *unheard >= floor)
                .map(|(_, unheard)| now_ms.saturating_sub((unheard - floor).as_millis() as u64));
            SilentNode { node_id: n.node_id.to_string(), path: n.name.to_string(), incarnation: n.incarnation_id.clone(), marked_at_ms }
        })
        .collect()
}

/// A command its birth never received for a reachability reason (node-tickle.md step 1): the
/// mesh-primary marks the birth `PendingReconnect` at once, so its tickle ladder runs from this mark
/// instead of after the staleness floor. An observer mark: it ends when the birth is heard after it,
/// answers its tickle, or leaves the view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnreachableMark {
    /// The exact birth the command named.
    pub incarnation: crate::model::IncarnationId,
    /// When the command ended unsent.
    pub at: std::time::Instant,
    /// The same instant on the local clock, ms since the Unix epoch.
    pub at_ms: u64,
    /// `connect-refused`, `connect-timed-out`, `connect-failed` or `endpoint-unresolved`.
    pub reason: &'static str,
}

/// Whether a reachability `NotSent` marks a birth whose mesh-primary holds `recorded` for it
/// (node-recovery.md step 1): an observer mark beside a live status (`Pending`, `ReadyForTraffic`, or
/// none held), never over a lifecycle state the node reported and the mesh-primary recorded
/// (connections-deleting, stopping, stopped).
pub(crate) fn marks_over(recorded: Option<rdm_core::MemberStatus>) -> bool {
    match recorded {
        Some(rdm_core::MemberStatus::ConnectionsDeleting | rdm_core::MemberStatus::Leaving) => false,
        Some(rdm_core::MemberStatus::Pending | rdm_core::MemberStatus::ReadyForTraffic) | None => true,
    }
}

/// The marks a mesh-primary holds, by node id: written by its executor's observer, read by its tick.
pub type UnreachableMarks = std::sync::Arc<std::sync::Mutex<std::collections::HashMap<crate::model::NodeId, UnreachableMark>>>;

/// The marks of `mesh`'s births that stand at `now`: a mark ends when the view no longer holds that
/// exact birth of `mesh`, or when `book` heard the birth after the mark. The ended ones are dropped
/// from `marks`.
pub(crate) fn held_marks(marks: &UnreachableMarks, view: &crate::topology::Topology, mesh: &str, book: &rdm_transport::membership::DigestBook, now: std::time::Instant) -> std::collections::HashMap<crate::model::NodeId, UnreachableMark> {
    let mut marks = marks.lock().unwrap();
    marks.retain(|id, m| {
        let held = view.births().any(|n| &n.node_id == id && n.incarnation_id.as_ref() == Some(&m.incarnation) && n.mesh == mesh);
        let heard_after = book.get_at(id.as_str(), now).is_some_and(|(d, unheard)| d.node.incarnation == m.incarnation && now.saturating_duration_since(m.at) > unheard);
        held && !heard_after
    });
    marks.clone()
}

/// Every marked birth the view holds, other than this admin itself, is watched `PendingReconnect`.
pub(crate) fn watch_marked(watched: &mut std::collections::BTreeMap<String, crate::model::Node>, marks: &std::collections::HashMap<crate::model::NodeId, UnreachableMark>, view: &crate::topology::Topology, me: &crate::model::NodeId) {
    for (id, m) in marks {
        if let Some(n) = view.births().find(|n| &n.node_id == id && n.incarnation_id.as_ref() == Some(&m.incarnation) && &n.node_id != me) {
            watched.entry(id.to_string()).or_insert_with(|| crate::model::Node { status: crate::model::NodeStatus::PendingReconnect, ..n.clone() });
        }
    }
}

/// A silent birth's mark is the earlier of its book's floor mark and its unreachable mark.
pub(crate) fn marked_from(mut s: SilentNode, marks: &std::collections::HashMap<crate::model::NodeId, UnreachableMark>) -> SilentNode {
    if let Some((_, m)) = marks.iter().find(|(id, m)| id.as_str() == s.node_id && s.incarnation.as_ref() == Some(&m.incarnation)) {
        s.marked_at_ms = Some(s.marked_at_ms.map_or(m.at_ms, |at| at.min(m.at_ms)));
    }
    s
}

/// How many live peers the via-peer step asks.
pub const VIA_PEER_TICKLE_FANOUT: usize = 2;

/// The longest one offline-tickle round takes when each call waits `per_call`: the direct connect
/// (a ping that fails, or a ping that answers and the status kick after it), then one via-peer rung
/// with every carrier probed at once (node-recovery.md step 2). The fan-out adds no wait.
pub fn tickle_round(per_call: std::time::Duration) -> std::time::Duration {
    per_call * 2
}

/// [`tickle_round`] at the default call budget, the one every call of the tickle spends.
pub fn tickle_round_at_default_budget() -> std::time::Duration {
    match rdm_transport::CallOptions::default().budget {
        rdm_transport::Budget::Overall(d) => tickle_round(d),
        other => panic!("the default call budget is not one overall deadline: {other:?}"),
    }
}

/// The mesh-primary's recovery tick (node-recovery.md "Detection and proof": "The mesh-primary runs
/// one loop for its own members, on every gossip interval, never on a timer of its own"): the loop
/// runs a pass, then waits here for the next one. Passes start on the grid of the gossip interval
/// laid from the cadence's start, whatever a pass spends: the next pass is one interval after the
/// previous one STARTED, never one interval after it ended.
///
/// A pass that runs past a grid point ([`tokio::time::MissedTickBehavior::Skip`]) is followed by the
/// one pass it owes at once, then the passes return to the same grid; the ticks it ran past are not
/// run back to back (`Burst`: passes that would read the same view an instant apart), and the grid
/// does not move to the late pass (`Delay`: one interval after the late pass, which is the interval
/// plus the pass's own work again).
pub(crate) struct Cadence {
    ticks: tokio::time::Interval,
}

impl Cadence {
    /// The cadence at `period`, the gossip interval; the first pass is the caller's own, and the
    /// first wait ends one interval after this call. A zero period has no interval to run on and is
    /// refused by name.
    pub(crate) fn every(period: std::time::Duration) -> Result<Self, String> {
        if period.is_zero() {
            return Err("the gossip interval (RDM_GOSSIP_INTERVAL_MS) is 0 ms: the mesh-primary's recovery tick runs on every gossip interval, and a zero interval has none".to_string());
        }
        let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Ok(Self { ticks })
    }

    /// Wait for the next pass.
    pub(crate) async fn next(&mut self) {
        self.ticks.tick().await;
    }
}

/// Verdict of asking live peer nodes to tickle a target node.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ViaPeerVerdict {
    /// A peer answered the tickle: reachable via that peer, unreachable from this observer.
    Answered { via: String },
    /// No candidate path answered: either no candidates, or every asked peer failed or refused.
    NoPath { asked: Vec<String> },
    /// The candidates could not be read: no status change.
    CandidatesUnreadable(String),
}

/// The via-peer rung over one exact birth (node-recovery.md step 2, ruling R-SW1), run by the
/// mesh-primary's tickle and by the seat watch's ladder alike: the kick through every carrier in
/// `peers` at once, each probe naming the same `(node_id, incarnation)`. The first answer from the
/// exact birth (`seat_watch::birth_answered`) is `Answered` by its carrier and drops the probes still
/// running; a failed probe cancels none of the others; with every probe ended unanswered it is
/// `NoPath`, naming every carrier asked. A failure, a deadline included, proves nothing.
pub(crate) async fn via_peer_verdict<P>(node_id: &crate::model::NodeId, incarnation: &crate::model::IncarnationId, peers: Vec<(String, P)>) -> ViaPeerVerdict
where
    P: std::future::Future<Output = Option<rdm_core::status::StatusReply>>,
{
    use futures_util::StreamExt as _;
    let asked = peers.iter().map(|(carrier, _)| carrier.clone()).collect();
    let mut running: futures_util::stream::FuturesUnordered<_> = peers.into_iter().map(|(carrier, probe)| async move { (carrier, probe.await) }).collect();
    while let Some((carrier, reply)) = running.next().await {
        if crate::seat_watch::birth_answered(reply.as_ref(), node_id, incarnation) {
            return ViaPeerVerdict::Answered { via: carrier };
        }
    }
    ViaPeerVerdict::NoPath { asked }
}

/// What one [`OfflineTickle::tick`] did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct OfflineTickReport {
    pub tickled: Vec<String>,
    pub reachable_silent: Vec<String>,
    pub offline: Vec<String>,
    /// Silent nodes heard again: the view holds them live.
    pub returned: Vec<String>,
    /// Silent nodes the view no longer holds (deleted, or recovered under a new node.id at their
    /// path.name): not heard again, gone.
    pub gone: Vec<String>,
}

/// How one round of the ladder ended (node-recovery.md step 2): the direct connect answered, or it
/// failed and the via-peer rung gave its verdict.
#[derive(Debug)]
enum RoundEnd {
    /// The exact birth answered the direct connect.
    Direct,
    /// The direct connect failed with `error`; `verdict` is the via-peer rung's, given at
    /// `verdict_at_ms` on the tick clock (the instant the round's probes ended).
    ViaPeer { error: String, verdict: ViaPeerVerdict, verdict_at_ms: u64 },
}

/// One round of the ladder, running beside the loop: started by one tick, folded by a later one.
#[derive(Debug)]
struct Round {
    /// The clock of the tick that started it, named on the round's hold-down span; the hold-down
    /// itself opens at the round's verdict (`RoundEnd::ViaPeer::verdict_at_ms`).
    started_at_ms: u64,
    path: String,
    /// The mark (or first sighting) the round ran on, named on its spans.
    seen_at: u64,
    /// `None`: the round was cancelled at a stop's park.
    task: tokio::task::JoinHandle<Option<RoundEnd>>,
}

/// The exact birth a round runs for: node id and incarnation.
type Birth = (String, Option<crate::model::IncarnationId>);

#[derive(Default)]
pub(crate) struct OfflineTickle {
    /// Every node held as silent: node id -> the instant (ms) it was first seen silent. Kept
    /// whether or not this node-admin holds the seat.
    first_seen: HashMap<String, u64>,
    /// Silent nodes whose round 1 returned NoPath, waiting for round 2: node id -> no-path-since (ms),
    /// the instant round 1's verdict came back, after its probes spent their budgets.
    hold_down: HashMap<String, u64>,
    /// Silent nodes whose tickle has settled (answered, or true offline): not tickled again until
    /// they leave and re-enter the silent set.
    settled: HashSet<String>,
    /// The nodes held true offline now, by node id.
    offline: HashSet<String>,
    /// The round in flight for each exact birth, at most one: a birth whose round runs is not
    /// tickled again until a tick folds the round.
    running: HashMap<Birth, Round>,
    /// This node-admin's path.name, named on every span it emits: a seat handed over mid-window is
    /// read from which admin ran each round.
    observer: String,
    /// The admin's stop gate: a stop's park retires the rounds in flight.
    gate: crate::stop_gate::StopGate,
    /// Node ids held TRUE OFFLINE whose runtime still runs (`held-running`) and are due another
    /// probe round on this tick (R-FM1).
    reprobe: HashSet<String>,
}

impl Drop for OfflineTickle {
    /// The rounds are the tickle's: their probes end with it.
    fn drop(&mut self) {
        for round in self.running.values() {
            round.task.abort();
        }
    }
}

impl OfflineTickle {
    #[cfg(test)]
    pub fn new() -> Self {
        Self::default()
    }

    /// The tickle of node-admin `observer` (its path.name), whose rounds a park of `gate` retires.
    pub fn observed_by(observer: String, gate: crate::stop_gate::StopGate) -> Self {
        let mut tickle = Self::default();
        tickle.observer = observer;
        tickle.gate = gate;
        tickle
    }

    /// Schedule another probe round on the next tick for a `TRUE OFFLINE` birth whose runtime
    /// still runs (`held-running`, R-FM1).
    pub fn reprobe(&mut self, id: String) {
        self.reprobe.insert(id);
    }

    #[cfg(test)]
    pub fn is_in_hold_down(&self, id: &str) -> bool {
        self.hold_down.contains_key(id)
    }

    /// Is this node held true offline?
    #[cfg(test)]
    pub(crate) fn is_offline(&self, id: &str) -> bool {
        self.offline.contains(id)
    }

    /// How many rounds are in flight, and how many of them have ended (not yet folded).
    #[cfg(test)]
    pub(crate) fn rounds(&self) -> (usize, usize) {
        (self.running.len(), self.running.values().filter(|r| r.task.is_finished()).count())
    }

    /// One tick. `holds_seat` is this node-admin's primary seat, sampled once for the tick;
    /// `silent` is this tick's silent set (every `PendingReconnect` node it watches); `in_view` says
    /// whether the view still holds a node id at all (a node that left the silent set is heard
    /// again only if it does); `now_ms` is the tick's start instant, and an instant inside the tick
    /// (a verdict that came back after awaited probes) is `now_ms` plus the time the tick has run;
    /// `probe_delay_ms`
    /// is how long a node stays silent before its one tickle; `staleness_floor_ms` is the minimum
    /// delay between round 1 and round 2; `tickle` makes the direct connect and `via_peer` the
    /// via-peer step. `observe` reads iroh's local view of a node once for the answered and the
    /// failed span (`rdm_transport::iroh_obs`); it decides nothing.
    ///
    /// A tick waits on no probe (node-recovery.md: one loop, on every gossip interval). Each round it
    /// starts runs beside the loop, at once for every member due, so each member's connect is made
    /// half a floor after its own mark (step 2), whatever another member's connect spends; the first
    /// tick after a round ends folds what it found, and nothing is decided before that. A round ends
    /// with its birth's silence and with the seat: its probes are dropped and nothing of it is folded.
    #[allow(clippy::too_many_arguments)]
    pub async fn tick<P, T, TF, V, VF, O, OF>(
        &mut self,
        now_ms: u64,
        holds_seat: bool,
        silent: Vec<SilentNode>,
        in_view: P,
        probe_delay_ms: u64,
        staleness_floor_ms: u64,
        tickle: T,
        via_peer: V,
        observe: O,
    ) -> OfflineTickReport
    where
        P: Fn(&str) -> bool,
        T: Fn(String) -> TF,
        TF: std::future::Future<Output = Result<(), String>> + Send + 'static,
        V: Fn(String) -> VF,
        VF: std::future::Future<Output = ViaPeerVerdict> + Send + 'static,
        O: Fn(String) -> OF,
        OF: std::future::Future<Output = rdm_transport::iroh_obs::IrohObservation>,
    {
        let began = tokio::time::Instant::now();
        let mut report = OfflineTickReport::default();
        let silent_ids: HashSet<String> = silent.iter().map(|n| n.node_id.clone()).collect();
        // A birth this tick does not list silent (heard again, gone, or another birth now at its node
        // id), and every birth while this admin does not hold the seat: its round's probes are
        // dropped (step 2: an answer "cancels the probes still running"; a new holder runs its own).
        let births: HashSet<Birth> = silent.iter().map(|n| (n.node_id.clone(), n.incarnation.clone())).collect();
        self.running.retain(|birth, round| {
            let keep = holds_seat && births.contains(birth);
            if !keep {
                round.task.abort();
            }
            keep
        });
        let left: Vec<String> = self.first_seen.keys().filter(|id| !silent_ids.contains(*id)).cloned().collect();
        self.first_seen.retain(|id, _| silent_ids.contains(id));
        self.offline.retain(|id| silent_ids.contains(id));
        for id in left {
            // Heard again only while the view still holds the node; a node id the view no longer
            // holds was deleted (or recovered under a new node.id at its path.name): gone.
            if in_view(&id) {
                tracing::info_span!("rdm.node_admin.node.update.via-offline-returned", node_id = %id, observer = %self.observer, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!(node_id = %id, "a silent node is heard again: its tickle and its status end"));
                report.returned.push(id);
            } else {
                tracing::info_span!("rdm.node_admin.node.remove.via-offline-gone", node_id = %id, observer = %self.observer, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!(node_id = %id, "a silent node left the view: its tickle and its status end, it was not heard again"));
                report.gone.push(id);
            }
        }
        if holds_seat {
            self.hold_down.retain(|id, _| silent_ids.contains(id));
            self.settled.retain(|id| silent_ids.contains(id));
            self.reprobe.retain(|id| silent_ids.contains(id));
        } else {
            self.hold_down.clear();
            self.settled.clear();
            self.reprobe.clear();
        }
        // The window runs from the mark the admin's book holds for the birth (node-recovery.md step 2:
        // "Half a floor after the mark"); a birth with no mark is first seen at this tick.
        for n in &silent {
            self.first_seen.entry(n.node_id.clone()).or_insert(n.marked_at_ms.unwrap_or(now_ms));
        }
        if !holds_seat {
            return report;
        }
        // Every round that ended since the last tick, folded in the order the silent set lists its birth.
        for n in &silent {
            let birth = (n.node_id.clone(), n.incarnation.clone());
            if !self.running.get(&birth).is_some_and(|r| r.task.is_finished()) {
                continue;
            }
            let Some(Round { started_at_ms, path, seen_at, task }) = self.running.remove(&birth) else { continue };
            // A round that panicked takes the loop with it, as one run inside the pass did. A round
            // cancelled at a stop's park found nothing: nothing is folded, and the birth is due again.
            let end = match task.await {
                Ok(Some(end)) => end,
                Ok(None) => continue,
                Err(e) => std::panic::resume_unwind(e.into_panic()),
            };
            let id = n.node_id.clone();
            match end {
                RoundEnd::Direct => {
                    self.hold_down.remove(&id);
                    self.offline.remove(&id);
                    self.settled.insert(id.clone());
                    let seen = observe(id.clone()).await;
                    tracing::info_span!(
                        "rdm.node_admin.node.resolve.via-offline-tickle-answered",
                        node_id = %id,
                        path = %path,
                        observer = %self.observer,
                        first_seen_ms = seen_at as i64,
                        iroh_known_addrs = %seen.known_addrs,
                        iroh_active_addrs = %seen.active_addrs,
                        "otel.kind" = "internal"
                    )
                        .in_scope(|| tracing::info!(node_id = %id, "offline tickle answered — reachable-silent, recorded, no action"));
                    report.reachable_silent.push(id);
                }
                RoundEnd::ViaPeer { error: e, verdict, verdict_at_ms } => match verdict {
                    ViaPeerVerdict::Answered { via } => {
                        self.hold_down.remove(&id);
                        self.offline.remove(&id);
                        self.settled.insert(id.clone());
                        tracing::info_span!("rdm.node_admin.node.resolve.via-peer-tickle-answered", node_id = %id, path = %path, via = %via, error = %e, observer = %self.observer, first_seen_ms = seen_at as i64, "otel.kind" = "internal")
                            .in_scope(|| tracing::info!(node_id = %id, via = %via, error = %e, "offline tickle failed direct but answered via peer — unreachable from me, no record"));
                        report.reachable_silent.push(id);
                    }
                    ViaPeerVerdict::CandidatesUnreadable(reason) => {
                        tracing::info_span!("rdm.node_admin.node.reject.via-peer-tickle-candidates-unreadable", node_id = %id, path = %path, reason = %reason, error = %e, observer = %self.observer, "otel.kind" = "internal")
                            .in_scope(|| tracing::warn!(node_id = %id, reason = %reason, error = %e, "via-peer candidates unreadable — no status change, hold-down left as it was"));
                    }
                    ViaPeerVerdict::NoPath { asked } => {
                        if self.offline.contains(&id) {
                            self.settled.insert(id.clone());
                        } else if let Some(no_path_since) = self.hold_down.remove(&id) {
                            self.settled.insert(id.clone());
                            self.offline.insert(id.clone());
                            let seen = observe(id.clone()).await;
                            tracing::info_span!(
                                "rdm.node_admin.node.resolve.via-offline-tickle-failed",
                                node_id = %id,
                                path = %path,
                                error = %e,
                                iroh_known_addrs = %seen.known_addrs,
                                iroh_active_addrs = %seen.active_addrs,
                                round = 2i64,
                                first_no_path_ms = no_path_since as i64,
                                observer = %self.observer,
                                first_seen_ms = seen_at as i64,
                                "otel.kind" = "internal",
                            )
                            .in_scope(|| tracing::info!(node_id = %id, error = %e, "offline tickle failed round 2 — TRUE OFFLINE, no restart, no delete"));
                            report.offline.push(id);
                        } else {
                            // The hold-down runs from the instant no path was known: the round's
                            // verdict, when its probes ended, never the tick that started it.
                            self.hold_down.insert(id.clone(), verdict_at_ms);
                            let asked = asked.join(",");
                            tracing::info_span!("rdm.node_admin.node.update.via-offline-hold-down-opened", node_id = %id, path = %path, asked = %asked, observer = %self.observer, first_seen_ms = seen_at as i64, no_path_at_ms = verdict_at_ms as i64, round_began_ms = started_at_ms as i64, "otel.kind" = "internal")
                                .in_scope(|| tracing::info!(node_id = %id, asked = %asked, "offline hold down opened after round 1 no path"));
                        }
                    }
                },
            }
        }
        for SilentNode { node_id: id, path, incarnation, .. } in silent {
            // The mark (or, with none, the instant this admin first saw the node silent): a seat taken
            // mid-window tickles on it.
            let seen_at = self.first_seen.get(&id).copied().unwrap_or(now_ms);
            let reprobe = self.reprobe.remove(&id);
            if now_ms.saturating_sub(seen_at) < probe_delay_ms || (self.settled.contains(&id) && !reprobe) {
                continue;
            }
            let birth = (id.clone(), incarnation);
            if self.running.contains_key(&birth) {
                continue;
            }
            if let Some(&no_path_since) = self.hold_down.get(&id) {
                if now_ms < no_path_since + staleness_floor_ms {
                    continue;
                }
            }
            report.tickled.push(id.clone());
            // The round: the direct connect, and only when it fails, the via-peer rung, its probes
            // under one span of this round. It takes no lock and waits on nothing of this admin's; a
            // stop's park cancels it where it stands (`StopGate::until_parked`), and it never resumes.
            let (direct, carried) = (tickle(id.clone()), via_peer(id.clone()));
            let (node_id, at, observer) = (id.clone(), path.clone(), self.observer.clone());
            // The verdict's instant on the tick clock: this tick's `now_ms` plus the time since it began.
            let (tick_ms, tick_began) = (now_ms, began);
            let (cancelled_id, cancelled_path, cancelled_by) = (id.clone(), path.clone(), self.observer.clone());
            let round = self.gate.until_parked(async move {
                match direct.await {
                    Ok(()) => RoundEnd::Direct,
                    Err(error) => {
                        let step = tracing::info_span!("rdm.node_admin.node.resolve.via-peer-tickle", node_id = %node_id, path = %at, observer = %observer, verdict = tracing::field::Empty, "otel.kind" = "internal");
                        let verdict = tracing::Instrument::instrument(carried, step.clone()).await;
                        step.record("verdict", match &verdict {
                            ViaPeerVerdict::Answered { .. } => "answered",
                            ViaPeerVerdict::NoPath { .. } => "no-path",
                            ViaPeerVerdict::CandidatesUnreadable(_) => "candidates-unreadable",
                        });
                        let verdict_at_ms = tick_ms + tick_began.elapsed().as_millis() as u64;
                        RoundEnd::ViaPeer { error, verdict, verdict_at_ms }
                    }
                }
            });
            let task = tokio::spawn(async move {
                let end = round.await;
                if end.is_none() {
                    tracing::info!(node_id = %cancelled_id, path = %cancelled_path, observer = %cancelled_by, "the tickle round was cancelled at the stop's park: it proves nothing, and the birth is due again");
                }
                end
            });
            self.running.insert(birth, Round { started_at_ms: now_ms, path, seen_at, task });
        }
        report
    }
}

/// The recovery of a TRUE OFFLINE birth (`follow`: the provider's proof, the submit, and the Build
/// followed to its end, `RecoverySubmitter::recover_if_proven`), run beside the mesh-primary's loop.
/// A stop's park cancels it where it stands, the view lock it holds or waits on included, and does not
/// wait for it (`StopGate::until_parked`); a submit it already made is answered AlreadyApplied or
/// BuildInProgress when it comes again. The birth leaves `in_flight` either way, so the first tick
/// that finds it due again, after a start or at the seat's next holder, submits it again.
pub(crate) fn spawn_recovery_follower<F>(gate: &crate::stop_gate::StopGate, in_flight: std::sync::Arc<std::sync::Mutex<HashSet<(crate::model::NodeId, crate::model::IncarnationId)>>>, birth: (crate::model::NodeId, crate::model::IncarnationId), follow: F) -> tokio::task::JoinHandle<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let follow = gate.until_parked(follow);
    tokio::spawn(async move {
        if follow.await.is_none() {
            tracing::info!(node_id = %birth.0, incarnation = %birth.1 .as_str(), "the recovery follower was cancelled at the stop's park: the next tick that finds the birth due submits it again");
        }
        in_flight.lock().unwrap().remove(&birth);
    })
}

/// Write the status the mesh primary holds for a silent birth (`PendingReconnect`, `Dead`, or
/// `None` once it is heard again) as that birth's own silence row in status.storage: a blind put of
/// its `(node_id, incarnation)` key, the one writer of that fact. No other row is touched and no
/// row is read back. A held record with no incarnation yet has no birth to key the row by: that is
/// named, never a silent skip.
///
/// Each refusal is its own span, named where it happened: `via-no-birth` for a record with no
/// incarnation, `via-status-storage` (the birth, the status, the store's own cause) for a put the
/// store refused. Nothing is put again from here: the caller's memo still holds the last status
/// that stands, and node-recovery.md's next step (the provider's exact-exit proof) does not wait on
/// this row ("Dead is observer-inferred and is never itself a reason to act").
pub(crate) async fn put_silence_status(store: &dyn crate::status_storage::StatusStorage, node: &crate::model::Node, status: Option<crate::model::NodeStatus>) -> Result<bool, String> {
    let written = silence_word(status);
    let Some(incarnation_id) = node.incarnation_id.clone() else {
        let why = format!("{} ({}) holds no incarnation: no birth to key its silence row by", node.name, node.node_id);
        tracing::info_span!("rdm.node_admin.node.reject.via-no-birth", node = %node.name, node_id = %node.node_id, status = written, store = "status.storage", error = %why)
            .in_scope(|| tracing::info!("the node's silence status names no birth: no row is written"));
        return Err(why);
    };
    let row = crate::status_storage::NodeSilenceRow { node_id: node.node_id.clone(), name: node.name.clone(), incarnation_id: incarnation_id.clone(), status };
    if let Err(e) = store.put_node_silence(&row).await {
        tracing::info_span!("rdm.node_admin.node.reject.via-status-storage", node = %node.name, node_id = %node.node_id, incarnation_id = %incarnation_id.as_str(), status = written, store = "status.storage", error = %e)
            .in_scope(|| tracing::info!("status.storage refused the birth's silence row"));
        return Err(format!("status.storage put of {} ({}) silence row at {} ({written}): {e}", node.name, node.node_id, incarnation_id.as_str()));
    }
    Ok(true)
}

/// The silence status as a span names it: the view's own word, or `cleared` for a birth heard again.
fn silence_word(status: Option<crate::model::NodeStatus>) -> &'static str {
    use crate::model::NodeStatus::*;
    match status {
        None => "cleared",
        Some(Pending) => "pending",
        Some(ReadyForTraffic) => "ready-for-traffic",
        Some(ConnectionsDeleting) => "connections-deleting",
        Some(Leaving) => "leaving",
        Some(PendingReconnect) => "pending-reconnect",
        Some(Restarting) => "restarting",
        Some(Dead) => "dead",
        Some(Deleted) => "deleted",
    }
}

/// Record the provider's proof that a birth of the mesh primary's own mesh exited
/// (node-recovery.md "Exited is the proof, recorded durably"): the birth's own exit-proof row in
/// status.storage, a blind put of its `(node_id, incarnation)` key. Nothing is read back. A held
/// record with no incarnation names no birth: that is named, never a silent skip.
pub(crate) async fn put_exit_proof(store: &dyn crate::status_storage::StatusStorage, node: &crate::model::Node, exit_code: Option<i32>, proof: &str, observed_at_ms: u64) -> Result<crate::status_storage::NodeExitRow, String> {
    let Some(incarnation_id) = node.incarnation_id.clone() else {
        return Err(format!("{} ({}) holds no incarnation: no birth to key its exit-proof row by", node.name, node.node_id));
    };
    let row = crate::status_storage::NodeExitRow { node_id: node.node_id.clone(), name: node.name.clone(), incarnation_id, exit_code, proof: proof.to_string(), observed_at_ms };
    store.put_node_exit(&row).await.map_err(|e| format!("status.storage put of {} ({}) exit-proof row: {e}", node.name, node.node_id))?;
    Ok(row)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// CONTRACT (node-tickle.md step 1; i144 R9): a command its birth never received for a
    /// reachability reason marks the birth `PendingReconnect` beside a live status, and never over a
    /// lifecycle state the node reported and its mesh-primary recorded (stopping, connections-deleting).
    // @feature: node-lifecycle
    #[test]
    fn a_pending_reconnect_mark_never_overwrites_an_admitted_stopping() {
        use rdm_core::MemberStatus as M;
        for live in [None, Some(M::Pending), Some(M::ReadyForTraffic)] {
            assert!(marks_over(live), "{live:?}: marked");
        }
        for lifecycle in [M::Leaving, M::ConnectionsDeleting] {
            assert!(!marks_over(Some(lifecycle)), "{lifecycle:?}: kept, never overwritten");
        }
    }

    const FLOOR: u64 = 30_000;
    const PROBE_DELAY: u64 = FLOOR / 2;

    fn silent(id: &str) -> Vec<SilentNode> {
        vec![SilentNode { node_id: id.into(), path: format!("mesh1.rpc.{id}"), incarnation: None, marked_at_ms: None }]
    }

    async fn tick(t: &mut OfflineTickle, now: u64, seat: bool, set: Vec<SilentNode>, direct: bool, via: ViaPeerVerdict) -> OfflineTickReport {
        tick_observed(t, now, seat, set, direct, via, &std::sync::atomic::AtomicUsize::new(0)).await
    }

    /// `tick`, counting every iroh observation read in `reads`. One round as the loop runs it, at
    /// one clock reading `now`: the tick that starts what is due, the rounds it started run to their
    /// end beside it (every probe here answers at once), and the next tick, which folds them. The
    /// two ticks' reports, joined in order.
    async fn tick_observed(t: &mut OfflineTickle, now: u64, seat: bool, set: Vec<SilentNode>, direct: bool, via: ViaPeerVerdict, reads: &std::sync::atomic::AtomicUsize) -> OfflineTickReport {
        let once = async |t: &mut OfflineTickle| {
            t.tick(
                now,
                seat,
                set.clone(),
                |_| true,
                PROBE_DELAY,
                FLOOR,
                |_| async move { if direct { Ok(()) } else { Err("no answer".to_string()) } },
                |_| {
                    let via = via.clone();
                    async move { via }
                },
                |_| {
                    reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    async { rdm_transport::iroh_obs::IrohObservation::unavailable() }
                },
            )
            .await
        };
        let mut report = once(t).await;
        while t.rounds().1 < t.rounds().0 {
            tokio::task::yield_now().await;
        }
        let folded = once(t).await;
        report.tickled.extend(folded.tickled);
        report.reachable_silent.extend(folded.reachable_silent);
        report.offline.extend(folded.offline);
        report.returned.extend(folded.returned);
        report.gone.extend(folded.gone);
        report
    }

    /// CONTRACT (A1, diagnostics only): iroh is read exactly once per answered or round-2 failed
    /// event, never on a tick that records neither, and the reports are the tickle's own: the
    /// answered node is reachable-silent and round 2 with no path is TRUE OFFLINE.
    #[tokio::test(start_paused = true)]
    async fn iroh_is_read_once_per_answered_or_failed_event_and_decides_nothing() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let reads = AtomicUsize::new(0);
        let mut t = OfflineTickle::new();
        assert!(tick_observed(&mut t, 0, true, silent("a"), true, no_path(), &reads).await.tickled.is_empty());
        assert_eq!(reads.load(Ordering::SeqCst), 0, "no event, no read");
        let r = tick_observed(&mut t, PROBE_DELAY, true, silent("a"), true, no_path(), &reads).await;
        assert_eq!(r.reachable_silent, vec!["a".to_string()]);
        assert_eq!(reads.load(Ordering::SeqCst), 1, "one read for the answered event");

        let reads = AtomicUsize::new(0);
        let mut t = OfflineTickle::new();
        tick_observed(&mut t, 0, true, silent("b"), false, no_path(), &reads).await;
        let r1 = tick_observed(&mut t, PROBE_DELAY, true, silent("b"), false, no_path(), &reads).await;
        assert!(r1.offline.is_empty(), "round 1 opens the hold-down");
        assert_eq!(reads.load(Ordering::SeqCst), 0, "the hold-down opening is not an observed event");
        let r2 = tick_observed(&mut t, PROBE_DELAY + FLOOR, true, silent("b"), false, no_path(), &reads).await;
        assert_eq!(r2.offline, vec!["b".to_string()], "round 2 is TRUE OFFLINE, as without the read");
        assert_eq!(reads.load(Ordering::SeqCst), 1, "one read for the failed event");
    }

    mod cadence {
        //! The recovery tick's cadence (node-recovery.md "Detection and proof": "The mesh-primary
        //! runs one loop for its own members, on every gossip interval"), under paused time: each
        //! pass spends a known time, and the clock moves only while the loop waits.
        use super::super::Cadence;
        use std::time::Duration;
        use tokio::time::{sleep, Instant};

        /// The gate's gossip interval (`RDM_GOSSIP_INTERVAL_MS=500`).
        const INTERVAL: Duration = Duration::from_millis(500);

        /// Run one pass per spent time in `work`, each followed by the cadence's wait, and return
        /// the instant each pass started, measured from the loop's start.
        async fn pass_starts(work: &[Duration]) -> Vec<Duration> {
            let start = Instant::now();
            let mut cadence = Cadence::every(INTERVAL).expect("a non-zero interval");
            let mut starts = Vec::new();
            for spent in work {
                starts.push(start.elapsed());
                sleep(*spent).await;
                cadence.next().await;
            }
            starts
        }

        fn ms(v: &[u64]) -> Vec<Duration> {
            v.iter().map(|m| Duration::from_millis(*m)).collect()
        }

        /// CONTRACT (node-recovery.md "Detection and proof", on every gossip interval): a pass that
        /// spends 100 ms leaves the next pass on the interval's grid, starting 500 ms after the
        /// previous pass STARTED: 0, 500, 1000 ms. Must NOT happen: the next pass starting one
        /// interval after the previous pass ENDED (0, 600, 1200 ms), its own work added to each tick.
        #[tokio::test(start_paused = true)]
        async fn the_recovery_tick_starts_every_pass_on_the_gossip_interval_grid() {
            let w = Duration::from_millis(100);
            assert_eq!(pass_starts(&[w, w, w]).await, ms(&[0, 500, 1000]), "each pass starts one interval after the previous one started");
        }

        /// CONTRACT: a zero gossip interval is refused by name, never a loop that spins or a
        /// panic inside the timer.
        #[tokio::test(start_paused = true)]
        async fn a_zero_gossip_interval_is_refused_by_name() {
            let refused = Cadence::every(Duration::ZERO).err().expect("a zero interval is refused");
            assert!(refused.contains("RDM_GOSSIP_INTERVAL_MS") && refused.contains("0 ms"), "{refused}");
        }

        /// CONTRACT (a late tick does not burst, and the grid holds): a pass that spends 1200 ms, past
        /// two grid points (an inline tickle ladder spends whole call budgets), is followed by the one
        /// pass it owes at once (1200 ms), then the passes return to the loop's own grid (1500,
        /// 2000 ms). Must NOT happen: the two missed ticks run back to back (a burst: 1200, 1300,
        /// 1500), the grid moved to the late pass (1200, 1700, 2200), or the interval added after
        /// the pass's work (1700, 2300, 2900).
        #[tokio::test(start_paused = true)]
        async fn a_pass_past_the_interval_owes_one_pass_then_the_tick_returns_to_its_grid() {
            let (long, w) = (Duration::from_millis(1200), Duration::from_millis(100));
            assert_eq!(pass_starts(&[long, w, w, w]).await, ms(&[0, 1200, 1500, 2000]), "one owed pass at once, then the loop's own grid");
        }
    }

    mod ladder {
        //! The tickle ladder beside the loop (node-recovery.md "Detection and proof": "The
        //! mesh-primary runs one loop for its own members, on every gossip interval"; step 2: "Half
        //! a floor after the mark it makes one QUIC connect"), under paused time: a direct connect to
        //! an exited runtime spends a known time, and the clock moves only while every task waits.
        use super::*;
        use std::sync::{Arc, Mutex};
        use std::time::Duration;
        use tokio::time::{sleep, Instant};

        /// What a direct connect to an exited runtime spends at the gate's cadence: the dial ends at
        /// the QUIC idle timeout, the 3 s staleness floor (production's ends at the 10 s call budget).
        const DIAL: Duration = Duration::from_secs(3);

        /// The gate's gossip interval, the loop's tick.
        const INTERVAL: Duration = Duration::from_millis(500);

        fn marked(id: &str) -> SilentNode {
            SilentNode { node_id: id.into(), path: format!("mesh1.rpc.{id}"), incarnation: Some(crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60(&format!("inc-of-{id}"))).unwrap()), marked_at_ms: Some(0) }
        }

        /// CONTRACT (node-recovery.md step 2, per member, and the one loop on every gossip interval):
        /// two members marked at the same instant each get their direct connect half a floor after the
        /// mark, at once, and the pass that starts them waits on neither connect. Their outcomes
        /// are folded by a later tick: both rounds ended with no path, so both hold-downs open on the
        /// clock of the tick that started them. Must NOT happen: the second member's connect waiting
        /// for the first one's to end (3 s late at the gate's cadence, a whole 10 s call budget at
        /// production's), or a pass that lasts as long as the ladders it starts.
        #[tokio::test(start_paused = true)]
        async fn members_marked_together_are_each_tickled_half_a_floor_after_the_mark_at_once() {
            let start = Instant::now();
            let connects: Arc<Mutex<Vec<(String, Duration)>>> = Arc::default();
            let mut t = OfflineTickle::new();
            let tick = async |t: &mut OfflineTickle, now: u64| {
                t.tick(
                    now,
                    true,
                    vec![marked("a"), marked("b")],
                    |_| true,
                    PROBE_DELAY,
                    FLOOR,
                    |id| {
                        let connects = connects.clone();
                        async move {
                            connects.lock().unwrap().push((id, start.elapsed()));
                            sleep(DIAL).await;
                            Err::<(), String>("the dial to the exited runtime ended at the idle timeout".into())
                        }
                    },
                    |_| async { no_path() },
                    |_| async { rdm_transport::iroh_obs::IrohObservation::unavailable() },
                )
                .await
            };
            let started = tick(&mut t, PROBE_DELAY).await;
            let pass = start.elapsed();
            sleep(DIAL).await;
            assert_eq!(*connects.lock().unwrap(), vec![("a".to_string(), Duration::ZERO), ("b".to_string(), Duration::ZERO)], "both direct connects at half a floor after the mark, at once");
            assert_eq!(pass, Duration::ZERO, "the pass waits on no connect");
            assert_eq!(started.tickled, vec!["a".to_string(), "b".to_string()]);
            // The next tick on the gossip interval's grid after both rounds ended folds them.
            sleep(INTERVAL).await;
            let folded = tick(&mut t, PROBE_DELAY + (DIAL + INTERVAL).as_millis() as u64).await;
            assert!(folded.tickled.is_empty(), "a birth whose round was folded with no path waits out its hold-down");
            assert!(t.is_in_hold_down("a") && t.is_in_hold_down("b"), "both rounds ended with no path: both hold-downs are open");
        }

        /// CONTRACT (node-stop retires the work the loop runs beside it; the lead's decision: "a stop
        /// cancels it as part of retiring the birth, and does not wait for it"): a stop's park while a
        /// tickle round and a recovery follower are in flight ends both at the park, with no time
        /// passing. The follower, queued on the view lock a handler holds, leaves that queue, and its
        /// birth leaves `in_flight`, so the first tick after a start submits again; neither goes on
        /// when the gate opens again, and the next tick folds nothing of the cancelled round and starts
        /// it again. Must NOT happen: a round's probe or a follower running on past the park, one held
        /// at the gate while it waits on the view lock, or one resumed by the start.
        #[tokio::test(start_paused = true)]
        async fn a_stops_park_cancels_the_tickle_round_and_the_recovery_follower_in_flight() {
            use crate::model::{IncarnationId, NodeId};
            use std::collections::HashSet;
            use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
            let gate = crate::stop_gate::StopGate::new();
            let ended = Arc::new(AtomicUsize::new(0));
            let mut t = OfflineTickle::observed_by("mesh1.admin.1".into(), gate.clone());
            let tick = async |t: &mut OfflineTickle, now: u64| {
                t.tick(
                    now,
                    true,
                    vec![marked("a")],
                    |_| true,
                    PROBE_DELAY,
                    FLOOR,
                    |_| {
                        let ended = ended.clone();
                        async move {
                            sleep(DIAL).await;
                            ended.fetch_add(1, Ordering::SeqCst);
                            Err::<(), String>("the dial to the exited runtime ended at the idle timeout".into())
                        }
                    },
                    |_| async { no_path() },
                    |_| async { rdm_transport::iroh_obs::IrohObservation::unavailable() },
                )
                .await
            };
            assert_eq!(tick(&mut t, PROBE_DELAY).await.tickled, vec!["a".to_string()]);
            // The follower, queued on the view lock, which a handler holds for write.
            let view = Arc::new(tokio::sync::RwLock::new(()));
            let held = view.clone().write_owned().await;
            let read = Arc::new(AtomicBool::new(false));
            let in_flight: Arc<Mutex<HashSet<(NodeId, IncarnationId)>>> = Arc::default();
            let birth = (NodeId::mint(), IncarnationId::parse(&rdm_core::fixture_crockford60("inc-of-b")).unwrap());
            in_flight.lock().unwrap().insert(birth.clone());
            let follower = {
                let (view, read) = (view.clone(), read.clone());
                spawn_recovery_follower(&gate, in_flight.clone(), birth, async move {
                    let _view = view.read().await;
                    read.store(true, Ordering::SeqCst);
                })
            };
            tokio::task::yield_now().await;
            assert_eq!(t.rounds(), (1, 0), "the round runs, at its direct connect");
            assert!(!follower.is_finished(), "the follower runs, queued on the view lock");
            let at = Instant::now();
            gate.quiesce().await;
            tokio::task::yield_now().await;
            assert_eq!(t.rounds(), (1, 1), "the round ended at the park");
            assert!(follower.is_finished(), "the follower ended at the park");
            assert!(in_flight.lock().unwrap().is_empty(), "its birth left in_flight: a tick after the start submits again");
            assert_eq!(at.elapsed(), Duration::ZERO, "the park waited on neither");
            gate.resume();
            drop(held);
            sleep(DIAL + INTERVAL).await;
            assert_eq!(ended.load(Ordering::SeqCst), 0, "the round's probe never ran on to its end");
            assert!(!read.load(Ordering::SeqCst), "the follower never took the view lock: cancelled, not held");
            let again = tick(&mut t, PROBE_DELAY + (DIAL + INTERVAL).as_millis() as u64).await;
            assert_eq!(again.tickled, vec!["a".to_string()], "nothing is folded of the cancelled round: a's round starts again");
            assert!(!t.is_in_hold_down("a"), "a cancelled round opens no hold-down");
        }

        /// CONTRACT (at most one ladder in flight per exact birth; step 2: an answer "cancels the
        /// probes still running"): the ticks that come while a birth's round runs never start another
        /// for it; a birth heard again has its round's probes dropped, never folded; another birth at
        /// the same node id is its own birth, its round starts and the old birth's is dropped. Must NOT
        /// happen: a second round for a birth whose first is in flight (a retry by tick), or a dropped
        /// round's probe running on to its end.
        #[tokio::test(start_paused = true)]
        async fn one_round_per_exact_birth_and_a_birth_that_leaves_the_silent_set_drops_its_probes() {
            use std::sync::atomic::{AtomicUsize, Ordering};
            let ended = Arc::new(AtomicUsize::new(0));
            let tick = async |t: &mut OfflineTickle, now: u64, set: Vec<SilentNode>| {
                t.tick(
                    now,
                    true,
                    set,
                    |_| true,
                    PROBE_DELAY,
                    FLOOR,
                    |_| {
                        let ended = ended.clone();
                        async move {
                            sleep(DIAL).await;
                            ended.fetch_add(1, Ordering::SeqCst);
                            Err::<(), String>("the dial to the exited runtime ended at the idle timeout".into())
                        }
                    },
                    |_| async { no_path() },
                    |_| async { rdm_transport::iroh_obs::IrohObservation::unavailable() },
                )
                .await
            };
            let mut t = OfflineTickle::new();
            assert_eq!(tick(&mut t, PROBE_DELAY, vec![marked("a")]).await.tickled, vec!["a".to_string()]);
            for k in 1..=5u64 {
                sleep(INTERVAL).await;
                assert!(tick(&mut t, PROBE_DELAY + k * 500, vec![marked("a")]).await.tickled.is_empty(), "tick {k}: the round of a's birth is in flight");
            }
            assert_eq!(t.rounds(), (1, 0), "one round, still running");
            let r = tick(&mut t, PROBE_DELAY + 2_500, vec![]).await;
            assert_eq!((r.returned, t.rounds()), (vec!["a".to_string()], (0, 0)), "heard again: its round is dropped");
            sleep(DIAL).await;
            assert_eq!(ended.load(Ordering::SeqCst), 0, "the dropped round's probe never ran to its end");

            let mut t = OfflineTickle::new();
            assert_eq!(tick(&mut t, PROBE_DELAY, vec![marked("b")]).await.tickled, vec!["b".to_string()]);
            let reborn = SilentNode { incarnation: Some(crate::model::IncarnationId::parse(&rdm_core::fixture_crockford60("inc-of-b-again")).unwrap()), ..marked("b") };
            sleep(INTERVAL).await;
            assert_eq!(tick(&mut t, PROBE_DELAY + 500, vec![reborn]).await.tickled, vec!["b".to_string()], "another birth at b's node id: its own round");
            assert_eq!(t.rounds(), (1, 0), "the old birth's round was dropped");
            sleep(DIAL + INTERVAL).await;
            assert_eq!(ended.load(Ordering::SeqCst), 1, "only the new birth's probe ran to its end");
        }
    }

    mod via_peer {
        //! The tickle's via-peer step runs the seat watch's ladder (node-recovery.md step 2, ruling
        //! R-SW1), under paused time: a probe's wait is its budget, and the clock moves only when
        //! every probe waits.
        use super::*;
        use crate::model::{IncarnationId, NodeId};
        use rdm_core::status::{NodeState, StatusReply};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::time::{sleep, Instant};

        /// The default call budget each probe spends against a process that never answers.
        const BUDGET: Duration = Duration::from_secs(10);

        fn birth() -> (NodeId, IncarnationId) {
            (NodeId::mint(), IncarnationId::parse(&rdm_core::fixture_crockford60("inc-of-the-silent-node")).unwrap())
        }

        fn current(b: &(NodeId, IncarnationId)) -> StatusReply {
            StatusReply::Current { node_id: b.0.clone(), incarnation: b.1.clone(), state: NodeState::ReadyForTraffic }
        }

        /// A probe that spends `after`, records that it ended in `ended`, and ends with `reply`
        /// (`None`: no answer from the birth, a deadline or a carrier's refusal).
        fn probe(after: Duration, reply: Option<StatusReply>, ended: Arc<AtomicUsize>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<StatusReply>> + Send>> {
            Box::pin(async move {
                sleep(after).await;
                ended.fetch_add(1, Ordering::SeqCst);
                reply
            })
        }

        /// CONTRACT (R-SW1 acceptance 1, on the mesh-primary's tickle): after the direct connect
        /// failed, both peer probes run at once, so two peers each spending a whole budget cost one
        /// budget, not two; with no answer the step is `NoPath`, naming both carriers.
        #[tokio::test(start_paused = true)]
        async fn the_tickle_spends_one_budget_on_both_peer_probes() {
            let b = birth();
            let ended = Arc::new(AtomicUsize::new(0));
            let start = Instant::now();
            let peers = vec![("mesh1.rpc.2".to_string(), probe(BUDGET, None, ended.clone())), ("mesh1.rpc.3".to_string(), probe(BUDGET, None, ended.clone()))];
            let verdict = via_peer_verdict(&b.0, &b.1, peers).await;
            assert_eq!(start.elapsed(), BUDGET, "the two peer probes overlap: one budget, not two");
            assert_eq!(verdict, ViaPeerVerdict::NoPath { asked: vec!["mesh1.rpc.2".into(), "mesh1.rpc.3".into()] });
            assert_eq!(ended.load(Ordering::SeqCst), 2, "no answer: both probes ran to their end");
        }

        /// CONTRACT (R-SW1 completion rule, on the tickle): one failed peer probe does not cancel
        /// the other. The first peer fails at 1 s; the second answers for the exact birth at 3 s and
        /// ends the step there, naming that carrier.
        #[tokio::test(start_paused = true)]
        async fn the_tickle_takes_the_second_peers_answer_after_the_first_fails() {
            let b = birth();
            let ended = Arc::new(AtomicUsize::new(0));
            let start = Instant::now();
            let peers = vec![("mesh1.rpc.2".to_string(), probe(Duration::from_secs(1), None, ended.clone())), ("mesh1.rpc.3".to_string(), probe(Duration::from_secs(3), Some(current(&b)), ended.clone()))];
            let verdict = via_peer_verdict(&b.0, &b.1, peers).await;
            assert_eq!(verdict, ViaPeerVerdict::Answered { via: "mesh1.rpc.3".into() });
            assert_eq!(start.elapsed(), Duration::from_secs(3), "the second peer's answer, not the first's failure, ends the step");
        }

        /// CONTRACT (R-SW1 completion rule, on the tickle): the first answer from the exact birth
        /// ends the step and drops the probe still running, which never completes.
        #[tokio::test(start_paused = true)]
        async fn the_tickle_drops_the_peer_probe_still_running_at_an_answer() {
            let b = birth();
            let ended = Arc::new(AtomicUsize::new(0));
            let start = Instant::now();
            let peers = vec![("mesh1.rpc.2".to_string(), probe(BUDGET, None, ended.clone())), ("mesh1.rpc.3".to_string(), probe(Duration::from_secs(1), Some(current(&b)), ended.clone()))];
            let verdict = via_peer_verdict(&b.0, &b.1, peers).await;
            assert_eq!(verdict, ViaPeerVerdict::Answered { via: "mesh1.rpc.3".into() });
            assert_eq!(start.elapsed(), Duration::from_secs(1));
            sleep(BUDGET * 2).await;
            assert_eq!(ended.load(Ordering::SeqCst), 1, "the probe still running was dropped with the step");
        }

        /// CONTRACT (R-SW1 acceptance: the provider check stays mandatory and comes last): TRUE
        /// OFFLINE, the only tickle outcome that sends the mesh-primary to the provider
        /// (`recovery_due`), is reached on round 2 only after both peer probes of each round ended
        /// unanswered, each round costing the direct connect's budget plus one peer budget.
        #[tokio::test(start_paused = true)]
        async fn the_provider_is_owed_only_after_both_peer_probes_of_round_two_ended() {
            let b = birth();
            let ended = Arc::new(AtomicUsize::new(0));
            let mut t = OfflineTickle::new();
            let id = b.0.to_string();
            let set = || vec![SilentNode { node_id: b.0.to_string(), path: "mesh1.rpc.1".into(), incarnation: Some(b.1.clone()), marked_at_ms: None }];
            let direct_failed = Arc::new(AtomicBool::new(false));
            // The instant each round's via-peer rung gave its verdict.
            let verdict_at: Arc<std::sync::Mutex<Vec<Instant>>> = Arc::default();
            let round = async |t: &mut OfflineTickle, now: u64| {
                t.tick(
                    now,
                    true,
                    set(),
                    |_| true,
                    PROBE_DELAY,
                    FLOOR,
                    |_| {
                        let direct_failed = direct_failed.clone();
                        async move {
                            sleep(BUDGET).await;
                            direct_failed.store(true, Ordering::SeqCst);
                            Err::<(), String>("the direct connect spent its budget".into())
                        }
                    },
                    |_| {
                        let peers = vec![("mesh1.rpc.2".to_string(), probe(BUDGET, None, ended.clone())), ("mesh1.rpc.3".to_string(), probe(BUDGET, None, ended.clone()))];
                        let (b, verdict_at) = (b.clone(), verdict_at.clone());
                        async move {
                            let verdict = via_peer_verdict(&b.0, &b.1, peers).await;
                            verdict_at.lock().unwrap().push(Instant::now());
                            verdict
                        }
                    },
                    |_| async { rdm_transport::iroh_obs::IrohObservation::unavailable() },
                )
                .await
            };
            let ms = |d: Duration| d.as_millis() as u64;
            round(&mut t, 0).await;
            // Round 1, started by the tick at half a floor: the tick waits on none of it.
            let start = Instant::now();
            assert_eq!(round(&mut t, PROBE_DELAY).await.tickled, vec![id.clone()]);
            assert_eq!(start.elapsed(), Duration::ZERO, "the tick that starts round 1 waits on none of it");
            sleep(tickle_round(BUDGET) + Duration::from_millis(1)).await;
            assert_eq!(verdict_at.lock().unwrap()[0] - start, tickle_round(BUDGET), "round 1: the direct budget, then one budget for both peers");
            let r1 = round(&mut t, PROBE_DELAY + ms(tickle_round(BUDGET)) + 1).await;
            assert!(r1.offline.is_empty() && t.is_in_hold_down(&id), "round 1 no path opens the hold-down: nothing is owed to the provider");
            // Round 2 is due one floor after round 1's verdict, when its probes ended (the hold-down
            // is stamped there, not at the tick that started the round): one floor after round 1
            // STARTED is still inside it.
            assert!(round(&mut t, PROBE_DELAY + FLOOR).await.tickled.is_empty(), "one floor after round 1 started is inside the hold-down that runs from its verdict");
            let start = Instant::now();
            assert_eq!(round(&mut t, PROBE_DELAY + ms(tickle_round(BUDGET)) + FLOOR).await.tickled, vec![id.clone()], "round 2 starts one floor after round 1's verdict");
            assert_eq!(start.elapsed(), Duration::ZERO, "the tick that starts round 2 waits on none of it");
            sleep(tickle_round(BUDGET) + Duration::from_millis(1)).await;
            assert_eq!(verdict_at.lock().unwrap()[1] - start, tickle_round(BUDGET), "round 2: the direct budget, then one budget for both peers");
            let r2 = round(&mut t, PROBE_DELAY + ms(tickle_round(BUDGET)) + FLOOR + ms(tickle_round(BUDGET)) + 1).await;
            assert!(direct_failed.load(Ordering::SeqCst));
            assert_eq!(r2.offline, vec![id.clone()], "round 2 no path is TRUE OFFLINE: the provider is owed");
            assert_eq!(ended.load(Ordering::SeqCst), 4, "TRUE OFFLINE only once both peer probes of both rounds ended");
        }
    }

    fn no_path() -> ViaPeerVerdict {
        ViaPeerVerdict::NoPath { asked: vec!["mesh1.rpc.2".into()] }
    }

    #[tokio::test(start_paused = true)]
    async fn the_one_tickle_waits_half_a_floor_after_the_mark() {
        let mut t = OfflineTickle::new();
        assert!(tick(&mut t, 0, true, silent("a"), true, no_path()).await.tickled.is_empty(), "marked silent now: no tickle yet");
        assert!(tick(&mut t, PROBE_DELAY - 1, true, silent("a"), true, no_path()).await.tickled.is_empty());
        let r = tick(&mut t, PROBE_DELAY, true, silent("a"), true, no_path()).await;
        assert_eq!(r.tickled, vec!["a".to_string()], "one connect at floor + half a floor");
        assert_eq!(r.reachable_silent, vec!["a".to_string()], "an answer is reachable-silent, no action");
        assert!(tick(&mut t, PROBE_DELAY * 4, true, silent("a"), true, no_path()).await.tickled.is_empty(), "settled: not tickled again");
    }

    /// CONTRACT (node-recovery.md "Detection and proof" step 2: "Half a floor after the mark it makes
    /// one QUIC connect"; step 1: "a node-admin that takes the seat mid-window continues the window
    /// already running"): the window runs from the mark, the instant the birth went unheard past the
    /// staleness floor, not from the tick that first lists it. A birth marked at 0 and first listed
    /// by a tick 700 ms later is tickled by the tick at half a floor after the mark, and the round's
    /// span names the mark as `first_seen_ms`. Must NOT happen: the connect half a floor after the
    /// sighting (a window that grows by the time between the mark and the tick that saw it).
    #[tokio::test(start_paused = true)]
    async fn the_one_tickle_is_made_half_a_floor_after_the_mark_not_after_the_tick_that_first_saw_it() {
        let marked = |id: &str| vec![SilentNode { node_id: id.into(), path: format!("mesh1.rpc.{id}"), incarnation: None, marked_at_ms: Some(0) }];
        let mut t = OfflineTickle::new();
        assert!(tick(&mut t, 700, true, marked("a"), false, no_path()).await.tickled.is_empty(), "listed 700 ms after its mark: no tickle yet");
        let r = tick(&mut t, PROBE_DELAY, true, marked("a"), false, no_path()).await;
        assert_eq!(r.tickled, vec!["a".to_string()], "one connect half a floor after the mark, not half a floor after the tick that first listed it");
        assert!(t.is_in_hold_down("a"), "round 1 with no path opens the hold-down");
        // A node-admin that takes the seat later still runs on the mark its own book holds.
        let mut successor = OfflineTickle::new();
        assert!(tick(&mut successor, 700, false, marked("a"), false, no_path()).await.tickled.is_empty(), "not the seat: bookkeeping only");
        let r = tick(&mut successor, PROBE_DELAY, true, marked("a"), false, no_path()).await;
        assert_eq!(r.tickled, vec!["a".to_string()], "the successor tickles on the window from the mark");
    }

    /// CONTRACT (node-recovery.md "Detection and proof" step 1: a member is marked once it is unheard
    /// past the staleness floor): the silent set carries, for each watched birth, the mark its digest
    /// book holds, the last receipt plus the floor, on the caller's local clock; a birth the book holds
    /// no receipt of, a receipt of another incarnation, or one heard within the floor carries none.
    #[test]
    fn the_silent_set_marks_each_birth_at_its_last_receipt_plus_the_floor() {
        use rdm_core::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshNode};
        let floor = std::time::Duration::from_secs(3);
        let digest = |name: &str| MeshDigest { held_hash: Default::default(),
            fabric_id: FabricId::parse("fab000000001").unwrap(),
            node: MeshNode { node_id: crate::model::NodeId::mint(), name: name.parse().unwrap(), endpoint_id: EndpointId("key".into()), transport_addr: "127.0.0.1:41000".parse().unwrap(), incarnation: IncarnationId::mint(), supersedes: None, runtime: None },
            status: MemberStatus::ReadyForTraffic,
            node_state: Default::default(),
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
        let node_of = |d: &MeshDigest| {
            let mut n = crate::model::Node::allocated(d.node.name.clone());
            n.node_id = d.node.node_id.clone();
            n.incarnation_id = Some(d.node.incarnation.clone());
            n
        };
        let book = rdm_transport::membership::DigestBook::default();
        let t0 = std::time::Instant::now();
        let (silent, fresh, reborn, unheld) = (digest("mesh1.rpc.1"), digest("mesh1.rpc.2"), digest("mesh1.rpc.3"), digest("mesh1.rpc.4"));
        book.record_at(silent.clone(), t0);
        book.record_at(fresh.clone(), t0 + floor);
        book.record_at(reborn.clone(), t0);
        let mut other_birth = node_of(&reborn);
        other_birth.incarnation_id = Some(IncarnationId::mint());
        let watched = [node_of(&silent), node_of(&fresh), other_birth, node_of(&unheld)];
        let now = t0 + floor + std::time::Duration::from_millis(700);
        let set = silent_set(watched.iter(), &book, now, 10_000, floor);
        let marks: Vec<(String, Option<u64>)> = set.iter().map(|s| (s.path.clone(), s.marked_at_ms)).collect();
        assert_eq!(
            marks,
            vec![("mesh1.rpc.1".into(), Some(9_300)), ("mesh1.rpc.2".into(), None), ("mesh1.rpc.3".into(), None), ("mesh1.rpc.4".into(), None)],
            "marked 700 ms before now, at its receipt plus the floor; heard within the floor, another incarnation and no receipt carry no mark"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn only_the_seat_holder_tickles_and_a_new_holder_keeps_the_window() {
        let mut t = OfflineTickle::new();
        assert!(tick(&mut t, 0, false, silent("a"), false, no_path()).await.tickled.is_empty(), "not the seat: bookkeeping only");
        let r = tick(&mut t, PROBE_DELAY, true, silent("a"), true, no_path()).await;
        assert_eq!(r.tickled, vec!["a".to_string()], "the window started before this admin took the seat");
    }

    #[tokio::test(start_paused = true)]
    async fn true_offline_needs_two_rounds_with_no_path_at_least_one_floor_apart() {
        let mut t = OfflineTickle::new();
        tick(&mut t, 0, true, silent("a"), false, no_path()).await;
        let r1 = tick(&mut t, PROBE_DELAY, true, silent("a"), false, no_path()).await;
        assert_eq!(r1.tickled, vec!["a".to_string()]);
        assert!(r1.offline.is_empty() && t.is_in_hold_down("a"), "round 1 no path opens the hold-down, no verdict");
        assert!(tick(&mut t, PROBE_DELAY + FLOOR - 1, true, silent("a"), false, no_path()).await.tickled.is_empty(), "within the hold-down");
        let r2 = tick(&mut t, PROBE_DELAY + FLOOR, true, silent("a"), false, no_path()).await;
        assert_eq!(r2.offline, vec!["a".to_string()], "round 2 no path is TRUE OFFLINE");
        assert!(t.is_offline("a"));
    }

    /// CONTRACT (node-recovery.md "Detection and proof" step 2: "No path opens a hold-down; no
    /// path again at least one floor later is TRUE OFFLINE"): the hold-down opens at the instant
    /// round 1's no path was known, when its probes ended, carried by the round to the tick that
    /// folds it, never the instant the tick that started the round began. Round 1 starts at
    /// PROBE_DELAY and the tick waits on none of it; its direct connect and its via-peer step each
    /// spend a 10 s budget, so no path is known at PROBE_DELAY + 20 s, and the next tick folds it. A
    /// tick one floor after round 1 STARTED is only 10 s after no path was known: it starts nothing.
    /// Round 2 starts one floor after the verdict and, folded, is TRUE OFFLINE naming that verdict.
    /// Must NOT happen: round 2 started, and TRUE OFFLINE declared, less than a floor after round 1's
    /// no path (gate 0484f457: hold-down opened 1483.7 s, round 2 began 1485.7 s).
    // @feature: node-lifecycle
    #[tokio::test(start_paused = true)]
    async fn the_hold_down_runs_one_floor_from_the_no_path_verdict_not_from_the_tick_that_probed() {
        const BUDGET: u64 = 10_000;
        let verdict_at = PROBE_DELAY + 2 * BUDGET;
        let capture = crate::span_capture::Capture::new("i143-3068-hold-down-from-verdict");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);
        let mut t = OfflineTickle::observed_by("mesh1.admin.1".into(), crate::stop_gate::StopGate::new());
        let round = async |t: &mut OfflineTickle, now: u64| {
            t.tick(
                now,
                true,
                silent("a"),
                |_| true,
                PROBE_DELAY,
                FLOOR,
                |_| async {
                    tokio::time::sleep(std::time::Duration::from_millis(BUDGET)).await;
                    Err::<(), String>("the direct connect spent its budget: NotSent".into())
                },
                |_| async {
                    tokio::time::sleep(std::time::Duration::from_millis(BUDGET)).await;
                    no_path()
                },
                |_| async { rdm_transport::iroh_obs::IrohObservation::unavailable() },
            )
            .await
        };
        let ended = std::time::Duration::from_millis(2 * BUDGET + 1);
        round(&mut t, 0).await;
        let started = tokio::time::Instant::now();
        let r1 = round(&mut t, PROBE_DELAY).await;
        assert_eq!(r1.tickled, vec!["a".to_string()], "round 1 runs half a floor after the mark");
        assert_eq!(started.elapsed(), std::time::Duration::ZERO, "the tick that starts round 1 waits on none of it");
        assert!(!t.is_in_hold_down("a"), "nothing is decided before a tick folds the round");
        tokio::time::sleep(ended).await;
        let folded = round(&mut t, verdict_at + 1).await;
        assert!(folded.offline.is_empty() && t.is_in_hold_down("a"), "the tick after round 1 ended folds its no path: the hold-down opens");
        let early = round(&mut t, PROBE_DELAY + FLOOR).await;
        assert!(early.tickled.is_empty() && early.offline.is_empty(), "one floor after round 1 started is {} ms after its no path: still inside the hold-down: {early:?}", PROBE_DELAY + FLOOR - verdict_at);
        assert!(round(&mut t, verdict_at + FLOOR - 1).await.tickled.is_empty(), "one ms short of a floor after the no path: inside the hold-down");
        let r2 = round(&mut t, verdict_at + FLOOR).await;
        assert_eq!(r2.tickled, vec!["a".to_string()], "round 2 starts one floor after round 1's no path");
        tokio::time::sleep(ended).await;
        let folded = round(&mut t, verdict_at + FLOOR + 2 * BUDGET + 1).await;
        assert_eq!(folded.offline, vec!["a".to_string()], "round 2's no path, folded, is TRUE OFFLINE");
        let failed = capture.named("rdm.node_admin.node.resolve.via-offline-tickle-failed");
        assert_eq!(failed.len(), 1, "one TRUE OFFLINE span");
        assert_eq!(failed[0].attributes.get("first_no_path_ms").map(String::as_str), Some(verdict_at.to_string().as_str()), "the hold-down ran from round 1's no path: {:?}", failed[0].attributes);
        let opened = capture.named("rdm.node_admin.node.update.via-offline-hold-down-opened");
        assert_eq!(opened.len(), 1, "one hold-down");
        assert_eq!((opened[0].attributes.get("no_path_at_ms").map(String::as_str), opened[0].attributes.get("round_began_ms").map(String::as_str)), (Some(verdict_at.to_string().as_str()), Some(PROBE_DELAY.to_string().as_str())), "{:?}", opened[0].attributes);
        eprintln!("span rdm.node_admin.node.update.via-offline-hold-down-opened trace_id={} {:?}", opened[0].trace_id, opened[0].attributes);
        eprintln!("span rdm.node_admin.node.resolve.via-offline-tickle-failed trace_id={} {:?}", failed[0].trace_id, failed[0].attributes);
    }

    #[tokio::test(start_paused = true)]
    async fn an_answer_via_a_peer_is_reachable_silent() {
        let mut t = OfflineTickle::new();
        tick(&mut t, 0, true, silent("a"), false, no_path()).await;
        let r = tick(&mut t, PROBE_DELAY, true, silent("a"), false, ViaPeerVerdict::Answered { via: "mesh1.rpc.2".into() }).await;
        assert_eq!(r.reachable_silent, vec!["a".to_string()]);
        assert!(!t.is_in_hold_down("a") && !t.is_offline("a"));
    }

    #[tokio::test(start_paused = true)]
    async fn unreadable_candidates_change_no_status() {
        let mut t = OfflineTickle::new();
        tick(&mut t, 0, true, silent("a"), false, no_path()).await;
        let r = tick(&mut t, PROBE_DELAY, true, silent("a"), false, ViaPeerVerdict::CandidatesUnreadable("no topology".into())).await;
        assert!(r.offline.is_empty() && r.reachable_silent.is_empty() && !t.is_in_hold_down("a"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_node_heard_again_ends_its_tickle_and_status() {
        let mut t = OfflineTickle::new();
        tick(&mut t, 0, true, silent("a"), false, no_path()).await;
        tick(&mut t, PROBE_DELAY, true, silent("a"), false, no_path()).await;
        tick(&mut t, PROBE_DELAY + FLOOR, true, silent("a"), false, no_path()).await;
        assert!(t.is_offline("a"));
        let r = tick(&mut t, PROBE_DELAY + FLOOR + 1, true, vec![], false, no_path()).await;
        assert_eq!(r.returned, vec!["a".to_string()]);
        assert!(r.gone.is_empty());
        assert!(!t.is_offline("a"));
    }

    /// CONTRACT (node-recovery.md "Detection and proof": the tickle and the TRUE OFFLINE mark are
    /// the mesh-primary's own observations of a member): a TRUE OFFLINE node whose node id leaves
    /// the view (node.deleted, or a recovery that created a new node.id at its path.name) ends its
    /// tickle and its status as GONE, named apart from a node heard again.
    /// Must NOT happen: a deleted birth reported and spanned `via-offline-returned` ("heard again").
    #[tokio::test(start_paused = true)]
    async fn a_silent_node_that_left_the_view_is_gone_never_heard_again() {
        let mut t = OfflineTickle::new();
        tick(&mut t, 0, true, silent("a"), false, no_path()).await;
        tick(&mut t, PROBE_DELAY, true, silent("a"), false, no_path()).await;
        tick(&mut t, PROBE_DELAY + FLOOR, true, silent("a"), false, no_path()).await;
        assert!(t.is_offline("a"));
        let capture = crate::span_capture::Capture::new("i143-3051-offline-gone");
        let r = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            t.tick(
                PROBE_DELAY + FLOOR + 1,
                true,
                vec![],
                |id: &str| id != "a",
                PROBE_DELAY,
                FLOOR,
                |_| async { Err("no answer".to_string()) },
                |_| async { no_path() },
                |_| async { rdm_transport::iroh_obs::IrohObservation::unavailable() },
            )
            .await
        };
        assert!(r.returned.is_empty(), "a node the view no longer holds was not heard again: {r:?}");
        assert_eq!(r.gone, vec!["a".to_string()], "it is gone");
        assert!(!t.is_offline("a"));
        assert!(capture.named("rdm.node_admin.node.update.via-offline-returned").is_empty(), "no heard-again span for a gone node");
        let gone = capture.named("rdm.node_admin.node.remove.via-offline-gone");
        assert_eq!(gone.len(), 1, "one gone span");
        assert_eq!(gone[0].attributes.get("node_id").map(String::as_str), Some("a"));
        eprintln!("span rdm.node_admin.node.remove.via-offline-gone trace_id={} {:?}", gone[0].trace_id, gone[0].attributes);
    }

    /// A status store counting every read of any of its rows.
    #[derive(Default)]
    struct ReadCounting {
        inner: crate::status_storage::MemoryStatusStorage,
        reads: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl crate::status_storage::StatusStorage for ReadCounting {
        async fn put_mesh_status(&self, row: &crate::status_storage::MeshStatusRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_mesh_status(row).await
        }
        async fn mesh_statuses(&self) -> Result<std::collections::HashMap<crate::model::MeshId, rdm_core::status::MeshState>, crate::storage_error::StorageError> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.mesh_statuses().await
        }
        async fn put_fabric_event(&self, row: &crate::status_storage::FabricEventRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_fabric_event(row).await
        }
        async fn fabric_events(&self) -> Result<Vec<crate::status_storage::FabricEventRow>, crate::storage_error::StorageError> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.fabric_events().await
        }
        async fn fabric_events_of(&self, mesh: &str) -> Result<Vec<crate::status_storage::FabricEventRow>, crate::storage_error::StorageError> {
            self.inner.fabric_events_of(mesh).await
        }
        async fn put_node_operation(&self, row: &crate::status_storage::NodeOperationRow) -> Result<(), crate::storage_error::StorageError> { self.inner.put_node_operation(row).await }
        async fn node_operations(&self) -> Result<Vec<crate::status_storage::NodeOperationRow>, crate::storage_error::StorageError> { self.inner.node_operations().await }
        async fn remove_node_operation(&self, row: &crate::status_storage::NodeOperationRow) -> Result<(), crate::storage_error::StorageError> { self.inner.remove_node_operation(row).await }

        async fn put_node_declared(&self, row: &crate::status_storage::NodeDeclaredRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_declared(row).await
        }
        async fn remove_node_declared(&self, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<(), crate::storage_error::StorageError> {
            self.inner.remove_node_declared(node_id, incarnation_id).await
        }
        async fn node_declared(&self) -> Result<Vec<crate::status_storage::NodeDeclaredRow>, crate::storage_error::StorageError> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.node_declared().await
        }
        async fn put_node_silence(&self, row: &crate::status_storage::NodeSilenceRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_silence(row).await
        }
        async fn node_silences(&self) -> Result<Vec<crate::status_storage::NodeSilenceRow>, crate::storage_error::StorageError> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.node_silences().await
        }
        async fn put_node_exit(&self, row: &crate::status_storage::NodeExitRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_exit(row).await
        }
        async fn node_exit(&self, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<Option<crate::status_storage::NodeExitRow>, crate::storage_error::StorageError> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.node_exit(node_id, incarnation_id).await
        }
        async fn mesh_status_held_of(&self, mesh: &str, row: &crate::status_storage::MeshStatusRow) -> Result<bool, crate::storage_error::StorageError> {
            self.inner.mesh_status_held_of(mesh, row).await
        }
        async fn put_mesh_status_of(&self, mesh: &str, row: &crate::status_storage::MeshStatusRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_mesh_status_of(mesh, row).await
        }
        async fn node_exit_of(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<Option<crate::status_storage::NodeExitRow>, crate::storage_error::StorageError> {
            self.inner.node_exit_of(mesh, node_id, incarnation_id).await
        }
        async fn put_node_exit_of(&self, mesh: &str, row: &crate::status_storage::NodeExitRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_exit_of(mesh, row).await
        }
            async fn node_held_running_of(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<Option<crate::status_storage::NodeHeldRunningRow>, crate::storage_error::StorageError> {
            self.inner.node_held_running_of(mesh, node_id, incarnation_id).await
        }
        async fn put_node_held_running_of(&self, mesh: &str, row: &crate::status_storage::NodeHeldRunningRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_held_running_of(mesh, row).await
        }
        async fn remove_node_held_running_of(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<(), crate::storage_error::StorageError> {
            self.inner.remove_node_held_running_of(mesh, node_id, incarnation_id).await
        }
        async fn node_os_stopped_of(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<Option<crate::status_storage::NodeOsStoppedRow>, crate::storage_error::StorageError> {
            self.inner.node_os_stopped_of(mesh, node_id, incarnation_id).await
        }
        async fn put_node_os_stopped_of(&self, mesh: &str, row: &crate::status_storage::NodeOsStoppedRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_os_stopped_of(mesh, row).await
        }
        async fn remove_node_os_stopped_of(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<bool, crate::storage_error::StorageError> {
            self.inner.remove_node_os_stopped_of(mesh, node_id, incarnation_id).await
        }
        async fn node_data_dir_lost_of(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<Option<crate::status_storage::NodeDataDirLostRow>, crate::storage_error::StorageError> {
            self.inner.node_data_dir_lost_of(mesh, node_id, incarnation_id).await
        }
        async fn node_data_dir_lost_at(&self, mesh: &str, name: &crate::model::PathName) -> Result<Option<crate::status_storage::NodeDataDirLostRow>, crate::storage_error::StorageError> {
            self.inner.node_data_dir_lost_at(mesh, name).await
        }
        async fn put_node_data_dir_lost_of(&self, mesh: &str, row: &crate::status_storage::NodeDataDirLostRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_data_dir_lost_of(mesh, row).await
        }
        async fn node_data_dir_unreadable_of(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<Option<crate::status_storage::NodeDataDirUnreadableRow>, crate::storage_error::StorageError> {
            self.inner.node_data_dir_unreadable_of(mesh, node_id, incarnation_id).await
        }
        async fn node_data_dir_unreadable_at(&self, mesh: &str, name: &crate::model::PathName) -> Result<Option<crate::status_storage::NodeDataDirUnreadableRow>, crate::storage_error::StorageError> {
            self.inner.node_data_dir_unreadable_at(mesh, name).await
        }
        async fn put_node_data_dir_unreadable_of(&self, mesh: &str, row: &crate::status_storage::NodeDataDirUnreadableRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_data_dir_unreadable_of(mesh, row).await
        }
        async fn remove_node_data_dir_unreadable_of(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId, name: &crate::model::PathName) -> Result<(), crate::storage_error::StorageError> {
            self.inner.remove_node_data_dir_unreadable_of(mesh, node_id, incarnation_id, name).await
        }
}

    /// CONTRACT (#3051, node-recovery.md "Exited is the proof, recorded durably"): the mesh
    /// primary records a member's exit proof as that birth's own keyed row, a blind put that reads
    /// nothing; a later birth of the same node is its own row and leaves the first standing.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn mesh_primary_records_an_exit_proof_as_the_births_own_row() {
        use crate::model::{IncarnationId, Node, PathName};
        use crate::status_storage::StatusStorage;
        let mut held = Node::allocated("mesh1.rpc.1".parse::<PathName>().unwrap());
        let first = IncarnationId::mint();
        held.incarnation_id = Some(first.clone());
        let store = ReadCounting::default();
        let row = put_exit_proof(&store, &held, Some(4), "exit-record", 7).await.unwrap();
        assert_eq!(store.reads.load(std::sync::atomic::Ordering::SeqCst), 0, "the exit-proof put reads no row");
        let next = IncarnationId::mint();
        held.incarnation_id = Some(next.clone());
        put_exit_proof(&store, &held, None, "none", 9).await.unwrap();
        assert_eq!(store.node_exit(&held.node_id, &first).await.unwrap(), Some(row), "the first birth's proof stands");
        assert_eq!(store.node_exit(&held.node_id, &next).await.unwrap().map(|r| (r.exit_code, r.proof)), Some((None, "none".to_string())));
        held.incarnation_id = None;
        assert!(put_exit_proof(&store, &held, Some(4), "provider", 1).await.is_err(), "no birth is named, never skipped");
    }

    // @feature: node-lifecycle
    #[tokio::test]
    async fn mesh_primary_status_write_puts_only_the_births_own_silence_row() {
        use crate::model::{IncarnationId, Node, NodeStatus, PathName};
        use crate::status_storage::StatusStorage;
        let mut held = Node::allocated("mesh1.rpc.1".parse::<PathName>().unwrap());
        held.incarnation_id = Some(IncarnationId::mint());
        let store = ReadCounting::default();
        let declared = crate::status_storage::NodeDeclaredRow { node_id: held.node_id.clone(), incarnation_id: held.incarnation_id.clone().unwrap(), name: held.name.clone(), state: rdm_core::status::NodeState::ReadyForTraffic };
        store.put_node_declared(&declared).await.unwrap();

        assert_eq!(put_silence_status(&store, &held, Some(NodeStatus::Dead)).await, Ok(true), "the silence row is put");
        assert_eq!(store.reads.load(std::sync::atomic::Ordering::SeqCst), 0, "the status write reads no row of status.storage");
        let rows = store.node_silences().await.unwrap();
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!((rows[0].node_id.clone(), rows[0].incarnation_id.clone(), rows[0].status), (held.node_id.clone(), held.incarnation_id.clone().unwrap(), Some(NodeStatus::Dead)));
        assert_eq!(store.node_declared().await.unwrap(), vec![declared], "the declared-state row is another fact and stands");
        held.incarnation_id = None;
        assert!(put_silence_status(&store, &held, None).await.is_err(), "a held record with no birth is named, never skipped");
    }

    /// A status store whose puts of silence rows and, when `exit`, of exit-proof rows are refused,
    /// as a full or read-only disk would.
    #[derive(Default)]
    pub(crate) struct SilenceRefusing {
        pub inner: crate::status_storage::MemoryStatusStorage,
        pub exit: bool,
    }

    #[async_trait::async_trait]
    impl crate::status_storage::StatusStorage for SilenceRefusing {
        async fn put_mesh_status(&self, row: &crate::status_storage::MeshStatusRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_mesh_status(row).await
        }
        async fn mesh_statuses(&self) -> Result<std::collections::HashMap<crate::model::MeshId, rdm_core::status::MeshState>, crate::storage_error::StorageError> {
            self.inner.mesh_statuses().await
        }
        async fn put_fabric_event(&self, row: &crate::status_storage::FabricEventRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_fabric_event(row).await
        }
        async fn fabric_events(&self) -> Result<Vec<crate::status_storage::FabricEventRow>, crate::storage_error::StorageError> {
            self.inner.fabric_events().await
        }
        async fn fabric_events_of(&self, mesh: &str) -> Result<Vec<crate::status_storage::FabricEventRow>, crate::storage_error::StorageError> {
            self.inner.fabric_events_of(mesh).await
        }
        async fn put_node_operation(&self, row: &crate::status_storage::NodeOperationRow) -> Result<(), crate::storage_error::StorageError> { self.inner.put_node_operation(row).await }
        async fn node_operations(&self) -> Result<Vec<crate::status_storage::NodeOperationRow>, crate::storage_error::StorageError> { self.inner.node_operations().await }
        async fn remove_node_operation(&self, row: &crate::status_storage::NodeOperationRow) -> Result<(), crate::storage_error::StorageError> { self.inner.remove_node_operation(row).await }

        async fn put_node_declared(&self, row: &crate::status_storage::NodeDeclaredRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_declared(row).await
        }
        async fn remove_node_declared(&self, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<(), crate::storage_error::StorageError> {
            self.inner.remove_node_declared(node_id, incarnation_id).await
        }
        async fn node_declared(&self) -> Result<Vec<crate::status_storage::NodeDeclaredRow>, crate::storage_error::StorageError> {
            self.inner.node_declared().await
        }
        async fn put_node_silence(&self, row: &crate::status_storage::NodeSilenceRow) -> Result<(), crate::storage_error::StorageError> {
            Err(crate::storage_error::StorageError::Io { file: format!("status/silence/{}", row.key()), reason: "No space left on device (os error 28)".into() })
        }
        async fn node_silences(&self) -> Result<Vec<crate::status_storage::NodeSilenceRow>, crate::storage_error::StorageError> {
            self.inner.node_silences().await
        }
        async fn put_node_exit(&self, row: &crate::status_storage::NodeExitRow) -> Result<(), crate::storage_error::StorageError> {
            if self.exit {
                return Err(crate::storage_error::StorageError::Io { file: format!("status/exit/{}", row.key()), reason: "No space left on device (os error 28)".into() });
            }
            self.inner.put_node_exit(row).await
        }
        async fn node_exit(&self, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<Option<crate::status_storage::NodeExitRow>, crate::storage_error::StorageError> {
            self.inner.node_exit(node_id, incarnation_id).await
        }
        async fn mesh_status_held_of(&self, mesh: &str, row: &crate::status_storage::MeshStatusRow) -> Result<bool, crate::storage_error::StorageError> {
            self.inner.mesh_status_held_of(mesh, row).await
        }
        async fn put_mesh_status_of(&self, mesh: &str, row: &crate::status_storage::MeshStatusRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_mesh_status_of(mesh, row).await
        }
        async fn node_exit_of(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<Option<crate::status_storage::NodeExitRow>, crate::storage_error::StorageError> {
            self.inner.node_exit_of(mesh, node_id, incarnation_id).await
        }
        async fn put_node_exit_of(&self, mesh: &str, row: &crate::status_storage::NodeExitRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_exit_of(mesh, row).await
        }
            async fn node_held_running_of(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<Option<crate::status_storage::NodeHeldRunningRow>, crate::storage_error::StorageError> {
            self.inner.node_held_running_of(mesh, node_id, incarnation_id).await
        }
        async fn put_node_held_running_of(&self, mesh: &str, row: &crate::status_storage::NodeHeldRunningRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_held_running_of(mesh, row).await
        }
        async fn remove_node_held_running_of(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<(), crate::storage_error::StorageError> {
            self.inner.remove_node_held_running_of(mesh, node_id, incarnation_id).await
        }
        async fn node_os_stopped_of(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<Option<crate::status_storage::NodeOsStoppedRow>, crate::storage_error::StorageError> {
            self.inner.node_os_stopped_of(mesh, node_id, incarnation_id).await
        }
        async fn put_node_os_stopped_of(&self, mesh: &str, row: &crate::status_storage::NodeOsStoppedRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_os_stopped_of(mesh, row).await
        }
        async fn remove_node_os_stopped_of(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<bool, crate::storage_error::StorageError> {
            self.inner.remove_node_os_stopped_of(mesh, node_id, incarnation_id).await
        }
        async fn node_data_dir_lost_of(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<Option<crate::status_storage::NodeDataDirLostRow>, crate::storage_error::StorageError> {
            self.inner.node_data_dir_lost_of(mesh, node_id, incarnation_id).await
        }
        async fn node_data_dir_lost_at(&self, mesh: &str, name: &crate::model::PathName) -> Result<Option<crate::status_storage::NodeDataDirLostRow>, crate::storage_error::StorageError> {
            self.inner.node_data_dir_lost_at(mesh, name).await
        }
        async fn put_node_data_dir_lost_of(&self, mesh: &str, row: &crate::status_storage::NodeDataDirLostRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_data_dir_lost_of(mesh, row).await
        }
        async fn node_data_dir_unreadable_of(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<Option<crate::status_storage::NodeDataDirUnreadableRow>, crate::storage_error::StorageError> {
            self.inner.node_data_dir_unreadable_of(mesh, node_id, incarnation_id).await
        }
        async fn node_data_dir_unreadable_at(&self, mesh: &str, name: &crate::model::PathName) -> Result<Option<crate::status_storage::NodeDataDirUnreadableRow>, crate::storage_error::StorageError> {
            self.inner.node_data_dir_unreadable_at(mesh, name).await
        }
        async fn put_node_data_dir_unreadable_of(&self, mesh: &str, row: &crate::status_storage::NodeDataDirUnreadableRow) -> Result<(), crate::storage_error::StorageError> {
            self.inner.put_node_data_dir_unreadable_of(mesh, row).await
        }
        async fn remove_node_data_dir_unreadable_of(&self, mesh: &str, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId, name: &crate::model::PathName) -> Result<(), crate::storage_error::StorageError> {
            self.inner.remove_node_data_dir_unreadable_of(mesh, node_id, incarnation_id, name).await
        }
}

    /// CONTRACT (node-recovery.md "Detection and proof" step 2: TRUE OFFLINE is "written as status
    /// Dead on the node's row"; CLAUDE.md: an error state names what failed and where, and distinct
    /// facts get distinct spans): when status.storage refuses the mesh primary's put of a birth's
    /// silence row, the refusal is one `rdm.node_admin.node.reject.via-status-storage` span naming the
    /// node, its exact birth, the status being written, the store and the store's own cause, and the
    /// write answers that refusal; no row stands. A held record with no incarnation is another fact:
    /// one `via-no-birth` span, never the storage span. Must NOT happen: a refused write with no span
    /// (an info line only), or the two refusals under one span.
    // @feature: node-lifecycle
    #[tokio::test]
    async fn a_refused_silence_status_write_is_spanned_with_the_birth_the_status_and_the_stores_cause() {
        use crate::model::{IncarnationId, Node, NodeStatus, PathName};
        use crate::status_storage::StatusStorage;
        let mut held = Node::allocated("mesh1.rpc.1".parse::<PathName>().unwrap());
        held.incarnation_id = Some(IncarnationId::mint());
        let store = SilenceRefusing::default();
        let capture = crate::span_capture::Capture::new("i143-3051-silence-refused");
        let (refused, unborn) = {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            let refused = put_silence_status(&store, &held, Some(NodeStatus::Dead)).await;
            let mut unborn = held.clone();
            unborn.incarnation_id = None;
            (refused, put_silence_status(&store, &unborn, Some(NodeStatus::PendingReconnect)).await)
        };
        let why = refused.expect_err("a refused put answers its refusal");
        assert!(why.contains("No space left on device"), "the answer carries the store's cause: {why}");
        assert!(store.node_silences().await.unwrap().is_empty(), "no silence row stands");
        let spans = capture.named("rdm.node_admin.node.reject.via-status-storage");
        assert_eq!(spans.len(), 1, "one span for the one refused put: {spans:?}");
        let a = &spans[0].attributes;
        let attr = |k: &str| a.get(k).map(String::as_str).unwrap_or_default().to_string();
        assert_eq!(attr("node"), "mesh1.rpc.1", "{a:?}");
        assert_eq!(attr("node_id"), held.node_id.to_string(), "{a:?}");
        assert_eq!(attr("incarnation_id"), held.incarnation_id.clone().unwrap().as_str(), "the exact birth: {a:?}");
        assert_eq!(attr("status"), "dead", "the status it was writing: {a:?}");
        assert_eq!(attr("store"), "status.storage", "{a:?}");
        assert!(attr("error").contains("status/silence/") && attr("error").contains("No space left on device (os error 28)"), "the store's own cause, with its file: {a:?}");
        eprintln!("span rdm.node_admin.node.reject.via-status-storage trace_id={} {a:?}", spans[0].trace_id);
        assert!(unborn.is_err(), "a held record with no birth is named, never skipped");
        let no_birth = capture.named("rdm.node_admin.node.reject.via-no-birth");
        assert_eq!(no_birth.len(), 1, "the unborn record is its own refusal: {no_birth:?}");
        let b = &no_birth[0].attributes;
        assert_eq!((b.get("node").map(String::as_str), b.get("status").map(String::as_str)), (Some("mesh1.rpc.1"), Some("pending-reconnect")), "{b:?}");
        eprintln!("span rdm.node_admin.node.reject.via-no-birth trace_id={} {b:?}", no_birth[0].trace_id);
    }
}
