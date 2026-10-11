//! The offline tickle (fabric-node-lifecycle.md §7.3, i77 PRD row 18), ported from rafka-v2's
//! `node-admin/src/lib.rs::OfflineTickle`.
//!
//! A silent node is never restarted or deleted for being silent. Every node's own pruner marks it
//! `PendingReconnect` at the staleness floor. Every node-admin keeps the silence bookkeeping
//! (`first_seen`), so a node-admin that takes the seat mid-window tickles on the window already
//! running; only the node-admin holding its mesh's primary seat tickles. Half a floor after the
//! mark (floor + half a floor in all) it makes one QUIC connect, direct then via a peer. An answer
//! is `reachable-silent`: recorded, no action. No path on round 1 opens the hold-down; no path again
//! at least one staleness floor later is TRUE OFFLINE. A node that leaves the silent set has
//! returned. The `Dead` status is observer-inferred, never sent, never a reason to restart or delete.
//! The admin writes the node's status on its row in nodes.storage and shows it in the view.

use std::collections::{HashMap, HashSet};

/// One silent node of this tick's silent set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SilentNode {
    pub node_id: String,
    pub path: String,
}

/// How many live peers the via-peer step asks.
pub const VIA_PEER_TICKLE_FANOUT: usize = 2;

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
    P: std::future::Future<Output = Option<rafka_node_rpc_contract::status::StatusReply>>,
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

#[derive(Debug, Default)]
pub(crate) struct OfflineTickle {
    /// Every node held as silent: node id -> the instant (ms) it was first seen silent. Kept
    /// whether or not this node-admin holds the seat.
    first_seen: HashMap<String, u64>,
    /// Silent nodes whose round 1 returned NoPath, waiting for round 2: node id -> no-path-since (ms).
    hold_down: HashMap<String, u64>,
    /// Silent nodes whose tickle has settled (answered, or true offline): not tickled again until
    /// they leave and re-enter the silent set.
    settled: HashSet<String>,
    /// The nodes held true offline now, by node id.
    offline: HashSet<String>,
    /// This node-admin's path.name, named on every span it emits: a seat handed over mid-window is
    /// read from which admin ran each round.
    observer: String,
}

impl OfflineTickle {
    #[cfg(test)]
    pub fn new() -> Self {
        Self::default()
    }

    /// The tickle of node-admin `observer` (its path.name).
    pub fn observed_by(observer: String) -> Self {
        Self { observer, ..Self::default() }
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

    /// One tick. `holds_seat` is this node-admin's primary seat, sampled once for the tick;
    /// `silent` is this tick's silent set (every `PendingReconnect` node it watches); `in_view` says
    /// whether the view still holds a node id at all (a node that left the silent set is heard
    /// again only if it does); `probe_delay_ms`
    /// is how long a node stays silent before its one tickle; `staleness_floor_ms` is the minimum
    /// delay between round 1 and round 2; `tickle` makes the direct connect and `via_peer` the
    /// via-peer step. `observe` reads iroh's local view of a node once for the answered and the
    /// failed span (`rafka_mesh_transport::iroh_obs`); it decides nothing.
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
        TF: std::future::Future<Output = Result<(), String>>,
        V: Fn(String) -> VF,
        VF: std::future::Future<Output = ViaPeerVerdict>,
        O: Fn(String) -> OF,
        OF: std::future::Future<Output = rafka_mesh_transport::iroh_obs::IrohObservation>,
    {
        let mut report = OfflineTickReport::default();
        let silent_ids: HashSet<String> = silent.iter().map(|n| n.node_id.clone()).collect();
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
        } else {
            self.hold_down.clear();
            self.settled.clear();
        }
        for n in &silent {
            self.first_seen.entry(n.node_id.clone()).or_insert(now_ms);
        }
        if !holds_seat {
            return report;
        }
        for SilentNode { node_id: id, path } in silent {
            // The instant this admin first saw the node silent: a seat taken mid-window tickles on it.
            let seen_at = self.first_seen.get(&id).copied().unwrap_or(now_ms);
            if now_ms.saturating_sub(seen_at) < probe_delay_ms || self.settled.contains(&id) {
                continue;
            }
            if let Some(&no_path_since) = self.hold_down.get(&id) {
                if now_ms < no_path_since + staleness_floor_ms {
                    continue;
                }
            }
            report.tickled.push(id.clone());
            match tickle(id.clone()).await {
                Ok(()) => {
                    self.hold_down.remove(&id);
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
                Err(e) => match {
                    // The via-peer rung's probes run under one span of this round.
                    let step = tracing::info_span!("rdm.node_admin.node.resolve.via-peer-tickle", node_id = %id, path = %path, observer = %self.observer, verdict = tracing::field::Empty, "otel.kind" = "internal");
                    let verdict = tracing::Instrument::instrument(via_peer(id.clone()), step.clone()).await;
                    step.record("verdict", match &verdict {
                        ViaPeerVerdict::Answered { .. } => "answered",
                        ViaPeerVerdict::NoPath { .. } => "no-path",
                        ViaPeerVerdict::CandidatesUnreadable(_) => "candidates-unreadable",
                    });
                    verdict
                } {
                    ViaPeerVerdict::Answered { via } => {
                        self.hold_down.remove(&id);
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
                        if let Some(no_path_since) = self.hold_down.remove(&id) {
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
                            self.hold_down.insert(id.clone(), now_ms);
                            let asked = asked.join(",");
                            tracing::info_span!("rdm.node_admin.node.update.via-offline-hold-down-opened", node_id = %id, path = %path, asked = %asked, observer = %self.observer, first_seen_ms = seen_at as i64, "otel.kind" = "internal")
                                .in_scope(|| tracing::info!(node_id = %id, asked = %asked, "offline hold down opened after round 1 no path"));
                        }
                    }
                },
            }
        }
        report
    }
}

/// Write the status the mesh primary holds for a silent birth (`PendingReconnect`, `Dead`, or
/// `None` once it is heard again) as that birth's own silence row in status.storage: a blind put of
/// its `(node_id, incarnation)` key, the one writer of that fact. No other row is touched and no
/// row is read back. A held record with no incarnation yet has no birth to key the row by: that is
/// named, never a silent skip.
pub(crate) async fn put_silence_status(store: &dyn crate::status_storage::StatusStorage, node: &crate::model::Node, status: Option<crate::model::NodeStatus>) -> Result<bool, String> {
    let Some(incarnation_id) = node.incarnation_id.clone() else {
        return Err(format!("{} ({}) holds no incarnation: no birth to key its silence row by", node.name, node.node_id));
    };
    let row = crate::status_storage::NodeSilenceRow { node_id: node.node_id.clone(), name: node.name.clone(), incarnation_id, status };
    store.put_node_silence(&row).await.map_err(|e| format!("status.storage put of {} ({}) silence row: {e}", node.name, node.node_id))?;
    Ok(true)
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
mod tests {
    use super::*;

    const FLOOR: u64 = 30_000;
    const PROBE_DELAY: u64 = FLOOR / 2;

    fn silent(id: &str) -> Vec<SilentNode> {
        vec![SilentNode { node_id: id.into(), path: format!("mesh1.rpc.{id}") }]
    }

    async fn tick(t: &mut OfflineTickle, now: u64, seat: bool, set: Vec<SilentNode>, direct: bool, via: ViaPeerVerdict) -> OfflineTickReport {
        tick_observed(t, now, seat, set, direct, via, &std::sync::atomic::AtomicUsize::new(0)).await
    }

    /// `tick`, counting every iroh observation read in `reads`.
    async fn tick_observed(t: &mut OfflineTickle, now: u64, seat: bool, set: Vec<SilentNode>, direct: bool, via: ViaPeerVerdict, reads: &std::sync::atomic::AtomicUsize) -> OfflineTickReport {
        t.tick(
            now,
            seat,
            set,
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
                async { rafka_mesh_transport::iroh_obs::IrohObservation::unavailable() }
            },
        )
        .await
    }

    /// CONTRACT (A1, diagnostics only): iroh is read exactly once per answered or round-2 failed
    /// event, never on a tick that records neither, and the reports are the tickle's own: the
    /// answered node is reachable-silent and round 2 with no path is TRUE OFFLINE.
    #[tokio::test]
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

    mod via_peer {
        //! The tickle's via-peer step runs the seat watch's ladder (node-recovery.md step 2, ruling
        //! R-SW1), under paused time: a probe's wait is its budget, and the clock moves only when
        //! every probe waits.
        use super::*;
        use crate::model::{IncarnationId, NodeId};
        use rafka_node_rpc_contract::status::{NodeState, StatusReply};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;
        use tokio::time::{sleep, Instant};

        /// The default call budget each probe spends against a process that never answers.
        const BUDGET: Duration = Duration::from_secs(10);

        fn birth() -> (NodeId, IncarnationId) {
            (NodeId::mint(), IncarnationId("inc-of-the-silent-node".into()))
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
            let set = || vec![SilentNode { node_id: b.0.to_string(), path: "mesh1.rpc.1".into() }];
            let direct_failed = AtomicBool::new(false);
            let round = async |t: &mut OfflineTickle, now: u64| {
                t.tick(
                    now,
                    true,
                    set(),
                    |_| true,
                    PROBE_DELAY,
                    FLOOR,
                    |_| async {
                        sleep(BUDGET).await;
                        direct_failed.store(true, Ordering::SeqCst);
                        Err::<(), String>("the direct connect spent its budget".into())
                    },
                    |_| {
                        let peers = vec![("mesh1.rpc.2".to_string(), probe(BUDGET, None, ended.clone())), ("mesh1.rpc.3".to_string(), probe(BUDGET, None, ended.clone()))];
                        let b = b.clone();
                        async move { via_peer_verdict(&b.0, &b.1, peers).await }
                    },
                    |_| async { rafka_mesh_transport::iroh_obs::IrohObservation::unavailable() },
                )
                .await
            };
            round(&mut t, 0).await;
            let start = Instant::now();
            let r1 = round(&mut t, PROBE_DELAY).await;
            assert!(r1.offline.is_empty() && t.is_in_hold_down(&id), "round 1 no path opens the hold-down: nothing is owed to the provider");
            assert_eq!(start.elapsed(), BUDGET * 2, "round 1: the direct budget, then one budget for both peers");
            let start = Instant::now();
            let r2 = round(&mut t, PROBE_DELAY + FLOOR).await;
            assert!(direct_failed.load(Ordering::SeqCst));
            assert_eq!(r2.offline, vec![id.clone()], "round 2 no path is TRUE OFFLINE: the provider is owed");
            assert_eq!(ended.load(Ordering::SeqCst), 4, "TRUE OFFLINE only once both peer probes of both rounds ended");
            assert_eq!(start.elapsed(), BUDGET * 2, "round 2: the direct budget, then one budget for both peers");
        }
    }

    fn no_path() -> ViaPeerVerdict {
        ViaPeerVerdict::NoPath { asked: vec!["mesh1.rpc.2".into()] }
    }

    #[tokio::test]
    async fn the_one_tickle_waits_half_a_floor_after_the_mark() {
        let mut t = OfflineTickle::new();
        assert!(tick(&mut t, 0, true, silent("a"), true, no_path()).await.tickled.is_empty(), "marked silent now: no tickle yet");
        assert!(tick(&mut t, PROBE_DELAY - 1, true, silent("a"), true, no_path()).await.tickled.is_empty());
        let r = tick(&mut t, PROBE_DELAY, true, silent("a"), true, no_path()).await;
        assert_eq!(r.tickled, vec!["a".to_string()], "one connect at floor + half a floor");
        assert_eq!(r.reachable_silent, vec!["a".to_string()], "an answer is reachable-silent, no action");
        assert!(tick(&mut t, PROBE_DELAY * 4, true, silent("a"), true, no_path()).await.tickled.is_empty(), "settled: not tickled again");
    }

    #[tokio::test]
    async fn only_the_seat_holder_tickles_and_a_new_holder_keeps_the_window() {
        let mut t = OfflineTickle::new();
        assert!(tick(&mut t, 0, false, silent("a"), false, no_path()).await.tickled.is_empty(), "not the seat: bookkeeping only");
        let r = tick(&mut t, PROBE_DELAY, true, silent("a"), true, no_path()).await;
        assert_eq!(r.tickled, vec!["a".to_string()], "the window started before this admin took the seat");
    }

    #[tokio::test]
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

    #[tokio::test]
    async fn an_answer_via_a_peer_is_reachable_silent() {
        let mut t = OfflineTickle::new();
        tick(&mut t, 0, true, silent("a"), false, no_path()).await;
        let r = tick(&mut t, PROBE_DELAY, true, silent("a"), false, ViaPeerVerdict::Answered { via: "mesh1.rpc.2".into() }).await;
        assert_eq!(r.reachable_silent, vec!["a".to_string()]);
        assert!(!t.is_in_hold_down("a") && !t.is_offline("a"));
    }

    #[tokio::test]
    async fn unreadable_candidates_change_no_status() {
        let mut t = OfflineTickle::new();
        tick(&mut t, 0, true, silent("a"), false, no_path()).await;
        let r = tick(&mut t, PROBE_DELAY, true, silent("a"), false, ViaPeerVerdict::CandidatesUnreadable("no topology".into())).await;
        assert!(r.offline.is_empty() && r.reachable_silent.is_empty() && !t.is_in_hold_down("a"));
    }

    #[tokio::test]
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
    #[tokio::test]
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
                |_| async { rafka_mesh_transport::iroh_obs::IrohObservation::unavailable() },
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
        async fn put_mesh_status(&self, row: &crate::status_storage::MeshStatusRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_mesh_status(row).await
        }
        async fn mesh_statuses(&self) -> Result<std::collections::HashMap<crate::model::MeshId, rafka_node_rpc_contract::status::MeshState>, crate::record_store::StorageError> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.mesh_statuses().await
        }
        async fn put_fabric_event(&self, row: &crate::status_storage::FabricEventRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_fabric_event(row).await
        }
        async fn fabric_events(&self) -> Result<Vec<crate::status_storage::FabricEventRow>, crate::record_store::StorageError> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.fabric_events().await
        }
        async fn put_node_declared(&self, row: &crate::status_storage::NodeDeclaredRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_declared(row).await
        }
        async fn remove_node_declared(&self, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<(), crate::record_store::StorageError> {
            self.inner.remove_node_declared(node_id, incarnation_id).await
        }
        async fn node_declared(&self) -> Result<Vec<crate::status_storage::NodeDeclaredRow>, crate::record_store::StorageError> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.node_declared().await
        }
        async fn put_node_silence(&self, row: &crate::status_storage::NodeSilenceRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_silence(row).await
        }
        async fn node_silences(&self) -> Result<Vec<crate::status_storage::NodeSilenceRow>, crate::record_store::StorageError> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.node_silences().await
        }
        async fn put_node_exit(&self, row: &crate::status_storage::NodeExitRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_exit(row).await
        }
        async fn node_exit(&self, node_id: &crate::model::NodeId, incarnation_id: &crate::model::IncarnationId) -> Result<Option<crate::status_storage::NodeExitRow>, crate::record_store::StorageError> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.node_exit(node_id, incarnation_id).await
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
        let declared = crate::status_storage::NodeDeclaredRow { node_id: held.node_id.clone(), incarnation_id: held.incarnation_id.clone().unwrap(), name: held.name.clone(), state: rafka_node_rpc_contract::status::NodeState::ReadyForTraffic };
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
}
