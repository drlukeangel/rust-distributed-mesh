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
pub struct SilentNode {
    pub node_id: String,
    pub path: String,
}

/// How many live peers the via-peer step asks.
pub const VIA_PEER_TICKLE_FANOUT: usize = 2;

/// Verdict of asking live peer nodes to tickle a target node.
#[derive(Debug, Clone, PartialEq)]
pub enum ViaPeerVerdict {
    /// A peer answered the tickle: reachable via that peer, unreachable from this observer.
    Answered { via: String },
    /// No candidate path answered: either no candidates, or every asked peer failed or refused.
    NoPath { asked: Vec<String> },
    /// The candidates could not be read: no status change.
    CandidatesUnreadable(String),
}

/// What one [`OfflineTickle::tick`] did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct OfflineTickReport {
    pub tickled: Vec<String>,
    pub reachable_silent: Vec<String>,
    pub offline: Vec<String>,
    pub returned: Vec<String>,
}

#[derive(Debug, Default)]
pub struct OfflineTickle {
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
}

impl OfflineTickle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_in_hold_down(&self, id: &str) -> bool {
        self.hold_down.contains_key(id)
    }

    /// Is this node held true offline?
    pub fn is_offline(&self, id: &str) -> bool {
        self.offline.contains(id)
    }

    /// One tick. `holds_seat` is this node-admin's primary seat, sampled once for the tick;
    /// `silent` is this tick's silent set (every `PendingReconnect` node it watches); `probe_delay_ms`
    /// is how long a node stays silent before its one tickle; `staleness_floor_ms` is the minimum
    /// delay between round 1 and round 2; `tickle` makes the direct connect and `via_peer` the
    /// via-peer step.
    pub async fn tick<T, TF, V, VF>(
        &mut self,
        now_ms: u64,
        holds_seat: bool,
        silent: Vec<SilentNode>,
        probe_delay_ms: u64,
        staleness_floor_ms: u64,
        tickle: T,
        via_peer: V,
    ) -> OfflineTickReport
    where
        T: Fn(String) -> TF,
        TF: std::future::Future<Output = Result<(), String>>,
        V: Fn(String) -> VF,
        VF: std::future::Future<Output = ViaPeerVerdict>,
    {
        let mut report = OfflineTickReport::default();
        let silent_ids: HashSet<String> = silent.iter().map(|n| n.node_id.clone()).collect();
        let returned: Vec<String> = self.first_seen.keys().filter(|id| !silent_ids.contains(*id)).cloned().collect();
        self.first_seen.retain(|id, _| silent_ids.contains(id));
        self.offline.retain(|id| silent_ids.contains(id));
        for id in returned {
            tracing::info_span!("rdm.node_admin.node.update.via-offline-returned", node_id = %id, "otel.kind" = "internal")
                .in_scope(|| tracing::info!(node_id = %id, "a silent node is heard again: its tickle and its status end"));
            report.returned.push(id);
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
            let seen = self.first_seen.get(&id).copied().unwrap_or(now_ms);
            if now_ms.saturating_sub(seen) < probe_delay_ms || self.settled.contains(&id) {
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
                    tracing::info_span!("rdm.node_admin.node.resolve.via-offline-tickle-answered", node_id = %id, path = %path, "otel.kind" = "internal")
                        .in_scope(|| tracing::info!(node_id = %id, "offline tickle answered — reachable-silent, recorded, no action"));
                    report.reachable_silent.push(id);
                }
                Err(e) => match via_peer(id.clone()).await {
                    ViaPeerVerdict::Answered { via } => {
                        self.hold_down.remove(&id);
                        self.settled.insert(id.clone());
                        tracing::info_span!("rdm.node_admin.node.resolve.via-peer-tickle-answered", node_id = %id, path = %path, via = %via, error = %e, "otel.kind" = "internal")
                            .in_scope(|| tracing::info!(node_id = %id, via = %via, error = %e, "offline tickle failed direct but answered via peer — unreachable from me, no record"));
                        report.reachable_silent.push(id);
                    }
                    ViaPeerVerdict::CandidatesUnreadable(reason) => {
                        tracing::info_span!("rdm.node_admin.node.reject.via-peer-tickle-candidates-unreadable", node_id = %id, path = %path, reason = %reason, error = %e, "otel.kind" = "internal")
                            .in_scope(|| tracing::warn!(node_id = %id, reason = %reason, error = %e, "via-peer candidates unreadable — no status change, hold-down left as it was"));
                    }
                    ViaPeerVerdict::NoPath { asked } => {
                        if let Some(no_path_since) = self.hold_down.remove(&id) {
                            self.settled.insert(id.clone());
                            self.offline.insert(id.clone());
                            tracing::info_span!(
                                "rdm.node_admin.node.resolve.via-offline-tickle-failed",
                                node_id = %id,
                                path = %path,
                                error = %e,
                                round = 2i64,
                                first_no_path_ms = no_path_since as i64,
                                "otel.kind" = "internal",
                            )
                            .in_scope(|| tracing::info!(node_id = %id, error = %e, "offline tickle failed round 2 — TRUE OFFLINE, no restart, no delete"));
                            report.offline.push(id);
                        } else {
                            self.hold_down.insert(id.clone(), now_ms);
                            let asked = asked.join(",");
                            tracing::info_span!("rdm.node_admin.node.update.via-offline-hold-down-opened", node_id = %id, path = %path, asked = %asked, "otel.kind" = "internal")
                                .in_scope(|| tracing::info!(node_id = %id, asked = %asked, "offline hold down opened after round 1 no path"));
                        }
                    }
                },
            }
        }
        report
    }
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
        t.tick(now, seat, set, PROBE_DELAY, FLOOR, |_| async move { if direct { Ok(()) } else { Err("no answer".to_string()) } }, |_| {
            let via = via.clone();
            async move { via }
        })
        .await
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
        assert!(!t.is_offline("a"));
    }
}
