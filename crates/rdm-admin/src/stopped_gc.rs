//! The stopped-node garbage collector (node-identity-and-state.md §6.9, R-GC1).
//!
//! A stopped node is garbage once it has been stopped longer than the fabric's
//! `stopped_node_ttl` (`RDM_STOPPED_NODE_TTL`). The fabric-primary, the sole Build driver, sweeps
//! every ttl: it ages each stopped birth from the current stopped period's own stop, recorded on the
//! node record (`stopped_at_rafka_ms`, D50), skips a birth an open Build still references, and submits
//! one ordinary node.delete Build per sweep. The sweep deletes nothing itself; the delete's admission rechecks the stopped
//! and open-Build facts, serialized with it.

use crate::accepted::AttemptAction;
use crate::build::{BuildId, BuildReject};
use crate::build_state::BuildState;
use crate::control_plane::Refusal;
use crate::model::{IncarnationId, NodeId};
use std::time::Duration;

/// The variable that sets the stopped-node ttl (`docs/architecture/node-identity-and-state.md` §6.9).
pub const ENV: &str = "RDM_STOPPED_NODE_TTL";

/// RDM's shipped stopped-node ttl (`docs/architecture/node-identity-and-state.md` §6.9): what an
/// unset variable means.
pub const SHIPPED_TTL: Duration = Duration::from_secs(30 * 60);

/// The ttl `raw` sets (`docs/architecture/node-identity-and-state.md` §6.9): unset is RDM's shipped
/// 30 minutes, blank is forever (`None`, no sweep). A value is a whole number with its unit, `ms`,
/// `s`, `m` or `h`; zero and anything else are refused by name.
pub fn ttl_from_env(raw: Option<&str>) -> Result<Option<Duration>, String> {
    let Some(raw) = raw else { return Ok(Some(SHIPPED_TTL)) };
    let v = raw.trim();
    if v.is_empty() {
        return Ok(None);
    }
    let refuse = |why: &str| format!("{ENV} `{raw}`: {why}; a ttl is a whole number with its unit (750ms, 2s, 30m, 1h), or blank for forever");
    let digits = v.find(|c: char| !c.is_ascii_digit()).unwrap_or(v.len());
    let n: u64 = v[..digits].parse().map_err(|_| refuse("no number"))?;
    let d = match &v[digits..] {
        "ms" => Duration::from_millis(n),
        "s" => Duration::from_secs(n),
        "m" => Duration::from_secs(n.saturating_mul(60)),
        "h" => Duration::from_secs(n.saturating_mul(3600)),
        _ => return Err(refuse("no unit it knows")),
    };
    if d.is_zero() {
        return Err(refuse("a zero ttl"));
    }
    Ok(Some(d))
}

/// How old the current stopped period of one birth is, from its node record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Age {
    /// The current stopped period's own stop, recorded on the node record in rafka-time.
    StoppedAt(u64),
    /// The node record holds no stop for the birth's current stopped period (a record written before
    /// the stop was recorded, or a birth stopped by no node.stop).
    NotRecorded(String),
    /// The node record cannot be read.
    Unreadable(String),
}

/// What the scan decided for one stopped birth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Stopped longer than the ttl: a candidate.
    Expired(Candidate),
    /// Stopped, not yet longer than the ttl.
    Young {
        /// The node.
        path: String,
        /// The name, beside its path.
        name: String,
        /// When its current stopped period began.
        stopped_at_ms: u64,
    },
    /// An open Build references the birth.
    Referenced {
        /// The node.
        path: String,
        /// The name, beside its path.
        name: String,
        /// The open Build.
        build_id: BuildId,
    },
    /// The accepted Build no longer names the node: its delete is under way.
    Deleting {
        /// The node.
        path: String,
        /// The name, beside its path.
        name: String,
    },
    /// The view holds no birth (incarnation) for the stopped node: there is no birth to age.
    NoIncarnation {
        /// The node.
        path: String,
        /// The name, beside its path.
        name: String,
    },
    /// The node record holds no stop for the birth's current stopped period.
    NotRecorded {
        /// The node.
        path: String,
        /// The name, beside its path.
        name: String,
        /// Why.
        reason: String,
    },
    /// The birth's node record cannot be read.
    UnreadableRecord {
        /// The node.
        path: String,
        /// The name, beside its path.
        name: String,
        /// The read failure.
        reason: String,
    },
    /// No Build in the builds store is in progress or completed (R-BI3): there is no accepted
    /// topology to hold the node against, so nothing is decided for it.
    NoCurrentBuild {
        /// The node.
        path: String,
        /// The name, beside its path.
        name: String,
    },
    /// The Build log (or its active Builds) cannot be read: neither the age nor the open-Build
    /// reference can be decided.
    UnreadableBuildLog {
        /// The node.
        path: String,
        /// The name, beside its path.
        name: String,
        /// The read failure.
        reason: String,
    },
}

/// One stopped birth the scan selected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// The node's path.
    pub path: String,
    /// The name, beside its path.
    pub name: String,
    /// The logical node.
    pub node_id: NodeId,
    /// The stopped birth.
    pub incarnation: IncarnationId,
    /// When its current stopped period began (its node record's `stopped_at_rafka_ms`).
    pub stopped_at_ms: u64,
}

impl Candidate {
    /// The `(path, name)` lookup index key of path and name.
    pub fn location(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.path.clone(), name: self.name.clone() } }
}

/// What one sweep did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Swept {
    /// The ttl is blank: no sweep.
    Forever,
    /// This admin does not hold the fabric-primary role: it sweeps nothing.
    NotFabricPrimary,
    /// Nothing is stopped longer than the ttl.
    Nothing,
    /// One node.delete Build was accepted for `node`.
    Submitted {
        /// The node collected.
        path: String,
        /// The name, beside its path.
        name: String,
        /// The node.delete Build.
        build_id: BuildId,
    },
    /// Another Build is in flight: the candidate is seen again next sweep.
    BuildInProgress {
        /// The candidate.
        path: String,
        /// The name, beside its path.
        name: String,
        /// The Build in flight.
        current_build_id: BuildId,
    },
    /// The delete's admission refused the candidate by name.
    Refused {
        /// The candidate.
        path: String,
        /// The name, beside its path.
        name: String,
        /// Why.
        reason: String,
    },
}

/// Whether an attempt's targeted action acts on the birth `incarnation`: the one birth a delete is
/// targeted to.
fn acts_on(action: &AttemptAction, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
    match action {
        AttemptAction::Delete { node_id: id, from_incarnation, .. } => id == node_id && from_incarnation == incarnation,
    }
}

fn references_birth(p: &crate::build_state::BuildProjection, current: bool, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
    let root_open = matches!(p.state, BuildState::Pending | BuildState::Running) || (current && p.state != BuildState::Complete);
    (root_open && p.action.as_ref().is_some_and(|a| acts_on(a, node_id, incarnation)))
        || p.lanes.values().any(|lane| matches!(lane.state, BuildState::Pending | BuildState::Running)
            && lane.action.as_ref().is_some_and(|a| acts_on(a, node_id, incarnation)))
}

impl crate::control_plane::ControlPlane {
    /// This admin's view as it is now.
    async fn view(&self) -> crate::topology::Topology {
        match self.view_now.get() {
            Some(project) => project().await,
            None => self.topology.read().await.clone(),
        }
    }

    /// The age of the birth `(node_id, incarnation)`'s current stopped period: the stop its node record
    /// holds for it (`stopped_at_rafka_ms`, D50), never a Build receipt.
    async fn stop_age(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Age {
        let Some(rows) = self.node_rows.get() else { return Age::Unreadable("node records are not bound".into()) };
        match rows.node(node_id).await {
            Err(e) => Age::Unreadable(format!("the node record of {node_id} cannot be read: {e}")),
            Ok(None) => Age::NotRecorded(format!("no node record of {node_id}")),
            Ok(Some(row)) => match row.births.iter().find(|b| b.incarnation_id == *incarnation).and_then(|b| b.stopped_at_rafka_ms) {
                Some(at) => Age::StoppedAt(at),
                None => Age::NotRecorded(format!("the node record of {node_id} holds no stop of incarnation {}", incarnation.as_str())),
            },
        }
    }

    /// The open Build that references the birth: a Build still Pending or Running, or the accepted
    /// Build while it is not complete (a replacement left mid-transfer keeps its rollback point),
    /// whose current attempt's targeted action acts on the birth.
    async fn open_reference(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<BuildId>, String> {
        let current = self.builds.current().await.map_err(|e| format!("the current Build cannot be read: {e}"))?.map(|p| p.build_id);
        let facts = self.builds.facts().await.map_err(|e| format!("the Build facts cannot be read: {e}"))?;
        Ok(crate::build_state::fold(&facts).into_values().find(|p| references_birth(p, current.as_ref() == Some(&p.build_id), node_id, incarnation)).map(|p| p.build_id))
    }

    /// The scan: every stopped birth this admin's view holds, with its verdict at `now_ms`.
    pub async fn scan_stopped(&self, ttl: Duration, now_ms: u64) -> Vec<Verdict> {
        let view = self.view().await;
        // The current Build's topology, or why it cannot be read: no current Build this admin can
        // read is not the node's absence from it.
        // "No current Build" and "the store cannot be read" are distinct verdicts.
        let desired: Result<Option<crate::accepted::FabricTopology>, String> = match self.builds.current_build().await {
            Err(e) => Err(format!("the builds store cannot be read for the current Build: {e}")),
            Ok(current) => match current.into_build_id() {
                None => Ok(None),
                Some(id) => self.builds.read_build(&id).await.map(|b| Some(b.topology)).map_err(|e| format!("the current Build {id} cannot be read: {e}")),
            },
        };
        let mut stopped: Vec<&crate::model::Node> = view.members().filter(|n| n.stopped).collect();
        stopped.sort_by(|a, b| a.location().cmp(&b.location()));
        let mut out = Vec::new();
        for n in stopped {
            let node = n.location();
            let Some(incarnation) = n.incarnation_id.clone() else {
                out.push(Verdict::NoIncarnation { path: node.path.clone(), name: node.name.clone()});
                continue;
            };
            match &desired {
                Err(reason) => {
                    out.push(Verdict::UnreadableBuildLog { path: node.path.clone(), name: node.name.clone(), reason: reason.clone() });
                    continue;
                }
                Ok(None) => {
                    out.push(Verdict::NoCurrentBuild { path: node.path.clone(), name: node.name.clone()});
                    continue;
                }
                Ok(Some(t)) if !t.contains(&node) => {
                    out.push(Verdict::Deleting { path: node.path.clone(), name: node.name.clone()});
                    continue;
                }
                Ok(Some(_)) => {}
            }
            match self.open_reference(&n.node_id, &incarnation).await {
                Ok(Some(build_id)) => {
                    out.push(Verdict::Referenced { path: node.path.clone(), name: node.name.clone(), build_id });
                    continue;
                }
                Ok(None) => {}
                Err(reason) => {
                    out.push(Verdict::UnreadableBuildLog { path: node.path.clone(), name: node.name.clone(), reason });
                    continue;
                }
            }
            match self.stop_age(&n.node_id, &incarnation).await {
                Age::StoppedAt(stopped_at_ms) if now_ms.saturating_sub(stopped_at_ms) > ttl.as_millis() as u64 => {
                    out.push(Verdict::Expired(Candidate { path: node.path.clone(), name: node.name.clone(), node_id: n.node_id.clone(), incarnation, stopped_at_ms }))
                }
                Age::StoppedAt(stopped_at_ms) => out.push(Verdict::Young { path: node.path.clone(), name: node.name.clone(), stopped_at_ms }),
                Age::NotRecorded(reason) => out.push(Verdict::NotRecorded { path: node.path.clone(), name: node.name.clone(), reason }),
                Age::Unreadable(reason) => out.push(Verdict::UnreadableRecord { path: node.path.clone(), name: node.name.clone(), reason }),
            }
        }
        out
    }

    /// One sweep at `ttl` (blank: none): the scan, then one node.delete Build for the first
    /// candidate. Only the fabric-primary sweeps; one topology Build runs at a time, so the other
    /// candidates wait for the next sweep.
    pub async fn sweep_stopped(&self, ttl: Option<Duration>) -> Swept {
        let Some(ttl) = ttl else { return Swept::Forever };
        let Some(me_id) = self.node_id.get() else { return Swept::NotFabricPrimary };
        if !self.topology.read().await.is_fabric_primary(me_id) {
            return Swept::NotFabricPrimary;
        }
        let now_ms = self.time.now_ms();
        let ttl_ms = ttl.as_millis() as u64;
        let mut first = None;
        for v in self.scan_stopped(ttl, now_ms).await {
            match v {
                Verdict::Expired(c) => {
                    first.get_or_insert(c);
                }
                Verdict::Young { .. } => {}
                Verdict::Referenced { path, name, build_id } => tracing::info_span!("rdm.node_admin.node.reject.via-open-build-reference", node_path = %path, node_name = %name, build_id = %build_id.log(), ttl = ttl_ms)
                    .in_scope(|| tracing::info!("the stopped birth is referenced by an open Build: it is not collected")),
                Verdict::Deleting { path, name } => tracing::info_span!("rdm.node_admin.node.reject.via-delete-under-way", node_path = %path, node_name = %name, ttl = ttl_ms)
                    .in_scope(|| tracing::info!("the accepted Build no longer names the stopped node: its delete is under way, not submitted again")),
                Verdict::NoIncarnation { path, name } => tracing::info_span!("rdm.node_admin.node.reject.via-no-incarnation", node_path = %path, node_name = %name, ttl = ttl_ms)
                    .in_scope(|| tracing::info!("the view holds no birth for the stopped node: there is no birth to age, skipped")),
                Verdict::NotRecorded { path, name, reason } => tracing::info_span!("rdm.node_admin.node.reject.via-no-recorded-stop", node_path = %path, node_name = %name, reason = %reason, ttl = ttl_ms)
                    .in_scope(|| tracing::info!(%reason, "the node record holds no stop of the birth's current stopped period: skipped, never treated as expired")),
                Verdict::UnreadableRecord { path, name, reason } => tracing::info_span!("rdm.node_admin.node.reject.via-unreadable-node-record", node_path = %path, node_name = %name, reason = %reason, ttl = ttl_ms)
                    .in_scope(|| tracing::info!(%reason, "the birth's node record cannot be read: skipped, never treated as expired")),
                Verdict::NoCurrentBuild { path, name } => tracing::info_span!("rdm.node_admin.node.reject.via-no-current-build", node_path = %path, node_name = %name, ttl = ttl_ms)
                    .in_scope(|| tracing::info!("no Build in the builds store is in progress or completed: there is no accepted topology to hold the stopped node against, skipped")),
                Verdict::UnreadableBuildLog { path, name, reason } => tracing::info_span!("rdm.node_admin.node.reject.via-unreadable-build-log", node_path = %path, node_name = %name, reason = %reason, ttl = ttl_ms)
                    .in_scope(|| tracing::info!(%reason, "the Build log cannot be read: the birth's age is not decided, skipped")),
            }
        }
        let Some(c) = first else { return Swept::Nothing };
        match self.collect_stopped(&c, ttl).await {
            Ok(o) => Swept::Submitted { path: c.path.clone(), name: c.name.clone(), build_id: o.build_id },
            Err(Refusal::BuildInProgress(held)) => Swept::BuildInProgress { path: c.path.clone(), name: c.name.clone(), current_build_id: held.current_build_id },
            Err(Refusal::Reject(r)) => Swept::Refused { path: c.path.clone(), name: c.name.clone(), reason: r.to_string() },
            Err(other) => Swept::Refused { path: c.path.clone(), name: c.name.clone(), reason: format!("{other:?}") },
        }
    }

    /// The node.delete Build of `candidate`: a `Delete` of its node.id (build.md §1), admitted only after the stopped and open-Build checks run again under the admission,
    /// serialized with the write (§6.9 Recheck).
    pub async fn collect_stopped(&self, candidate: &Candidate, ttl: Duration) -> Result<crate::control_plane::Opened, Refusal> {
        let span = tracing::info_span!(
            "rdm.node_admin.node.delete.via-stopped-ttl",
            node_path = %candidate.path, node_name = %candidate.name,
            stopped_at = candidate.stopped_at_ms,
            ttl = ttl.as_millis() as u64,
            build_id = tracing::field::Empty,
            previous_build_id = tracing::field::Empty,
        );
        self.submit_checked(span, "stopped-ttl", rdm_core::build::BuildChange::Delete { meshes: Vec::new(), node_ids: vec![candidate.node_id.clone()] }, Some(candidate)).await
    }

    /// The recheck at the delete's admission: the view still holds the candidate's exact birth
    /// stopped, no open Build references it, and its current stopped period is the one the scan aged.
    pub(crate) async fn recheck_stopped(&self, c: &Candidate) -> Result<(), BuildReject> {
        let not_stopped = |found: String| BuildReject::NodeNotStopped { path: c.path.clone(), name: c.name.clone(), found };
        let view = self.view().await;
        let n = view.own(&c.node_id).ok_or_else(|| not_stopped("the view holds no node with this ID".into()))?;
        if n.node_id != c.node_id || n.incarnation_id.as_ref() != Some(&c.incarnation) {
            return Err(not_stopped(format!("the view holds another birth there ({} at {:?})", n.node_id, n.incarnation_id)));
        }
        if !n.stopped {
            return Err(not_stopped(format!("it is {:?} and not stopped: started since the scan", n.status)));
        }
        match self.open_reference(&c.node_id, &c.incarnation).await {
            Ok(Some(build_id)) => return Err(BuildReject::ReferencedByOpenBuild { path: c.path.clone(), name: c.name.clone(), build_id: build_id.to_string() }),
            Ok(None) => {}
            Err(e) => return Err(not_stopped(e)),
        }
        match self.stop_age(&c.node_id, &c.incarnation).await {
            Age::StoppedAt(t) if t == c.stopped_at_ms => Ok(()),
            other => Err(not_stopped(format!("its current stopped period is no longer the one the scan aged at {} ms: {other:?}", c.stopped_at_ms))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;


    // CONTRACT (`docs/architecture/node-identity-and-state.md` §6.9 — "Setting: `stopped_node_ttl`
    // (`RDM_STOPPED_NODE_TTL`), read by the fabric-primary only. Blank means forever: no sweep, a
    // stopped node stays until its own start or delete. RDM's shipped config sets 30 minutes"):
    // `ttl_from_env` parses `ms`, `s`, `m`, `h`, treats unset as 30 minutes and blank as `None`, and
    // refuses zero or unknown units.
    // @feature: stopped-node-collection
    #[test]
    fn ttl_from_env_reads_unset_blank_and_each_unit_and_refuses_zero_or_an_unknown_unit() {
        assert_eq!(ttl_from_env(None), Ok(Some(SHIPPED_TTL)));
        assert_eq!(ttl_from_env(Some("")), Ok(None));
        assert_eq!(ttl_from_env(Some("5m")), Ok(Some(Duration::from_secs(300))));
        assert_eq!(ttl_from_env(Some("750ms")), Ok(Some(Duration::from_millis(750))));
        assert_eq!(ttl_from_env(Some("2s")), Ok(Some(Duration::from_secs(2))));
        assert_eq!(ttl_from_env(Some("1h")), Ok(Some(Duration::from_secs(3600))));
        assert!(ttl_from_env(Some("0ms")).is_err());
        assert!(ttl_from_env(Some("10d")).is_err());

    }
}
