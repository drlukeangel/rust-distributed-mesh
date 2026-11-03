//! The stopped-node garbage collector (node-identity-and-state.md §6.9, R-GC1).
//!
//! A stopped node is garbage once it has been stopped longer than the fabric's
//! `stopped_node_ttl` (`RDM_STOPPED_NODE_TTL`). The fabric-primary, the sole Build driver, sweeps
//! every ttl: it ages each stopped birth from the current stopped period's own stop receipt in the
//! Build log, skips a birth an open Build still references, and submits one ordinary node.delete
//! Build per sweep. The sweep deletes nothing itself; the delete's admission rechecks the stopped
//! and open-Build facts, serialized with it.

use crate::accepted::{AttemptAction, TopologyChange};
use crate::build::{BuildId, BuildReject};
use crate::build_state::{BuildFact, BuildState, StepOutcome};
use crate::control_plane::Refusal;
use crate::model::{IncarnationId, NodeId, PathName};
use rdm_core::LifecycleOp;
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

/// How old the current stopped period of one birth is, from the Build log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Age {
    /// The current stopped period's own stop receipt, stamped in rafka-time.
    StoppedAt(u64),
    /// No stop receipt opens the current stopped period (a later start ended the last one
    /// receipted, or the birth was stopped by something other than a standalone stop).
    NoCurrentReceipt(String),
    /// A stop receipt is held but cannot be read.
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
        node: PathName,
        /// When its current stopped period began.
        stopped_at_ms: u64,
    },
    /// An open Build references the birth.
    Referenced {
        /// The node.
        node: PathName,
        /// The open Build.
        build_id: BuildId,
    },
    /// The accepted Build no longer names the node: its delete is under way.
    Deleting {
        /// The node.
        node: PathName,
    },
    /// The view holds no birth (incarnation) for the stopped node: there is no birth to age.
    NoIncarnation {
        /// The node.
        node: PathName,
    },
    /// No stop receipt opens the birth's current stopped period.
    NoCurrentReceipt {
        /// The node.
        node: PathName,
        /// Why.
        reason: String,
    },
    /// The birth's current stop receipt is held but cannot be read.
    UnreadableReceipt {
        /// The node.
        node: PathName,
        /// The decode failure.
        reason: String,
    },
    /// No Build in the builds store is in progress or completed (R-BI3): there is no accepted
    /// topology to hold the node against, so nothing is decided for it.
    NoCurrentBuild {
        /// The node.
        node: PathName,
    },
    /// The Build log (or its active Builds) cannot be read: neither the age nor the open-Build
    /// reference can be decided.
    UnreadableBuildLog {
        /// The node.
        node: PathName,
        /// The read failure.
        reason: String,
    },
}

/// One stopped birth the scan selected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// The node's `(path, name)`.
    pub node: PathName,
    /// The logical node.
    pub node_id: NodeId,
    /// The stopped birth.
    pub incarnation: IncarnationId,
    /// When its current stopped period began (its stop receipt's rafka-time).
    pub stopped_at_ms: u64,
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
        node: PathName,
        /// The node.delete Build.
        build_id: BuildId,
    },
    /// Another Build is in flight: the candidate is seen again next sweep.
    BuildInProgress {
        /// The candidate.
        node: PathName,
        /// The Build in flight.
        current_build_id: BuildId,
    },
    /// The delete's admission refused the candidate by name.
    Refused {
        /// The candidate.
        node: PathName,
        /// Why.
        reason: String,
    },
}

/// Whether an attempt's targeted action acts on the birth `incarnation`: the one birth a node op is
/// targeted to, or any birth a Build pinned.
fn acts_on(action: &AttemptAction, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
    match action {
        AttemptAction::DeleteConnections { node_id: id, from_incarnation, .. }
        | AttemptAction::Stop { node_id: id, from_incarnation, .. }
        | AttemptAction::Start { node_id: id, from_incarnation, .. }
        | AttemptAction::Recover { node_id: id, from_incarnation, .. }
        | AttemptAction::Delete { node_id: id, from_incarnation, .. }
        | AttemptAction::Terminate { node_id: id, from_incarnation, .. } => id == node_id && from_incarnation == incarnation,
        AttemptAction::Steps { pinned, .. } => pinned.iter().any(|(_, b)| b.node_id == *node_id && b.incarnation == *incarnation),
    }
}

/// The age of the birth `(node_id, incarnation)`'s current stopped period, from the Build log in the
/// order it holds its facts: the latest standalone stop's `Complete` receipt naming the birth opens a
/// period, and a later `Complete` of a start of the same birth ends it. An earlier stop
/// before a later start never counts.
fn receipt_birth(action: &AttemptAction, operation: &rdm_core::OperationId, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
    if !acts_on(action, node_id, incarnation) { return false; }
    match action {
        AttemptAction::Stop { .. } => operation.action == "node.stop" && operation.recipe_step.is_none(),
        AttemptAction::Start { .. } => operation.action == "node.start" && operation.recipe_step.is_none(),
        AttemptAction::Steps { steps, pinned, .. } => {
            let Some(index) = operation.recipe_step.and_then(|n| n.checked_sub(1)).map(|n| n as usize) else { return false };
            let Ok(births) = crate::accepted::step_births(steps, pinned) else { return false };
            let Some(step) = steps.get(index) else { return false };
            let Some((Some(birth), _, _)) = births.get(index) else { return false };
            birth.node_id == *node_id && birth.incarnation == *incarnation
                && crate::build::admitted_step_key(step, Some(birth), operation.recipe_step) == *operation
        }
        _ => false,
    }
}

/// The age of admitted stop evidence for this exact node birth.
pub fn stop_age(facts: &[BuildFact], node_id: &NodeId, incarnation: &IncarnationId) -> Age {
    let projections = crate::build_state::fold(facts);
    let mut current: Option<Age> = None;
    let mut ended: Option<String> = None;
    for f in facts {
        let BuildFact::Step(s) = f else { continue };
        if s.step != "Complete" || s.outcome != StepOutcome::Complete || !matches!(s.operation.action.as_str(), "node.stop" | "node.start") { continue; }
        let subject_matches = match &s.operation.subject {
            Some(rdm_core::OperationSubject::Node(id)) => id == node_id,
            Some(rdm_core::OperationSubject::Birth { node_id: id, incarnation: inc }) => id == node_id && inc == incarnation,
            _ => false,
        };
        if !subject_matches { continue; }
        let Some(projection) = projections.get(&s.build_id).map(|p| p.in_lane(s.mesh.as_deref())) else {
            current = Some(Age::Unreadable("the completed node operation has no accepted Build".into()));
            continue;
        };
        let Some(action) = projection.attempt_actions.get(&s.attempt) else {
            current = Some(Age::Unreadable("the completed node operation has no admitted action in its mesh and attempt".into()));
            continue;
        };
        if !acts_on(action, node_id, incarnation) { continue; }
        if !receipt_birth(action, &s.operation, node_id, incarnation) {
            current = Some(Age::Unreadable("the completed node operation does not match its admitted step and birth".into()));
            continue;
        }
        if s.operation.action == "node.stop" {
            current = Some(match s.output.as_ref() {
                Some(value) => match serde_json::from_value::<LifecycleOp>(value.clone()) {
                    Ok(op) if crate::build_state::validate_carried_lifecycle(&projection, s, &op, (node_id, incarnation)).is_some() => Age::StoppedAt(op.event_at_rafka_ms),
                    Ok(_) => Age::Unreadable("the stop lifecycle payload does not match its receipt scope and exact birth".into()),
                    Err(e) => Age::Unreadable(format!("the stop lifecycle payload cannot be decoded: {e}")),
                },
                None => Age::Unreadable("the stop receipt commits no stop time".into()),
            });
        } else if current.is_some() {
            current = None;
            ended = Some(format!("its last stop receipt precedes its {:?} in Build {} attempt {}", s.operation, s.build_id, s.attempt));
        }
    }
    current.unwrap_or_else(|| Age::NoCurrentReceipt(ended.unwrap_or_else(|| "no standalone stop receipt in the Build log names this birth".into()) + ": no stop receipt opens its current stopped period"))
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
        let facts = self.builds.facts().await;
        let mut stopped: Vec<&crate::model::Node> = view.members().filter(|n| n.stopped).collect();
        stopped.sort_by(|a, b| a.name.cmp(&b.name));
        let mut out = Vec::new();
        for n in stopped {
            let node = n.name.clone();
            let Some(incarnation) = n.incarnation_id.clone() else {
                out.push(Verdict::NoIncarnation { node });
                continue;
            };
            match &desired {
                Err(reason) => {
                    out.push(Verdict::UnreadableBuildLog { node, reason: reason.clone() });
                    continue;
                }
                Ok(None) => {
                    out.push(Verdict::NoCurrentBuild { node });
                    continue;
                }
                Ok(Some(t)) if !t.contains(&node) => {
                    out.push(Verdict::Deleting { node });
                    continue;
                }
                Ok(Some(_)) => {}
            }
            match self.open_reference(&n.node_id, &incarnation).await {
                Ok(Some(build_id)) => {
                    out.push(Verdict::Referenced { node, build_id });
                    continue;
                }
                Ok(None) => {}
                Err(reason) => {
                    out.push(Verdict::UnreadableBuildLog { node, reason });
                    continue;
                }
            }
            let facts = match &facts {
                Ok(f) => f,
                Err(e) => {
                    out.push(Verdict::UnreadableBuildLog { node, reason: format!("the Build log cannot be read: {e}") });
                    continue;
                }
            };
            match stop_age(facts, &n.node_id, &incarnation) {
                Age::StoppedAt(stopped_at_ms) if now_ms.saturating_sub(stopped_at_ms) > ttl.as_millis() as u64 => {
                    out.push(Verdict::Expired(Candidate { node, node_id: n.node_id.clone(), incarnation, stopped_at_ms }))
                }
                Age::StoppedAt(stopped_at_ms) => out.push(Verdict::Young { node, stopped_at_ms }),
                Age::NoCurrentReceipt(reason) => out.push(Verdict::NoCurrentReceipt { node, reason }),
                Age::Unreadable(reason) => out.push(Verdict::UnreadableReceipt { node, reason }),
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
                Verdict::Referenced { node, build_id } => tracing::info_span!("rdm.node_admin.node.reject.via-open-build-reference", node_path = %node.path, node_name = %node.name, build_id = %build_id.log(), ttl = ttl_ms)
                    .in_scope(|| tracing::info!("the stopped birth is referenced by an open Build: it is not collected")),
                Verdict::Deleting { node } => tracing::info_span!("rdm.node_admin.node.reject.via-delete-under-way", node_path = %node.path, node_name = %node.name, ttl = ttl_ms)
                    .in_scope(|| tracing::info!("the accepted Build no longer names the stopped node: its delete is under way, not submitted again")),
                Verdict::NoIncarnation { node } => tracing::info_span!("rdm.node_admin.node.reject.via-no-incarnation", node_path = %node.path, node_name = %node.name, ttl = ttl_ms)
                    .in_scope(|| tracing::info!("the view holds no birth for the stopped node: there is no birth to age, skipped")),
                Verdict::NoCurrentReceipt { node, reason } => tracing::info_span!("rdm.node_admin.node.reject.via-no-current-stop-receipt", node_path = %node.path, node_name = %node.name, reason = %reason, ttl = ttl_ms)
                    .in_scope(|| tracing::info!(%reason, "no stop receipt opens the birth's current stopped period: skipped, never treated as expired")),
                Verdict::UnreadableReceipt { node, reason } => tracing::info_span!("rdm.node_admin.node.reject.via-unreadable-stop-receipt", node_path = %node.path, node_name = %node.name, reason = %reason, ttl = ttl_ms)
                    .in_scope(|| tracing::info!(%reason, "the birth's stop receipt cannot be read: skipped, never treated as expired")),
                Verdict::NoCurrentBuild { node } => tracing::info_span!("rdm.node_admin.node.reject.via-no-current-build", node_path = %node.path, node_name = %node.name, ttl = ttl_ms)
                    .in_scope(|| tracing::info!("no Build in the builds store is in progress or completed: there is no accepted topology to hold the stopped node against, skipped")),
                Verdict::UnreadableBuildLog { node, reason } => tracing::info_span!("rdm.node_admin.node.reject.via-unreadable-build-log", node_path = %node.path, node_name = %node.name, reason = %reason, ttl = ttl_ms)
                    .in_scope(|| tracing::info!(%reason, "the Build log cannot be read: the birth's age is not decided, skipped")),
            }
        }
        let Some(c) = first else { return Swept::Nothing };
        match self.collect_stopped(&c, ttl).await {
            Ok(o) => Swept::Submitted { node: c.node, build_id: o.build_id },
            Err(Refusal::BuildInProgress(held)) => Swept::BuildInProgress { node: c.node, current_build_id: held.current_build_id },
            Err(Refusal::Reject(r)) => Swept::Refused { node: c.node, reason: r.to_string() },
            Err(other) => Swept::Refused { node: c.node, reason: format!("{other:?}") },
        }
    }

    /// The node.delete Build of `candidate`: the ordinary `RemoveNode` Build `DELETE /api/nodes/{name}`
    /// accepts, admitted only after the stopped and open-Build checks run again under the admission,
    /// serialized with the write (§6.9 Recheck).
    pub async fn collect_stopped(&self, candidate: &Candidate, ttl: Duration) -> Result<crate::control_plane::Opened, Refusal> {
        let span = tracing::info_span!(
            "rdm.node_admin.node.delete.via-stopped-ttl",
            node_path = %candidate.node.path, node_name = %candidate.node.name,
            stopped_at = candidate.stopped_at_ms,
            ttl = ttl.as_millis() as u64,
            build_id = tracing::field::Empty,
            previous_build_id = tracing::field::Empty,
        );
        self.submit_checked(span, "stopped-ttl", TopologyChange::RemoveNode { node: candidate.node.clone() }, Some(candidate)).await
    }

    /// The recheck at the delete's admission: the view still holds the candidate's exact birth
    /// stopped, no open Build references it, and its current stopped period is the one the scan aged.
    pub(crate) async fn recheck_stopped(&self, c: &Candidate) -> Result<(), BuildReject> {
        let not_stopped = |found: String| BuildReject::NodeNotStopped { node: c.node.clone(), found };
        let view = self.view().await;
        let n = view.own(&c.node_id).ok_or_else(|| not_stopped("the view holds no node with this ID".into()))?;
        if n.node_id != c.node_id || n.incarnation_id.as_ref() != Some(&c.incarnation) {
            return Err(not_stopped(format!("the view holds another birth there ({} at {:?})", n.node_id, n.incarnation_id)));
        }
        if !n.stopped {
            return Err(not_stopped(format!("it is {:?} and not stopped: started since the scan", n.status)));
        }
        match self.open_reference(&c.node_id, &c.incarnation).await {
            Ok(Some(build_id)) => return Err(BuildReject::ReferencedByOpenBuild { node: c.node.clone(), build_id: build_id.to_string() }),
            Ok(None) => {}
            Err(e) => return Err(not_stopped(e)),
        }
        let facts = self.builds.facts().await.map_err(|e| not_stopped(format!("the Build log cannot be read: {e}")))?;
        match stop_age(&facts, &c.node_id, &c.incarnation) {
            Age::StoppedAt(t) if t == c.stopped_at_ms => Ok(()),
            other => Err(not_stopped(format!("its current stopped period is no longer the one the scan aged at {} ms: {other:?}", c.stopped_at_ms))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::PinnedBirth;
    use crate::build_state::{AttemptOpened, AttemptReason, BuildStepReceipt};

    // CONTRACT (`docs/architecture/node-identity-and-state.md:570` — "Setting: `stopped_node_ttl`
    // (`RDM_STOPPED_NODE_TTL`), read by the fabric-primary only. Blank means forever: no sweep, a
    // stopped node stays until its own start or delete. RDM's shipped config sets 30 minutes" and
    // `docs/architecture/node-identity-and-state.md:573` — "Age: The current stopped period's own stop
    // receipt in the Build log, never an earlier stop before a later start. A birth whose receipt cannot
    // be read is skipped by name, never treated as expired"): `ttl_from_env` parses `ms`, `s`, `m`, `h`,
    // treats unset as 30 minutes and blank as `None`, and refuses zero or unknown units; `stop_age`
    // returns the current stop receipt's timestamp, resets when a later start of the same birth completes,
    // and reports `Unreadable` when a targeted stop receipt lacks a valid `LifecycleOp`.
    // @feature: stopped-node-collection
    #[test]
    fn ttl_from_env_and_stop_age_follow_current_stopped_period_and_flag_unreadable_receipts() {
        assert_eq!(ttl_from_env(None), Ok(Some(SHIPPED_TTL)));
        assert_eq!(ttl_from_env(Some("")), Ok(None));
        assert_eq!(ttl_from_env(Some("5m")), Ok(Some(Duration::from_secs(300))));
        assert!(ttl_from_env(Some("0ms")).is_err());
        assert!(ttl_from_env(Some("10d")).is_err());

        let path: PathName = rdm_core::PathName::new("mesh1.rpc", "1").unwrap();
        let node_id = NodeId::mint();
        let inc = IncarnationId::mint();
        let build_id = BuildId::mint();

        let stop_action = AttemptAction::Stop { kind: rdm_core::NodeKind::RpcNode, mesh: "mesh1".into(),
            path: path.clone(),
            node_id: node_id.clone(),
            from_incarnation: inc.clone(),
        };
        let start_action = AttemptAction::Start { kind: rdm_core::NodeKind::RpcNode, mesh: "mesh1".into(),
            path: path.clone(),
            node_id: node_id.clone(),
            from_incarnation: inc.clone(),
        };
        let steps_action = AttemptAction::Steps { rename_intents: Default::default(),
            steps: vec![],
            pinned: vec![(path.clone(), PinnedBirth { mesh: "mesh1".into(), kind: rdm_core::NodeKind::RpcNode, node_id: node_id.clone(), incarnation: inc.clone() })],
        };
        assert!(acts_on(&stop_action, &node_id, &inc));
        assert!(acts_on(&steps_action, &node_id, &inc));
        assert!(!acts_on(&stop_action, &node_id, &IncarnationId::mint()));

        assert!(!acts_on(&stop_action, &NodeId::mint(), &inc));

        let op1 = LifecycleOp { kind: rdm_core::NodeKind::RpcNode, mesh: "mesh1".to_owned(),
            build_id: build_id.to_string(),
            attempt: 1,
            operation: rdm_core::OperationId::node("node.stop", &node_id),
            node_id: node_id.clone(),
            incarnation: inc.clone(),
            name: path.clone(),
            event_at_rafka_ms: 1_000,
            traceparent: None,
            tracestate: None,
        };
        // Each attempt is claimed before its executor writes a receipt: the fold holds an attempt's action from its claim.
        let claim = |attempt: u32| BuildFact::Claim(crate::build_state::BuildAttemptClaim { claimed_mesh: None, executor_mesh: "mesh1".into(), claimant: None, mesh: None, build_id: build_id.clone(), attempt, executor: rdm_core::framing::Birth { node_id: rdm_core::NodeId::parse("100000000000").unwrap(), incarnation: IncarnationId::parse("n4w3mjy921q8").unwrap() } });
        // Each attempt ends before the next opens: a Build in flight takes no new attempt.
        let ended = |attempt: u32| BuildFact::Attempt(crate::build_state::BuildAttemptReceipt { mesh: None, build_id: build_id.clone(), attempt, outcome: crate::build_state::AttemptOutcome::Converged });
        let mut facts = vec![
            BuildFact::Accepted(crate::build_state::BuildAccepted {
                build_id: build_id.clone(), submitted_change: None, submitted_at_ms: 0,
                topology: crate::accepted::FabricTopology { fabric: "fabric1".into(), meshes: Default::default(), topology_storage_provider: None, pinned: vec![], section_of: None },
            }),
            BuildFact::Opened(AttemptOpened {
                mesh: None,
                build_id: build_id.clone(),
                attempt: 1,
                reason: AttemptReason::Stop,
                action: Some(stop_action.clone()),
                opened_by: rdm_core::NodeId::parse("100000000000").unwrap(),
                opened_at_ms: 900,
                context: None,
            }),
            claim(1),
            BuildFact::Step(BuildStepReceipt {
                mesh: None,
                build_id: build_id.clone(),
                attempt: 1,
                operation: rdm_core::OperationId::node("node.stop", &node_id),
                step: "Complete".into(),
                outcome: StepOutcome::Complete,
                output: Some(serde_json::to_value(&op1).unwrap()),
                executor: None,
            }),
        ];
        assert_eq!(stop_age(&facts, &node_id, &inc), Age::StoppedAt(1_000));

        let mut wrong_payload = facts.clone();
        if let Some(BuildFact::Step(receipt)) = wrong_payload.last_mut() {
            let mut wrong = op1.clone(); wrong.operation = rdm_core::OperationId::node("node.stop", &NodeId::mint());
            receipt.output = Some(serde_json::to_value(wrong).unwrap());
        }
        assert!(matches!(stop_age(&wrong_payload, &node_id, &inc), Age::Unreadable(_)));
        let mut wrong_scope = facts.clone();
        if let Some(BuildFact::Step(receipt)) = wrong_scope.last_mut() { receipt.mesh = Some("other-mesh".into()); }
        assert!(matches!(stop_age(&wrong_scope, &node_id, &inc), Age::Unreadable(_)));

        let mut lane_facts = facts.clone();
        if let BuildFact::Opened(mut opened) = facts[1].clone() {
            opened.mesh = Some("mesh1".into()); lane_facts.push(BuildFact::Opened(opened));
        }
        let mut held = crate::build_state::fold(&lane_facts).remove(&build_id).unwrap();
        held.state = BuildState::Complete;
        let lane = held.lanes.get_mut("mesh1").unwrap();
        lane.state = BuildState::Running;
        lane.action = Some(stop_action.clone());
        assert!(references_birth(&held, false, &node_id, &inc));
        assert!(!references_birth(&held, false, &NodeId::mint(), &inc));
        held.lanes.get_mut("mesh1").unwrap().state = BuildState::Complete;
        assert!(!references_birth(&held, false, &node_id, &inc));

        // A later start of the same birth ends the stopped period.
        facts.push(ended(1));
        facts.push(BuildFact::Opened(AttemptOpened {
            mesh: None,
            build_id: build_id.clone(),
            attempt: 2,
            reason: AttemptReason::Start,
            action: Some(start_action),
            opened_by: rdm_core::NodeId::parse("100000000000").unwrap(),
            opened_at_ms: 2_000,
            context: None,
        }));
        facts.push(claim(2));
        facts.push(BuildFact::Step(BuildStepReceipt {
            mesh: None,
            build_id: build_id.clone(),
            attempt: 2,
            operation: rdm_core::OperationId::node("node.start", &node_id),
            step: "Complete".into(),
            outcome: StepOutcome::Complete,
            output: None,
            executor: None,
        }));
        { let age = stop_age(&facts, &node_id, &inc); assert!(matches!(age, Age::NoCurrentReceipt(ref r) if r.contains("node.start")), "{age:?}"); }

        // A targeted stop receipt with missing output is reported as Unreadable, never expired.
        facts.push(ended(2));
        facts.push(BuildFact::Opened(AttemptOpened {
            mesh: None,
            build_id: build_id.clone(),
            attempt: 3,
            reason: AttemptReason::Stop,
            action: Some(stop_action),
            opened_by: rdm_core::NodeId::parse("100000000000").unwrap(),
            opened_at_ms: 3_000,
            context: None,
        }));
        facts.push(claim(3));
        facts.push(BuildFact::Step(BuildStepReceipt {
            mesh: None,
            build_id,
            attempt: 3,
            operation: rdm_core::OperationId::node("node.stop", &node_id),
            step: "Complete".into(),
            outcome: StepOutcome::Complete,
            output: None,
            executor: None,
        }));
        assert!(matches!(stop_age(&facts, &node_id, &inc), Age::Unreadable(ref r) if r.contains("commits no stop time")));
    }
}
