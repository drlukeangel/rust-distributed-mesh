//! Node recovery submitter, exit verdicts, and recovery due checks.

use super::*;

/// What the mesh primary's recovery of one TRUE OFFLINE member reads and calls
/// (node-recovery.md "Detection and proof" step 3, sequence step 1).
#[derive(Clone)]
pub(crate) struct RecoverySubmitter {
    /// This mesh primary's `path.name`: the submitter.
    pub me: PathName,
    pub topology: Arc<RwLock<Topology>>,
    pub book: DigestBook,
    pub provider: Arc<dyn DeploymentProvider>,
    pub nodes_storage: Arc<dyn crate::storage::NodesStorage>,
    pub status_storage: Arc<dyn crate::status_storage::StatusStorage>,
    /// This admin's Build door: its own control plane, where it opens and claims its mesh's drift
    /// attempts, and its own executor, which runs them.
    pub door: crate::build_op::BuildSlot,
    /// This admin's Build state: the fabric's Build facts as its own log holds them. The tick reads
    /// from it which exited births of its mesh are owed a submit ([`RecoverySubmitter::owed_submits`]).
    pub builds: Arc<dyn crate::build_state::BuildStateAdapter>,
    /// Parent directory under which birth data directories are allocated (`<data_root>/<name>-<node_id>`).
    pub data_root: PathBuf,
    /// Per exited birth whose inherited `data_dir` was observed absent (`Gone`, `R-FM2`):
    /// `(first_seen_ms, consecutive_rounds)`.
    pub gone_dirs: Arc<Mutex<HashMap<(NodeId, IncarnationId), (u64, u64)>>>,
    /// The births whose `os-stopped` row this admin's own inspection has confirmed (R-PV1): a row
    /// another admin wrote is folded only after this admin's inspection reads `OsStopped` too.
    pub os_stopped_confirmed: Arc<Mutex<std::collections::HashSet<(NodeId, IncarnationId)>>>,
    /// The refused terminates this admin has named (i144 R21), by birth and the receipt's Build,
    /// lane and attempt: each is named once, on the first expired hold that reads it. The refusal
    /// itself is the durable receipt; this only keeps its span from repeating every tick.
    pub terminate_refusals_named: Arc<Mutex<std::collections::HashSet<(NodeId, IncarnationId, crate::build::BuildId, Option<String>, u32)>>>,
}

/// What a hold of a birth may do this tick, read from the birth's durable Failed `TerminateRuntime`
/// receipts (node-recovery.md step 4's end, i144 R21).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HoldGate {
    /// No receipt forbids it: an expired hold submits the birth's terminate.
    Open,
    /// The latest failed terminate failed at its own inspection, after this hold began: it asked
    /// the provider nothing and the hold is void; a fresh one begins.
    Void(crate::build_state::FailedTerminate),
    /// A terminate the provider refused or could not prove: no hold submits the birth's terminate
    /// again; a person decides.
    Refused(crate::build_state::FailedTerminate),
    /// The receipts could not be read: nothing is submitted this tick.
    Unreadable(String),
}

/// The gate of a hold that began at `since` (none held yet), from the birth's failed terminates
/// `failures`, oldest first: any terminate the provider refused or could not prove forbids every
/// later one; else the latest, when it failed at its own inspection after the hold began (or its
/// opening is not held yet), voids the hold.
pub(crate) fn hold_gate(failures: &[crate::build_state::FailedTerminate], since: Option<u64>) -> HoldGate {
    if let Some(f) = failures.iter().rev().find(|f| f.stage.stops_resubmission()) {
        return HoldGate::Refused(f.clone());
    }
    match (failures.last(), since) {
        (Some(f), Some(since)) if matches!(f.stage, crate::build_state::TerminateStage::Inspection { .. }) && f.opened_at_ms.is_none_or(|opened| since <= opened) => HoldGate::Void(f.clone()),
        _ => HoldGate::Open,
    }
}

/// What a loop that proves exits last recorded per birth `(node_id, incarnation)`: the mesh primary's
/// tick ([`RecoverySubmitter::record_verdict`]) and the fabric primary's drift pass
/// ([`reconcile_drift`]) each hold one, and record through [`record_verdict_once`].
pub type ExitVerdicts = Mutex<HashMap<(NodeId, IncarnationId), Inspected>>;

/// The verdict rule of a loop that proves exits (node-recovery.md "Exited is the proof, recorded
/// durably"): `outcome` of `birth` is recorded when it differs from what `verdicts` holds for that
/// birth, by `record` (its span, and for an exit the birth's exit-proof row, a blind put of its own
/// key); a verdict that stands is neither named nor put again by a later pass. A record that fails
/// (the store refused the put) leaves `verdicts` as it was, so the next pass that proves the exit
/// records it. The answer is where an exit's row stands: `put` by this call, `recorded` by an earlier
/// one, or the refusal.
pub(crate) async fn record_verdict_once<F>(verdicts: &ExitVerdicts, birth: (NodeId, IncarnationId), outcome: &Inspected, record: impl FnOnce() -> F) -> Result<&'static str, String>
where
    F: std::future::Future<Output = Result<(), String>>,
{
    if verdicts.lock().unwrap().get(&birth) == Some(outcome) {
        return Ok("recorded");
    }
    record().await?;
    verdicts.lock().unwrap().insert(birth, outcome.clone());
    Ok("put")
}

/// What the provider's inspection of a TRUE OFFLINE member found (node-runtime-inspect.md "Result").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Inspected {
    /// Exited: the exit code and where it came from.
    Exited(Option<i32>, &'static str),
    /// The exact runtime runs, however silent (`R-FM1`'s hold).
    Running,
    /// The operating system holds the exact runtime suspended (R-PV1): an observation, never a hold.
    OsStopped,
    /// Nothing proven (no runtime fact, an unreadable row, the provider's `Unknown`): why.
    Unavailable(String),
}

impl Inspected {
    /// True when the provider inspection explicitly reported the runtime `Running` (`R-FM1`).
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }
}

impl RecoverySubmitter {
    /// The name this admin currently holds in its membership digest book, or its boot name before its
    /// birth is in the book.
    pub(crate) fn held_name(&self) -> PathName {
        self.book.held_path_name().unwrap_or_else(|| self.me.clone())
    }

    /// Resolve `node`'s inherited `data_dir` when known (`R-FM2`): from `node.data_dir`, its digest in
    /// `self.book`, its durable runtime row in `self.nodes_storage`, or `<data_root>/<name>-<node_id>`.
    pub(crate) async fn birth_data_dir(&self, node: &Node) -> Option<PathBuf> {
        if let Some(d) = node.data_dir.as_deref() {
            return Some(PathBuf::from(d));
        }
        if let Some(d) = self.book.get(node.node_id.as_str()).and_then(|(dg, _)| dg.data_dir.map(PathBuf::from)) {
            return Some(d);
        }
        if let Some(inc) = node.incarnation_id.as_ref() {
            if let Some(d) = self.nodes_storage.runtime(&node.node_id, inc).await.ok().flatten().and_then(|r| r.data_dir.map(PathBuf::from)) {
                return Some(d);
            }
        }
        if !self.data_root.as_os_str().is_empty() {
            return Some(self.data_root.join(format!("{}-{}", node.name, node.node_id)));
        }
        None
    }

    /// The provider's exact-exit proof of `node`'s birth: the runtime fact the book's digest of that
    /// birth carries, else the birth's own durable runtime row (a point read by its key).
    pub(crate) async fn inspect(&self, node: &Node) -> Inspected {
        let Some(incarnation) = node.incarnation_id.clone() else { return Inspected::Unavailable(format!("{} holds no incarnation to inspect", node.name)) };
        // The birth's own digest, else the birth as its mesh-primary's projection carried it to this
        // admin (gossip.md §3: the projection preserves the RuntimeFact an authority-bearing admin
        // needs to manage the birth).
        let held_row = self.book.get(node.node_id.as_str()).or_else(|| self.book.projected_at(node.node_id.as_str(), std::time::Instant::now())).filter(|(dg, _)| dg.node.incarnation == incarnation);
        let row = if held_row.as_ref().is_some_and(|(dg, _)| dg.node.runtime.is_some()) {
            None
        } else {
            match self.nodes_storage.runtime(&node.node_id, &incarnation).await {
                Ok(r) => r,
                Err(e) => return Inspected::Unavailable(format!("the durable runtime row of {} at {} could not be read: {e}", node.node_id, incarnation.as_str())),
            }
        };
        match inspection_of(&*self.provider, &held_row, row.as_ref()).await {
            Ok((code, source)) => Inspected::Exited(code, source),
            Err(NotProven::Running) => Inspected::Running,
            Err(NotProven::OsStopped) => Inspected::OsStopped,
            Err(NotProven::Unavailable(why)) => Inspected::Unavailable(why),
        }
    }

    /// The durable runtime rows the tick watches births from, read once per tick. A refused read
    /// leaves the tick watching the view's births only.
    pub(crate) async fn durable_rows(&self) -> Vec<crate::storage::RuntimeRow> {
        match self.nodes_storage.runtimes().await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::info_span!("rdm.node_admin.node.reject.via-runtime-rows-unreadable", reader = %self.held_name(), store = "nodes.storage", error = %e)
                    .in_scope(|| tracing::info!("the durable runtime rows could not be read: this tick watches the view's births only"));
                Vec::new()
            }
        }
    }

    /// One TRUE OFFLINE birth on the mesh primary's tick (node-recovery.md "Detection and proof"
    /// step 3, sequence step 1): the provider's inspection, its verdict recorded when it changes for
    /// the birth ([`Self::record_verdict`]: named, and an exit put as the birth's exit-proof row once,
    /// on the tick that proved it), and the recovery submitted only on an exit proof of a member that
    /// exited outside any workflow. Any other verdict ends here, and so does the exit of a birth whose
    /// removal this admin's book holds announced, read after the inspection (the tick's view predates
    /// it).
    pub(crate) async fn recover_if_proven(&self, n: &Node, verdicts: &ExitVerdicts) -> Inspected {
        let outcome = self.inspect(n).await;
        let row = self.record_verdict(n, &outcome, verdicts).await;
        match &outcome {
            Inspected::Exited(code, source) => {
                if let Some(inc) = n.incarnation_id.as_ref() {
                    let _ = self.status_storage.remove_node_held_running_of(&n.mesh, &n.node_id, inc).await;
                }
                self.clear_os_stopped(n, "exited").await;
                if let Some(Announced { announced, operation, build_id, attempt }) = self.removal_announced(n).await {
                    let exit_proof_row = match &row {
                        Ok(stands) => stands.to_string(),
                        Err(e) => e.clone(),
                    };
                    let incarnation = n.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default();
                    tracing::info_span!(
                        "rdm.node_admin.build.reject.via-exit-inside-workflow",
                        node = %n.name,
                        node_id = %n.node_id,
                        incarnation_id = %incarnation,
                        submitter = %self.held_name(),
                        announced,
                        operation = %operation,
                        build_id = %build_id,
                        attempt,
                        exit_code = code.map(|c| c.to_string()).unwrap_or_default(),
                        exit_proof = *source,
                        exit_proof_row = %exit_proof_row,
                    )
                    .in_scope(|| tracing::info!("the birth's exit belongs to the workflow that announced its removal: it is no drift, and no recovery is submitted"));
                } else {
                    if let (Some(inc), Some(dir)) = (n.incarnation_id.clone(), self.birth_data_dir(n).await) {
                        if let Some(unreadable) = self.status_storage.node_data_dir_unreadable_of(&n.mesh, &n.node_id, &inc).await.ok().flatten() {
                            match crate::deployment::pipeline::inspect_transport_key(&dir) {
                                Ok(_) => {
                                    let _ = self.status_storage.remove_node_data_dir_unreadable_of(&n.mesh, &n.node_id, &inc).await;
                                    tracing::info_span!(
                                        "rdm.node_admin.node.resolve.via-data-dir-repaired",
                                        node = %n.name,
                                        node_id = %n.node_id,
                                        incarnation_id = %inc.as_str(),
                                        path = %unreadable.path,
                                        "otel.kind" = "internal",
                                    )
                                    .in_scope(|| tracing::info!("repaired unreadable inherited data dir"));
                                }
                                Err(_) => return outcome,
                            }
                        } else if self.status_storage.node_data_dir_lost_of(&n.mesh, &n.node_id, &inc).await.ok().flatten().is_none() {
                            match crate::deployment::pipeline::inspect_transport_key(&dir) {
                                Err(crate::deployment::pipeline::InheritedDirError::Gone(_)) => {
                                    let now_ms = local_now_ms();
                                    let (since, count) = {
                                        let mut gone = self.gone_dirs.lock().unwrap();
                                        let entry = gone.entry((n.node_id.clone(), inc.clone())).or_insert((now_ms, 0));
                                        entry.1 = entry.1.saturating_add(1);
                                        *entry
                                    };
                                    let hold_ms = data_dir_gone_hold_ms();
                                    if count < 2 || now_ms.saturating_sub(since) < hold_ms {
                                        return outcome;
                                    }
                                    let lost_row = crate::status_storage::NodeDataDirLostRow {
                                        node_id: n.node_id.clone(),
                                        name: n.name.clone(),
                                        incarnation_id: inc.clone(),
                                        path: dir.display().to_string(),
                                        since,
                                    };
                                    let _ = self.status_storage.put_node_data_dir_lost_of(&n.mesh, &lost_row).await;
                                    tracing::info_span!(
                                        "rdm.node_admin.node.update.via-data-dir-lost",
                                        node = %n.name,
                                        node_id = %n.node_id,
                                        incarnation_id = %inc.as_str(),
                                        path = %dir.display(),
                                        since = since as i64,
                                        "otel.kind" = "internal",
                                    )
                                    .in_scope(|| tracing::info!("inherited data dir absent past gone-floors hold; recovering over a fresh data dir"));
                                }
                                Ok(_) | Err(crate::deployment::pipeline::InheritedDirError::Broken(_)) => {
                                    self.gone_dirs.lock().unwrap().remove(&(n.node_id.clone(), inc));
                                }
                            }
                        }
                    }
                    if let Err(e) = self.submit(n, *code, source, row).await {
                        tracing::info!(node = %n.name, error = %e, "the recovery submit ended without the recovery; the next tick submits again");
                    }
                }
            }
            Inspected::Running => {
                // Running again: the os-stopped fact clears and the hold rule applies (a runtime that
                // resumed but stays unreachable opens a fresh hold, deployment-provider.md §2.5).
                self.clear_os_stopped(n, "running").await;
                if self.removal_announced(n).await.is_none() {
                    if let Some(inc) = n.incarnation_id.clone() {
                        let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
                        let prev = self.status_storage.node_held_running_of(&n.mesh, &n.node_id, &inc).await.ok().flatten();
                        // node-recovery.md step 4's end: the birth's failed terminates gate the hold.
                        let gate = self.hold_gate_of(n, prev.as_ref().map(|r| r.since)).await;
                        let prev = match &gate {
                            HoldGate::Void(f) => {
                                let _ = self.status_storage.remove_node_held_running_of(&n.mesh, &n.node_id, &inc).await;
                                self.span_hold_voided(n, f);
                                None
                            }
                            _ => prev,
                        };
                        let (since, count) = match prev {
                            Some(r) => (r.since, r.verdicts.saturating_add(1)),
                            None => (now_ms, 1),
                        };
                        let held_row = crate::status_storage::NodeHeldRunningRow { node_id: n.node_id.clone(), name: n.name.clone(), incarnation_id: inc.clone(), since, verdicts: count };
                        let _ = self.status_storage.put_node_held_running_of(&n.mesh, &held_row).await;
                        tracing::info_span!(
                            "rdm.node_admin.node.update.via-held-running",
                            node = %n.name,
                            node_id = %n.node_id,
                            incarnation_id = %inc.as_str(),
                            recorder = %self.held_name(),
                            since = since as i64,
                            verdicts = count as i64,
                            outcome = "recorded",
                            "otel.kind" = "internal",
                        )
                        .in_scope(|| tracing::info!("recorded held-running verdict"));
                        let hold_ms = hung_hold_ms();
                        if count > 1 && now_ms.saturating_sub(since) >= hold_ms {
                            self.submit_hold_terminate(n, gate).await;
                        }
                    }
                }
            }
            Inspected::OsStopped => self.record_os_stopped(n).await,
            // Nothing proven: the span names the uncertainty; no hold, no exit proof, the next tick asks again.
            Inspected::Unavailable(_) => {}
        }
        outcome
    }

    /// The gate of `n`'s hold that began at `since` (none held yet): the birth's failed terminates as
    /// this admin's Build log holds them, in every Build ([`hold_gate`]).
    pub(crate) async fn hold_gate_of(&self, n: &Node, since: Option<u64>) -> HoldGate {
        let Some(inc) = n.incarnation_id.as_ref() else { return HoldGate::Open };
        match self.builds.terminate_failures_of(&n.node_id, inc).await {
            Ok(failures) => hold_gate(&failures, since),
            Err(e) => HoldGate::Unreadable(format!("{}: the failed terminates of {} ({}) at {} could not be read from its Build log: {e}", self.held_name(), n.name, n.node_id, inc.as_str())),
        }
    }

    /// `n`'s hold expired: its terminate is submitted unless `gate` forbids it (node-recovery.md step
    /// 4's end). A refused terminate is named once per receipt; an unreadable Build log is named and
    /// submits nothing this tick.
    pub(crate) async fn submit_hold_terminate(&self, n: &Node, gate: HoldGate) {
        let incarnation = n.incarnation_id.as_ref().map(|i| i.as_str().to_string()).unwrap_or_default();
        match gate {
            HoldGate::Open | HoldGate::Void(_) => {
                if let Err(e) = self.submit(n, None, "held-running-expired", Ok("held-running")).await {
                    tracing::info!(node = %n.name, error = %e, "the held-running recovery submit ended without the recovery; the next tick submits again");
                }
            }
            HoldGate::Refused(f) => {
                let Some(inc) = n.incarnation_id.clone() else { return };
                if !self.terminate_refusals_named.lock().unwrap().insert((n.node_id.clone(), inc, f.build_id.clone(), f.mesh.clone(), f.attempt)) {
                    return;
                }
                let error = match &f.stage {
                    crate::build_state::TerminateStage::Provider { error } => error.clone(),
                    _ => String::new(),
                };
                tracing::info_span!(
                    "rdm.node_admin.build.reject.via-terminate-refused",
                    node = %n.name,
                    node_id = %n.node_id,
                    incarnation_id = %incarnation,
                    submitter = %self.held_name(),
                    build_id = %f.build_id,
                    lane = f.mesh.as_deref().unwrap_or("fabric"),
                    attempt = f.attempt,
                    stage = f.stage.name(),
                    error = %error,
                    "otel.kind" = "internal",
                )
                .in_scope(|| tracing::info!("a terminate of this birth the provider refused or could not prove: no hold submits its terminate again; a person decides"));
            }
            HoldGate::Unreadable(why) => {
                tracing::info_span!("rdm.node_admin.build.reject.via-terminate-receipts-unreadable", node = %n.name, node_id = %n.node_id, incarnation_id = %incarnation, submitter = %self.held_name(), error = %why, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("the birth's failed terminates could not be read: no terminate is submitted this tick"));
            }
        }
    }

    /// `n`'s hold voided by the terminate `f` that failed at its own inspection: its `held-running` row
    /// is removed and a fresh hold begins (node-recovery.md step 4's end).
    pub(crate) fn span_hold_voided(&self, n: &Node, f: &crate::build_state::FailedTerminate) {
        let verdict = match &f.stage {
            crate::build_state::TerminateStage::Inspection { verdict: crate::build_state::InspectionVerdict::OsStopped } => "os-stopped",
            crate::build_state::TerminateStage::Inspection { verdict: crate::build_state::InspectionVerdict::Unknown } => "unavailable",
            _ => "",
        };
        tracing::info_span!(
            "rdm.node_admin.node.update.via-held-running",
            node = %n.name,
            node_id = %n.node_id,
            incarnation_id = %n.incarnation_id.as_ref().map(|i| i.as_str().to_string()).unwrap_or_default(),
            recorder = %self.held_name(),
            outcome = "voided",
            build_id = %f.build_id,
            attempt = f.attempt,
            verdict,
            "otel.kind" = "internal",
        )
        .in_scope(|| tracing::info!("the hold's terminate failed at its own inspection and asked the provider nothing: the hold is void and a fresh one begins"));
    }

    /// The verdict `outcome` of `n`'s birth, recorded by the tick once per change: the inspection's
    /// span, and for an exit the birth's exit-proof row in status.storage (node-recovery.md "Exited
    /// is the proof, recorded durably"; [`crate::offline::put_exit_proof`], a blind put of the birth's
    /// own key). `verdicts` holds what this loop last recorded per birth, so a verdict that stands is
    /// neither named nor put again by a later tick; a put the store refused records nothing, so the
    /// next tick that proves the exit puts the row. For an exit, the answer is where its row stands:
    /// `put` by this tick, `recorded` by an earlier one, or the store's refusal of this tick's put.
    pub(crate) async fn record_verdict(&self, n: &Node, outcome: &Inspected, verdicts: &ExitVerdicts) -> Result<&'static str, String> {
        let incarnation = n.incarnation_id.clone().ok_or_else(|| format!("{} ({}) holds no incarnation: no birth to key its verdict by", n.name, n.node_id))?;
        record_verdict_once(verdicts, (n.node_id.clone(), incarnation), outcome, || async {
            self.span_inspection(n, outcome);
            if let Inspected::Exited(code, source) = outcome {
                crate::offline::put_exit_proof(&*self.status_storage, n, *code, source, local_now_ms()).await?;
            }
            Ok(())
        })
        .await
    }

    /// The `os-stopped` fact of `n`'s exact birth (R-PV1; deployment-provider.md §2.5): this
    /// mesh-primary puts the birth's own row, its `since` kept and its inspections counted, opens no
    /// hold and terminates nothing; a `held-running` hold already open on the birth is cancelled (its
    /// row removed: the pending terminate it would submit is void). Spanned when the fact is first
    /// recorded, or first confirmed by this admin over a row another admin wrote (`revalidated`).
    pub(crate) async fn record_os_stopped(&self, n: &Node) {
        let Some(inc) = n.incarnation_id.clone() else { return };
        let birth = (n.node_id.clone(), inc.clone());
        let span = |outcome: &str, since: u64, detail: &str| {
            tracing::info_span!("rdm.node_admin.node.update.via-os-stopped", node = %n.name, node_id = %n.node_id, incarnation_id = %inc.as_str(), since = since as i64, outcome, detail, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("the operating system holds the birth's exact runtime suspended: a recorded fact, no hold and no terminate"));
        };
        let prev = match self.status_storage.node_os_stopped_of(&n.mesh, &n.node_id, &inc).await {
            Ok(prev) => prev,
            Err(e) => return span("refused", 0, &format!("the os-stopped row could not be read: {e}")),
        };
        let first_here = !self.os_stopped_confirmed.lock().unwrap().contains(&birth);
        let (since, inspections, outcome) = match &prev {
            None => (local_now_ms(), 1, Some("recorded")),
            Some(r) => (r.since, r.inspections.saturating_add(1), first_here.then_some("revalidated")),
        };
        let row = crate::status_storage::NodeOsStoppedRow { node_id: n.node_id.clone(), name: n.name.clone(), incarnation_id: inc.clone(), since, inspections };
        match self.status_storage.put_node_os_stopped_of(&n.mesh, &row).await {
            // A refused put confirms nothing: the next tick records or revalidates it again.
            Err(e) => span("refused", since, &format!("the os-stopped row could not be written: {e}")),
            Ok(()) => {
                self.os_stopped_confirmed.lock().unwrap().insert(birth);
                if let Some(outcome) = outcome {
                    span(outcome, since, "");
                }
            }
        }
        if let Ok(Some(held)) = self.status_storage.node_held_running_of(&n.mesh, &n.node_id, &inc).await {
            let removed = self.status_storage.remove_node_held_running_of(&n.mesh, &n.node_id, &inc).await;
            tracing::info_span!(
                "rdm.node_admin.node.update.via-held-running",
                node = %n.name,
                node_id = %n.node_id,
                incarnation_id = %inc.as_str(),
                since = held.since as i64,
                verdicts = held.verdicts as i64,
                outcome = "cancelled-os-stopped",
                voided_deadline = held.since.saturating_add(hung_hold_ms()) as i64,
                removed = %removed.as_ref().map(|_| "removed".to_string()).unwrap_or_else(|e| e.to_string()),
                "otel.kind" = "internal",
            )
            .in_scope(|| tracing::info!("the hung-runtime hold is cancelled: the runtime is suspended by the operating system, and the terminate it would submit is void"));
        }
    }

    /// Remove the `os-stopped` fact of `n`'s exact birth when an inspection reads its runtime
    /// `verdict` (`running`, `exited`) or its digest returns (`returned`): spanned `cleared` when a row
    /// stood.
    pub(crate) async fn clear_os_stopped(&self, n: &Node, verdict: &str) {
        let Some(inc) = n.incarnation_id.clone() else { return };
        self.os_stopped_confirmed.lock().unwrap().remove(&(n.node_id.clone(), inc.clone()));
        let Ok(Some(row)) = self.status_storage.node_os_stopped_of(&n.mesh, &n.node_id, &inc).await else { return };
        let removed = self.status_storage.remove_node_os_stopped_of(&n.mesh, &n.node_id, &inc).await;
        let (outcome, detail) = match &removed {
            Ok(()) => ("cleared", verdict.to_string()),
            Err(e) => ("refused", format!("{verdict}: the os-stopped row could not be removed: {e}")),
        };
        tracing::info_span!("rdm.node_admin.node.update.via-os-stopped", node = %n.name, node_id = %n.node_id, incarnation_id = %inc.as_str(), since = row.since as i64, outcome, detail = %detail, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the os-stopped fact is cleared"));
    }

    /// The workflow whose announced removal `n`'s exact birth is under, as this admin's book holds it
    /// now (node-recovery.md: recovery is drift reconciliation for a member that exits outside any
    /// workflow; "Failure and receipts": an exit during a workflow belongs to that workflow): its
    /// node's departure (`node-deleted`), else an open `NodeDeleting` overlay naming the exact birth
    /// (`node-deleting`), else the mesh.delete of its mesh (mesh-delete.md: from mesh.deleting, the
    /// mesh's status deleting, to mesh.deleted, every member's removal is the mesh.delete's;
    /// `mesh-deleting`, `mesh-deleted`), the status the book holds for the mesh. A recovery's own
    /// node.delete of the exited birth (`node.delete:<path>`) is such a workflow too: its overlay
    /// closes when its attempt's verdict is terminal (`DigestBook::deletions_from_facts`), and a
    /// recovery whose Build ended Failed is submitted again by the next tick: from the view while it
    /// holds the birth, else from the Failed attempt the accepted Build holds once its delete departed
    /// the birth ([`Self::owed_submits`]).
    pub(crate) async fn removal_announced(&self, n: &Node) -> Option<Announced> {
        let incarnation = n.incarnation_id.as_ref()?;
        let of = |announced: &'static str, op: rdm_core::LifecycleOp| Announced { announced, operation: op.operation, build_id: op.build_id, attempt: op.attempt };
        if let Some(op) = self.book.departed().into_iter().find(|op| op.node_id == n.node_id) {
            return Some(of("node-deleted", op));
        }
        if let Some(op) = self.book.in_flight().into_iter().find(|op| op.node_id == n.node_id && &op.incarnation == incarnation && !op.is_restart() && !op.is_stop()) {
            return Some(of("node-deleting", op));
        }
        let announced = match self.book.statuses.mesh(&n.mesh)?.status.as_str() {
            "leaving" => "mesh-deleting",
            "dead" => "mesh-deleted",
            _ => return None,
        };
        let operation = self.topology.read().await.meshes.iter().find(|m| m.name == n.mesh).and_then(|m| m.id.clone()).map(|id| crate::mesh_delete::operation(&id)).unwrap_or_else(|| format!("mesh.delete of {}", n.mesh));
        Some(Announced { announced, operation, build_id: String::new(), attempt: 0 })
    }

    /// The inspection's span, named when its verdict for the birth changes (a hung member is
    /// inspected every tick and named once).
    pub(crate) fn span_inspection(&self, node: &Node, outcome: &Inspected) {
        let (verdict, exit_code, exit_proof, reason) = match outcome {
            Inspected::Exited(code, source) => ("exited", code.map(|c| c.to_string()).unwrap_or_default(), *source, String::new()),
            Inspected::Running => ("running", String::new(), "none", NotProven::Running.to_string()),
            Inspected::OsStopped => ("os-stopped", String::new(), "none", NotProven::OsStopped.to_string()),
            Inspected::Unavailable(why) => ("unavailable", String::new(), "none", why.clone()),
        };
        let incarnation = node.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default();
        tracing::info_span!("rdm.node_admin.node.resolve.via-provider-inspection", node = %node.name, node_id = %node.node_id, incarnation_id = %incarnation, inspector = %self.held_name(), verdict, exit_code, exit_proof, reason = %reason)
            .in_scope(|| tracing::info!("the mesh primary asked the provider about the TRUE OFFLINE member's exact runtime"));
    }

    /// The recovery of `node`'s exited birth on its exit-proof row, `row` as the tick recorded it
    /// ([`Self::record_verdict`]): a row the store refused ends the submit here, named on its own
    /// span; on a row that stands this mesh primary opens and runs the recovery itself, keyed on the
    /// exited birth ([`Self::submit_in`], R-BI7). The run is followed to its end, so the caller holds
    /// the birth as submitted until then. A repeat answers AlreadyApplied or BuildInProgress.
    pub(crate) async fn submit(&self, node: &Node, code: Option<i32>, source: &'static str, row: Result<&'static str, String>) -> Result<(), String> {
        let incarnation = node.incarnation_id.clone().ok_or_else(|| format!("{} holds no incarnation: no exited birth to key a recovery on", node.name))?;
        let me = self.held_name();
        let span = submit_span(&me, node, &incarnation, code, source);
        let end = match row {
            Ok(_) => self.submit_in(&span, node, &incarnation).instrument(span.clone()).await,
            Err(e) => Err(e),
        };
        settled(&span, end)
    }

    /// The exited births the tick submits again (node-recovery.md "Failure and receipts": "the
    /// mesh-primary is the submitter that resubmits by `build_id` ... the mesh-primary's next tick
    /// finds the member still proven exited and submits"): none unless this admin holds its mesh's
    /// primary seat (as [`recovery_due`]); else every exited
    /// birth of its own mesh whose node workflow in the accepted Build stopped at a Failed attempt and
    /// is not done, read from the durable facts on every tick (the current Build, then the
    /// Build log's index by that Build, `BuildStateAdapter::exits_owed`), whether or not the view still
    /// holds the birth and whichever admin submitted it before. Nothing of it is held between ticks: an
    /// admin that takes the seat, or restarts, reads the same facts. A refused read is named on its
    /// own span and owes nothing this tick.
    pub(crate) async fn owed_submits(&self, holds_seat: bool, mesh: &str) -> Vec<crate::build_state::OwedExit> {
        if !holds_seat {
            return Vec::new();
        }
        let me = self.held_name();
        let unreadable = |store: &str, build_id: &str, error: String| {
            tracing::info_span!("rdm.node_admin.build.reject.via-owed-exits-unreadable", reader = %me, mesh, store, build_id, error = %error)
                .in_scope(|| tracing::info!("the durable facts of the exits owed a submit could not be read: none is submitted this tick"));
        };
        let build_id = match self.builds.current_build().await {
            Ok(current) => match current.into_build_id() {
                Some(id) => id,
                // No current Build is held: no attempt can have been opened on it.
                None => return Vec::new(),
            },
            Err(e) => {
                unreadable("builds.storage", "", e.to_string());
                return Vec::new();
            }
        };
        let owed = match self.builds.exits_owed(&build_id, mesh).await {
            Ok(owed) => owed,
            Err(e) => {
                unreadable("builds.storage", &build_id.as_str(), e.to_string());
                return Vec::new();
            }
        };
        let mut ready = Vec::with_capacity(owed.len());
        for o in owed {
            if let Some(held) = self.status_storage.node_data_dir_unreadable_of(mesh, &o.node_id, &o.incarnation).await.ok().flatten() {
                match crate::deployment::pipeline::inspect_transport_key(std::path::Path::new(&held.path)) {
                    Ok(_) => {
                        let _ = self.status_storage.remove_node_data_dir_unreadable_of(mesh, &o.node_id, &o.incarnation).await;
                        tracing::info_span!(
                            "rdm.node_admin.node.resolve.via-data-dir-repaired",
                            node = %o.path,
                            node_id = %o.node_id,
                            incarnation_id = %o.incarnation.as_str(),
                            path = %held.path,
                            "otel.kind" = "internal",
                        )
                        .in_scope(|| tracing::info!("repaired unreadable inherited data dir"));
                        ready.push(o);
                    }
                    Err(_) => continue,
                }
            } else {
                ready.push(o);
            }
        }
        ready
    }

    /// The submit of an exited birth whose node workflow stopped at a Failed attempt (node-recovery.md
    /// "Failure and receipts"), on the tick: the birth's exit-proof row is point-read (the provider's
    /// proof the first submit recorded; nothing is inspected or written again), then the same submit,
    /// keyed on the exited birth. A missing or unreadable row is no proof: named on its own span, and
    /// nothing is submitted. The fabric-primary answers the submit as any repeat: AlreadyApplied once a
    /// workflow completed for the birth, BuildInProgress while an attempt runs, else the next attempt
    /// of the Build with this birth's action, whose plan runs only what the receipts leave (a delete
    /// that completed is never run again).
    pub(crate) async fn submit_owed(&self, owed: &crate::build_state::OwedExit) -> Result<(), String> {
        let me = self.held_name();
        let refused = |reason: &str, error: String| {
            tracing::info_span!(
                "rdm.node_admin.node.reject.via-exit-proof-unread",
                node = %owed.path,
                node_id = %owed.node_id,
                incarnation_id = %owed.incarnation.as_str(),
                submitter = %me,
                failed_attempt = owed.attempt,
                reason,
                error = %error,
            )
            .in_scope(|| tracing::info!("an exit owed a submit has no readable exit-proof row: nothing is submitted"));
            format!("{me}: no exit-proof row of {} at {} to submit on ({reason}{})", owed.node_id, owed.incarnation.as_str(), if error.is_empty() { String::new() } else { format!(": {error}") })
        };
        let row = match self.status_storage.node_exit(&owed.node_id, &owed.incarnation).await {
            Ok(Some(row)) => row,
            Ok(None) => return Err(refused("missing", String::new())),
            Err(e) => return Err(refused("unreadable", e.to_string())),
        };
        let mut node = Node::allocated(owed.path.clone());
        node.node_id = owed.node_id.clone();
        node.incarnation_id = Some(owed.incarnation.clone());
        let span = submit_span(&me, &node, &owed.incarnation, row.exit_code, &row.proof);
        span.record("after_failed_attempt", owed.attempt);
        let end = self.submit_in(&span, &node, &owed.incarnation).instrument(span.clone()).await;
        settled(&span, end)
    }

    /// The recovery of `node`'s exited birth at `incarnation`, run by this admin as its mesh's primary
    /// (R-BI7; node-recovery.md as R-BI7 moves it: "the mesh-primary opens the recovery attempt of the
    /// current Build itself: claim on its mesh's rows, then run; no call to the fabric-primary"):
    /// opened and claimed at its own door, run through its own executor in its mesh's lane and
    /// followed to the terminal ([`crate::build_run::run_recovery`]).
    pub(crate) async fn submit_in(&self, span: &tracing::Span, node: &Node, incarnation: &IncarnationId) -> Result<RecoveryEnd, String> {
        use crate::build_run::{Recovered, RecoveryNotRun};
        use rdm_core::build::BuildReply;
        let door = self.door.get().cloned().ok_or_else(|| format!("{} holds its mesh's primary seat but its Build door is not built yet", self.held_name()))?;
        let opened = |o: &crate::control_plane::Opened, disposition: &str| {
            span.record("build_id", o.build_id.as_str());
            span.record("mesh", o.mesh.as_deref().unwrap_or(""));
            span.record("attempt", o.attempt);
            span.record("disposition", disposition);
        };
        match crate::build_run::run_recovery(&door.control, &door.run, "node-recovery", node.name.clone(), node.node_id.clone(), incarnation.clone()).await {
            Ok(Recovered::Ran { opened: o, terminal }) => {
                opened(&o, "created");
                match terminal {
                    BuildReply::Complete { attempt, .. } => Ok(RecoveryEnd::Complete { attempt }),
                    BuildReply::Failed { attempt, operation, step, reason, .. } => Ok(RecoveryEnd::Failed { attempt, operation, step, reason }),
                    other => Err(format!("attempt {} of {} in mesh {}'s lane ended {}: {other:?}", o.attempt, o.build_id, node.mesh, other.name())),
                }
            }
            Ok(Recovered::AlreadyApplied(o)) => {
                opened(&o, "already-applied");
                Ok(RecoveryEnd::Complete { attempt: o.attempt })
            }
            Err(RecoveryNotRun::Refused(r)) => {
                let reply = crate::build_op::refusal_reply(r);
                Err(format!("{}: {reply:?}", reply.name()))
            }
            Err(RecoveryNotRun::RunRefused { opened: o, reply }) => {
                opened(&o, "created");
                Err(format!("attempt {} of {} in mesh {}'s lane was opened and its run refused: {}: {reply:?}", o.attempt, o.build_id, node.mesh, reply.name()))
            }
            Err(RecoveryNotRun::Broke { opened: o, why }) => {
                opened(&o, "created");
                Err(why)
            }
        }
    }
}

/// How a followed recovery Build ended: its terminal frame.
pub(crate) fn settled(span: &tracing::Span, end: Result<RecoveryEnd, String>) -> Result<(), String> {
    let (outcome, r) = settle(end);
    span.record("outcome", outcome.as_str());
    span.in_scope(|| tracing::info!("the mesh primary opens and runs the recovery of its proven-exited member itself"));
    r
}

/// The span of one submit of the exited birth `node` at `incarnation` by the mesh primary `me`
/// (node-recovery.md "Observability contract": the submit has a span on the mesh-primary).
/// `after_failed_attempt` is recorded on a submit that follows a Failed one of the same birth.
pub(crate) fn submit_span(me: &PathName, node: &Node, incarnation: &IncarnationId, code: Option<i32>, source: &str) -> tracing::Span {
    tracing::info_span!(
        "rdm.node_admin.build.update.via-node-recovery",
        node = %node.name,
        node_id = %node.node_id,
        incarnation_id = %incarnation.as_str(),
        submitter = %me,
        exit_code = code.map(|c| c.to_string()).unwrap_or_default(),
        exit_proof = source,
        mesh = tracing::field::Empty,
        build_id = tracing::field::Empty,
        attempt = tracing::field::Empty,
        disposition = tracing::field::Empty,
        after_failed_attempt = tracing::field::Empty,
        outcome = tracing::field::Empty,
    )
}

/// The silence-status memo of the mesh primary's tick: what it last put per node.
pub(crate) type WrittenSilence = HashMap<NodeId, (Option<IncarnationId>, Option<NodeStatus>)>;

/// The births the durable runtime rows name at a path `view` holds nothing at: a birth this admin
/// never heard and never launched, at a path the accepted Build names (`planned`), whose departure
/// is not held (a departed birth's row is history). Each stands in the map as `PendingReconnect`,
/// to be inspected like any unheard birth: only an exited runtime counts. One birth per path.
pub fn durable_births(view: &Topology, durable: &[crate::storage::RuntimeRow], planned: &dyn Fn(&PathName) -> bool, departed: &dyn Fn(&str) -> bool) -> Vec<Node> {
    let mut births: Vec<Node> = Vec::new();
    for row in durable {
        if view.node(&row.name).is_none() && !births.iter().any(|n| n.name == row.name) && planned(&row.name) && !departed(row.node_id.as_str()) {
            let mut n = Node::allocated(row.name.clone());
            n.node_id = row.node_id.clone();
            n.incarnation_id = Some(row.incarnation_id.clone());
            n.status = NodeStatus::PendingReconnect;
            births.push(n);
        }
    }
    births
}

/// What the mesh primary's tick watches, by node id: every silent (`PendingReconnect` or `Dead`)
/// member of its own mesh in its view other than itself (the birth `me_id`: never by path.name,
/// which a recovery keeps under a new node id); and every birth of its own mesh only a durable
/// runtime row names (`durable_births`: never heard, never launched by this admin), which is node
/// recovery driven by this mesh-primary like any other member's (mesh-recovery.md:8-9). The tickle
/// ladder is the mesh-primary's for its own members (node-recovery.md); a peer mesh's node-admins
/// are reached by the fabric-primary's investigation alone (`crate::investigate`).
pub fn watched_births(view: &Topology, me_id: &NodeId, mesh: &str, durable: &[Node]) -> BTreeMap<String, Node> {
    view.members()
        .filter(|n| n.node_id != *me_id && n.mesh == mesh && matches!(n.status, NodeStatus::PendingReconnect | NodeStatus::Dead))
        .chain(durable.iter().filter(|n| n.mesh == mesh && n.node_id != *me_id))
        .map(|n| (n.node_id.to_string(), n.clone()))
        .collect()
}

/// The watched births the tick inspects for recovery (node-recovery.md): none unless this admin
/// holds its mesh's primary seat; else every watched birth of its own mesh (its secondary
/// node-admins included, mesh-recovery.md:8-9) held TRUE OFFLINE at its exact incarnation.
pub fn recovery_due(holds_seat: bool, mesh: &str, watched: &BTreeMap<String, Node>, offline: &HashSet<(NodeId, IncarnationId)>) -> Vec<Node> {
    if !holds_seat {
        return Vec::new();
    }
    watched.values().filter(|n| n.mesh == mesh && n.incarnation_id.as_ref().is_some_and(|i| offline.contains(&(n.node_id.clone(), i.clone())))).cloned().collect()
}

/// The mesh primary's per-birth bookkeeping speaks only for the births it still watches: a birth
/// whose node left the view (deleted, or recovered under a new node.id at its path.name) or whose
/// node the view now holds at another incarnation (superseded) leaves the TRUE OFFLINE set; a
/// verdict lives only while its birth is TRUE OFFLINE; the silence memo only while its node is
/// watched. Run every tick, after the tick's writes, so nothing of a gone birth outlives one tick.
pub(crate) fn clear_unwatched(watched: &BTreeMap<String, Node>, offline: &mut HashSet<(NodeId, IncarnationId)>, verdicts: &mut HashMap<(NodeId, IncarnationId), Inspected>, written: &mut WrittenSilence) {
    offline.retain(|(node_id, incarnation)| watched.get(node_id.as_str()).is_some_and(|n| n.incarnation_id.as_ref() == Some(incarnation)));
    verdicts.retain(|birth, _| offline.contains(birth));
    written.retain(|node_id, _| watched.contains_key(node_id.as_str()));
}

/// How a followed recovery Build ended: its terminal frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecoveryEnd {
    /// The Build converged at this attempt.
    Complete { attempt: u32 },
    /// A step of the attempt failed (node-recovery.md "Failure and receipts": `Failed { step, reason }`).
    Failed { attempt: u32, operation: String, step: String, reason: String },
}

/// The submit span's `outcome` and the submit's result, from how the followed recovery ended.
/// A Failed recovery ends the submit in an error: the Build holds the Failed attempt, and the next
/// tick reads it and submits the birth again (node-recovery.md "Failure and receipts",
/// `RecoverySubmitter::owed_submits`).
pub(crate) fn settle(end: Result<RecoveryEnd, String>) -> (String, Result<(), String>) {
    match end {
        Ok(RecoveryEnd::Complete { attempt }) => (format!("complete: attempt {attempt}"), Ok(())),
        Ok(RecoveryEnd::Failed { attempt, operation, step, reason }) => {
            let failed = format!("failed: attempt {attempt} {operation} step {step}: {reason}");
            (failed.clone(), Err(failed))
        }
        Err(e) => (e.clone(), Err(e)),
    }
}

#[cfg(test)]
mod hold_gate_tests {
    use super::*;
    use crate::build_state::{FailedTerminate, InspectionVerdict, TerminateStage};

    /// CONTRACT (i144 R21; node-recovery.md step 4's end: "A terminate the provider refuses or cannot
    /// prove ... no hold submits that birth's terminate again: a person decides ... A
    /// `TerminateRuntime` that failed at its own inspection (`OsStopped` or `Unknown`) asked the
    /// provider nothing: it voids the hold, and a birth that reads `Running` and stays unreachable
    /// opens a fresh hold under the ordinary rule"): any failed terminate of the birth whose stage is
    /// `provider` or `unproven` refuses every later hold, whatever came after it; else the latest, when
    /// it failed at its own inspection after the hold began (or its opening is not held yet), voids
    /// the hold; a hold that began after that failure, or no failure at all, is open. Must NOT happen:
    /// a refused birth resubmitted after a later inspection failure, a hold carried across the
    /// failure it submitted, or a fresh hold voided again.
    // @feature: node-lifecycle
    #[test]
    fn a_hold_is_refused_by_any_provider_or_unproven_terminate_and_voided_by_a_later_inspection_failure() {
        let failed = |attempt: u32, opened_at_ms: Option<u64>, stage: TerminateStage| FailedTerminate { build_id: crate::build::BuildId::mint(), mesh: Some("mesh2".into()), attempt, opened_at_ms, stage };
        let inspection = || TerminateStage::Inspection { verdict: InspectionVerdict::OsStopped };
        let provider = || TerminateStage::Provider { error: "refused".into() };
        assert_eq!(hold_gate(&[], Some(5)), HoldGate::Open, "no failed terminate");
        let void = failed(1, Some(10), inspection());
        assert_eq!(hold_gate(std::slice::from_ref(&void), Some(5)), HoldGate::Void(void.clone()), "the hold that submitted the failed terminate is void");
        assert_eq!(hold_gate(std::slice::from_ref(&void), Some(10)), HoldGate::Void(void.clone()), "begun in the same millisecond: void");
        assert_eq!(hold_gate(std::slice::from_ref(&void), Some(15)), HoldGate::Open, "a fresh hold, begun after the failure, runs");
        assert_eq!(hold_gate(std::slice::from_ref(&void), None), HoldGate::Open, "no hold held: nothing to void");
        let unopened = failed(1, None, TerminateStage::Inspection { verdict: InspectionVerdict::Unknown });
        assert_eq!(hold_gate(std::slice::from_ref(&unopened), Some(15)), HoldGate::Void(unopened.clone()), "the failure's opening not held yet: the hold restarts rather than terminate early");
        let refused = failed(1, Some(10), provider());
        assert_eq!(hold_gate(std::slice::from_ref(&refused), Some(50)), HoldGate::Refused(refused.clone()), "a provider refusal forbids every later hold");
        let after = failed(2, Some(20), inspection());
        assert_eq!(hold_gate(&[refused.clone(), after], Some(5)), HoldGate::Refused(refused), "a later inspection failure never lifts a refusal");
        let unproven = failed(1, Some(10), TerminateStage::Unproven);
        assert_eq!(hold_gate(std::slice::from_ref(&unproven), Some(50)), HoldGate::Refused(unproven.clone()), "an unproven exit forbids every later hold");
    }
}
