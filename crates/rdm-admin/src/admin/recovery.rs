//! Node recovery submitter and recovery due checks.

use super::*;

/// What the mesh primary's recovery of one TRUE OFFLINE member reads and calls
/// (node-recovery.md "Detection and proof" step 3, sequence step 1).
#[derive(Clone)]
pub(crate) struct RecoverySubmitter {
    /// This mesh primary's path: the submitter.
    pub me_path: String,
    /// This node's name, beside its path.
    pub me_name: String,
    /// Globally unique identity of the submitting admin.
    pub me_id: NodeId,
    pub topology: Arc<RwLock<Topology>>,
    pub digest_view: DigestView,
    pub provider: Arc<dyn DeploymentProvider>,
    pub nodes_storage: Arc<dyn crate::storage::NodesStorage>,
    /// The node records this admin writes, as it holds them: the data-dir facts are written on them.
    pub written: Arc<crate::node_rows::WrittenRows>,
    /// This admin's runner, once built: it runs the recovery and the terminate of a deleted birth
    /// heard again, the primary's own workflow (Q342).
    pub runner: Arc<std::sync::OnceLock<Arc<super::runner::AdminRunner>>>,
    /// This admin's Build door: fabric recovery under an in-force fabric.delete claims the delete's next
    /// attempt through its control plane (fabric-recovery.md step 4, Q155); a fabric.delete is a Build.
    pub door: crate::build_op::BuildSlot,
    /// This admin's Node RPC client; none in a rig that runs no Node RPC.
    pub client: Option<Arc<rdm_transport::NodeRpcClient>>,
    /// This admin's Build state: the current Build's paths a refused caller's decision reads (Q217).
    pub builds: Arc<dyn crate::build_state::BuildStateAdapter>,
    /// Parent directory under which birth data directories are allocated (`<data_root>/<name>-<node_id>`).
    pub data_root: PathBuf,
    /// Per exited birth whose inherited `data_dir` was observed absent (`Gone`, `R-FM2`):
    /// `(first_seen_ms, consecutive_observations)`.
    pub gone_dirs: Arc<Mutex<HashMap<(NodeId, IncarnationId), (u64, u64)>>>,
    /// Per birth whose last recovery ended without completing: when it is eligible again, under the
    /// one backoff rule ([`RecoverySubmitter::submit_ended`]). A birth with none is due at once.
    pub resubmits: Arc<Mutex<HashMap<(NodeId, IncarnationId), Resubmit>>>,
    /// The births whose recovery failed by a retryable cause, with the node each is of: run again once
    /// eligible, whether or not the view still holds the birth (its delete may have completed)
    /// (node-recovery.md "Detection and the verdict", "Failure"). Removed once a recovery of it completed.
    pub failed: Arc<Mutex<HashMap<(NodeId, IncarnationId), Node>>>,
    /// Where a verdict that opens a re-create is written as its `rdm.node.investigated` event, and the
    /// application's `on_node_investigated`, sent from that write (Q312).
    pub investigated: crate::node_investigation::NodeInvestigatedWriter,
}

impl RecoverySubmitter {
    /// The `(path, name)` lookup index key of me_path and me_name.
    pub fn me(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.me_path.clone(), name: self.me_name.clone() } }
}

/// The next submit owed to one birth whose submits ended without the recovery: the one backoff rule
/// (node-rpc.md §43, [`rdm_transport::delivery::Pacing`]): the first again after the gossip interval,
/// each later one after twice the delay before, to the staleness floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Resubmit {
    /// The submits in a row that ended without the recovery.
    pub failures: u32,
    /// When the next submit is due.
    pub due: std::time::Instant,
}

/// What the provider's inspection of a TRUE OFFLINE member found: node.runtime.inspect's own answer
/// ([`crate::deployment::provider::inspect_runtime`]).
pub use crate::deployment::provider::Inspected;

/// node.runtime.inspect's answer as its span names it (node-runtime-inspect.md "Spans"): `running`,
/// `os-stopped`, `exited` or `unavailable`.
pub(crate) fn verdict_name(outcome: &Inspected) -> &'static str {
    match outcome {
        Inspected::Running => "running",
        Inspected::OsStopped => "os-stopped",
        Inspected::Exited(..) => "exited",
        Inspected::Unavailable(_) => "unavailable",
    }
}

/// The investigation's verdict on an unreached birth from the inspection's answer `name`
/// ([`verdict_name`]): `unavailable` is `unproven` (node-investigation.md step 4).
fn investigated_verdict(name: &str) -> crate::wiring::NodeInvestigatedVerdict {
    use crate::wiring::NodeInvestigatedVerdict as V;
    match name {
        "running" => V::Running,
        "os-stopped" => V::OsStopped,
        "exited" => V::Exited,
        _ => V::Unproven,
    }
}

impl RecoverySubmitter {

    /// The name this admin currently holds in its topology digest view, or its boot name before its
    /// birth is in the view.
    pub(crate) fn held_name(&self) -> PathName {
        self.digest_view.held_path_name().unwrap_or_else(|| self.me())
    }

    /// Resolve `node`'s inherited `data_dir` when known (`R-FM2`): from `node.data_dir`, its digest in
    /// `self.digest_view`, its durable runtime row in `self.nodes_storage`, or `<data_root>/<node_id>`.
    pub(crate) async fn birth_data_dir(&self, node: &Node) -> Option<PathBuf> {
        if let Some(d) = node.data_dir.as_deref() {
            return Some(PathBuf::from(d));
        }
        if let Some(d) = self.digest_view.get(node.node_id.as_str()).filter(|(dg, _)| node.incarnation_id.as_ref() == Some(&dg.node.incarnation)).and_then(|(dg, _)| dg.data_dir.map(PathBuf::from)) {
            return Some(d);
        }
        if let Some(inc) = node.incarnation_id.as_ref() {
            if let Some(d) = self.nodes_storage.runtime(&node.node_id, inc).await.ok().flatten().and_then(|r| r.data_dir.map(PathBuf::from)) {
                return Some(d);
            }
        }
        if !self.data_root.as_os_str().is_empty() {
            return Some(crate::deployment::node_data_dir(&self.data_root, &node.node_id));
        }
        None
    }

    /// The provider's exact-exited verdict of `node`'s birth: node.runtime.inspect over the one runtime
    /// resolution (`node_investigation::birth_runtime`: the view's digest of that exact birth, heard,
    /// projected by its mesh-primary or stopped, else the birth's own durable runtime row). Beside it,
    /// the exact runtime handle the provider was asked about (node-runtime-inspect.md Target), for the
    /// inspection's span.
    pub(crate) async fn inspect(&self, node: &Node) -> (Inspected, Option<DeploymentHandle>) {
        crate::node_investigation::inspect_birth_with_handle(&*self.provider, &self.digest_view, Some(&*self.nodes_storage), node).await
    }

    /// One TRUE OFFLINE birth on the mesh primary's tick (node-recovery.md step 3, sequence step 1):
    /// its ladder ended unreached, so the provider's inspection is the investigation's last step, and
    /// its verdict, whatever it is, opens the recovery at once (Q110, Q111): nothing holds, nothing is
    /// inspected again, nothing waits for a proof of termination. The recovery's node.delete runs
    /// `TerminateRuntime` first when the runtime runs ([`crate::deployment::pipeline`]). A birth whose
    /// removal this admin's view holds announced is that workflow's, read after the inspection (the
    /// tick's view predates it); an `exited` birth whose inherited data dir is absent waits out
    /// `RDM_DATA_DIR_GONE_FLOORS` before its loss is recorded (`R-FM2` arm A).
    pub(crate) async fn recover(&self, n: &Node) -> Inspected {
        let (outcome, handle) = self.inspect(n).await;
        self.span_inspection(n, &outcome, handle.as_ref());
        if let Some(Announced { announced, operation, build_id, attempt }) = self.removal_announced(n).await {
            let incarnation = n.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default();
            tracing::info_span!(
                "rdm.node_admin.build.reject.via-removal-announced",
                node_path = %n.path, node_name = %n.name,
                node_id = %n.node_id,
                incarnation_id = %incarnation,
                submitter = %self.me_id,
                announced,
                operation = %operation.action, operation_subject = ?operation.subject, operation_step = ?operation.step,
                build_id = %build_id,
                attempt,
                verdict = verdict_name(&outcome),
            )
            .in_scope(|| tracing::info!("the birth's removal belongs to the workflow that announced it: no recovery is submitted"));
            return outcome;
        }
        if let (Inspected::Exited(..), Some(inc), Some(dir)) = (&outcome, n.incarnation_id.clone(), self.birth_data_dir(n).await) {
            let facts = self.written.birth_facts(&*self.nodes_storage, &n.node_id, &inc).await.ok().flatten();
            if let Some(unreadable) = facts.as_ref().and_then(|f| f.data_dir_unreadable.clone()) {
                if self.holds_on_unreadable_dir(n, &inc, &dir, &unreadable).await {
                    return outcome;
                }
            } else if facts.as_ref().and_then(|f| f.data_dir_lost.as_ref()).is_none() {
                match crate::deployment::pipeline::inspect_transport_key(&dir) {
                    Err(crate::deployment::pipeline::InheritedDirError::Gone(_)) => {
                        let now_ms = local_now_ms();
                        let (since, count) = {
                            let mut gone = self.gone_dirs.lock().unwrap();
                            let entry = gone.entry((n.node_id.clone(), inc.clone())).or_insert((now_ms, 0));
                            entry.1 = entry.1.saturating_add(1);
                            *entry
                        };
                        if count < 2 || now_ms.saturating_sub(since) < data_dir_gone_hold_ms() {
                            return outcome;
                        }
                        let lost = crate::storage::DataDirLost { path: dir.display().to_string(), since };
                        let _ = self.written.update_birth(&*self.nodes_storage, n, &inc, |b| b.data_dir_lost = Some(lost)).await;
                        tracing::info_span!(
                            "rdm.node_admin.node.update.via-data-dir-lost",
                            node_path = %n.path, node_name = %n.name,
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
        match n.incarnation_id.clone() {
            Some(incarnation) => {
                let _ = self.submit_on_tick(n, &incarnation, verdict_name(&outcome), false).await;
            }
            None => {
                let _ = self.submit(n, &outcome).await;
            }
        }
        outcome
    }

    /// One recovery the tick runs of `node`'s birth at `incarnation` (node-recovery.md "Detection and
    /// the verdict": the loop on every gossip interval), on the investigation's `verdict`, or again after
    /// it failed (`again`). How it ends sets the birth's backoff ([`Self::submit_ended`]), recorded on
    /// the span as `backoff_ms`; a recovery that failed stays owed to the birth ([`Self::failed`]).
    async fn submit_on_tick(&self, node: &Node, incarnation: &IncarnationId, verdict: &str, again: bool) -> Result<(), String> {
        let span = submit_span(&self.me_id, node, incarnation, verdict);
        if again {
            span.record("again", true);
        } else {
            // The verdict opens the re-create: it is written as its event, and the application is told
            // once from that write, under the recovery's span (fabric-node-hooks.md "on_node_investigated").
            let ids = crate::node_investigation::fabric_and_mesh_of(&*self.topology.read().await, node);
            crate::node_investigation::write_node_investigated(&self.investigated, ids, node, incarnation, investigated_verdict(verdict), crate::wiring::NodeInvestigatedNext::Recreate).instrument(span.clone()).await;
        }
        let end = self.submit_in(&span, node, incarnation, "node-recovery").instrument(span.clone()).await;
        let ended = settled(&span, end);
        let birth = (node.node_id.clone(), incarnation.clone());
        match &ended {
            Ok(()) => {
                self.failed.lock().unwrap().remove(&birth);
            }
            Err(_) => {
                self.failed.lock().unwrap().insert(birth.clone(), node.clone());
            }
        }
        if let Some(backoff) = self.submit_ended(&birth, &ended) {
            span.record("backoff_ms", backoff.as_millis() as u64);
        }
        ended
    }

    /// Whether `birth` is eligible for a submit at `now` (node-recovery.md "Detection and the verdict":
    /// after a retryable recovery-submit failure, that exact birth becomes eligible again after a backoff
    /// doubling from the gossip interval to the staleness floor): at once unless its last submit ended
    /// without the recovery, then once its backoff has passed ([`Self::submit_ended`]). The tick asks
    /// before it inspects or submits, so a birth whose submits keep failing (its mesh cut from the
    /// fabric-primary: Q118) is neither inspected nor submitted on every tick.
    pub(crate) fn submit_due(&self, birth: &(NodeId, IncarnationId), now: std::time::Instant) -> bool {
        self.resubmits.lock().unwrap().get(birth).is_none_or(|r| now >= r.due)
    }

    /// The tick's one gate for a submit of `birth` at `now`: eligible ([`Self::submit_due`]) and no
    /// submit of it in flight, which it then marks in flight, for the follower to clear at its end
    /// (`offline::spawn_recovery_follower`). A submit whose recovery was accepted and still runs holds
    /// the birth in flight, so it is followed, never submitted again beside it (Q209).
    pub(crate) fn take_eligible(&self, birth: &(NodeId, IncarnationId), now: std::time::Instant, in_flight: &Mutex<HashSet<(NodeId, IncarnationId)>>) -> bool {
        self.submit_due(birth, now) && in_flight.lock().unwrap().insert(birth.clone())
    }

    /// How a submit of `birth` ended: a recovery that ran to Complete owes nothing more; one that ended
    /// without it (refused, failed by name, or its attempt Failed) is eligible again after the backoff's
    /// next delay (node-recovery.md "Failure and receipts": the mesh-primary submits it again). The
    /// delay, none once the recovery ran.
    pub(crate) fn submit_ended(&self, birth: &(NodeId, IncarnationId), ended: &Result<(), String>) -> Option<std::time::Duration> {
        self.resubmit_after(birth, ended.is_ok(), &rdm_transport::delivery::Pacing::from_cadences(), std::time::Instant::now())
    }

    /// [`Self::submit_ended`] on `pacing` at `now`: the delay before the next submit, none once the
    /// recovery ran.
    pub(crate) fn resubmit_after(&self, birth: &(NodeId, IncarnationId), recovered: bool, pacing: &rdm_transport::delivery::Pacing, now: std::time::Instant) -> Option<std::time::Duration> {
        let mut resubmits = self.resubmits.lock().unwrap();
        if recovered {
            resubmits.remove(birth);
            return None;
        }
        let failures = resubmits.get(birth).map_or(0, |r| r.failures).saturating_add(1);
        let after = pacing.after(failures);
        resubmits.insert(birth.clone(), Resubmit { failures, due: now + after });
        Some(after)
    }

    /// Forget the backoff of every birth the tick no longer owes a submit (`owed` false: heard again,
    /// left the view or replaced at its path and name, or this admin is no longer its mesh-primary), so a
    /// birth unheard again later is eligible at once (Q209: the birth returning or its replacement
    /// clears the backoff).
    pub(crate) fn forget_resubmits(&self, owed: impl Fn(&(NodeId, IncarnationId)) -> bool) {
        self.resubmits.lock().unwrap().retain(|birth, _| owed(birth));
    }

    /// The workflow whose announced removal `n`'s exact birth is under, as this admin's view holds it
    /// now (node-recovery.md: recovery is drift reconciliation for a member that exits outside any
    /// workflow; "Failure and receipts": an exit during a workflow belongs to that workflow): its
    /// node's departure (`node-deleted`), else an open `NodeDeleting` overlay naming the exact birth
    /// (`node-deleting`), else the mesh.delete of its mesh (mesh-delete.md: from mesh.deleting, the
    /// mesh's status deleting, to mesh.deleted, every member's removal is the mesh.delete's;
    /// `mesh-deleting`, `mesh-deleted`), the status the view holds for the mesh. A recovery's own
    /// node.delete of the exited birth (the typed node.delete operation on its node ID) is such a workflow too: its overlay
    /// closes when its attempt's verdict is terminal (`DigestView::deletions_from_facts`), and a
    /// recovery whose Build ended Failed is submitted again by the next tick: from the view while it
    /// holds the birth, else from the Failed attempt the accepted Build holds once its delete deleted
    /// the birth ([`Self::owed_submits`]).
    pub(crate) async fn removal_announced(&self, n: &Node) -> Option<Announced> {
        let incarnation = n.incarnation_id.as_ref()?;
        let of = |announced: &'static str, op: rdm_core::LifecycleOp| Announced { announced, operation: op.operation, build_id: op.build_id, attempt: op.attempt };
        if let Some(op) = self.digest_view.deleted().into_iter().find(|op| op.node_id == n.node_id) {
            return Some(of("node-deleted", op));
        }
        if let Some(op) = self.digest_view.in_flight().into_iter().find(|op| op.node_id == n.node_id && &op.incarnation == incarnation && !op.is_stop()) {
            return Some(of("node-deleting", op));
        }
        let announced = match self.digest_view.statuses.mesh(&n.mesh)?.status.as_str() {
            "leaving" => "mesh-deleting",
            "dead" => "mesh-deleted",
            _ => return None,
        };
        let operation = self.topology.read().await.meshes.iter().find(|m| m.name == n.mesh).and_then(|m| m.id.clone()).map(|id| crate::mesh_delete::operation(&id)).unwrap_or_else(|| rdm_core::OperationId::new("mesh.delete", rdm_core::OperationSubject::Named(n.mesh.clone())));
        Some(Announced { announced, operation, build_id: String::new(), attempt: 0 })
    }

    /// The inspection's span (node-runtime-inspect.md "Spans"), with `handle` the exact runtime handle
    /// the provider was asked about (none when nothing was asked).
    pub(crate) fn span_inspection(&self, node: &Node, outcome: &Inspected, handle: Option<&DeploymentHandle>) {
        crate::node_investigation::span_inspection(&self.me_id.to_string(), node, outcome, handle, "the mesh primary asked the provider about the TRUE OFFLINE member's exact runtime");
    }

    /// The recovery of `node`'s unreached birth on its verdict `outcome`: this mesh primary submits
    /// it, keyed on the birth ([`Self::submit_in`], Q118). The run is followed to its
    /// end, so the caller holds the birth as submitted until then. A repeat answers AlreadyApplied or
    /// BuildInProgress.
    pub(crate) async fn submit(&self, node: &Node, outcome: &Inspected) -> Result<(), String> {
        self.submit_route(node, outcome, "node-recovery").await
    }

    /// [`Self::submit`] on `route`: `node-recovery`, or `fabric-recovery` for the re-create fabric recovery
    /// runs of the node-admin at the fabric-primary's path and name (fabric-recovery.md step 3, Q122).
    pub async fn submit_route(&self, node: &Node, outcome: &Inspected, route: &'static str) -> Result<(), String> {
        let incarnation = node.incarnation_id.clone().ok_or_else(|| format!("{} holds no incarnation: no birth to key a recovery on", node.node_id))?;
        let span = submit_span(&self.me_id, node, &incarnation, verdict_name(outcome));
        let end = self.submit_in(&span, node, &incarnation, route).instrument(span.clone()).await;
        settled(&span, end)
    }

    /// The births whose recovery the tick runs again (node-recovery.md "Detection and the verdict":
    /// "After a recovery step fails by a retryable cause, that exact birth becomes eligible again after a
    /// backoff"): none unless this admin holds its mesh's primary role; else every birth of its own mesh
    /// whose recovery failed here, whether or not the view still holds it.
    pub(crate) fn failed_recoveries(&self, holds_role: bool, mesh: &str) -> Vec<Node> {
        if !holds_role {
            return Vec::new();
        }
        self.failed.lock().unwrap().values().filter(|n| n.mesh == mesh).cloned().collect()
    }

    /// The recovery of a birth whose last recovery failed ([`Self::failed_recoveries`]), run again on the
    /// tick: the same workflow, keyed on the birth; nothing is inspected again.
    pub(crate) async fn recover_again(&self, node: &Node) -> Result<(), String> {
        let incarnation = node.incarnation_id.clone().ok_or_else(|| format!("{} holds no incarnation: no birth to recover", node.node_id))?;
        // A create that found the inherited data dir unreadable failed by no retryable cause: the
        // recovery holds while the dir stays unreadable, owed still, and runs once it reads again
        // (node-recovery.md "Lost data dir (R-FM2)").
        if let Some(dir) = self.birth_data_dir(node).await {
            let facts = self.written.birth_facts(&*self.nodes_storage, &node.node_id, &incarnation).await.ok().flatten();
            if let Some(unreadable) = facts.and_then(|f| f.data_dir_unreadable) {
                if self.holds_on_unreadable_dir(node, &incarnation, &dir, &unreadable).await {
                    return Ok(());
                }
            }
        }
        self.submit_on_tick(node, &incarnation, "", true).await
    }

    /// Whether `n`'s recovery holds on its inherited data dir `dir`, which its create found unreadable
    /// (`unreadable`, recorded on the birth): it holds while the dir's transport key still does not
    /// read; once it reads, the fact is cleared on the birth (`via-data-dir-repaired`) and the recovery
    /// runs.
    async fn holds_on_unreadable_dir(&self, n: &Node, inc: &IncarnationId, dir: &std::path::Path, unreadable: &crate::storage::DataDirUnreadable) -> bool {
        if crate::deployment::pipeline::inspect_transport_key(dir).is_err() {
            return true;
        }
        let _ = self.written.update_birth(&*self.nodes_storage, n, inc, |b| b.data_dir_unreadable = None).await;
        tracing::info_span!(
            "rdm.node_admin.node.resolve.via-data-dir-repaired",
            node_path = %n.path, node_name = %n.name,
            node_id = %n.node_id,
            incarnation_id = %inc.as_str(),
            path = %unreadable.path,
            "otel.kind" = "internal",
        )
        .in_scope(|| tracing::info!("repaired unreadable inherited data dir"));
        false
    }

    /// The recovery of `node`'s unreached birth at `incarnation`, run by this admin, the primary above
    /// the node (node-recovery.md "Execution"): the mesh-primary for its own members, the fabric-primary
    /// for a mesh-primary (Q335), and fabric recovery's executor for the node-admin at the lost
    /// fabric-primary's path and name (`route` `fabric-recovery`, fabric-recovery.md step 3). The primary's
    /// own workflow, never a Build or part of one (Q342): node.delete then node.create, each step recorded
    /// on the node record. It runs on the node's work turn: a node command or a Build's step on the same
    /// node runs before or after it, never beside it; a Build in flight elsewhere neither holds it nor is
    /// held by it.
    pub(crate) async fn submit_in(&self, span: &tracing::Span, node: &Node, incarnation: &IncarnationId, route: &'static str) -> Result<RecoveryEnd, String> {
        let runner = self.runner.get().cloned().ok_or_else(|| format!("{} holds its mesh's primary role but its runner is not built yet", self.me_id))?;
        span.record("route", route);
        // An admin that stopped reconciling (its stop, or a rig that runs no reconciler) starts no repair,
        // as it starts no Build drive or attempt run: nothing is deleted or re-created by it (rdm#305).
        if runner.repairs_stopped.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(RecoveryEnd::Failed { reason: format!("{} stopped reconciling: it starts no repair", self.me_id) });
        }
        // Under an in-force fabric.delete nothing is recovered for being absent and nothing is re-created
        // (fabric-delete.md "Failure": "its executor continues the delete as fabric-primary and
        // re-creates nothing"; fabric-recovery.md step 4, Q155): the delete's own node.delete of the
        // birth is its end. Refused naming the delete before anything is read or written.
        if let Some((delete, _)) = crate::build_state::fabric_delete_in_force(&*self.builds).await.map_err(|e| format!("{}: the Build store could not be read for an in-force fabric.delete: {e}", self.me_id))? {
            span.in_scope(|| {
                tracing::info_span!("rdm.node_admin.build.reject.via-fabric-delete-in-force", route, build_id = %delete.build_id.log(), node_path = %node.path, node_name = %node.name, node_id = %node.node_id, incarnation_id = %incarnation.as_str())
                    .in_scope(|| tracing::info!("a fabric.delete is in force: nothing is recovered and nothing is re-created"))
            });
            return Ok(RecoveryEnd::Failed { reason: format!("fabric.delete {} is in force: nothing is recovered and nothing is re-created", delete.build_id) });
        }
        // From the verdict the investigated birth is a deleted birth here (node-recovery.md step 3,
        // Q342): its digests are refused by name while its replacement is born, as after it.
        self.digest_view.hold_recovering(&node.node_id, incarnation);
        let _turn = runner.node_work(&node.node_id).await;
        let from = crate::build::ExitedBirth { node_id: node.node_id.clone(), incarnation: incarnation.clone() };
        match runner.recover(&node.location(), &from, route == "fabric-recovery-backbone").await {
            Ok(()) => Ok(RecoveryEnd::Complete),
            Err(end) => Ok(RecoveryEnd::Failed { reason: end.reason().to_string() }),
        }
    }
}

/// How a followed recovery Build ended: its terminal frame.
pub(crate) fn settled(span: &tracing::Span, end: Result<RecoveryEnd, String>) -> Result<(), String> {
    let (outcome, r) = settle(end);
    span.record("outcome", outcome.as_str());
    span.in_scope(|| tracing::info!("the primary above the unreached node runs its recovery itself, never a Build"));
    r
}

/// The span of one submit of the unreached birth `node` at `incarnation` by the mesh primary `me` on
/// the investigation's `verdict` (node-recovery.md "Observability contract": the submit has a span on
/// the mesh-primary). `after_failed_attempt` is recorded on a submit that follows a Failed one of the
/// same birth, whose verdict is not read again. `backoff_ms` is recorded on a tick's submit that ended
/// without the recovery: the delay before its birth is eligible again (Q209).
pub(crate) fn submit_span(me: &NodeId, node: &Node, incarnation: &IncarnationId, verdict: &str) -> tracing::Span {
    tracing::info_span!(
        "rdm.node_admin.node.update.via-node-recovery",
        node_path = %node.path, node_name = %node.name,
        node_id = %node.node_id,
        incarnation_id = %incarnation.as_str(),
        submitter = %me,
        verdict,
        mesh = %node.mesh,
        again = tracing::field::Empty,
        route = tracing::field::Empty,
        outcome = tracing::field::Empty,
        backoff_ms = tracing::field::Empty,
    )
}

/// The silence-status memo of the mesh primary's tick: what it last put per node.
pub(crate) type WrittenSilence = HashMap<NodeId, (Option<IncarnationId>, Option<NodeStatus>)>;

/// The durable runtime rows of its own mesh the offline tick of a mesh-primary watches births from
/// ([`durable_births`]). Only the mesh-primary runs node investigation on its mesh's births
/// (fabric-node-lifecycle-elections.md §4.1; node-recovery.md step 2: "Only the mesh-primary acts"), so
/// only it reads them: once, at the first tick that finds this admin its mesh's primary, and held while it
/// keeps the role. A row written after that read is a launch in a mesh whose mesh-primary runs every
/// attempt (node-recovery.md "Execution"), and the launching admin writes it (build.md §5.3): this
/// admin's own executor, whose records put the birth in its view. An admin that is not its mesh's primary
/// holds none and reads none; the role lost, the rows go, and a later tenure reads them again. A read
/// that is refused holds none, so the next tick reads again.
#[derive(Debug, Default)]
pub(crate) struct TenureRows {
    rows: Option<Vec<crate::storage::RuntimeRow>>,
}

impl TenureRows {
    /// The rows for one tick of the admin `reader` of `mesh`, `primary` when its view names it its
    /// mesh's primary: those this tenure read from `nodes`, read now at its first tick.
    pub(crate) async fn for_tick(&mut self, primary: bool, nodes: &dyn crate::storage::NodesStorage, reader: &PathName, mesh: &str) -> &[crate::storage::RuntimeRow] {
        if !primary {
            self.rows = None;
            return &[];
        }
        if self.rows.is_none() {
            self.rows = read_runtime_rows(nodes, reader, mesh).await;
        }
        self.rows.as_deref().unwrap_or_default()
    }
}

/// The durable runtime rows of `mesh` that `nodes` holds, read for `reader` as it takes its mesh's
/// primary role ([`runtime_rows_read`]).
async fn read_runtime_rows(nodes: &dyn crate::storage::NodesStorage, reader: &PathName, mesh: &str) -> Option<Vec<crate::storage::RuntimeRow>> {
    runtime_rows_read(reader, Some(mesh), nodes.runtimes().await)
}

/// What a primary's one read of the durable runtime rows (`read`) holds for its tenure: `mesh`'s rows
/// for a mesh-primary (`Some`, [`TenureRows`]), every mesh's for the fabric-primary (`None`,
/// `FabricTenureRows`), named on one `rdm.node_admin.node.resolve.via-runtime-rows-read` with how many;
/// `None` when the read was refused, named on its own span, so the next tick or pass reads again.
pub(crate) fn runtime_rows_read(reader: &PathName, mesh: Option<&str>, read: Result<Vec<crate::storage::RuntimeRow>, crate::storage_error::StorageError>) -> Option<Vec<crate::storage::RuntimeRow>> {
    let role = if mesh.is_some() { "mesh-primary" } else { "fabric-primary" };
    match read {
        Ok(rows) => {
            let rows: Vec<crate::storage::RuntimeRow> = rows.into_iter().filter(|r| mesh.is_none_or(|m| r.mesh == m)).collect();
            let span = tracing::info_span!("rdm.node_admin.node.resolve.via-runtime-rows-read", reader_path = %reader.path, reader_name = %reader.name, role, mesh = tracing::field::Empty, store = "nodes.storage", rows = rows.len() as u64);
            match mesh {
                Some(mesh) => {
                    span.record("mesh", mesh);
                    span.in_scope(|| tracing::info!("this admin took its mesh's primary role: its mesh's durable runtime rows are read once and held while it keeps the role"));
                }
                None => span.in_scope(|| tracing::info!("this admin took the fabric-primary role: every mesh's durable runtime rows are read once and held while it keeps the role")),
            }
            Some(rows)
        }
        Err(e) => {
            tracing::info_span!("rdm.node_admin.node.reject.via-runtime-rows-unreadable", reader_path = %reader.path, reader_name = %reader.name, role, store = "nodes.storage", error = %e).in_scope(|| match mesh {
                Some(_) => tracing::info!("the durable runtime rows could not be read: this tick watches the view's births only, and the next tick reads them again"),
                None => tracing::info!("the durable runtime rows could not be read: this pass's drift check reads the view's births only, and the next pass reads them again"),
            });
            None
        }
    }
}

/// The births the durable runtime rows name at a path `view` holds nothing at: a birth this admin
/// never heard and never launched, at a path the accepted Build names (`planned`), whose departure
/// is not held (a deleted birth's row is history). Each stands in the map as `PendingReconnect`,
/// to be inspected like any unheard birth: only an exited runtime counts. One birth per path.
pub fn durable_births(view: &Topology, durable: &[crate::storage::RuntimeRow], planned: &dyn Fn(&PathName) -> bool, deleted: &dyn Fn(&str) -> bool) -> Vec<Node> {
    let mut births: Vec<Node> = Vec::new();
    for row in durable {
        if view.node(&row.location()).is_none() && !births.iter().any(|n| n.location() == row.location()) && planned(&row.location()) && !deleted(row.node_id.as_str()) {
            let mut n = Node::allocated(row.location(), row.mesh.clone(), &row.r#type);
            n.node_id = row.node_id.clone();
            n.incarnation_id = Some(row.incarnation_id.clone());
            n.status = NodeStatus::PendingReconnect;
            births.push(n);
        }
    }
    births
}

/// What the mesh primary's tick watches, by node id: every silent (`PendingReconnect` or `Dead`)
/// member of its own mesh in its view other than itself (the birth `me_id`: never by path and name,
/// which a recovery keeps under a new node id); and every birth of its own mesh only a durable
/// runtime row names (`durable_births`: never heard, never launched by this admin), which is node
/// recovery driven by this mesh-primary like any other member's (mesh-recovery.md:8-9). The tickle
/// ladder is the mesh-primary's for its own members (node-recovery.md); a peer mesh's node-admins
/// are reached by the fabric-primary's primary watch alone (`crate::primary_watch`).
pub fn watched_births(view: &Topology, me_id: &NodeId, mesh: &str, durable: &[Node]) -> BTreeMap<String, Node> {
    view.births()
        .filter(|n| n.node_id != *me_id && n.mesh == mesh && matches!(n.status, NodeStatus::PendingReconnect | NodeStatus::Dead))
        .chain(durable.iter().filter(|n| n.mesh == mesh && n.node_id != *me_id))
        .map(|n| (n.node_id.to_string(), n.clone()))
        .collect()
}

/// What the mesh primary's tick hears a member again against (`offline-returned`): the node ids of
/// `view`'s members and of every birth the tick watches (`watched_births`), the births only durable rows
/// name included, none whose departure the view holds. A birth this admin knew only from durable rows
/// (one born while it was silent) enters the view once its digest is installed, after the tick's
/// snapshot: heard again, it is returned, never gone. A node id in neither is gone.
pub fn heard_against(view: &Topology, watched: &BTreeMap<String, Node>) -> HashSet<String> {
    view.births().map(|n| n.node_id.to_string()).chain(watched.keys().cloned()).collect()
}

/// The watched births the tick inspects for recovery (node-recovery.md): none unless this admin
/// holds its mesh's primary role; else every watched birth of its own mesh (its secondary
/// node-admins included, mesh-recovery.md:8-9) held TRUE OFFLINE at its exact incarnation.
pub fn recovery_due(holds_role: bool, mesh: &str, watched: &BTreeMap<String, Node>, offline: &HashSet<(NodeId, IncarnationId)>) -> Vec<Node> {
    if !holds_role {
        return Vec::new();
    }
    watched.values().filter(|n| n.mesh == mesh && n.incarnation_id.as_ref().is_some_and(|i| offline.contains(&(n.node_id.clone(), i.clone())))).cloned().collect()
}

/// The mesh primary's per-birth bookkeeping speaks only for the births it still watches: a birth
/// whose node left the view (deleted, or recovered under a new node.id at its path and name) or whose
/// node the view now holds at another incarnation (superseded) leaves the TRUE OFFLINE set; the
/// silence memo lives only while its node is watched. Run every tick, after the tick's writes, so
/// nothing of a gone birth outlives one tick.
pub(crate) fn clear_unwatched(watched: &BTreeMap<String, Node>, offline: &mut HashSet<(NodeId, IncarnationId)>, written: &mut WrittenSilence) {
    offline.retain(|(node_id, incarnation)| watched.get(node_id.as_str()).is_some_and(|n| n.incarnation_id.as_ref() == Some(incarnation)));
    written.retain(|node_id, _| watched.contains_key(node_id.as_str()));
}

/// How a recovery ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RecoveryEnd {
    /// The node was re-created and is ready.
    Complete,
    /// A step failed, named in `reason` (node-recovery.md "Failure").
    Failed { reason: String },
}

/// The recovery span's `outcome` and the recovery's result, from how it ended. A failed recovery ends
/// in an error: the birth stays owed its recovery ([`RecoverySubmitter::failed_recoveries`]) and the
/// tick runs it again once its backoff passed (node-recovery.md "Failure").
pub(crate) fn settle(end: Result<RecoveryEnd, String>) -> (String, Result<(), String>) {
    match end {
        Ok(RecoveryEnd::Complete) => ("complete".to_string(), Ok(())),
        Ok(RecoveryEnd::Failed { reason }) => {
            let failed = format!("failed: {reason}");
            (failed.clone(), Err(failed))
        }
        Err(e) => (e.clone(), Err(e)),
    }
}


impl RecoverySubmitter {
    /// The second births at a `(path, name)` of `mesh` in `view` (node-investigation.md, Q142): another
    /// executor made a birth at the path across a cut, and the path's name claim names the other birth.
    /// Each is a superseded birth: this mesh-primary writes its record deleted, releasing the name if it
    /// held it, and answers its typed node.delete operation, which the caller publishes as
    /// `NodeDeleted`; its next digest is then a deleted birth heard again, refused and reported to the
    /// fabric-primary, which terminates its runtime ([`Self::report_terminate`], Q207).
    pub(crate) async fn superseded_births(&self, view: &Topology, mesh: &str) -> Vec<rdm_core::LifecycleOp> {
        // The mesh-primary's own repair, never a Build or part of one (Q342).
        superseded_births(&self.me(), view, mesh, &*self.nodes_storage, &self.written, rdm_core::LifecycleOp::NO_BUILD).await
    }
}

/// [`RecoverySubmitter::superseded_births`] on `storage` and the records `written` holds, by `me`, the
/// operations it answers named under the current Build `build_id`.
pub(crate) async fn superseded_births(me: &PathName, view: &Topology, mesh: &str, storage: &dyn crate::storage::NodesStorage, written: &crate::node_rows::WrittenRows, build_id: &str) -> Vec<rdm_core::LifecycleOp> {
    let mut at: BTreeMap<PathName, Vec<&Node>> = BTreeMap::new();
    for n in view.births().filter(|n| n.mesh == mesh && n.status != NodeStatus::Deleted && n.incarnation_id.is_some()) {
        at.entry(n.location()).or_default().push(n);
    }
    let mut ops = Vec::new();
    for (path, births) in at.into_iter().filter(|(_, b)| b.len() > 1) {
        let Ok(Some(holder)) = crate::node_rows::record_at(storage, &path).await.map(|r| r.map(|r| r.node_id)) else { continue };
        if !births.iter().any(|b| b.node_id == holder) {
            continue;
        }
        for b in births.into_iter().filter(|b| b.node_id != holder) {
            let incarnation = b.incarnation_id.clone().expect("filtered above");
            let span = tracing::info_span!("rdm.node_admin.node.delete.via-superseded-birth", node_path = %me.path, node_name = %me.name, subject_path = %path.path, subject_name = %path.name, node_id = %b.node_id, incarnation_id = %incarnation.as_str(), holder = %holder, outcome = tracing::field::Empty);
            let row = written.update(storage, &b.node_id, None, |r| {
                r.status = NodeStatus::Deleted;
                r.allocated = None;
            }).await;
            match row {
                Ok(_) => {
                    span.record("outcome", "row-deleted");
                    span.in_scope(|| tracing::info!("a second birth at the same path and name, made across a cut, is superseded: its row is written deleted"));
                    ops.push(rdm_core::LifecycleOp { r#type: b.r#type.clone(), mesh: mesh.to_string(), build_id: build_id.to_string(), attempt: 0, operation: rdm_core::OperationId::node("node.delete", &b.node_id), node_id: b.node_id.clone(), incarnation, path: path.path.clone(), name: path.name.clone(), event_at_rafka_ms: local_now_ms(), traceparent: None, tracestate: None });
                }
                Err(e) => {
                    span.record("outcome", "row-refused");
                    span.in_scope(|| tracing::info!(error = %e, "the superseded birth's row could not be written deleted: the next tick tries again"));
                }
            }
        }
    }
    ops
}

impl RecoverySubmitter {
    /// The caller birth `(node_id, incarnation)` at `path` Q207's decision deletes: its runtime
    /// terminated under `node.terminate:<path>@<incarnation>` by this admin, the primary whose atlas
    /// decided it (node-investigation.md), its own workflow and never a Build or part of one (Q342). One
    /// `rdm.node_admin.node.update.via-returned-reported` span; it runs on the node's work turn.
    pub(crate) async fn terminate(&self, path: PathName, node_id: NodeId, incarnation: IncarnationId) -> Result<(), String> {
        let me = self.held_name();
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-returned-reported",
            node_path = %me.path, node_name = %me.name, returned_path = %path.path, returned_name = %path.name, node_id = %node_id, incarnation_id = %incarnation.as_str(),
            outcome = tracing::field::Empty, detail = tracing::field::Empty,
        );
        let r = async {
            let runner = self.runner.get().cloned().ok_or_else(|| format!("{}: its runner is not built yet", self.me_id))?;
            if runner.repairs_stopped.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(format!("{} stopped reconciling: it starts no repair", self.me_id));
            }
            let _turn = runner.node_work(&node_id).await;
            runner.terminate(&path, &node_id, &incarnation).await.map_err(|e| e.reason().to_string())
        }
        .instrument(span.clone())
        .await;
        match &r {
            Ok(()) => span.record("outcome", "terminated"),
            Err(e) => span.record("outcome", "failed").record("detail", e.as_str()),
        };
        span.in_scope(|| tracing::info!("a deleted birth heard again: its runtime is terminated by the primary that decided it"));
        r
    }
}

#[cfg(test)]
mod superseded_tests {
    use super::*;
    use crate::storage::{MemoryNodesStorage, NodeRow, NodesStorage};

    fn birth(path: PathName, mesh: &str, r#type: &str) -> Node {
        let mut n = Node::allocated(path, mesh, r#type);
        n.incarnation_id = Some(IncarnationId::mint());
        n.status = NodeStatus::Ready;
        n
    }

    fn view(nodes: Vec<Node>) -> Topology {
        Topology {
            fabric: crate::model::Fabric { id: crate::model::FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Ready, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes,
        }
    }

    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:46
    /// "A second birth another executor made at the path and name across a cut is made a deleted birth by its
    ///  mesh-primary, which writes its row deleted ([node.delete](node-delete.md), releasing the name) and
    ///  reports it the same way (Q142)."
    #[tokio::test]
    async fn a_second_birth_at_a_path_after_a_heal_is_written_deleted_and_the_records_birth_stands() {
        let storage = MemoryNodesStorage::default();
        let written = crate::node_rows::WrittenRows::default();
        let (first, second, alone) = (birth(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin"), birth(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin"), birth(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"));
        for n in [&first, &alone] {
            crate::node_rows::write_record(&storage, &NodeRow::new(n.location(), n.mesh.clone(), &n.r#type, n.node_id.clone(), n.status)).await.unwrap().unwrap();
        }
        // The second birth's create was refused at the name the first birth's record holds: its own record
        // stands Withdrawn, and the view holds both births.
        storage.put_node(&NodeRow::new(second.location(), second.mesh.clone(), &second.r#type, second.node_id.clone(), NodeStatus::Withdrawn)).await.unwrap();
        let me: PathName = rdm_core::PathName::new("mesh1.node-admin", "2").unwrap();
        let ops = superseded_births(&me, &view(vec![first.clone(), second.clone(), alone.clone()]), "mesh1", &storage, &written, "b1").await;
        assert_eq!(ops.iter().map(|o| (o.node_id.clone(), o.operation.clone())).collect::<Vec<_>>(), vec![(second.node_id.clone(), rdm_core::OperationId::node("node.delete", &second.node_id))], "only the second birth is answered");
        assert_eq!(storage.node(&second.node_id).await.unwrap().unwrap().status, NodeStatus::Deleted, "the second birth's row is written deleted");
        assert_eq!(storage.node(&first.node_id).await.unwrap().unwrap().status, NodeStatus::Ready, "the record's birth stands");
        assert_eq!(crate::node_rows::record_at(&storage, &first.location()).await.unwrap().map(|r| r.node_id), Some(first.node_id.clone()), "the name stays the first birth's");
        let (one, other, holder) = (birth(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node"), birth(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node"), birth(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node"));
        storage.put_node(&NodeRow::new(holder.location(), holder.mesh.clone(), &holder.r#type, holder.node_id.clone(), holder.status)).await.unwrap();
        assert!(superseded_births(&me, &view(vec![one, other]), "mesh1", &storage, &written, "b1").await.is_empty(), "a path whose live record names neither birth answers nothing");
    }
}
