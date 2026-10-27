//! Node recovery submitter and recovery due checks.

use super::*;

/// What the mesh primary's recovery of one TRUE OFFLINE member reads and calls
/// (node-recovery.md "Detection and proof" step 3, sequence step 1).
#[derive(Clone)]
pub(crate) struct RecoverySubmitter {
    /// This mesh primary's `path.name`: the submitter.
    pub me: PathName,
    pub topology: Arc<RwLock<Topology>>,
    pub digest_view: DigestView,
    pub provider: Arc<dyn DeploymentProvider>,
    pub nodes_storage: Arc<dyn crate::storage::NodesStorage>,
    /// The node records this admin writes, as it holds them: the data-dir facts are written on them.
    pub written: Arc<crate::node_rows::WrittenRows>,
    /// This admin's Build door: its own control plane, where it opens and claims its mesh's drift
    /// attempts, and its own executor, which runs them.
    pub door: crate::build_op::BuildSlot,
    /// What this mesh-primary reports its drift to the fabric-primary through (Q118); none in a rig
    /// that runs no Node RPC.
    pub client: Option<Arc<rdm_transport::NodeRpcClient>>,
    /// This admin's Build state: the fabric's Build facts as its own log holds them. The tick reads
    /// from it which births of its mesh are owed a submit ([`RecoverySubmitter::owed_submits`]).
    pub builds: Arc<dyn crate::build_state::BuildStateAdapter>,
    /// Parent directory under which birth data directories are allocated (`<data_root>/<name>-<node_id>`).
    pub data_root: PathBuf,
    /// Per exited birth whose inherited `data_dir` was observed absent (`Gone`, `R-FM2`):
    /// `(first_seen_ms, consecutive_observations)`.
    pub gone_dirs: Arc<Mutex<HashMap<(NodeId, IncarnationId), (u64, u64)>>>,
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

impl RecoverySubmitter {

    /// The name this admin currently holds in its topology digest view, or its boot name before its
    /// birth is in the view.
    pub(crate) fn held_name(&self) -> PathName {
        self.digest_view.held_path_name().unwrap_or_else(|| self.me.clone())
    }

    /// Resolve `node`'s inherited `data_dir` when known (`R-FM2`): from `node.data_dir`, its digest in
    /// `self.digest_view`, its durable runtime row in `self.nodes_storage`, or `<data_root>/<name>-<node_id>`.
    pub(crate) async fn birth_data_dir(&self, node: &Node) -> Option<PathBuf> {
        if let Some(d) = node.data_dir.as_deref() {
            return Some(PathBuf::from(d));
        }
        if let Some(d) = self.digest_view.get(node.node_id.as_str()).and_then(|(dg, _)| dg.data_dir.map(PathBuf::from)) {
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

    /// The provider's exact-exit proof of `node`'s birth: node.runtime.inspect over the one runtime
    /// resolution (`node_investigation::birth_runtime`: the view's digest of that exact birth, heard,
    /// projected by its mesh-primary or stopped, else the birth's own durable runtime row). Beside it,
    /// the exact runtime handle the provider was asked about (node-runtime-inspect.md Target), for the
    /// inspection's span.
    pub(crate) async fn inspect(&self, node: &Node) -> (Inspected, Option<DeploymentHandle>) {
        crate::node_investigation::inspect_birth_with_handle(&*self.provider, &self.digest_view, Some(&*self.nodes_storage), node).await
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
                node = %n.name,
                node_id = %n.node_id,
                incarnation_id = %incarnation,
                submitter = %self.held_name(),
                announced,
                operation = %operation,
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
                match crate::deployment::pipeline::inspect_transport_key(&dir) {
                    Ok(_) => {
                        let _ = self.written.update_birth(&*self.nodes_storage, n, &inc, |b| b.data_dir_unreadable = None).await;
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
        if let Err(e) = self.submit(n, &outcome).await {
            tracing::info!(node = %n.name, error = %e, "the recovery submit ended without the recovery; the next tick submits again");
        }
        outcome
    }

    /// The workflow whose announced removal `n`'s exact birth is under, as this admin's view holds it
    /// now (node-recovery.md: recovery is drift reconciliation for a member that exits outside any
    /// workflow; "Failure and receipts": an exit during a workflow belongs to that workflow): its
    /// node's departure (`node-deleted`), else an open `NodeDeleting` overlay naming the exact birth
    /// (`node-deleting`), else the mesh.delete of its mesh (mesh-delete.md: from mesh.deleting, the
    /// mesh's status deleting, to mesh.deleted, every member's removal is the mesh.delete's;
    /// `mesh-deleting`, `mesh-deleted`), the status the view holds for the mesh. A recovery's own
    /// node.delete of the exited birth (`node.delete:<path>`) is such a workflow too: its overlay
    /// closes when its attempt's verdict is terminal (`DigestView::deletions_from_facts`), and a
    /// recovery whose Build ended Failed is submitted again by the next tick: from the view while it
    /// holds the birth, else from the Failed attempt the accepted Build holds once its delete departed
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
        let operation = self.topology.read().await.meshes.iter().find(|m| m.name == n.mesh).and_then(|m| m.id.clone()).map(|id| crate::mesh_delete::operation(&id)).unwrap_or_else(|| format!("mesh.delete of {}", n.mesh));
        Some(Announced { announced, operation, build_id: String::new(), attempt: 0 })
    }

    /// The inspection's span (node-runtime-inspect.md "Spans"), with `handle` the exact runtime handle
    /// the provider was asked about (none when nothing was asked).
    pub(crate) fn span_inspection(&self, node: &Node, outcome: &Inspected, handle: Option<&DeploymentHandle>) {
        let (exit_code, reason) = match outcome {
            Inspected::Exited(code) => (code.map(|c| c.to_string()).unwrap_or_default(), String::new()),
            Inspected::Running | Inspected::OsStopped => (String::new(), String::new()),
            Inspected::Unavailable(why) => (String::new(), why.clone()),
        };
        let incarnation = node.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default();
        let handle = handle.map(|h| format!("{h:?}")).unwrap_or_default();
        tracing::info_span!("rdm.node_admin.node.resolve.via-provider-inspection", node = %node.name, node_id = %node.node_id, incarnation_id = %incarnation, inspector = %self.held_name(), handle = %handle, verdict = verdict_name(outcome), exit_code, reason = %reason)
            .in_scope(|| tracing::info!("the mesh primary asked the provider about the TRUE OFFLINE member's exact runtime"));
    }

    /// The recovery of `node`'s unreached birth on its verdict `outcome`: this mesh primary submits
    /// it, keyed on the birth ([`Self::submit_in`], Q118). The run is followed to its
    /// end, so the caller holds the birth as submitted until then. A repeat answers AlreadyApplied or
    /// BuildInProgress.
    pub(crate) async fn submit(&self, node: &Node, outcome: &Inspected) -> Result<(), String> {
        self.submit_route(node, outcome, "node-recovery").await
    }

    /// [`Self::submit`] on `route`: `node-recovery`, or `fabric-recovery` for the re-create fabric recovery
    /// runs of the node-admin at the fabric-primary's path.name (fabric-recovery.md step 3, Q122).
    pub async fn submit_route(&self, node: &Node, outcome: &Inspected, route: &'static str) -> Result<(), String> {
        let incarnation = node.incarnation_id.clone().ok_or_else(|| format!("{} holds no incarnation: no birth to key a recovery on", node.name))?;
        let span = submit_span(&self.held_name(), node, &incarnation, verdict_name(outcome));
        let end = self.submit_in(&span, node, &incarnation, route).instrument(span.clone()).await;
        settled(&span, end)
    }

    /// The births the tick submits again (node-recovery.md "Failure and receipts": "the mesh-primary
    /// is the submitter that resubmits by `build_id`"): none unless this admin holds its mesh's
    /// primary seat (as [`recovery_due`]); else every
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
            if let Some(held) = self.written.birth_facts(&*self.nodes_storage, &o.node_id, &o.incarnation).await.ok().flatten().and_then(|f| f.data_dir_unreadable) {
                match crate::deployment::pipeline::inspect_transport_key(std::path::Path::new(&held.path)) {
                    Ok(_) => {
                        let _ = self.written.update(&*self.nodes_storage, &o.node_id, None, |r| {
                            if let Some(b) = r.births.iter_mut().find(|b| b.incarnation_id == o.incarnation) {
                                b.data_dir_unreadable = None;
                            }
                        }).await;
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

    /// The submit of a birth whose recovery stopped at a Failed attempt (node-recovery.md "Failure and
    /// receipts"), on the tick: the same submit, keyed on the birth; nothing is inspected again. The
    /// fabric-primary answers the submit as any repeat: AlreadyApplied once a workflow completed for
    /// the birth, BuildInProgress while an attempt runs, else the next attempt of the Build with this
    /// birth's action, whose plan runs only what the receipts leave (a delete that completed is never
    /// run again).
    pub(crate) async fn submit_owed(&self, owed: &crate::build_state::OwedExit) -> Result<(), String> {
        let mut node = Node::allocated(owed.path.clone());
        node.node_id = owed.node_id.clone();
        node.incarnation_id = Some(owed.incarnation.clone());
        let span = submit_span(&self.held_name(), &node, &owed.incarnation, "");
        span.record("after_failed_attempt", owed.attempt);
        let end = self.submit_in(&span, &node, &owed.incarnation, "node-recovery").instrument(span.clone()).await;
        settled(&span, end)
    }

    /// The recovery of `node`'s exited birth at `incarnation`, submitted by this admin as its mesh's
    /// primary (node-recovery.md "Execution", Q118): a mesh-primary that is not the fabric-primary
    /// reports the drift up and runs the attempt the fabric-primary opens; one whose view names no
    /// fabric-primary fails by name; the fabric-primary opens and claims it at its own door, runs it
    /// through its own executor in its mesh's lane and follows it to the terminal
    /// ([`crate::build_run::run_recovery`]). Fabric recovery's re-create is its executor's own.
    pub(crate) async fn submit_in(&self, span: &tracing::Span, node: &Node, incarnation: &IncarnationId, route: &'static str) -> Result<RecoveryEnd, String> {
        use crate::build_run::{Recovered, RecoveryNotRun};
        use rdm_core::build::BuildReply;
        let door = self.door.get().cloned().ok_or_else(|| format!("{} holds its mesh's primary seat but its Build door is not built yet", self.held_name()))?;
        let opened = |o: &crate::control_plane::Opened, disposition: &str| {
            span.record("build_id", o.build_id.as_str());
            span.record("mesh", o.mesh.as_deref().unwrap_or(""));
            span.record("attempt", o.attempt);
            span.record("disposition", disposition);
        };
        // A mesh-primary that is not the fabric-primary reports its drift up (node-recovery.md
        // "Execution", Q118). Fabric recovery's re-create is its executor's own (fabric-recovery.md step 3).
        let fabric_primary = self
            .topology
            .read()
            .await
            .fabric_primary()
            .map(|n| (n.name.clone(), n.node_id.clone()));
        let no_fabric_primary = fabric_primary.is_none();
        if let Some((fp, fp_id)) = fabric_primary
            .filter(|(_, id)| route == "node-recovery" && *id != door.env.door.node_id)
        {
            let client = self.client.clone().ok_or_else(|| {
                format!(
                    "{}: no Node RPC client to report {}'s drift to {fp}",
                    self.held_name(),
                    node.name
                )
            })?;
            return self
                .report_drift(span, &door, &client, &fp, &fp_id, node, incarnation)
                .await;
        }
        // A mesh-primary whose view names no fabric-primary is behind a cut from it: the fabric-primary
        // writes every Build record and a mesh behind a cut is not repaired locally until the heal, so its
        // member's recovery fails by name and the tick submits it again while the birth stays unreached
        // (node-recovery.md "Execution": "While the mesh is cut from the fabric-primary, its recoveries fail
        // by name and run after the heal"; Q118). Only the fabric-primary opens and runs a recovery itself.
        if route == "node-recovery" && no_fabric_primary {
            return Err(format!(
                "{}: its view names no fabric-primary to report {}'s drift to: the recovery runs after the heal (Q118)",
                self.held_name(),
                node.name
            ));
        }
        match crate::build_run::run_recovery(
            &door.control,
            &door.run,
            route,
            node.name.clone(),
            node.node_id.clone(),
            incarnation.clone(),
        )
        .await
        {
            Ok(Recovered::Ran {
                opened: o,
                terminal,
            }) => {
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
    span.in_scope(|| tracing::info!("the mesh primary opens and runs the recovery of its unreached member itself"));
    r
}

/// The span of one submit of the unreached birth `node` at `incarnation` by the mesh primary `me` on
/// the investigation's `verdict` (node-recovery.md "Observability contract": the submit has a span on
/// the mesh-primary). `after_failed_attempt` is recorded on a submit that follows a Failed one of the
/// same birth, whose verdict is not read again.
pub(crate) fn submit_span(me: &PathName, node: &Node, incarnation: &IncarnationId, verdict: &str) -> tracing::Span {
    tracing::info_span!(
        "rdm.node_admin.build.update.via-node-recovery",
        node = %node.name,
        node_id = %node.node_id,
        incarnation_id = %incarnation.as_str(),
        submitter = %me,
        verdict,
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

/// What the mesh primary's tick hears a member again against (`offline-returned`): the node ids of
/// `view`'s members and of every birth the tick watches (`watched_births`), the births only durable rows
/// name included, none whose departure the view holds. A birth this admin knew only from durable rows
/// (one born while it was silent) enters the view once its digest is installed, after the tick's
/// snapshot: heard again, it is returned, never gone. A node id in neither is gone.
pub fn heard_against(view: &Topology, watched: &BTreeMap<String, Node>) -> HashSet<String> {
    view.members().map(|n| n.node_id.to_string()).chain(watched.keys().cloned()).collect()
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
/// node the view now holds at another incarnation (superseded) leaves the TRUE OFFLINE set; the
/// silence memo lives only while its node is watched. Run every tick, after the tick's writes, so
/// nothing of a gone birth outlives one tick.
pub(crate) fn clear_unwatched(watched: &BTreeMap<String, Node>, offline: &mut HashSet<(NodeId, IncarnationId)>, written: &mut WrittenSilence) {
    offline.retain(|(node_id, incarnation)| watched.get(node_id.as_str()).is_some_and(|n| n.incarnation_id.as_ref() == Some(incarnation)));
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


impl RecoverySubmitter {
    /// The second births at a `path.name` of `mesh` in `view` (node-investigation.md, Q142): another
    /// executor made a birth at the path across a cut, and the path's name claim names the other birth.
    /// Each is a superseded birth: this mesh-primary writes its record deleted, releasing the name if it
    /// held it, and answers its `node.delete:<path>` operation, which the caller publishes as
    /// `NodeDeleted`; its next digest is then a deleted birth heard again, refused and reported to the
    /// fabric-primary, which terminates its runtime ([`Self::report_terminate`], Q207).
    pub(crate) async fn superseded_births(&self, view: &Topology, mesh: &str) -> Vec<rdm_core::LifecycleOp> {
        let build_id = match self.builds.current_build().await {
            Ok(current) => current.into_build_id().map(|b| b.to_string()).unwrap_or_default(),
            Err(_) => String::new(),
        };
        superseded_births(&self.me, view, mesh, &*self.nodes_storage, &self.written, &build_id).await
    }
}

/// [`RecoverySubmitter::superseded_births`] on `storage` and the records `written` holds, by `me`, the
/// operations it answers named under the current Build `build_id`.
pub(crate) async fn superseded_births(me: &PathName, view: &Topology, mesh: &str, storage: &dyn crate::storage::NodesStorage, written: &crate::node_rows::WrittenRows, build_id: &str) -> Vec<rdm_core::LifecycleOp> {
    let mut at: BTreeMap<PathName, Vec<&Node>> = BTreeMap::new();
    for n in view.members().filter(|n| n.mesh == mesh && n.status != NodeStatus::Deleted && n.incarnation_id.is_some()) {
        at.entry(n.name.clone()).or_default().push(n);
    }
    let mut ops = Vec::new();
    for (path, births) in at.into_iter().filter(|(_, b)| b.len() > 1) {
        let Ok(Some(holder)) = storage.name_claim(&path).await else { continue };
        if !births.iter().any(|b| b.node_id == holder) {
            continue;
        }
        for b in births.into_iter().filter(|b| b.node_id != holder) {
            let incarnation = b.incarnation_id.clone().expect("filtered above");
            let span = tracing::info_span!("rdm.node_admin.node.delete.via-superseded-birth", node = %me, subject = %path, node_id = %b.node_id, incarnation_id = %incarnation.as_str(), holder = %holder, outcome = tracing::field::Empty);
            let row = written.update(storage, &b.node_id, None, |r| {
                r.status = NodeStatus::Deleted;
                r.allocated = None;
            }).await;
            match row {
                Ok(_) => {
                    span.record("outcome", "row-deleted");
                    span.in_scope(|| tracing::info!("a second birth at the path.name, made across a cut, is superseded: its row is written deleted"));
                    ops.push(rdm_core::LifecycleOp { build_id: build_id.to_string(), attempt: 0, operation: format!("node.delete:{path}"), node_id: b.node_id.clone(), incarnation, name: path.clone(), event_at_rafka_ms: local_now_ms() });
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
    /// The caller birth `(node_id, incarnation)` at `path` Q207's decision deletes, reported to the
    /// fabric-primary, which terminates its runtime under `node.terminate:<path>@<incarnation>`
    /// (node-investigation.md): `build.create { Change(Terminate) }`, opened here when this admin is the
    /// fabric-primary. `Ok` once the fabric-primary answered (opened, or already applied).
    pub(crate) async fn report_terminate(&self, client: &Arc<rdm_transport::NodeRpcClient>, path: PathName, node_id: NodeId, incarnation: IncarnationId) -> Result<(), String> {
        let me = self.held_name();
        let span = tracing::info_span!("rdm.node_admin.node.update.via-returned-reported", node = %me, returned = %path, node_id = %node_id, incarnation_id = %incarnation.as_str(), outcome = tracing::field::Empty);
        let fabric_primary = self.topology.read().await.fabric_primary().map(|n| (n.name.clone(), n.node_id.clone()));
        let Some((fp_name, fp_id)) = fabric_primary else {
            span.record("outcome", "no-fabric-primary");
            return Err(format!("{me}: no fabric-primary in this view to report {path} ({node_id}) to"));
        };
        let r = if fp_name == me {
            let door = self.door.get().cloned().ok_or_else(|| format!("{me}: its Build door is not built yet"))?;
            let opened = tracing::info_span!("rdm.node_admin.build.update.via-terminate-returned", route = "returned-birth", node = %path, node_id = %node_id, incarnation_id = %incarnation.as_str());
            match door
                .control
                .open_terminate(
                    opened,
                    "returned-birth",
                    path.clone(),
                    node_id.clone(),
                    incarnation.clone(),
                )
                .await
            {
                Ok(
                    crate::control_plane::Recovery::Opened(o)
                    | crate::control_plane::Recovery::Reported { opened: o, .. },
                ) => {
                    door.drives.ensure(&door.env, &o.build_id, o.attempt);
                    Ok("opened")
                }
                Ok(crate::control_plane::Recovery::AlreadyApplied(_)) => Ok("already-applied"),
                Err(r) => Err(format!("{me}: the termination of {path} ({node_id}) was refused: {:?}", crate::build_op::refusal_reply(r))),
            }
        } else {
            use rdm_core::build::{Build, BuildChange, BuildRequest, BuildSubmit};
            let req = BuildRequest::Create { submit: BuildSubmit::Change(BuildChange::Terminate { node: path.clone(), node_id: node_id.clone(), incarnation: incarnation.clone() }) };
            let opts = rdm_transport::CallOptions { budget: rdm_transport::Budget::Stream { send: std::time::Duration::from_secs(10) }, ..rdm_transport::CallOptions::default() };
            match client.call_stream::<Build>(&rdm_transport::NodeTarget::ExactNode(fp_id), &req, &opts).await {
                Ok(_) => Ok("sent"),
                Err((outcome, _)) => Err(format!("{me}: the report of {path} ({node_id}) to {fp_name} ended {}", outcome.name())),
            }
        };
        span.record("outcome", r.as_ref().map(|o| *o).unwrap_or("failed"));
        span.in_scope(|| tracing::info!("a deleted birth heard again, reported to the fabric-primary"));
        r.map(|_| ())
    }
}

impl RecoverySubmitter {
    /// The drift of `node`'s exited birth at `incarnation`, reported to the fabric-primary `fp`
    /// (node-recovery.md "Execution", Q118): `build.create { Change(Recover) }`; at its `Started` this
    /// mesh-primary claims the attempt through BuildClaim, and the fabric-primary runs it here with
    /// `build.attempt.run` naming the mesh and writes its records. Followed to its terminal.
    #[allow(clippy::too_many_arguments)]
    async fn report_drift(
        &self,
        span: &tracing::Span,
        door: &crate::build_op::BuildDoor,
        client: &Arc<rdm_transport::NodeRpcClient>,
        fp: &PathName,
        fp_id: &NodeId,
        node: &Node,
        incarnation: &IncarnationId,
    ) -> Result<RecoveryEnd, String> {
        use rdm_core::build::{
            Build, BuildChange, BuildReply, BuildRequest, BuildSubmit, Disposition,
        };
        use rdm_core::build_claim::{BuildClaim, BuildClaimReply, BuildClaimRequest};
        use rdm_core::outcome::RpcOutcome;
        use rdm_transport::stream::StreamItem;
        let target = rdm_transport::NodeTarget::ExactNode(fp_id.clone());
        let req = BuildRequest::Create {
            submit: BuildSubmit::Change(BuildChange::Recover {
                node: node.name.clone(),
                node_id: node.node_id.clone(),
                incarnation: incarnation.clone(),
            }),
        };
        let opts = rdm_transport::CallOptions {
            budget: rdm_transport::Budget::Stream {
                send: crate::build_op::DISPATCH_SEND,
            },
            ..rdm_transport::CallOptions::default()
        };
        let mut stream = match client.call_stream::<Build>(&target, &req, &opts).await {
            Ok((s, _)) => s.read_only(),
            Err((RpcOutcome::Reply(r), _)) => {
                return Err(format!("{}: {:?}", r.value().name(), r.value()))
            }
            Err((other, _)) => {
                return Err(format!(
                    "the report of {}'s drift to {fp} ended {}",
                    node.name,
                    other.name()
                ))
            }
        };
        loop {
            match stream.next().await {
                Some(StreamItem::Frame(
                    _,
                    BuildReply::Started {
                        build_id,
                        attempt,
                        disposition,
                    },
                )) => {
                    span.record("build_id", build_id.as_str());
                    span.record("mesh", node.mesh.as_str());
                    span.record("attempt", attempt);
                    span.record(
                        "disposition",
                        if disposition == Disposition::AlreadyApplied {
                            "already-applied"
                        } else {
                            "created"
                        },
                    );
                    if disposition == Disposition::AlreadyApplied {
                        continue;
                    }
                    let claim = BuildClaimRequest::ClaimAttempt {
                        build_id: build_id.clone(),
                        attempt,
                        executor_node_id: door.env.door.node_id.clone(),
                        executor_incarnation: door.incarnation.clone(),
                        mesh: Some(node.mesh.clone()),
                    };
                    match client.call::<BuildClaim>(&target, &claim, &rdm_transport::CallOptions::default()).await.0 {
                        RpcOutcome::Reply(r) if matches!(r.value(), BuildClaimReply::Won { .. }) => {}
                        RpcOutcome::Reply(r) => return Err(format!("the claim of attempt {attempt} of {build_id} in mesh {}'s lane was answered {}: {:?}", node.mesh, r.value().name(), r.value())),
                        other => return Err(format!("the claim of attempt {attempt} of {build_id} in mesh {}'s lane ended {}", node.mesh, other.name())),
                    }
                }
                Some(StreamItem::Frame(_, BuildReply::Complete { attempt, .. })) => {
                    return Ok(RecoveryEnd::Complete { attempt })
                }
                Some(StreamItem::Frame(
                    _,
                    BuildReply::Failed {
                        attempt,
                        operation,
                        step,
                        reason,
                        ..
                    },
                )) => {
                    return Ok(RecoveryEnd::Failed {
                        attempt,
                        operation,
                        step,
                        reason,
                    })
                }
                Some(StreamItem::Frame(
                    _,
                    BuildReply::Step { .. } | BuildReply::Blocked { .. },
                )) => {}
                Some(StreamItem::Frame(_, other)) => {
                    return Err(format!("{}: {other:?}", other.name()))
                }
                Some(StreamItem::Failed(f)) => {
                    return Err(format!(
                        "the report of {}'s drift to {fp} broke: {f:?}",
                        node.name
                    ))
                }
                None => {
                    return Err(format!(
                        "the report of {}'s drift to {fp} ended without its terminal",
                        node.name
                    ))
                }
            }
        }
    }
}

#[cfg(test)]
mod superseded_tests {
    use super::*;
    use crate::storage::{MemoryNodesStorage, NodeRow, NodesStorage};

    fn birth(path: &str) -> Node {
        let mut n = Node::allocated(path.parse().unwrap());
        n.incarnation_id = Some(IncarnationId::mint());
        n.status = NodeStatus::ReadyForTraffic;
        n
    }

    fn view(nodes: Vec<Node>) -> Topology {
        Topology {
            fabric: crate::model::Fabric { id: crate::model::FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::ReadyForTraffic, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes,
        }
    }

    /// CONTRACT (node-investigation.md, Q142: "a second birth another executor made at the path.name
    /// across a cut is a superseded birth ... its mesh-primary writes its row deleted"): after a heal the
    /// view holds two live births at mesh1.admin.1; the path's name claim names the first. The second's
    /// record is written deleted and its `node.delete:<path>` is answered for its NodeDeleted; the first is
    /// untouched, and a path with one birth, or whose claim names neither, answers nothing. Must NOT
    /// happen: the claim's own birth deleted, or two live records left at one path.name.
    #[tokio::test]
    async fn a_second_birth_at_a_path_after_a_heal_is_written_deleted_and_the_claims_birth_stands() {
        let storage = MemoryNodesStorage::default();
        let written = crate::node_rows::WrittenRows::default();
        let (first, second, alone) = (birth("mesh1.admin.1"), birth("mesh1.admin.1"), birth("mesh1.rpc.1"));
        for n in [&first, &alone] {
            crate::node_rows::write_record(&storage, &NodeRow::new(n.name.clone(), n.node_id.clone(), n.status)).await.unwrap().unwrap();
        }
        storage.put_node(&NodeRow::new(second.name.clone(), second.node_id.clone(), second.status)).await.unwrap();
        let me: PathName = "mesh1.admin.2".parse().unwrap();
        let ops = superseded_births(&me, &view(vec![first.clone(), second.clone(), alone.clone()]), "mesh1", &storage, &written, "b1").await;
        assert_eq!(ops.iter().map(|o| (o.node_id.clone(), o.operation.clone())).collect::<Vec<_>>(), vec![(second.node_id.clone(), "node.delete:mesh1.admin.1".to_string())], "only the second birth is answered");
        assert_eq!(storage.node(&second.node_id).await.unwrap().unwrap().status, NodeStatus::Deleted, "the second birth's row is written deleted");
        assert_eq!(storage.node(&first.node_id).await.unwrap().unwrap().status, NodeStatus::ReadyForTraffic, "the claim's birth stands");
        assert_eq!(storage.name_claim(&first.name).await.unwrap(), Some(first.node_id.clone()), "the name stays the first birth's");
        let unclaimed = birth("mesh1.rpc.2");
        let other = birth("mesh1.rpc.2");
        storage.put_node(&NodeRow::new(unclaimed.name.clone(), unclaimed.node_id.clone(), unclaimed.status)).await.unwrap();
        assert!(superseded_births(&me, &view(vec![unclaimed, other]), "mesh1", &storage, &written, "b1").await.is_empty(), "a path whose claim names neither birth answers nothing");
    }
}
