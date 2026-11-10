//! Node recovery submitter and recovery due checks.

use super::*;

/// What the mesh primary's recovery of one node written dead reads and calls
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
    /// This admin's runner, once built: it runs the recovery and the terminate of a deleted birth
    /// heard again, the primary's own workflow (Q342).
    pub runner: Arc<std::sync::OnceLock<Arc<super::runner::AdminRunner>>>,
    /// This admin's `BuildService`: fabric recovery under an in-force fabric.delete continues the delete
    /// through its control plane (fabric-recovery.md step 3, Q155); a fabric.delete is a Build.
    pub build_service: crate::build_op::BuildSlot,
    /// This admin's Node RPC client; none in a rig that runs no Node RPC.
    pub client: Option<Arc<rdm_transport::NodeRpcClient>>,
    /// This admin's Build state: the current Build's paths a refused caller's decision reads (Q217).
    pub builds: Arc<dyn crate::build_state::BuildStateAdapter>,
    /// The births whose recovery ended failed, with the node each is of and node.runtime.inspect's
    /// answer that led to its recovery: run again by the next tick that finds the node still lost,
    /// whether or not the view still holds the birth (its delete may have completed), with no wait of its
    /// own: the recovery's calls retry only under their `Send` (Q344; node-recovery.md "Detection and the
    /// result", "Failure"). Removed once a recovery of it completed.
    pub failed: Arc<Mutex<HashMap<(NodeId, IncarnationId), (Node, Inspected)>>>,
    /// Where an investigation that ends `re-created` is written as its `rdm.node.investigated` event,
    /// and the application's `on_node_investigated`, sent from that write (Q312, Q423); and where the
    /// tick writes `ready` for a birth that answered its tickle.
    pub investigated: crate::node_investigation::NodeInvestigatedWriter,
    /// Why each birth this tick wrote dead was investigated (Q393): its re-create's event names it; a
    /// birth with none held is a self-heal's.
    pub reasons: Arc<Mutex<HashMap<(NodeId, IncarnationId), crate::node_investigation::Reason>>>,
    /// The application's `pre_node_recreate`, asked before each re-create of a recovery this primary
    /// runs, on every route ([`Self::submit_in`]; fabric-node-hooks.md "pre_node_recreate", Q416); `None`
    /// when it registers none.
    pub pre_node_recreate: Option<crate::wiring::PreNodeRecreate>,
}

impl RecoverySubmitter {
    /// The `(path, name)` lookup index key of me_path and me_name.
    pub fn me(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.me_path.clone(), name: self.me_name.clone() } }
}

/// What the provider's inspection of a node written dead found: node.runtime.inspect's own answer
/// ([`crate::deployment::provider::inspect_runtime`]).
pub use crate::deployment::provider::Inspected;

impl RecoverySubmitter {

    /// The name this admin currently holds in its topology digest view, or its boot name before its
    /// birth is in the view.
    pub(crate) fn held_name(&self) -> PathName {
        self.digest_view.held_path_name().unwrap_or_else(|| self.me())
    }

    /// The provider's exact-exited verdict of `node`'s birth: node.runtime.inspect over the one runtime
    /// resolution (`node_investigation::birth_runtime`: the view's digest of that exact birth, heard,
    /// projected by its mesh-primary or stopped, else the birth's own durable runtime row). Beside it,
    /// the exact runtime handle the provider was asked about (node-runtime-inspect.md Target), for the
    /// inspection's span.
    pub(crate) async fn inspect(&self, node: &Node) -> (Inspected, Option<DeploymentHandle>) {
        crate::node_investigation::inspect_birth_with_handle(&*self.provider, &self.digest_view, Some(&*self.nodes_storage), node).await
    }

    /// One birth written dead on the mesh primary's tick (node-recovery.md step 3, sequence step 1,
    /// Q393): its investigation concluded in failure and ends `re-created`, and the recovery runs at once
    /// (Q110, Q111); the provider's inspection here is the inspect's answer its event carries, and
    /// nothing waits for a proof of termination. The recovery's node.delete runs
    /// `TerminateRuntime` first when the runtime runs ([`crate::deployment::pipeline`]). A birth whose
    /// removal this admin's view holds announced is that workflow's, read after the inspection (the
    /// tick's view predates it).
    pub(crate) async fn recover(&self, n: &Node) -> Inspected {
        self.recover_as(n, true).await
    }

    /// A node that will not move in a storage change's mesh.unblock, re-created by this mesh-primary's
    /// node recovery (storage.md §2 step 4): [`Self::recover`] of a birth no investigation wrote dead, so
    /// no result is written and the application is told nothing (note 0005 R5: `rdm.node.investigated`
    /// is written only by a node investigation).
    pub(crate) async fn recreate(&self, n: &Node) -> Inspected {
        self.recover_as(n, false).await
    }

    /// [`Self::recover`], ending the investigation that wrote the birth dead `re-created` when
    /// `investigated`.
    async fn recover_as(&self, n: &Node, investigated: bool) -> Inspected {
        let (outcome, handle) = self.inspect(n).await;
        self.span_inspection(n, &outcome, handle.as_ref());
        if let Some(Announced { announced, operation, build_id }) = self.removal_announced(n).await {
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
                verdict = outcome.name(),
            )
            .in_scope(|| tracing::info!("the birth's removal belongs to the workflow that announced it: no recovery is submitted"));
            return outcome;
        }
        // Node recovery acts only on a lost birth that wrote its record (Q356; rdm#334, A): a birth that
        // never wrote its own record is the Build's in-flight create (the Build and the atlas hold it),
        // which the Build runs; nothing is deleted or created for it here.
        if let Ok(None) = self.nodes_storage.node(&n.node_id).await {
            let incarnation = n.incarnation_id.as_ref().map(|i| i.to_string()).unwrap_or_default();
            tracing::info_span!(
                "rdm.node_admin.build.reject.via-in-flight-create",
                node_path = %n.path, node_name = %n.name,
                node_id = %n.node_id,
                incarnation_id = %incarnation,
                submitter = %self.me_id,
                verdict = outcome.name(),
            )
            .in_scope(|| tracing::info!("the birth never wrote its record: it is its Build's in-flight create, which the Build runs; no recovery is submitted"));
            return outcome;
        }
        match n.incarnation_id.clone() {
            Some(incarnation) => {
                let _ = self.submit_on_tick(n, &incarnation, &outcome, false, investigated).await;
            }
            None => {
                let _ = self.submit(n, &outcome).await;
            }
        }
        outcome
    }

    /// One recovery the tick runs of `node`'s birth at `incarnation` (node-recovery.md "Detection and
    /// the result": the loop on every gossip interval), on `inspected`, node.runtime.inspect's answer of
    /// the birth written dead, or again after it failed (`again`: nothing is inspected again, `inspected`
    /// is the answer that led to its recovery, and its span names no answer), ending the investigation
    /// that wrote it dead `re-created` when `investigated`. A recovery that ends failed is named on its
    /// span's `outcome` and stays owed to the birth, with `inspected` ([`Self::failed`]): the next tick
    /// that finds the node still lost runs it again (Q344).
    async fn submit_on_tick(&self, node: &Node, incarnation: &IncarnationId, inspected: &Inspected, again: bool, investigated: bool) -> Result<(), String> {
        let span = submit_span(&self.me_id, node, incarnation, if again { "" } else { inspected.name() });
        if again {
            span.record("again", true);
        } else if investigated {
            // The investigation ends `re-created`: written once as its event with the inspect's
            // answer that led to it, and the application is told once from that write, under the
            // recovery's span (fabric-node-hooks.md "on_node_investigated").
            let ids = crate::node_investigation::fabric_and_mesh_of(&*self.topology.read().await, node);
            let reason = self.reasons.lock().unwrap().get(&(node.node_id.clone(), incarnation.clone())).copied().unwrap_or(crate::node_investigation::Reason::SelfHeal);
            self.investigated.end(ids, node, incarnation, Some(crate::wiring::NodeInvestigatedResult::ReCreated), Some(inspected), reason).instrument(span.clone()).await;
        }
        let end = self.submit_in(&span, node, incarnation, inspected, "node-recovery").instrument(span.clone()).await;
        let ended = settled(&span, end);
        let birth = (node.node_id.clone(), incarnation.clone());
        match &ended {
            Ok(()) => {
                self.failed.lock().unwrap().remove(&birth);
            }
            Err(_) => {
                self.failed.lock().unwrap().insert(birth.clone(), (node.clone(), inspected.clone()));
            }
        }
        ended
    }

    /// The tick's one gate for a recovery of `birth`: none of it in flight, which it then marks in
    /// flight, for the follower to clear at its end (`offline::spawn_recovery_follower`). One recovery per
    /// birth in flight (node-recovery.md "Execution"): a recovery that still runs is followed, never run
    /// again beside it; one that ended, however it ended, is taken by the next tick that finds the node
    /// still lost, with no wait of its own (Q344).
    pub(crate) fn take(&self, birth: &(NodeId, IncarnationId), in_flight: &Mutex<HashSet<(NodeId, IncarnationId)>>) -> bool {
        in_flight.lock().unwrap().insert(birth.clone())
    }

    /// The workflow whose announced removal `n`'s exact birth is under, as this admin's view holds it
    /// now (node-recovery.md: recovery is drift reconciliation for a member that exits outside any
    /// workflow; "Failure": an exit during a workflow belongs to that workflow): its
    /// node's departure (`node-deleted`), else an open `NodeDeleting` overlay naming the exact birth
    /// (`node-deleting`), else the mesh.delete of its mesh (mesh-delete.md: from mesh.deleting, the
    /// mesh's status deleting, to mesh.deleted, every member's removal is the mesh.delete's;
    /// `mesh-deleting`, `mesh-deleted`), the status the view holds for the mesh. A recovery's own
    /// node.delete of the exited birth (the typed node.delete operation on its node ID) is such a workflow too: its overlay
    /// closes when its operation's verdict is terminal (`DigestView::deletions_from_facts`), and a
    /// recovery that ended failed is run again by the next tick that finds the node still lost, whether
    /// or not the view still holds the birth ([`Self::failed_recoveries`]).
    pub(crate) async fn removal_announced(&self, n: &Node) -> Option<Announced> {
        let incarnation = n.incarnation_id.as_ref()?;
        let of = |announced: &'static str, op: rdm_core::LifecycleOp| Announced { announced, operation: op.operation, build_id: op.build_id };
        if let Some(op) = self.digest_view.deleted().into_iter().find(|op| op.node_id == n.node_id) {
            return Some(of("node-deleted", op));
        }
        // A repair belongs to no Build, and is the primary's own work (Q342). This primary's own run is
        // held by its tick until it ends; any other's was announced by a primary that no longer holds the
        // role, and nobody finishes it: the next tick that finds the member still lost runs the recovery
        // (node-recovery.md "Failure").
        if let Some(op) = self.digest_view.in_flight().into_iter().find(|op| op.node_id == n.node_id && &op.incarnation == incarnation && !op.is_stop() && op.build_id != rdm_core::LifecycleOp::NO_BUILD) {
            return Some(of("node-deleting", op));
        }
        // The status held for the id this view holds for the member's mesh (Q432).
        let announced = match self.digest_view.mesh_id_of(&n.mesh).and_then(|id| self.digest_view.statuses.mesh(&id))?.status.as_str() {
            "leaving" => "mesh-deleting",
            "dead" => "mesh-deleted",
            _ => return None,
        };
        let operation = self.topology.read().await.meshes.iter().find(|m| m.name == n.mesh).and_then(|m| m.id.clone()).map(|id| crate::mesh_delete::operation(&id)).unwrap_or_else(|| rdm_core::OperationId::new("mesh.delete", rdm_core::OperationSubject::Named(n.mesh.clone())));
        Some(Announced { announced, operation, build_id: String::new() })
    }

    /// The inspection's span (node-runtime-inspect.md "Spans"), with `handle` the exact runtime handle
    /// the provider was asked about (none when nothing was asked).
    pub(crate) fn span_inspection(&self, node: &Node, outcome: &Inspected, handle: Option<&DeploymentHandle>) {
        crate::node_investigation::span_inspection(&self.me_id.to_string(), node, outcome, handle, "the mesh primary asked the provider about the exact runtime of the node written dead");
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
        let span = submit_span(&self.me_id, node, &incarnation, outcome.name());
        let end = self.submit_in(&span, node, &incarnation, outcome, route).instrument(span.clone()).await;
        settled(&span, end)
    }

    /// The births whose recovery the tick runs again (node-recovery.md "Detection and the result": "A
    /// recovery that ends failed is run again by the next check that finds the node still lost"): none
    /// unless this admin holds its mesh's primary role; else every birth of its own mesh whose recovery
    /// failed here, whether or not the view still holds it, with node.runtime.inspect's answer that led
    /// to its recovery.
    pub(crate) fn failed_recoveries(&self, holds_role: bool, mesh: &str) -> Vec<(Node, Inspected)> {
        if !holds_role {
            return Vec::new();
        }
        self.failed.lock().unwrap().values().filter(|(n, _)| n.mesh == mesh).cloned().collect()
    }

    /// The recovery of a birth whose last recovery failed ([`Self::failed_recoveries`]), run again on the
    /// tick on `inspected`, node.runtime.inspect's answer that led to it: the same workflow, keyed on the
    /// birth; nothing is inspected again. A birth already recovered is not recovered again
    /// (node-recovery.md sequence step 1: never a second recovery): once the node
    /// its recovery created stands at its path and name, that recovery is owed no more, however its step
    /// ended. A start that ended `Failed` at its hook leaves that node `Pending`, and nothing retries until
    /// someone acts (fabric-node-hooks.md "Outcomes and the gate", Q350).
    pub(crate) async fn recover_again(&self, node: &Node, inspected: Inspected) -> Result<(), String> {
        let incarnation = node.incarnation_id.clone().ok_or_else(|| format!("{} holds no incarnation: no birth to recover", node.node_id))?;
        let created = self.topology.read().await.births().find(|b| b.location() == node.location() && b.node_id != node.node_id).map(|b| b.node_id.clone());
        if let Some(created) = created {
            let birth = (node.node_id.clone(), incarnation.clone());
            self.failed.lock().unwrap().remove(&birth);
            let span = submit_span(&self.me_id, node, &incarnation, "");
            span.record("again", true);
            span.record("outcome", "already-recovered");
            span.in_scope(|| tracing::info!(created_node_id = %created, "a birth already recovered is not recovered again: the node its recovery created stands at its path and name"));
            return Ok(());
        }
        self.submit_on_tick(node, &incarnation, &inspected, true, false).await
    }

    /// The recovery of `node`'s unreached birth at `incarnation`, run by this admin, the primary above
    /// the node (node-recovery.md "Execution"): the mesh-primary for its own members, the fabric-primary
    /// for a mesh-primary (Q335), and fabric recovery's executor for the node-admin at the lost
    /// fabric-primary's path and name (`route` `fabric-recovery`, fabric-recovery.md step 3). The primary's
    /// own workflow, never a Build or part of one (Q342): node.delete then node.create, each step recorded
    /// on the node record. It runs on the node's work turn: a node command or a Build's step on the same
    /// node runs before or after it, never beside it; a Build in flight elsewhere neither holds it nor is
    /// held by it. Every recovery path runs through here, so each run, a run again included, asks the
    /// application's `pre_node_recreate` once, on `inspected`, node.runtime.inspect's answer that led to
    /// the recovery: after the refusals, the birth held recovering and the node's work turn taken, the
    /// last step before node.delete (fabric-node-hooks.md "pre_node_recreate", node-recovery.md sequence
    /// step 1, Q416). A refused run asks nothing.
    pub(crate) async fn submit_in(&self, span: &tracing::Span, node: &Node, incarnation: &IncarnationId, inspected: &Inspected, route: &'static str) -> Result<RecoveryEnd, String> {
        let runner = self.runner.get().cloned().ok_or_else(|| format!("{} holds its mesh's primary role but its runner is not built yet", self.me_id))?;
        span.record("route", route);
        // An admin that stopped reconciling (its stop, or a rig that runs no reconciler) starts no repair,
        // as it starts no Build drive or section run: nothing is deleted or re-created by it (rdm#305).
        if runner.repairs_stopped.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(RecoveryEnd::Failed { reason: format!("{} stopped reconciling: it starts no repair", self.me_id) });
        }
        // Under an in-force fabric.delete nothing is recovered for being absent and nothing is re-created but a
        // node-admin: fabric recovery re-creates the fabric-primary at its original path and name and the
        // restored fabric-primary continues the delete (fabric-delete.md "Failure"; fabric-recovery.md step 4),
        // and node recovery re-creates a lost mesh-primary at its path and name, which deletes its mesh
        // (mesh-delete.md "Resume after the primary's loss"; Q370). The delete's own node.delete of any other
        // birth is its end. Refused naming the delete before anything is read or written.
        let recovered = matches!(route, "fabric-recovery" | "fabric-recovery-backbone") || (route == "node-recovery" && rdm_core::is_node_admin(&node.r#type));
        if let Some((delete, _)) = crate::build_state::fabric_delete_in_force(&*self.builds).await.map_err(|e| format!("{}: the Build store could not be read for an in-force fabric.delete: {e}", self.me_id))?.filter(|_| !recovered) {
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
        // Nothing is deleted or created at the birth's path and name until the application answers
        // (fabric-node-hooks.md "pre_node_recreate", Q416). Short of `Ok` the recovery ends failed here as
        // a failed step below does: the birth stays held recovering, and the caller keeps it owed.
        let ids = crate::node_investigation::fabric_and_mesh_of(&*self.topology.read().await, node);
        if let Err(reason) = crate::node_investigation::ask_pre_node_recreate(self.pre_node_recreate.as_ref(), ids, node, incarnation, inspected).await {
            return Ok(RecoveryEnd::Failed { reason });
        }
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
/// the mesh-primary). `again` is recorded on a recovery the tick runs again after it ended failed,
/// whose verdict is not read again; `outcome` names how it ended.
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
    )
}

/// The node records of its own mesh the offline tick of a mesh-primary watches births from
/// ([`durable_births`]). Only the mesh-primary runs node investigation on its mesh's births
/// (fabric-node-lifecycle-elections.md §4.1; node-recovery.md step 2: "Only the mesh-primary acts"), so
/// only it reads them: once, at the first tick that finds this admin its mesh's primary, and held while it
/// keeps the role. A row written after that read is a launch in a mesh whose mesh-primary runs every
/// launch (node-recovery.md "Execution"), and the launching admin writes it (build.md §5.3): this
/// admin's own executor, whose records put the birth in its view. An admin that is not its mesh's primary
/// holds none and reads none; the role lost, the rows go, and a later tenure reads them again. A read
/// that is refused holds none, so the next tick reads again.
#[derive(Debug, Default)]
pub(crate) struct TenureRows {
    rows: Option<Vec<crate::storage::NodeRecord>>,
}

impl TenureRows {
    /// The rows for one tick of the admin `reader` of `mesh`, `primary` when its view names it its
    /// mesh's primary: those this tenure read from `nodes`, read now at its first tick.
    pub(crate) async fn for_tick(&mut self, primary: bool, nodes: &dyn crate::storage::NodesStorage, reader: &PathName, mesh: &str) -> &[crate::storage::NodeRecord] {
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

/// The node records of `mesh` that `nodes` holds, each with the runtime its node wrote, read for
/// `reader` as it takes its mesh's primary role ([`runtime_rows_read`]).
async fn read_runtime_rows(nodes: &dyn crate::storage::NodesStorage, reader: &PathName, mesh: &str) -> Option<Vec<crate::storage::NodeRecord>> {
    runtime_rows_read(reader, Some(mesh), nodes.nodes(mesh).await)
}

/// What a primary's one read of the durable runtime rows (`read`) holds for its tenure: `mesh`'s rows
/// for a mesh-primary (`Some`, [`TenureRows`]), every mesh's for the fabric-primary (`None`,
/// `FabricTenureRows`), named on one `rdm.node_admin.node.resolve.via-runtime-rows-read` with how many;
/// `None` when the read was refused, named on its own span, so the next tick or pass reads again.
pub(crate) fn runtime_rows_read(reader: &PathName, mesh: Option<&str>, read: Result<Vec<crate::storage::NodeRecord>, crate::storage_error::StorageError>) -> Option<Vec<crate::storage::NodeRecord>> {
    let role = if mesh.is_some() { "mesh-primary" } else { "fabric-primary" };
    match read {
        Ok(rows) => {
            let rows: Vec<crate::storage::NodeRecord> = rows.into_iter().filter(|r| mesh.is_none_or(|m| r.mesh() == m)).collect();
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
pub fn durable_births(view: &Topology, durable: &[crate::storage::NodeRecord], planned: &dyn Fn(&PathName) -> bool, deleted: &dyn Fn(&str) -> bool) -> Vec<Node> {
    let mut births: Vec<Node> = Vec::new();
    for row in durable {
        if view.node(&row.location()).is_none() && !births.iter().any(|n| n.location() == row.location()) && planned(&row.location()) && !deleted(row.node_id.as_str()) {
            let mut n = Node::allocated(row.location(), row.mesh(), &row.r#type);
            n.node_id = row.node_id.clone();
            n.incarnation_id = Some(row.incarnation.clone());
            n.status = NodeStatus::PendingReconnect;
            births.push(n);
        }
    }
    births
}

/// What the mesh primary's tick watches, by node id: every `PendingReconnect` or `Dead`
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
/// copy: heard again, it is returned, never gone. A node id in neither is gone.
pub fn heard_against(view: &Topology, watched: &BTreeMap<String, Node>) -> HashSet<String> {
    view.births().map(|n| n.node_id.to_string()).chain(watched.keys().cloned()).collect()
}

/// `n`'s exact birth written dead by its mesh-primary (node-recovery.md step 3, Q393): an investigation
/// run for `reason` concluded in failure. The node's record is written first
/// ([`crate::node_rows::NodeRows::investigated`]) under one `Send` (Q344: the only retry); only once the
/// store answers it `Ok` is the birth held in this admin's view (`Records::offline`, shown `Dead`) with
/// the reason kept for its re-create's event, and the tick's recovery takes it ([`recovery_due`]). A
/// write the store refuses by name, or one whose `Send` is spent, holds no mark: the view never shows
/// a status the record does not hold. Only a spent `Send` has the caller investigate the birth again
/// ([`crate::offline::OfflineTickle::unsettle`]): the tickle reports a birth offline once. A refusal by
/// name is the write's end, as `Send` itself holds it, named once and not written again. One
/// `rdm.node_admin.node.update.via-dead-record` span names how the write ended.
pub(crate) async fn write_dead(records: &Records, reasons: &Mutex<HashMap<(NodeId, IncarnationId), crate::node_investigation::Reason>>, rows: Option<&crate::node_rows::NodeRows>, n: &Node, reason: crate::node_investigation::Reason) -> DeadWrite {
    use crate::node_rows::RowRefused;
    use rdm_transport::client::SendEnd;
    let Some(incarnation) = n.incarnation_id.clone() else { return DeadWrite::Refused };
    let span = tracing::info_span!(
        "rdm.node_admin.node.update.via-dead-record",
        node_id = %n.node_id, node_path = %n.path, node_name = %n.name, incarnation_id = %incarnation.as_str(), reason = reason.as_str(),
        outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal",
    );
    let written = match rows {
        // No writer bound yet (an admin's first ticks): not a refusal of the write, so the birth is
        // investigated again once the writer is bound, as a spent `Send`'s is.
        None => Err((DeadWrite::Spent, "no-rows-writer", "this admin holds no writer of node records yet: the dead state is not written".to_string())),
        Some(rows) => {
            let send = rdm_transport::client::Send::default();
            let ran = tracing::Instrument::instrument(send.run(|e: &RowRefused| !matches!(e, RowRefused::Unwritten(_)), || rows.investigated(n, NodeStatus::Dead)), span.clone()).await;
            ran.map_err(|end| match &end {
                SendEnd::Answered(e) => (DeadWrite::Refused, e.outcome(), end.to_string()),
                SendEnd::Spent { .. } => (DeadWrite::Spent, "send-spent", end.to_string()),
            })
        }
    };
    match written {
        Ok(_) => {
            span.record("outcome", "written");
            reasons.lock().unwrap().insert((n.node_id.clone(), incarnation.clone()), reason);
            if records.offline.lock().unwrap().insert((n.node_id.clone(), incarnation)) {
                records.inputs_moved();
            }
            DeadWrite::Written
        }
        Err((end, outcome, detail)) => {
            span.record("outcome", outcome).record("detail", detail.as_str());
            span.in_scope(|| match end {
                DeadWrite::Spent => tracing::info!(node_id = %n.node_id, outcome, "the dead state was not written on the node's record, its Send spent: the birth is not marked dead, and it is investigated again"),
                _ => tracing::info!(node_id = %n.node_id, outcome, "the dead state was refused by name on the node's record: the birth is not marked dead, and the refusal is the write's end"),
            });
            end
        }
    }
}

/// How [`write_dead`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeadWrite {
    /// The record holds Dead, and the view's mark follows it.
    Written,
    /// The write's `Send` was spent unanswered: the birth is investigated again.
    Spent,
    /// The write was refused by name (or there is nothing to write it with): its end, not written again.
    Refused,
}

/// The watched births the tick recovers (node-recovery.md step 3, Q393): none unless this admin holds
/// its mesh's primary role; else every watched birth of its own mesh (its secondary node-admins
/// included, mesh-recovery.md:8-9) written dead at its exact incarnation.
pub fn recovery_due(holds_role: bool, mesh: &str, watched: &BTreeMap<String, Node>, offline: &HashSet<(NodeId, IncarnationId)>) -> Vec<Node> {
    if !holds_role {
        return Vec::new();
    }
    watched.values().filter(|n| n.mesh == mesh && n.incarnation_id.as_ref().is_some_and(|i| offline.contains(&(n.node_id.clone(), i.clone())))).cloned().collect()
}

/// The mesh primary's per-birth bookkeeping speaks only for the births it still watches: a birth
/// whose node left the view (deleted, or recovered under a new node.id at its path and name) or whose
/// node the view now holds at another incarnation (superseded) leaves the dead set. Run every
/// tick, so nothing of a gone birth outlives one tick.
pub(crate) fn clear_unwatched(watched: &BTreeMap<String, Node>, offline: &mut HashSet<(NodeId, IncarnationId)>) {
    offline.retain(|(node_id, incarnation)| watched.get(node_id.as_str()).is_some_and(|n| n.incarnation_id.as_ref() == Some(incarnation)));
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
/// next tick that finds the node still lost runs it again (node-recovery.md "Failure", Q344).
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
        superseded_births(&self.me(), view, mesh, &*self.nodes_storage, rdm_core::LifecycleOp::NO_BUILD).await
    }
}

/// [`RecoverySubmitter::superseded_births`] on `storage`, by `me`, the operations it answers named
/// under the current Build `build_id`. Each superseded birth's record is written `Deleted` from its latest
/// line (a killed node's record is its mesh-primary's to write, Q356).
pub(crate) async fn superseded_births(me: &PathName, view: &Topology, mesh: &str, storage: &dyn crate::storage::NodesStorage, build_id: &str) -> Vec<rdm_core::LifecycleOp> {
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
            let row = async {
                let Some(mut r) = storage.node(&b.node_id).await.map_err(|e| e.to_string())?.map(|l| l.record) else {
                    return Err(format!("node {} wrote no record", b.node_id));
                };
                r.state = NodeStatus::Deleted;
                storage.put_node(&r).await.map_err(|e| e.to_string())
            }.await;
            match row {
                Ok(_) => {
                    span.record("outcome", "row-deleted");
                    span.in_scope(|| tracing::info!("a second birth at the same path and name, made across a cut, is superseded: its row is written deleted"));
                    ops.push(rdm_core::LifecycleOp { r#type: b.r#type.clone(), mesh: mesh.to_string(), build_id: build_id.to_string(), operation: rdm_core::OperationId::node("node.delete", &b.node_id), node_id: b.node_id.clone(), incarnation, path: path.path.clone(), name: path.name.clone(), event_at_rafka_ms: local_now_ms(), traceparent: None, tracestate: None });
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
    use crate::storage::{MemoryNodesStorage, NodeRecord, NodesStorage};

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
        let (first, second, alone) = (birth(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin"), birth(PathName::new("mesh1.node-admin", "1").unwrap(), "mesh1", "node-admin"), birth(PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"));
        for n in [&first, &alone] {
            storage.put_node(&rdm_node::nodes_storage::fixture_record(&n.location(), &n.r#type, n.node_id.clone(), n.incarnation_id.clone().unwrap(), n.status)).await.unwrap();
        }
        // The second birth's record does not hold the name the first birth's record holds (it stands
        // Withdrawn), and the view holds both births.
        storage.put_node(&rdm_node::nodes_storage::fixture_record(&second.location(), &second.r#type, second.node_id.clone(), second.incarnation_id.clone().unwrap(), NodeStatus::Withdrawn)).await.unwrap();
        let me: PathName = rdm_core::PathName::new("mesh1.node-admin", "2").unwrap();
        let ops = superseded_births(&me, &view(vec![first.clone(), second.clone(), alone.clone()]), "mesh1", &storage, "b1").await;
        assert_eq!(ops.iter().map(|o| (o.node_id.clone(), o.operation.clone())).collect::<Vec<_>>(), vec![(second.node_id.clone(), rdm_core::OperationId::node("node.delete", &second.node_id))], "only the second birth is answered");
        assert_eq!(storage.node(&second.node_id).await.unwrap().unwrap().record.state, NodeStatus::Deleted, "the second birth's record is written deleted");
        assert_eq!(storage.node(&first.node_id).await.unwrap().unwrap().record.state, NodeStatus::Ready, "the record's birth stands");
        assert_eq!(crate::node_rows::record_at(&storage, &first.location()).await.unwrap().map(|r| r.node_id), Some(first.node_id.clone()), "the name stays the first birth's");
        let (one, other, holder) = (birth(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node"), birth(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node"), birth(PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node"));
        storage.put_node(&rdm_node::nodes_storage::fixture_record(&holder.location(), &holder.r#type, holder.node_id.clone(), rdm_core::IncarnationId::mint(), holder.status)).await.unwrap();
        assert!(superseded_births(&me, &view(vec![one, other]), "mesh1", &storage, "b1").await.is_empty(), "a path whose live record names neither birth answers nothing");
    }
}
