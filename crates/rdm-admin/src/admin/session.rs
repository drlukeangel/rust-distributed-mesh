//! Start gates, membership observer, admin session, and entry sweep.

use super::*;

/// A person-started admin that is not a restart (no row of its own in nodes.storage): what it
/// is must be stated by its parameters, and contradictory inputs are refused by name. Day 0 is
/// the start with no mesh, no seed and no flag on a data dir that holds nothing; a recovery start
/// names its Fabric, its mesh, a seed and both primary flags. `holds` names what the data dir
/// already holds, if anything.
pub(crate) fn refuse_contradictory_start(cfg: &AdminConfig, holds: Option<&str>) -> Result<(), String> {
    if cfg.launch.is_some() {
        return Ok(());
    }
    let dir = cfg.data_dir.display();
    let recovering = cfg.mesh_primary || cfg.fabric_primary;
    if let Some(what) = holds {
        return Err(format!(
            "{dir} holds {what} but no row for this admin in nodes.storage: it is neither a restart nor a fresh start, and this start will not take Day 0 or mint a Fabric, Build or MeshId over it"
        ));
    }
    if recovering {
        if !cfg.fabric_primary {
            return Err(format!(
                "RDM_MESH_PRIMARY without RDM_FABRIC_PRIMARY on {dir}: a mesh recovery is started by the fabric primary (it launches the admin and hands it its mesh's Pending); a person starts a fabric recovery with both flags"
            ));
        }
        let mut missing = Vec::new();
        if cfg.mesh_id.is_none() {
            missing.push(rdm_core::launch::ENV_MESH_ID);
        }
        if !cfg.fabric_id_named {
            missing.push(rdm_core::launch::ENV_FABRIC_ID);
        }
        if cfg.seeds.is_empty() {
            missing.push(rdm_core::launch::ENV_SEEDS);
        }
        if !missing.is_empty() {
            return Err(format!("a fabric recovery on {dir} names the Fabric, the mesh and a live node to dial; missing {}", missing.join(", ")));
        }
    } else if cfg.mesh_id.is_some() || !cfg.seeds.is_empty() {
        return Err(format!(
            "{dir} is started with {} but no recovery flag: Day 0 names no mesh and no seed, and a recovery sets RDM_MESH_PRIMARY and RDM_FABRIC_PRIMARY",
            [(cfg.mesh_id.is_some(), rdm_core::launch::ENV_MESH_ID), (!cfg.seeds.is_empty(), rdm_core::launch::ENV_SEEDS)]
                .iter()
                .filter(|(set, _)| *set)
                .map(|(_, k)| *k)
                .collect::<Vec<_>>()
                .join(" and ")
        ));
    }
    Ok(())
}

/// How a node-admin's own mesh's Pending holds its Ready (e4.s11; i143 export gate: normal
/// joining admins never self-Ready; only the Day-0 root, which has no upstream authority, applies
/// its own mesh's Pending).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PendingGate {
    /// Pending is applied here, or an admin joining a mesh whose admin cohort is live owes nothing.
    Clear,
    /// The Day-0 root applies its own mesh's Pending to itself.
    SelfApply,
    /// A mesh's first admin that is not the Day-0 root waits for the fabric primary to apply it.
    Blocked(String),
}

pub(crate) fn pending_gate(me: &PathName, mesh: &str, day0: bool, pending_applied: bool, cohort_live: bool) -> PendingGate {
    if pending_applied {
        PendingGate::Clear
    } else if day0 {
        PendingGate::SelfApply
    } else if cohort_live {
        PendingGate::Clear
    } else {
        PendingGate::Blocked(format!("{me}: the first admin of {mesh}: its mesh's Pending has not been applied here by the fabric primary"))
    }
}

/// Why this admin cannot yet take authority over `held` (every live birth it
/// hears but itself): each birth that publishes no runtime fact, or one this
/// admin's provider cannot adopt in its own control domain. Empty: it is
/// authority-capable, and may commit Ready.
pub fn authority_blockers(held: &[MeshDigest], provider: &dyn crate::deployment::provider::DeploymentProvider) -> Vec<String> {
    held.iter()
        .filter_map(|d| match &d.node.runtime {
            None => Some(format!("{}: publishes no runtime fact", d.node.name)),
            Some(f) => crate::deployment::provider::adopt(provider, f).err().map(|r| format!("{}: {r}", d.node.name)),
        })
        .collect()
}

/// Readiness, drain and admission seen through each member's own digest:
/// a node publishes `ReadyForTraffic` once it serves, and `ConnectionsDeleting` right
/// after it starts refusing new work with the typed `ConnectionsDeleting` refusal (node-rpc §35).
pub struct MembershipObserver {
    /// The digest book the observer reads.
    pub book: DigestBook,
    /// The process's one Node RPC client (ruling X), over which the typed drain reaches the
    /// exact birth; `None` only in a cell that observes without a transport.
    pub client: Option<Arc<rdm_transport::NodeRpcClient>>,
    /// The commands this admin sent and awaits a completion call for.
    pub commands: Arc<crate::node_commands::CommandBook>,
    /// This admin's status authority, once it holds a view: a start begins a new cycle of the birth's
    /// declared states.
    pub authority: crate::status_rpc::AuthoritySlot,
    /// This admin's 0x21 door, once it holds a view: node.primary.update goes through it.
    pub handover: crate::fabric_handover::HandoverSlot,
    /// The births a command never reached for a reachability reason, marked `PendingReconnect` at
    /// once for the mesh primary's tick (node-tickle.md step 1).
    pub unreachable: crate::offline::UnreachableMarks,
}

impl MembershipObserver {
    /// A command to `node` was never sent for a reachability reason: mark the birth `PendingReconnect`
    /// at once, an observer mark beside the status its mesh-primary recorded, never over it. A birth
    /// whose recorded status is a lifecycle state the node reported (connections-deleting, stopping,
    /// stopped) keeps it and is not marked (node-recovery.md step 1).
    fn mark_unreachable(&self, node: &Node, cmd: crate::node_commands::NodeCommand, ctx: &crate::deployment::pipeline::CommandContext, why: &crate::node_commands::Unreachable) {
        let Some(incarnation) = node.incarnation_id.clone() else { return };
        let recorded = self.book.member_status(&node.node_id, &incarnation);
        let outcome = if crate::offline::marks_over(recorded) {
            let mark = crate::offline::UnreachableMark { incarnation: incarnation.clone(), at: std::time::Instant::now(), at_ms: crate::admin::records::local_now_ms(), reason: why.reason };
            self.unreachable.lock().unwrap().insert(node.node_id.clone(), mark);
            "marked"
        } else {
            "kept-lifecycle-status"
        };
        tracing::info_span!(
            "rdm.node_admin.node.update.via-pending-reconnect",
            node = %node.name,
            node_id = %node.node_id,
            incarnation_id = %incarnation.as_str(),
            reason = why.reason,
            detail = %why.detail,
            command = cmd.operation_prefix(),
            operation = %ctx.operation,
            recorded = recorded.map(|s| format!("{s:?}")).unwrap_or_default(),
            outcome,
            "otel.kind" = "internal",
        )
        .in_scope(|| tracing::info!("a command its birth never received for a reachability reason: the birth is marked pending-reconnect at once, so its tickle ladder runs now"));
    }

    pub(crate) fn digest_of(&self, node: &Node) -> Option<MeshDigest> {
        self.book.get(node.node_id.as_str()).map(|(d, _)| d).filter(|d| Some(&d.node.incarnation) == node.incarnation_id.as_ref())
    }
}

/// The blocker an exact birth names for its Ready (node-start.md step 3): only while `book` holds the
/// birth Pending (the status its mesh-primary set, never its heartbeat's), one `ProbeNodeState` to
/// that birth over `client`, and the hook's blocker of
/// a Status `Blocked` answer, named with the birth and the event it runs again on and no attempt
/// counter. Every other answer names no blocker and frames nothing: a `NotReady` is the subject's
/// door or handler not serving yet (node-rpc-envelope.md, "NotReady ... is never a hook blocker"),
/// and the step waits on; so does a birth that answers its state (`Current`, `CurrentParked`), a
/// stale incarnation, or a call that ends unanswered: why it is not Ready is the Ready check's to
/// report.
pub async fn probed_ready_blocker(client: &rdm_transport::NodeRpcClient, book: &DigestBook, node: &Node) -> Option<String> {
    let incarnation = node.incarnation_id.clone()?;
    let (digest, _) = book.get(node.node_id.as_str()).filter(|(d, _)| d.node.incarnation == incarnation)?;
    if digest.status != MemberStatus::Pending {
        return None;
    }
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(node.node_id.clone()), node_id: node.node_id.clone(), incarnation: incarnation.clone() };
    match rdm_client::NodeRpc::new(client).get(&birth, &rdm_transport::CallOptions::default()).await {
        Ok(rdm_core::status::StatusReply::Blocked { node_id, incarnation: answered, reason, retry_on }) if node_id == node.node_id && answered == incarnation => {
            Some(format!("{}: hydrate_before_ready is blocked: {reason}; it runs again on {}", node.name, retry_on.name()))
        }
        _ => None,
    }
}

#[async_trait::async_trait]
impl NodeObserver for MembershipObserver {
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<Publication> {
        self.book
            .get(node_id.as_str())
            .filter(|(d, _)| &d.node.incarnation == incarnation)
            .map(|(d, _)| Publication { runtime: d.node.runtime, data_dir: d.data_dir })
    }

    /// Ready once this admin's book holds the birth ready-for-traffic: the status its mesh-primary
    /// set from the birth's node.started (this admin, as that primary, through its status authority;
    /// otherwise the primary's projection), never a heartbeat's. A refusal names the status the
    /// book's digest carries, the member status the book holds for the birth, and whether this
    /// admin took the birth's node.started (the state it holds the birth declared).
    async fn ready(&self, node: &Node) -> Result<(), String> {
        let Some(d) = self.digest_of(node) else {
            return Err(format!("no digest from {} for this birth", node.name));
        };
        if d.status == MemberStatus::ReadyForTraffic {
            return Ok(());
        }
        let member = match self.book.member_status(&node.node_id, &d.node.incarnation) {
            Some(s) => format!("the member status held for the birth is {s:?}"),
            None => "no member status is held for the birth (a birth of another mesh, or no node.joined taken here)".to_string(),
        };
        let started = match self.authority.get().map(|a| a.declared.lock().unwrap().node(&node.node_id, &d.node.incarnation)) {
            None => "node.started is not known here: this admin holds no declared states yet".to_string(),
            Some(None) => "this admin holds no declared state of the birth: it took no node.started".to_string(),
            Some(Some(rdm_core::status::NodeState::Pending)) => "this admin holds the birth declared Pending: it took no node.started".to_string(),
            Some(Some(rdm_core::status::NodeState::ReadyForTraffic)) => "this admin holds the birth declared ReadyForTraffic: it took node.started".to_string(),
            Some(Some(state)) => format!("this admin holds the birth declared {state:?}"),
        };
        Err(format!("{} is held {:?}, not ready-for-traffic: {member}; {started}", node.name, d.status))
    }

    async fn ready_blocker(&self, node: &Node) -> Option<String> {
        probed_ready_blocker(self.client.as_deref()?, &self.book, node).await
    }

    async fn send_command(&self, node: &Node, cmd: crate::node_commands::NodeCommand, ctx: &crate::deployment::pipeline::CommandContext) -> crate::deployment::pipeline::CommandAdmission {
        if matches!(cmd, crate::node_commands::NodeCommand::Stop | crate::node_commands::NodeCommand::Start) {
            let Some(authority) = self.authority.get() else {
                return crate::deployment::pipeline::CommandAdmission::NotSent { reason: "this admin holds no status authority".into(), code: rdm_core::outcome::Code::Unavailable };
            };
            let (admission, unreachable) = authority.carry_lifecycle_reaching(node, cmd == crate::node_commands::NodeCommand::Start, ctx).await;
            if let Some(why) = &unreachable { self.mark_unreachable(node, cmd, ctx, why); }
            let detail = format!("{admission:?}");
            tracing::info_span!("rdm.node_admin.node.update.via-command-sent", node = %node.name, command = cmd.operation_prefix(), operation = %ctx.operation, build_id = %ctx.build_id, attempt = ctx.attempt, admission = admission.name(), code = admission.code().name(), detail = %detail, "otel.kind" = "internal").in_scope(|| tracing::info!("the exact node's command stream ended"));
            return admission;
        }
        // node.connections.delete { All } goes through this admin's own status authority, the birth's
        // mesh-primary: it commands the birth and holds its node.connections.deleted for a rename.
        if cmd == crate::node_commands::NodeCommand::DeleteConnections {
            let (Some(authority), Some(incarnation)) = (self.authority.get(), node.incarnation_id.clone()) else {
                return crate::deployment::pipeline::CommandAdmission::NotSent { reason: format!("this admin holds no status authority yet to carry node.connections.delete to {}", node.name), code: rdm_core::outcome::Code::Unavailable };
            };
            // The carrier's end, in-process (i144 R22): a command never sent reaches the never-sent mark
            // as `NotSent` (node-recovery.md step 1); one broken after the node accepted it is
            // incomplete, never a completion.
            let (admission, unreachable) = carried_admission(authority.carry_connections_delete(&node.node_id, &incarnation, &rdm_core::status::ConnectionScope::All).await);
            if let Some(why) = &unreachable {
                self.mark_unreachable(node, cmd, ctx, why);
            }
            return admission;
        }
        let Some(client) = self.client.as_ref() else {
            return crate::deployment::pipeline::CommandAdmission::NotSent { reason: "this admin holds no Node RPC client".into(), code: rdm_core::outcome::Code::Unavailable };
        };
        let (admission, unreachable) = crate::node_commands::send_command_reaching(client, &self.commands, &rdm_transport::NodeTarget::ExactNode(node.node_id.clone()), node, cmd, ctx).await;
        if let Some(why) = &unreachable {
            self.mark_unreachable(node, cmd, ctx, why);
        }
        admission
    }

    async fn await_completion(&self, node: &Node, cmd: crate::node_commands::NodeCommand, ctx: &crate::deployment::pipeline::CommandContext, within: Duration) -> crate::deployment::pipeline::Completion {
        crate::node_commands::await_completion(&self.commands, node, cmd, ctx, within).await
    }

}

/// The admission of a carried `node.connections.delete { All }` (i144 R22), and the reachability
/// cause of one never sent (node-recovery.md step 1: it marks the birth `PendingReconnect` at once).
/// The node's completion is the step's receipt; a command lost before the node accepted it, or broken
/// after it did (incomplete), is indeterminate, never a completion; a refusal is named.
pub(crate) fn carried_admission(end: crate::status_rpc::CarryEnd) -> (crate::deployment::pipeline::CommandAdmission, Option<crate::node_commands::Unreachable>) {
    use crate::deployment::pipeline::CommandAdmission;
    use crate::status_rpc::CarryEnd;
    let code = end.code();
    match end {
        CarryEnd::Deleted { receipt: Some(receipt), .. } => (CommandAdmission::ConnectionsDeleted { receipt }, None),
        CarryEnd::NotSent(cause) => (CommandAdmission::NotSent { reason: format!("{cause:?}"), code }, crate::node_commands::unreachable_of(&cause)),
        end @ (CarryEnd::Lost { .. } | CarryEnd::Incomplete { .. }) => (CommandAdmission::Indeterminate { reason: end.detail(), code }, None),
        end => (CommandAdmission::Refused { reply: format!("{}: {}", end.name(), end.detail()), code }, None),
    }
}

/// How a node-admin leaves its mesh when parked and joins it again when started
/// (`node_self::Session`): its tasks are held at their gate, its gossip topics are left and its
/// connections are cut; a start joins the commanding admin again, takes the topology and runs its hook.
pub(crate) struct AdminSession {
    pub(crate) gate: crate::stop_gate::StopGate,
    pub(crate) node_id: NodeId,
    pub(crate) incarnation: IncarnationId,
    pub(crate) fabric_id: FabricId,
    pub(crate) mesh_id: MeshId,
    pub(crate) membership: Membership,
    pub(crate) backbone: Backbone,
    pub(crate) client: Arc<rdm_transport::NodeRpcClient>,
    pub(crate) server: rdm_transport::NodeRpcServer,
    pub(crate) digest: Arc<Mutex<MeshDigest>>,
    pub(crate) rafka_time: rdm_transport::clock::RafkaTime,
    pub(crate) accepted: Arc<AcceptedStore>,
    pub(crate) builds: Arc<dyn BuildStateAdapter>,
    pub(crate) entry_floor: Arc<Mutex<Option<(crate::build::BuildId, u32)>>>,
    pub(crate) hydration: crate::app_hydration::Hydration,
    pub(crate) addressed: Arc<crate::app_hydration::Addressed>,
    pub(crate) hydration_cell: Arc<Mutex<Option<crate::app_hydration::HydrationHandle>>>,
    pub(crate) hydrating: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    pub(crate) wake: Arc<Records>,
    /// This admin's own Build log: a start's Build-floor step absorbs the facts it reads into it.
    pub(crate) local: Arc<dyn crate::build_state::LocalBuildLog>,
    /// The node-admins a start's Build-facts read asks, in order.
    pub(crate) responders: crate::hydrate::Responders,
    /// What this admin owes its authorities: a start begins its next own-state cycle there.
    pub(crate) declarer: Arc<crate::status_declare::Declarer>,
}

#[async_trait::async_trait]
impl crate::node_self::Session for AdminSession {
    async fn park_tasks(&self) -> crate::node_self::TasksParked {
        // The gate parks, so no turn begins; the turns already past their wait point end on their
        // own completion, and only then does the stop go on to its cut.
        let at = self.gate.quiesce().await;
        crate::node_self::TasksParked { turns_in_progress: at.in_progress(), waited_for: at.named() }
    }

    async fn cut(&self, spare: &[iroh::PublicKey]) -> crate::node_self::CutReport {
        // The stop held this admin's tasks before it (`park_tasks`): nothing it was doing goes out on
        // the connections cut below.
        let turns_in_progress = self.gate.state().borrow().in_progress();
        if let Some(h) = self.hydrating.lock().unwrap().take() {
            h.abort();
        }
        self.hydration.gate().close();
        let neighbours = self.membership.neighbours();
        self.membership.park().await;
        tracing::info_span!(
            "rdm.node.mesh.update.via-left",
            node = %self.membership.node_name(),
            node_id = %self.node_id,
            mesh = %self.membership.mesh_name(),
            mesh_id = %self.mesh_id,
            which = rdm_core::status::WhichMesh::Own.name(),
        )
        .in_scope(|| tracing::info!("left the mesh channel"));
        self.backbone.park().await;
        tracing::info_span!(
            "rdm.node.mesh.update.via-left",
            node = %self.membership.node_name(),
            node_id = %self.node_id,
            mesh = %self.membership.mesh_name(),
            mesh_id = %self.mesh_id,
            which = rdm_core::status::WhichMesh::Backbone.name(),
        )
        .in_scope(|| tracing::info!("left the backbone channel"));
        let pooled = self.client.close_pooled("node.connections.delete");
        let accepted = self.server.close_connections_except(spare, "node.connections.delete");
        crate::node_self::CutReport { neighbours, pooled, accepted, turns_in_progress }
    }

    async fn rejoin(&self, retire: rdm_transport::CancelToken, commander: tokio::sync::watch::Receiver<iroh::PublicKey>, rows: Option<crate::connections_writer::RowsHeld>, operation: rdm_core::status::DeclaredOperation, progress: Option<crate::node_self::CommandProgress>) -> Result<(), crate::node_self::StartFailed> {
        // The name this admin holds (its row's, as its last join or apply handed it): what its join
        // presents and what it is named by until the join answer hands it the row's (R12).
        let held = self.digest.lock().unwrap().node.name.clone();
        let root = tracing::info_span!(parent: None, "rdm.mesh.node.update.via-start", node = %held, node_id = %self.node_id, incarnation_id = %self.incarnation.as_str());
        let mut start = AdminAfterStop { session: self, root, commander, retire: Some(retire), rows, operation, progress };
        crate::workflows::node_start::node_start(&mut start, crate::app_hydration::StartKind::AfterStop).await
    }

    async fn join_mesh(&self, spare: Option<iroh::PublicKey>, operation: &str) -> Result<Vec<rdm_core::status::WhichMesh>, String> {
        use rdm_core::status::WhichMesh;
        // node.mesh.join (node-mesh-join.md), the one primitive: reported to the admin whose call
        // commanded it, reached back over that call.
        let name = self.digest.lock().unwrap().node.name.to_string();
        let Some(key) = spare else {
            return Err(format!("{name}: commander-uncalled: JoinMesh came on no Node RPC call"));
        };
        let to = crate::workflows::node_start::JoinTo::Commander(tokio::sync::watch::channel(key).1);
        let (mesh, mesh_id, authority) = (self.membership.mesh_name(), self.mesh_id.to_string(), key.fmt_short().to_string());
        let call = crate::workflows::node_mesh_join::JoinCall { name, node_id: &self.node_id, mesh: &mesh, mesh_id: &mesh_id, client: &self.client, rafka_time: &self.rafka_time, to: &to, authority: &authority, operation, report_parent: None, reported: tracing::Span::none(), channel_parent: None };
        crate::workflows::node_mesh_join::node_mesh_join(&mut StandaloneJoin { session: self }, call).await.map_err(|f| f.reason)?;
        self.membership.wait_for_neighbour().await;
        let pending = self.digest.lock().unwrap().clone();
        let _ = self.membership.publish(&pending).await;
        Ok(vec![WhichMesh::Own, WhichMesh::Backbone])
    }
}

impl AdminSession {
    /// This node-admin's channel entry inside `node.mesh.join`, for its start and its standalone join
    /// alike: its mesh, then the backbone.
    async fn enter_mesh(&self, step: tracing::Span, joined: Option<&crate::wire::JoinAnswer>, rows: &mut Option<crate::connections_writer::RowsHeld>) -> Membership {
        let s = self;
        step.record("mesh_id", tracing::field::display(&s.mesh_id));
        if let Some(joined) = joined {
            // The row's name and config the join handed are held on this admin's digest and by its
            // membership before it re-enters its topic or says anything (R-JN1).
            {
                let mut d = s.digest.lock().unwrap();
                if let Some(n) = &joined.control.name {
                    s.membership.set_name(n.clone());
                    d.node.name = n.clone();
                }
                if let Some(c) = &joined.control.config {
                    s.membership.set_config(c.clone());
                    d.config = c.clone();
                }
            }
            // Only now are its connections recorded: the join's names the handed name.
            drop(rows.take());
        }
        s.membership.unpark().await;
        tracing::info_span!(
            "rdm.node.mesh.update.via-joined",
            node = %s.membership.node_name(),
            node_id = %s.node_id,
            mesh = %s.membership.mesh_name(),
            mesh_id = %s.mesh_id,
            which = rdm_core::status::WhichMesh::Own.name(),
        )
        .in_scope(|| tracing::info!("joined the mesh channel"));
        s.backbone.unpark().await;
        tracing::info_span!(
            "rdm.node.mesh.update.via-joined",
            node = %s.membership.node_name(),
            node_id = %s.node_id,
            mesh = %s.membership.mesh_name(),
            mesh_id = %s.mesh_id,
            which = rdm_core::status::WhichMesh::Backbone.name(),
        )
        .in_scope(|| tracing::info!("joined the backbone channel"));
        drop(step);
        s.membership.clone()
    }
}

/// A node-admin's standalone `JoinMesh`, as it plugs into `node.mesh.join`.
struct StandaloneJoin<'a> {
    session: &'a AdminSession,
}

#[async_trait::async_trait]
impl crate::workflows::node_mesh_join::MeshJoin for StandaloneJoin<'_> {
    type Error = crate::node_self::StartFailed;

    fn join_types(&self) -> Vec<String> { self.session.membership.app_types().into_iter().collect() }

    fn join_digest(&self) -> MeshDigest {
        self.session.digest.lock().unwrap().clone()
    }

    async fn enter_channel(&mut self, step: tracing::Span, joined: Option<&crate::wire::JoinAnswer>) -> Result<Membership, crate::node_self::StartFailed> {
        Ok(self.session.enter_mesh(step, joined, &mut None).await)
    }
}

/// A node-admin's start after its stop, as it plugs into `node.start`: it rejoins the admin that
/// commanded the start, takes the control state the join answers and runs its tasks again from
/// where they were held.
pub(crate) struct AdminAfterStop<'a> {
    pub(crate) session: &'a AdminSession,
    pub(crate) root: tracing::Span,
    /// The endpoint key of the call that commanded the start last: the join goes back over that
    /// call, and the admin that answers is the hook's accepting authority.
    pub(crate) commander: tokio::sync::watch::Receiver<iroh::PublicKey>,
    /// The new cycle's token: the hook runs under it.
    pub(crate) retire: Option<rdm_transport::CancelToken>,
    /// The start's hold on this birth's connections rows, dropped once the handed name is held.
    pub(crate) rows: Option<crate::connections_writer::RowsHeld>,
    /// The start's operation: the node.started this cycle reports names it (node-status.md "Status report").
    pub(crate) operation: rdm_core::status::DeclaredOperation,
    progress: Option<crate::node_self::CommandProgress>,
}

#[async_trait::async_trait]
impl crate::workflows::node_mesh_join::MeshJoin for AdminAfterStop<'_> {
    type Error = crate::node_self::StartFailed;

    fn join_types(&self) -> Vec<String> { self.session.membership.app_types().into_iter().collect() }

    fn join_digest(&self) -> MeshDigest {
        self.session.digest.lock().unwrap().clone()
    }

    async fn enter_channel(&mut self, step: tracing::Span, joined: Option<&crate::wire::JoinAnswer>) -> Result<Membership, crate::node_self::StartFailed> {
        Ok(self.session.enter_mesh(step, joined, &mut self.rows).await)
    }
}

#[async_trait::async_trait]
impl crate::workflows::node_start::StartNode for AdminAfterStop<'_> {

    fn starting(&self) -> crate::workflows::node_start::Starting {
        let s = self.session;
        let held = s.digest.lock().unwrap().node.name.clone();
        crate::workflows::node_start::Starting { root: self.root.clone(), name: held.to_string(), path: held, node_id: s.node_id.clone(), client: s.client.clone(), rafka_time: s.rafka_time.clone() }
    }

    fn joining(&self) -> crate::workflows::node_start::Joining {
        crate::workflows::node_start::Joining::Authority { label: self.commander.borrow().fmt_short().to_string() }
    }

    async fn resolve_authority(&mut self) -> Result<crate::workflows::node_start::JoinTo, crate::node_self::StartFailed> {
        Ok(crate::workflows::node_start::JoinTo::Commander(self.commander.clone()))
    }

    async fn joined(&mut self, membership: &Membership, through: Option<&crate::workflows::node_start::JoinThrough>, joined: Option<&crate::wire::JoinAnswer>) -> Result<crate::workflows::node_start::HookParts, crate::node_self::StartFailed> {
        let s = self.session;
        // From here on it is named by the name it installed from its join answer (`enter_channel`).
        let me = s.digest.lock().unwrap().node.name.clone();
        // The Fabric record and the Build floor the join answered, recorded as at a first start.
        if let Some(joined) = joined {
            take_entry_control(joined, &s.accepted, &s.entry_floor, &s.wake, &s.membership.mesh_name(), &s.mesh_id).await;
        }
        // node.joined begins this birth's next cycle: the node.started it reports, naming this start,
        // is owed afresh to its mesh's primary, before its declarer runs again.
        s.declarer.begin_own_cycle(&me, &s.incarnation.as_str(), &self.operation);
        if self.progress.is_some() {
            if let Some(authority) = s.membership.projected_member_at(&self.commander.borrow().to_string()) {
                s.declarer.own_on_stream(&me, &s.node_id, &s.incarnation, &self.operation, &authority);
            }
        }
        if let Some(progress) = &self.progress {
            let _ = progress.lock().await.progress(rdm_core::node_command::NodeCommandReply::NodeJoined).await;
        }
        // Back on the topic means a neighbour: only then does this admin say it is back, once, as
        // Pending, so every node that held it parked or restarting hears the rejoin.
        membership.wait_for_neighbour().await;
        let pending = s.digest.lock().unwrap().clone();
        let _ = membership.publish(&pending).await;
        // Its tasks run again, from where they were held.
        s.gate.resume();
        // The control entry this start repeats when nothing names the Build it is missing: the join it
        // made, to the admin that commanded the start, with the digest it presented.
        let entry = through.map(|t| {
            let mut digest = pending.clone();
            digest.status = MemberStatus::Pending;
            entry_repeat(s.client.clone(), t.target.clone(), t.anchor.clone(), digest, s.accepted.clone(), s.entry_floor.clone(), s.rafka_time.clone(), s.wake.clone(), s.membership.mesh_name().to_string(), s.mesh_id.clone())
        });
        let after = AdminAfterStopTail {
            name: me.clone(),
            node_id: s.node_id.clone(),
            fabric_id: s.fabric_id.clone(),
            digest: s.digest.clone(),
            wake: s.wake.clone(),
            root: self.root.clone(),
            floor: AdminFloor {
                me: me.clone(),
                builds: s.builds.clone(),
                floor: s.entry_floor.clone(),
                hydrator: crate::hydrate::Hydrator::new(me.clone(), s.client.clone(), s.local.clone(), s.responders.clone(), entry),
                progress: None,
            },
        };
        Ok(crate::workflows::node_start::HookParts {
            birth: crate::app_hydration::Birth { name: me.clone(), node_id: s.node_id.clone(), incarnation: s.incarnation.clone(), fabric_id: s.fabric_id.clone(), mesh_id: s.mesh_id.clone(), kind: NodeKind::NodeAdmin },
            hydration: s.hydration.clone(),
            events: crate::app_hydration::Events::new(membership.book.clone(), s.addressed.clone()),
            cancel: self.retire.take().expect("the hook's token is handed once"),
            state: s.hydration_cell.clone(),
            runs: crate::workflows::node_start::HookRuns::Inline { blocked: s.hydrating.clone() },
            after: Box::new(after),
        })
    }
}

/// A node-admin's steps after its hook at a start after its stop.
pub(crate) struct AdminAfterStopTail {
    /// The name the admin holds: its join answer's (R-JN1).
    pub(crate) name: PathName,
    /// Its node id: the topology's members it reaches are every other node of its mesh.
    pub(crate) node_id: NodeId,
    pub(crate) fabric_id: FabricId,
    pub(crate) digest: Arc<Mutex<MeshDigest>>,
    pub(crate) wake: Arc<Records>,
    pub(crate) root: tracing::Span,
    pub(crate) floor: AdminFloor,
}

#[async_trait::async_trait]
impl crate::workflows::node_start::AfterHook for AdminAfterStopTail {
    async fn topology_installed(&mut self, membership: &Membership, read: &crate::topology_read::TopologyRead) {
        let mesh_peers: Vec<EndpointAddr> = read
            .installed
            .iter()
            .flat_map(|m| m.members.iter())
            .filter(|d| d.fabric_id == self.fabric_id && d.node.node_id != self.node_id && d.node.name.mesh == self.name.mesh)
            .filter_map(rdm_transport::membership::gossip_addr)
            .collect();
        let _ = membership.join_peers(mesh_peers).await;
    }

    async fn floor(&mut self, step: &tracing::Span) {
        self.floor.until_held(step).await;
    }

    async fn report(self: Box<Self>) {
        let mut d = self.digest.lock().unwrap();
        if d.status == MemberStatus::Pending {
            d.status = MemberStatus::ReadyForTraffic;
            // The Ready names the birth that became ready, as the first start's does.
            let incarnation = d.node.incarnation.clone();
            drop(d);
            self.wake.wake.poke();
            tracing::info_span!(parent: &self.root, "rdm.mesh.node.update.via-ready", node = %self.name, node_id = %self.node_id, incarnation_id = %incarnation.as_str(), source = "self-published-membership").in_scope(|| tracing::info!("ready for traffic"));
        }
    }
}

/// How often a start below its Build floor checks the floor again and hands its blocker to the
/// hydrator: the cadence the hydrator's one-request-at-a-time rule and backoff are set against
/// (`crate::hydrate`).
pub(crate) const FLOOR_CHECK: Duration = Duration::from_millis(100);

/// A node-admin's Build floor at a start (node-start.md step 4): "Fetch Build facts only when the node
/// is below the authority's floor, after the hook and before node.started". The floor itself is the
/// attempt of the pointed Build the admin that served the join held, recorded where the join's answer
/// is taken ([`take_entry_control`]); this step reads the facts with `FetchBuildFacts` while the
/// admin's own holdings are below it, and completes when the check holds them, never on a received
/// frame alone.
pub(crate) struct AdminFloor {
    pub(crate) me: PathName,
    pub(crate) builds: Arc<dyn BuildStateAdapter>,
    pub(crate) floor: Arc<Mutex<Option<(crate::build::BuildId, u32)>>>,
    pub(crate) hydrator: crate::hydrate::Hydrator,
    /// Where the Ready check of a first start reads what the start waits on.
    pub(crate) progress: Option<StartProgress>,
}

impl AdminFloor {
    /// Resolves once this admin holds the Build facts its floor requires. Runs inside the floor step's
    /// span, so each read it starts is a child of it.
    pub(crate) async fn until_held(&mut self, step: &tracing::Span) {
        let owed = self.floor.lock().unwrap().clone();
        step.record("owed", owed.as_ref().map_or_else(|| "no floor named by the join".to_string(), |(b, a)| format!("{b} attempt {a}")).as_str());
        let mut seen: Vec<&'static str> = Vec::new();
        loop {
            let floor = self.floor.lock().unwrap().clone();
            let blocker = hydration_blocker(&self.me, &*self.builds, floor).await;
            self.hydrator.tick(blocker.as_ref()).await;
            let Some(blocker) = blocker else { break };
            if !seen.contains(&blocker.kind()) {
                seen.push(blocker.kind());
            }
            if let Some(p) = &self.progress {
                p.at(format!("node.start is at its Build floor: {blocker}"));
            }
            tokio::time::sleep(FLOOR_CHECK).await;
        }
        // Nothing is left outstanding at node.started: a read still in flight is let end.
        self.hydrator.settle().await;
        step.record("blockers", seen.join(",").as_str());
        step.record("outcome", if seen.is_empty() { "held" } else { "fetched" });
    }
}

/// What a node-admin's first start has yet to do once its hook runs, as its Ready check names it.
#[derive(Clone)]
pub(crate) struct StartProgress(pub(crate) Arc<Mutex<Option<String>>>);
impl StartProgress {
    pub(crate) fn begun(me: &PathName) -> Self {
        Self(Arc::new(Mutex::new(Some(format!("{me}: node.start has not run its topology, Build-floor and rafka-time steps")))))
    }

    pub(crate) fn at(&self, what: String) {
        *self.0.lock().unwrap() = Some(what);
    }

    pub(crate) fn started(&self) {
        *self.0.lock().unwrap() = None;
    }

    pub(crate) fn blocker(&self) -> Option<String> {
        self.0.lock().unwrap().clone()
    }
}

/// A node-admin's steps after its hook at its first start (node.create).
pub(crate) struct AdminFirstStart {
    pub(crate) me: PathName,
    pub(crate) me_id: NodeId,
    pub(crate) fabric_id: FabricId,
    pub(crate) mesh: String,
    pub(crate) backbone: Backbone,
    pub(crate) records: Arc<Records>,
    pub(crate) provider: ProviderKind,
    pub(crate) gate: crate::stop_gate::StopGate,
    pub(crate) entry: crate::reenter::EntryCtx,
    pub(crate) entering: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) sweep: Arc<std::sync::OnceLock<crate::reenter::SweepReport>>,
    pub(crate) start_tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    pub(crate) floor: AdminFloor,
    pub(crate) progress: StartProgress,
    pub(crate) read_seam: Option<Arc<dyn crate::wiring::ReadSeam>>,
}

#[async_trait::async_trait]
impl crate::workflows::node_start::AfterHook for AdminFirstStart {
    async fn before_topology_read(&mut self, authority: &PathName) {
        if let Some(seam) = &self.read_seam {
            seam.before_topology_read(&self.me.to_string(), &authority.to_string()).await;
        }
    }

    async fn topology_installed(&mut self, membership: &Membership, read: &crate::topology_read::TopologyRead) {
        self.progress.at(format!("{}: node.start is at its topology step", self.me));
        let mut mesh_peers = Vec::new();
        let mut admins = Vec::new();
        for d in read.installed.iter().flat_map(|m| m.members.iter()).filter(|d| d.fabric_id == self.fabric_id && d.node.name != self.me) {
            if let Some(a) = rdm_transport::membership::gossip_addr(d) {
                if d.node.name.mesh == self.mesh {
                    mesh_peers.push(a.clone());
                }
                if d.node.name.kind == NodeKind::NodeAdmin {
                    admins.push(a);
                }
            }
        }
        let _ = membership.join_peers(mesh_peers).await;
        self.backbone.join_admins(admins).await;
        // The maker's topology: when the own mesh already has nodes, this admin is entering an existing
        // mesh (recovery); a mesh's first birth finds none. A maker that holds no snapshot of this mesh
        // answers from its stored map: births to reach a local node with, never installed.
        let mut map = crate::reenter::map_of_read(read, &self.fabric_id);
        map.extend(crate::reenter::map_of_stored(read));
        // A birth starts from its maker's topology: every settled node the maker's projection holds is
        // in this admin's view as the maker knew it, until membership or the sweep says more. A fabric
        // primary this admin becomes then never plans from an empty view.
        for n in map.iter().filter(|n| n.node_id != self.me_id && n.transition.is_none()) {
            if let Some(res) = n.resolved() {
                self.entry.resolver.apply(res, None);
            }
            self.records.publish(n.as_node(if n.ready { NodeStatus::ReadyForTraffic } else { NodeStatus::PendingReconnect }, self.provider));
        }
        // The maker's stored ids are its meshes' rows on the shared root, written by each mesh's primary:
        // held in memory here, never written again by this admin. A mesh keeps its id through a
        // recovery, and an admin that deploys that mesh's births launches them with it.
        for m in &read.stored {
            if let Some(id) = &m.mesh_id {
                self.records.meshes.lock().unwrap().entry(m.mesh.clone()).or_insert_with(|| id.clone());
            }
        }
        if map.iter().any(|n| n.name.mesh == self.mesh && n.node_id != self.me_id) {
            self.entering.store(true, std::sync::atomic::Ordering::SeqCst);
            let sweep = spawn_entry_sweep(&self.gate, self.entry.clone(), "maker", map, self.sweep.clone(), self.records.clone(), self.provider);
            self.start_tasks.lock().unwrap().push(sweep);
        }
    }

    async fn floor(&mut self, step: &tracing::Span) {
        self.floor.until_held(step).await;
        self.progress.at(format!("{}: node.start is at its rafka-time step", self.me));
    }

    async fn report(self: Box<Self>) {
        // node.started: the Ready check, which waits on every other blocker too, makes the report.
        self.progress.started();
    }

    async fn start_failed(self: Box<Self>, failed: &crate::node_self::StartFailed, _: bool) {
        // The Ready check names the failed step as this admin's blocker: it is not started.
        self.progress.at(format!("{}: node.start failed at {}: {}", self.me, failed.step, failed.reason));
    }
}

/// Enter an existing mesh: connect to a local node, read its topology and sweep the own mesh once with
/// a Ping (mesh-recovery.md step 6). A reply proves a live exact birth; every node that did not answer
/// is unheard, and node recovery's to prove. `slot` holds the report the Ready check waits on.
pub(crate) fn spawn_entry_sweep(
    gate: &crate::stop_gate::StopGate,
    ctx: crate::reenter::EntryCtx,
    source: &'static str,
    map: Vec<crate::reenter::MapNode>,
    slot: Arc<std::sync::OnceLock<crate::reenter::SweepReport>>,
    records: Arc<Records>,
    provider_kind: ProviderKind,
) -> tokio::task::JoinHandle<()> {
    gate.spawn(async move {
        let report = crate::reenter::enter_existing_mesh(&ctx, source, map).await;
        // The entry read has told this admin what the mesh knows of its seats (or no one could).
        records.set_entering(false);
        // A reply proves a live exact birth: the view holds it (until membership speaks for
        // it), settled as ReadyForTraffic, in transition as its source held it (a Leaving
        // member that answers is alive and still leaving). A settled node that did not answer
        // stays in the map as not yet reached; one in transition is left to its own last digest.
        for n in &report.reached {
            records.publish(n.as_node(n.transition.unwrap_or(NodeStatus::ReadyForTraffic), provider_kind));
        }
        for (n, _) in report.not_reached.iter().filter(|(n, _)| n.transition.is_none()) {
            records.publish(n.as_node(NodeStatus::PendingReconnect, provider_kind));
        }
        // A node not reached is unheard, so the mesh-primary's tick watches it: its tickle, then
        // the provider's exact exit proof, then node recovery only on proof (mesh-recovery.md step 6).
        let _ = slot.set(report);
    })
}

/// The control entry retrieved again ([`crate::hydrate::EntryRepeat`]): the join a start made, to the
/// same authority with the same digest, its answer taken as the start took the first one.
#[allow(clippy::too_many_arguments)]
pub(crate) fn entry_repeat(
    client: Arc<rdm_transport::NodeRpcClient>,
    target: rdm_transport::NodeTarget,
    anchor: String,
    digest: MeshDigest,
    accepted: Arc<AcceptedStore>,
    floor: Arc<Mutex<Option<(crate::build::BuildId, u32)>>>,
    rafka_time: rdm_transport::clock::RafkaTime,
    records: Arc<Records>,
    mesh: String,
    mesh_id: MeshId,
) -> crate::hydrate::EntryRepeat {
    Arc::new(move || {
        let (client, target, anchor, digest, accepted, floor, rafka_time, records, mesh, mesh_id) = (client.clone(), target.clone(), anchor.clone(), digest.clone(), accepted.clone(), floor.clone(), rafka_time.clone(), records.clone(), mesh.clone(), mesh_id.clone());
        Box::pin(async move {
            let answer = crate::join::call_join(&client, &target, &anchor, &digest, 1).await.map_err(|e| e.to_string())?;
            crate::rafka_time::adopt_join_answer(&tracing::Span::current(), &rafka_time, &digest.node.name.to_string(), &answer);
            take_entry_control(&answer, &accepted, &floor, &records, &mesh, &mesh_id).await;
            Ok(())
        }) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>
    })
}

/// What this admin answers an entry pull with.
pub(crate) struct EntryState {
    pub(crate) name: PathName,
    pub(crate) provider: ProviderKind,
    pub(crate) accepted: Arc<AcceptedStore>,
    pub(crate) builds: Arc<dyn BuildStateAdapter>,
    pub(crate) membership: Membership,
    /// The rafka-time this admin adopted: a join is answered with it, never with the OS clock.
    pub(crate) time: rdm_transport::clock::RafkaTime,
}

impl EntryState {
    /// What this admin holds now: the answer to a join (its control state and statuses; the
    /// topology is read with `GetTopology`).
    pub(crate) async fn answer(&self) -> Result<crate::wire::JoinAnswer, String> {
        let e = self;
        let me = e.membership.book.held_path_name().unwrap_or_else(|| e.name.clone());
        // An admin that holds no rafka-time has none to hand a joiner: the join is not completed,
        // and the refusal says why.
        let rafka_time_ms = e.time.try_now_ms().ok_or_else(|| format!("{me}: this admin holds no rafka-time to answer a join with"))?;
        // The fabric control state a new admin hydrates before it may be Ready: the answering admin's
        // current Build (R-BI3) and the attempt it holds of it, the floor a joiner's own copy of that
        // Build's attempt facts must reach before it is Ready.
        // A store that cannot be read is no answer: the join is refused naming it, never answered as
        // an admin that holds no Build or no Fabric.
        let build = e.builds.current().await.map_err(|err| format!("{me}: its builds store cannot be read for the current Build: {err}"))?.map(|b| crate::wire::BuildFloor { build_id: b.build_id, attempt: b.attempt });
        let fabric = e.accepted.record().await.map_err(|err| format!("{me}: its fabric.storage cannot be read for the Fabric identity: {err}"))?;
        let control = crate::wire::JoinControl { provider: e.provider, fabric, build, rafka_time_ms, member_cert: Vec::new(), name: None, config: None };
        let me_str = me.to_string();
        Ok(crate::wire::JoinAnswer { served_by: me_str.clone(), control, statuses: e.membership.status_frames(&me_str) })
    }
}

#[cfg(test)]
mod carried_tests {
    use super::*;
    use crate::deployment::pipeline::CommandAdmission;
    use crate::status_rpc::CarryEnd;
    use rdm_core::outcome::NotSentReason;
    use rdm_core::status::ConnectionsReceipt;

    /// CONTRACT (i144 R22; Luke Q6: "NotSent before any node report changes no lifecycle state" and
    /// "An unsent call reaches the never-sent mark as NotSent, through an in-process type"; "A stream
    /// that breaks after admission is incomplete"): the carrier's end maps to the step's admission. A
    /// command never sent for a reachability reason is `NotSent` and names its cause for the mark; one
    /// never sent for a local cause marks nothing; one lost before acceptance or broken after it is
    /// indeterminate, never a completion; the node's completion is the receipt; a refusal is named.
    // @feature: node-lifecycle
    #[test]
    fn a_carried_connections_delete_maps_to_its_admission_and_only_a_reachability_not_sent_marks() {
        let (a, why) = carried_admission(CarryEnd::NotSent(NotSentReason::Connection("connect timed out".into())));
        assert!(matches!(a, CommandAdmission::NotSent { .. }) && why.as_ref().map(|w| w.reason) == Some("connect-timed-out"), "{a:?} {why:?}");
        let (a, why) = carried_admission(CarryEnd::NotSent(NotSentReason::NoActiveRoute));
        assert!(matches!(a, CommandAdmission::NotSent { .. }) && why.as_ref().map(|w| w.reason) == Some("endpoint-unresolved"), "{a:?} {why:?}");
        let (a, why) = carried_admission(CarryEnd::NotSent(NotSentReason::StreamBudget));
        assert!(matches!(a, CommandAdmission::NotSent { .. }) && why.is_none(), "a local cause marks nothing: {a:?} {why:?}");
        for end in [CarryEnd::Lost { reason: "before acceptance".into(), code: rdm_core::outcome::Code::Unknown }, CarryEnd::Incomplete { reported: true, reason: "broke after acceptance".into(), code: rdm_core::outcome::Code::Unknown }] {
            let (a, why) = carried_admission(end.clone());
            assert!(matches!(a, CommandAdmission::Indeterminate { .. }) && why.is_none(), "{end:?}: {a:?}");
        }
        let receipt = ConnectionsReceipt::Deadline { last_in_flight: 1, closed: vec![], held: vec![] };
        let (a, why) = carried_admission(CarryEnd::Deleted { closed: vec![], not_open: vec![], held: vec![], receipt: Some(receipt.clone()) });
        assert_eq!((a, why.is_none()), (CommandAdmission::ConnectionsDeleted { receipt }, true));
        let (a, _) = carried_admission(CarryEnd::RefusedByNode(rdm_core::node_command::NodeCommandReply::RejectedConnectionScope { reason: "empty".into() }));
        assert!(matches!(&a, CommandAdmission::Refused { reply , .. } if reply.contains("refused-by-node")), "{a:?}");
    }
}
