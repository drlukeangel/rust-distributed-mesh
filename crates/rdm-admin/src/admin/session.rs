//! Start gates, topology observer, admin session, and entry sweep.

use super::*;

/// What [`refuse_contradictory_start`] is told a store root holding the fabric's record holds.
pub(crate) const HOLDS_FABRIC: &str = "a Fabric record";

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
    // Day 0 given a fabric id whose store root already holds that Fabric's record (Q156): Day 0 creates a
    // fabric and never adopts one, told apart from an adoption launch by intent, never by the store.
    if !recovering && cfg.fabric_id_named && holds == Some(HOLDS_FABRIC) {
        return Err(format!(
            "Day 0 is given Fabric {} ({}) and the store root {} already holds that Fabric's record: Day 0 creates a fabric and never adopts one; a node-admin started into that fabric is an adoption launch, {} at the lost fabric-primary's path.name (fabric-recovery.md \"Nothing alive\")",
            cfg.fabric_id,
            rdm_core::launch::ENV_FABRIC_ID,
            cfg.store_dir.join(cfg.fabric_id.to_string()).display(),
            rdm_core::launch::ENV_NODE_NAME,
        ));
    }
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
/// its own mesh's Pending). Pending belongs to mesh.create's first admin alone (states.md): an admin
/// entering an existing mesh joins it, its cohort live or not (mesh-investigation.md, "A node-admin
/// joins the live mesh").
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PendingGate {
    /// Pending is applied here, or this admin enters an existing mesh and owes nothing.
    Clear,
    /// The Day-0 root applies its own mesh's Pending to itself.
    SelfApply,
    /// A mesh's first admin that is not the Day-0 root waits for the fabric primary to apply it.
    Blocked(String),
}

pub(crate) fn pending_gate(me: &PathName, mesh: &str, day0: bool, pending_applied: bool, enters_existing_mesh: bool) -> PendingGate {
    if pending_applied {
        PendingGate::Clear
    } else if day0 {
        PendingGate::SelfApply
    } else if enters_existing_mesh {
        PendingGate::Clear
    } else {
        PendingGate::Blocked(format!("{me}: the first admin of {mesh}: its mesh's Pending has not been applied here by the fabric primary"))
    }
}

/// Why this admin cannot yet take authority over `held` (every live birth it
/// hears but itself): each birth that publishes no runtime fact, or one this
/// admin's provider cannot adopt in its own control domain. Empty: it is
/// authority-capable, and may commit Ready.
pub fn take_over_blockers(held: &[MeshDigest], provider: &dyn crate::deployment::provider::DeploymentProvider) -> Vec<String> {
    held.iter()
        .filter_map(|d| match &d.node.runtime {
            None => Some(format!("{}: publishes no runtime fact", d.node.name)),
            Some(f) => crate::deployment::provider::adopt(provider, f).err().map(|r| format!("{}: {r}", d.node.name)),
        })
        .collect()
}

/// Readiness, connections delete and admission seen through each member's own digest:
/// a node publishes `Ready` once it serves, and `ConnectionsDeleting` right
/// after it starts refusing new work with the typed `ConnectionsDeleting` refusal (node-rpc §35).
pub struct TopologyObserver {
    /// The digest view the observer reads.
    pub digest_view: DigestView,
    /// The process's one Node RPC client (ruling X), over which the typed connections delete reaches the
    /// exact birth; `None` only in a cell that observes without a transport.
    pub client: Option<Arc<rdm_transport::NodeRpcClient>>,
    /// The commands this admin sent and awaits a completion call for.
    pub commands: Arc<crate::node_commands::CommandView>,
    /// This admin's Status door, once it holds a view: a start begins a new cycle of the birth's
    /// declared states.
    pub status_door: crate::status_rpc::StatusDoorSlot,
    /// This admin's 0x21 door, once it holds a view: node.primary.update goes through it.
    pub handover: crate::fabric_handover::HandoverSlot,
    /// The births a command never reached for a reachability reason, marked `PendingReconnect` at
    /// once for the mesh primary's tick (node-tickle.md step 1).
    pub unreachable: crate::offline::UnreachableMarks,
}

impl TopologyObserver {
    /// A command to `node` was never sent for a reachability reason: mark the birth `PendingReconnect`
    /// at once, an observer mark beside the status its mesh-primary recorded, never over it. A birth
    /// whose recorded status is a lifecycle state the node reported (connections-deleting, stopping,
    /// stopped) keeps it and is not marked (node-recovery.md step 1).
    fn mark_unreachable(&self, node: &Node, cmd: crate::node_commands::NodeCommand, ctx: &crate::deployment::pipeline::CommandContext, why: &crate::node_commands::Unreachable) {
        let Some(incarnation) = node.incarnation_id.clone() else { return };
        let recorded = self.digest_view.member_status(&node.node_id, &incarnation);
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
        self.digest_view.get(node.node_id.as_str()).map(|(d, _)| d).filter(|d| Some(&d.node.incarnation) == node.incarnation_id.as_ref())
    }
}

/// The blocker an exact birth names for its Ready (node-start.md step 3): only while `digest_view` holds the
/// birth Pending (the status its mesh-primary set, never its heartbeat's), one `ProbeNodeState` to
/// that birth over `client`, and the hook's blocker of
/// a Status `Blocked` answer, named with the birth and the event it runs again on and no attempt
/// counter. Every other answer names no blocker and frames nothing: a `NotReady` is the subject's
/// door or handler not serving yet (node-rpc-envelope.md, "NotReady ... is never a hook blocker"),
/// and the step waits on; so does a birth that answers its state (`Current`, `CurrentStopped`), a
/// stale incarnation, or a call that ends unanswered: why it is not Ready is the Ready check's to
/// report.
pub async fn probed_ready_blocker(client: &rdm_transport::NodeRpcClient, digest_view: &DigestView, node: &Node) -> Option<String> {
    let incarnation = node.incarnation_id.clone()?;
    let (digest, _) = digest_view.get(node.node_id.as_str()).filter(|(d, _)| d.node.incarnation == incarnation)?;
    if digest.status != MemberStatus::Pending {
        return None;
    }
    let birth = rdm_client::ExactBirth { target: rdm_transport::NodeTarget::ExactNode(node.node_id.clone()), node_id: node.node_id.clone(), incarnation: incarnation.clone() };
    match rdm_client::NodeRpc::new(client).get(&birth, &rdm_transport::CallOptions::default()).await {
        Ok(rdm_core::status::StatusReply::Blocked { node_id, incarnation: answered, reason, retry_on }) if node_id == node.node_id && answered == incarnation => {
            Some(format!("{}: pre_node_ready is blocked: {reason}; it runs again on {}", node.name, retry_on.name()))
        }
        _ => None,
    }
}

#[async_trait::async_trait]
impl NodeObserver for TopologyObserver {
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<Publication> {
        self.digest_view
            .get(node_id.as_str())
            .filter(|(d, _)| &d.node.incarnation == incarnation)
            .map(|(d, _)| Publication { runtime: d.node.runtime, data_dir: d.data_dir })
    }

    /// Ready once this admin's view holds the birth ready: the status its mesh-primary
    /// set from the birth's node.started (this admin, as that primary, through its status authority;
    /// otherwise the primary's projection), never a heartbeat's. A refusal names the status the
    /// view's digest carries, the member status the digest view holds for the birth, and whether this
    /// admin took the birth's node.started (the state it holds the birth declared).
    async fn ready(&self, node: &Node) -> Result<(), String> {
        let Some(d) = self.digest_of(node) else {
            return Err(format!("no digest from {} for this birth", node.name));
        };
        if d.status == MemberStatus::Ready {
            return Ok(());
        }
        // This admin's Status door took the birth's node.started: fabric recovery's executor, of
        // another mesh, writes the records of the node it re-creates there while that mesh has no
        // mesh-primary (build.md §5.3, Q122), and its view holds no member status of another mesh.
        if self.status_door.get().is_some_and(|a| a.declared.lock().unwrap().node(&node.node_id, &d.node.incarnation) == Some(rdm_core::status::NodeState::Ready)) {
            return Ok(());
        }
        let member = match self.digest_view.member_status(&node.node_id, &d.node.incarnation) {
            Some(s) => format!("the member status held for the birth is {s:?}"),
            None => "no member status is held for the birth (a birth of another mesh, or no node.joined taken here)".to_string(),
        };
        let started = match self.status_door.get().map(|a| a.declared.lock().unwrap().node(&node.node_id, &d.node.incarnation)) {
            None => "node.started is not known here: this admin holds no declared states yet".to_string(),
            Some(None) => "this admin holds no declared state of the birth: it took no node.started".to_string(),
            Some(Some(rdm_core::status::NodeState::Pending)) => "this admin holds the birth declared Pending: it took no node.started".to_string(),
            Some(Some(rdm_core::status::NodeState::Ready)) => "this admin holds the birth declared Ready: it took node.started".to_string(),
            Some(Some(state)) => format!("this admin holds the birth declared {state:?}"),
        };
        Err(format!("{} is held {:?}, not ready: {member}; {started}", node.name, d.status))
    }

    async fn ready_blocker(&self, node: &Node) -> Option<String> {
        probed_ready_blocker(self.client.as_deref()?, &self.digest_view, node).await
    }

    async fn send_command(&self, node: &Node, cmd: crate::node_commands::NodeCommand, ctx: &crate::deployment::pipeline::CommandContext) -> crate::deployment::pipeline::CommandAdmission {
        if matches!(cmd, crate::node_commands::NodeCommand::Stop | crate::node_commands::NodeCommand::Start) {
            let Some(door) = self.status_door.get() else {
                return crate::deployment::pipeline::CommandAdmission::NotSent { reason: "this admin holds no status authority".into(), code: rdm_core::outcome::Code::Unavailable };
            };
            let (admission, unreachable) = door.carry_lifecycle_reaching(node, cmd == crate::node_commands::NodeCommand::Start, ctx).await;
            if let Some(why) = &unreachable { self.mark_unreachable(node, cmd, ctx, why); }
            let detail = format!("{admission:?}");
            tracing::info_span!("rdm.node_admin.node.update.via-command-sent", node = %node.name, command = cmd.operation_prefix(), operation = %ctx.operation, build_id = %ctx.build_id, attempt = ctx.attempt, admission = admission.name(), code = admission.code().name(), detail = %detail, "otel.kind" = "internal").in_scope(|| tracing::info!("the exact node's command stream ended"));
            return admission;
        }
        // node.connection.delete { All } goes through this admin's own Status door, the birth's
        // mesh-primary: it commands the birth and holds its node.connections.deleted for a rename.
        if cmd == crate::node_commands::NodeCommand::DeleteConnections {
            let (Some(door), Some(incarnation)) = (self.status_door.get(), node.incarnation_id.clone()) else {
                return crate::deployment::pipeline::CommandAdmission::NotSent { reason: format!("this admin holds no status authority yet to carry node.connection.delete to {}", node.name), code: rdm_core::outcome::Code::Unavailable };
            };
            // The carrier's end, in-process (i144 R22): a command never sent reaches the never-sent mark
            // as `NotSent` (node-recovery.md step 1); one broken after the node accepted it is
            // incomplete, never a completion.
            let (admission, unreachable) = carried_admission(door.carry_connections_delete(&node.node_id, &incarnation, &rdm_core::status::ConnectionScope::All).await);
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

/// The admission of a carried `node.connection.delete { All }` (i144 R22), and the reachability
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

/// How a node-admin leaves its mesh when stopped and joins it again when started
/// (`node_self::Session`): its tasks are held at their gate, its gossip topics are left and its
/// connections are cut; a start joins the commanding admin again, takes the topology and runs its hook.
pub(crate) struct AdminSession {
    pub(crate) gate: crate::stop_gate::StopGate,
    pub(crate) node_id: NodeId,
    pub(crate) incarnation: IncarnationId,
    pub(crate) fabric_id: FabricId,
    pub(crate) mesh_id: MeshId,
    pub(crate) topology_view: TopologyView,
    pub(crate) backbone: Backbone,
    pub(crate) client: Arc<rdm_transport::NodeRpcClient>,
    pub(crate) server: rdm_transport::NodeRpcServer,
    pub(crate) digest: Arc<Mutex<MeshDigest>>,
    pub(crate) rafka_time: rdm_transport::clock::RafkaTime,
    pub(crate) accepted: Arc<AcceptedStore>,
    pub(crate) builds: Arc<dyn BuildStateAdapter>,
    pub(crate) entry_floor: Arc<Mutex<Option<(crate::build::BuildId, u32)>>>,
    pub(crate) node_hooks: crate::node_hooks::NodeHooks,
    pub(crate) addressed: Arc<crate::node_hooks::Addressed>,
    pub(crate) pre_node_ready_cell: Arc<Mutex<Option<crate::node_hooks::PreNodeReadyHandle>>>,
    pub(crate) pre_node_ready_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    pub(crate) wake: Arc<Records>,
    /// This admin's own Build log: a start's Build-floor step absorbs the facts it reads into it.
    pub(crate) local: Arc<dyn crate::build_state::LocalBuildLog>,
    /// The node-admins a start's Build-facts read asks, in order.
    pub(crate) responders: crate::fetch_build::Responders,
    /// What this admin owes its authorities: a start begins its next own-state cycle there.
    pub(crate) declarer: Arc<crate::status_declare::Declarer>,
    /// The app protocols this admin opens connections on: what the member cert each join answers is
    /// installed on.
    pub(crate) data_connections: rdm_transport::connections::ConnectionsRegistry,
    /// The Builds this admin drives: the callers reading a Build's frames are what a stop its own run
    /// commands spares (`Drive::callers`).
    pub(crate) drives: Arc<crate::build_drive::Drives>,
}

#[async_trait::async_trait]
impl crate::node_self::Session for AdminSession {
    async fn hold_tasks(&self, commanded_by: Option<&str>) -> crate::node_self::TasksStopped {
        // The gate stops, so no turn begins; the turns already past their wait point end on their
        // own completion, and only then does the stop go on to its cut. A stop this admin's own run
        // commanded waits for every turn but that Build's, which wait on it (stop_gate.rs).
        let at = match commanded_by {
            Some(build) => self.gate.quiesce_commanded_by(build).await,
            None => self.gate.quiesce().await,
        };
        crate::node_self::TasksStopped { turns_in_progress: at.in_progress(), waited_for: at.named() }
    }

    fn callers_of(&self, build: &str) -> Vec<iroh::PublicKey> {
        crate::build::BuildId::parse(build).map(|id| self.drives.callers_of(&id)).unwrap_or_default()
    }

    async fn leave_channels(&self) {
        if let Some(h) = self.pre_node_ready_task.lock().unwrap().take() {
            h.abort();
        }
        self.node_hooks.gate().close();
        crate::node_self::leave_channels(&self.topology_view, Some(&self.backbone), &self.node_id).await;
    }

    async fn cut(&self, spare: &[iroh::PublicKey], mesh_primary: Option<iroh::PublicKey>) -> crate::node_self::CutReport {
        // The stop held this admin's tasks before it (`hold_tasks`): nothing it was doing goes out on
        // the connections cut below.
        let turns_in_progress = self.gate.state().borrow().in_progress();
        let neighbours = self.topology_view.neighbours();
        self.leave_channels().await;
        let pooled = self.client.close_pooled_except(mesh_primary.as_slice(), "node.connection.delete");
        let accepted = self.server.close_connections_except(spare, "node.connection.delete");
        crate::node_self::CutReport { neighbours, pooled, accepted, turns_in_progress }
    }

    async fn rejoin(&self, birth_cancel: rdm_transport::CancelToken, commander: tokio::sync::watch::Receiver<iroh::PublicKey>, rows: Option<crate::connections_writer::RowsHeld>, operation: rdm_core::status::DeclaredOperation, progress: Option<crate::node_self::CommandProgress>, held_again: Option<tokio::sync::oneshot::Sender<()>>) -> Result<(), crate::node_self::StartFailed> {
        // The name this admin holds (its row's, as its last join or apply handed it): what its join
        // presents and what it is named by until the join answer hands it the row's (R12).
        let held = self.digest.lock().unwrap().node.name.clone();
        let root = tracing::info_span!(parent: None, "rdm.mesh.node.update.via-start", node = %held, node_id = %self.node_id, incarnation_id = %self.incarnation.as_str());
        let mut start = AdminAfterStop { session: self, root, commander, birth_cancel: Some(birth_cancel), rows, operation, progress, held_again };
        crate::workflows::node_start::node_start(&mut start, crate::node_hooks::StartKind::AfterStop).await
    }

}

impl AdminSession {
    /// This node-admin's channel entry inside `node.mesh.join`, for its start and its standalone join
    /// alike: its mesh, then the backbone.
    async fn enter_mesh(&self, step: tracing::Span, joined: Option<&crate::wire::JoinAnswer>, rows: &mut Option<crate::connections_writer::RowsHeld>) -> TopologyView {
        let s = self;
        step.record("mesh_id", tracing::field::display(&s.mesh_id));
        if let Some(joined) = joined {
            // The row's name and config the join handed are held on this admin's digest and by its
            // the topology view before it re-enters its topic or says anything (R-JN1).
            {
                let mut d = s.digest.lock().unwrap();
                if let Some(n) = &joined.control.name {
                    s.topology_view.set_name(n.clone());
                    d.node.name = n.clone();
                }
                if let Some(c) = &joined.control.config {
                    s.topology_view.set_config(c.clone());
                }
            }
            // The member cert this join was issued, as at every join (Q87).
            rdm_node::node::install_join_cert(&s.data_connections, &joined.control.member_cert, &s.topology_view.node_name().to_string(), &s.node_id, &s.incarnation);
            // Only now are its connections recorded: the join's names the handed name.
            drop(rows.take());
        }
        s.topology_view.start().await;
        tracing::info_span!(
            "rdm.node.mesh.update.via-joined",
            node = %s.topology_view.node_name(),
            node_id = %s.node_id,
            mesh = %s.topology_view.mesh_name(),
            mesh_id = %s.mesh_id,
            which = rdm_core::status::WhichMesh::Own.name(),
        )
        .in_scope(|| tracing::info!("joined the mesh channel"));
        s.backbone.start().await;
        tracing::info_span!(
            "rdm.node.mesh.update.via-joined",
            node = %s.topology_view.node_name(),
            node_id = %s.node_id,
            mesh = %s.topology_view.mesh_name(),
            mesh_id = %s.mesh_id,
            which = rdm_core::status::WhichMesh::Backbone.name(),
        )
        .in_scope(|| tracing::info!("joined the backbone channel"));
        drop(step);
        s.topology_view.clone()
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
    pub(crate) birth_cancel: Option<rdm_transport::CancelToken>,
    /// The start's hold on this birth's connections rows, dropped once the handed name is held.
    pub(crate) rows: Option<crate::connections_writer::RowsHeld>,
    /// The start's operation: the node.started this cycle reports names it (node-status.md "Status report").
    pub(crate) operation: rdm_core::status::DeclaredOperation,
    progress: Option<crate::node_self::CommandProgress>,
    /// Sent once this admin holds its mesh again: a commanded `node.mesh.join` answers on it.
    held_again: Option<tokio::sync::oneshot::Sender<()>>,
}

#[async_trait::async_trait]
impl crate::workflows::node_mesh_join::MeshJoin for AdminAfterStop<'_> {
    type Error = crate::node_self::StartFailed;

    fn join_types(&self) -> Vec<String> { self.session.topology_view.app_types().into_iter().collect() }

    fn join_digest(&self) -> MeshDigest {
        self.session.digest.lock().unwrap().clone()
    }

    async fn enter_channel(&mut self, step: tracing::Span, joined: Option<&crate::wire::JoinAnswer>) -> Result<TopologyView, crate::node_self::StartFailed> {
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
        crate::workflows::node_start::Joining::AcceptingPrimary { label: self.commander.borrow().fmt_short().to_string() }
    }

    async fn resolve_join_to(&mut self) -> Result<crate::workflows::node_start::JoinTo, crate::node_self::StartFailed> {
        Ok(crate::workflows::node_start::JoinTo::Commander(self.commander.clone()))
    }

    fn operation(&self) -> Option<String> {
        Some(self.operation.operation.clone())
    }

    async fn joined(&mut self, topology_view: &TopologyView, _: Option<&crate::workflows::node_start::JoinThrough>, joined: Option<&crate::wire::JoinAnswer>) -> Result<crate::workflows::node_start::HookParts, crate::node_self::StartFailed> {
        let s = self.session;
        // From here on it is named by the name it installed from its join answer (`enter_channel`).
        let me = s.digest.lock().unwrap().node.name.clone();
        // The Fabric record and the Build floor the join answered, recorded as at a first start.
        if let Some(joined) = joined {
            take_entry_control(joined, &s.accepted, &s.entry_floor, &s.wake, &s.topology_view.mesh_name(), &s.mesh_id).await;
        }
        // node.joined begins this birth's next cycle: the node.started it reports, naming this start,
        // is owed afresh to its mesh's primary, before its declarer runs again.
        s.declarer.begin_own_cycle(&me, &s.incarnation.as_str(), &self.operation);
        if self.progress.is_some() {
            if let Some(receiver) = s.topology_view.projected_member_at(&self.commander.borrow().to_string()) {
                s.declarer.own_on_stream(&me, &s.node_id, &s.incarnation, &self.operation, &receiver);
            }
        }
        // Back on its channels, this admin reports NodeJoined, as an rpc node does: the start finishes on
        // its direct calls and the admin is back in gossip whenever gossip gets there. Its mesh-primary
        // recorded its Pending at the accepted join report (node-mesh-join.md steps 9 and 10, Q97).
        if let Some(progress) = &self.progress {
            let _ = progress.lock().await.progress(rdm_core::node_command::NodeCommandReply::NodeJoined).await;
        }
        if let Some(held) = self.held_again.take() {
            let _ = held.send(());
        }
        // Its tasks run again, from where they were held.
        s.gate.resume();
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
                fetch_build: crate::fetch_build::FetchBuild::new(me.clone(), s.client.clone(), s.local.clone(), s.responders.clone()),
                progress: None,
                held: Some(s.local.clone()),
            },
            leave: self.operation.operation.starts_with("node.mesh.join:").then(|| AdminLeave { topology_view: s.topology_view.clone(), backbone: s.backbone.clone(), node_hooks: s.node_hooks.clone(), pre_node_ready_task: s.pre_node_ready_task.clone(), node_id: s.node_id.clone(), name: me.clone() }),
        };
        Ok(crate::workflows::node_start::HookParts {
            birth: crate::node_hooks::Birth { name: me.clone(), node_id: s.node_id.clone(), incarnation: s.incarnation.clone(), fabric_id: s.fabric_id.clone(), mesh_id: s.mesh_id.clone(), kind: NodeKind::NodeAdmin },
            node_hooks: s.node_hooks.clone(),
            events: crate::node_hooks::Events::new(topology_view.digest_view.clone(), s.addressed.clone()),
            cancel: self.birth_cancel.take().expect("the hook's token is handed once"),
            state: s.pre_node_ready_cell.clone(),
            runs: crate::workflows::node_start::HookRuns::Inline { blocked: s.pre_node_ready_task.clone() },
            after: Box::new(after),
        })
    }
}

/// What a node-admin does to leave its channels from a task that is not the session's: the parts
/// `Session::leave_channels` uses, cloned; the channel leave itself is the one
/// `crate::node_self::leave_channels`. A commanded `node.mesh.join` (Q87) whose start fails at a
/// step after its hook leaves the birth off its channels, Pending, for its authority's next command
/// (node-recovery.md step 2), as a failure inside the start's own task does.
pub(crate) struct AdminLeave {
    topology_view: TopologyView,
    backbone: Backbone,
    node_hooks: crate::node_hooks::NodeHooks,
    pre_node_ready_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    node_id: NodeId,
    name: PathName,
}

impl AdminLeave {
    async fn run(self, step: &str, reason: &str) {
        if let Some(h) = self.pre_node_ready_task.lock().unwrap().take() {
            h.abort();
        }
        self.node_hooks.gate().close();
        crate::node_self::leave_channels(&self.topology_view, Some(&self.backbone), &self.node_id).await;
        tracing::info_span!("rdm.mesh.node.reject.via-mesh-join-start-failed", node = %self.name, step, reason)
            .in_scope(|| tracing::info!("the commanded start stopped at a step after its hook: the admin leaves its channels for its authority's next command"));
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
    /// Set for a commanded `node.mesh.join`: a Build's start after a stop leaves nothing (its process stays stopped).
    pub(crate) leave: Option<AdminLeave>,
}

#[async_trait::async_trait]
impl crate::workflows::node_start::AfterHook for AdminAfterStopTail {
    async fn start_failed(self: Box<Self>, failed: &crate::node_self::StartFailed, _: bool) {
        if let Some(leave) = self.leave {
            leave.run(failed.step, &failed.reason).await;
        }
    }

    async fn topology_installed(&mut self, topology_view: &TopologyView, read: &crate::topology_read::TopologyRead) {
        let mesh_peers: Vec<EndpointAddr> = read
            .installed
            .iter()
            .flat_map(|m| m.members.iter())
            .filter(|d| d.fabric_id == self.fabric_id && d.node.node_id != self.node_id && d.node.name.mesh == self.name.mesh)
            .filter_map(rdm_transport::topology::gossip_addr)
            .collect();
        let _ = topology_view.join_peers_as("admin.session.mesh", mesh_peers).await;
    }

    async fn floor(&mut self, step: &tracing::Span) {
        self.floor.until_held(step).await;
    }

    async fn report(self: Box<Self>) {
        let (set, d) = set_own_status(&self.digest, Some(MemberStatus::Pending), MemberStatus::Ready);
        if set {
            // The Ready names the birth that became ready, as the first start's does.
            let incarnation = d.node.incarnation.clone();
            self.wake.wake.poke();
            tracing::info_span!(parent: &self.root, "rdm.mesh.node.update.via-ready", node = %self.name, node_id = %self.node_id, incarnation_id = %incarnation.as_str(), source = "self-published-topology").in_scope(|| tracing::info!("ready"));
        }
    }
}

/// Set this node-admin's own status in `digest`, the digest it publishes, to `to` when it holds `from`
/// (whatever it holds when `from` is `None`), under one hold of the digest: the one place a node-admin
/// sets its own status (every status its `NodeSelf` sets, and its Ready, from Pending only,
/// at its Ready gate and at a start's report). Answers whether it was set, and the digest as it stands.
pub(crate) fn set_own_status(digest: &Mutex<MeshDigest>, from: Option<MemberStatus>, to: MemberStatus) -> (bool, MeshDigest) {
    let mut d = digest.lock().unwrap();
    if from.is_some_and(|from| d.status != from) {
        return (false, d.clone());
    }
    d.status = to;
    (true, d.clone())
}

/// How often a start below its Build floor checks the floor again when no request of its own ends
/// first (facts a Build stream brings, a backoff passing, a responder appearing), and hands its
/// blocker to the fetch_build: the cadence the fetch_build's one-request-at-a-time rule and backoff are
/// set against (`crate::fetch_build`). A request's end checks again at once.
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
    pub(crate) fetch_build: crate::fetch_build::FetchBuild,
    /// Where the Ready check of a first start reads what the start waits on.
    pub(crate) progress: Option<StartProgress>,
    /// The Build log a start served a join checks its floor against as it holds the facts now, reading
    /// no store (Q227: "only FP reads the builds"): every launched node-admin's first start and every
    /// start after a stop. `None`: a start with no launcher reads its builds store.
    pub(crate) held: Option<Arc<dyn crate::build_state::LocalBuildLog>>,
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
            let blocker = match &self.held {
                Some(log) => crate::admin::reconcile::node_admin_not_ready_held(&self.me, &**log, floor).await,
                None => node_admin_not_ready(&self.me, &*self.builds, floor).await,
            };
            self.fetch_build.tick(blocker.as_ref()).await;
            let Some(blocker) = blocker else { break };
            if !seen.contains(&blocker.kind()) {
                seen.push(blocker.kind());
            }
            if let Some(p) = &self.progress {
                p.at(format!("node.start is at its Build floor: {blocker}"));
            }
            tokio::select! {
                () = self.fetch_build.request_ended() => {}
                () = tokio::time::sleep(FLOOR_CHECK) => {}
            }
        }
        // Nothing is left outstanding at node.started: a read still in flight is let end.
        self.fetch_build.settle().await;
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
    async fn before_topology_read(&mut self, primary: &PathName) {
        if let Some(seam) = &self.read_seam {
            seam.before_topology_read(&self.me.to_string(), &primary.to_string()).await;
        }
    }

    async fn topology_installed(&mut self, topology_view: &TopologyView, read: &crate::topology_read::TopologyRead) {
        self.progress.at(format!("{}: node.start is at its topology step", self.me));
        let mut mesh_peers = Vec::new();
        let mut admins = Vec::new();
        for d in read.installed.iter().flat_map(|m| m.members.iter()).filter(|d| d.fabric_id == self.fabric_id && d.node.name != self.me) {
            if let Some(a) = rdm_transport::topology::gossip_addr(d) {
                if d.node.name.mesh == self.mesh {
                    mesh_peers.push(a.clone());
                }
                if d.node.name.kind == NodeKind::NodeAdmin {
                    admins.push(a);
                }
            }
        }
        let _ = topology_view.join_peers_as("admin.session.rejoin", mesh_peers).await;
        self.backbone.join_admins(admins).await;
        // The maker's topology: when the own mesh already has nodes, this admin is entering an existing
        // mesh (recovery); a mesh's first birth finds none. A maker that holds no snapshot of this mesh
        // answers from its stored map: births to reach a local node with, never installed.
        let mut map = crate::reenter::map_of_read(read, &self.fabric_id);
        map.extend(crate::reenter::map_of_stored(read));
        // A birth starts from its maker's topology: every settled node the maker's projection holds is
        // in this admin's view as the maker knew it, until the topology view or the sweep says more. A fabric
        // primary this admin becomes then never plans from an empty view.
        for n in map.iter().filter(|n| n.node_id != self.me_id && n.transition.is_none()) {
            if let Some(res) = n.resolved() {
                self.entry.resolver.apply(res, None);
            }
            self.records.publish(n.as_node(if n.ready { NodeStatus::Ready } else { NodeStatus::PendingReconnect }, self.provider));
        }
        // The maker's stored ids are its meshes' rows on the shared root, written by each mesh's primary:
        // held in memory here, never written again by this admin. A mesh keeps its id through a
        // recovery, and an admin that deploys that mesh's births launches them with it.
        for m in &read.stored {
            if let Some(id) = &m.mesh_id {
                self.records.meshes.lock().unwrap().entry(m.mesh.clone()).or_insert_with(|| id.clone());
            }
        }
        // So are the ids the snapshots it installed carry on their members: a launched admin holds its
        // meshes' ids from this hydration sync, never from the Build records (Q227).
        for d in read.installed.iter().flat_map(|m| m.members.iter()).filter(|d| d.fabric_id == self.fabric_id) {
            if let Some(id) = &d.mesh_id {
                self.records.meshes.lock().unwrap().entry(d.node.name.mesh.clone()).or_insert_with(|| id.clone());
            }
        }
        // The mesh exists when the maker names another birth its primary, though its answer lists none
        // of the mesh's nodes (a maker that has published no snapshot of its own mesh yet answers it
        // absent, node-rpc-envelope.md's Stored, yet its Primaries frame names the mesh's primary):
        // this admin enters an existing mesh, never waits for a Pending that is not its own (rdm#130).
        let primary_elsewhere = read.primaries.iter().any(|(role, p, gone)| *role == rdm_core::PrimaryRole::MeshPrimary && p.mesh == self.mesh && p.node_id != self.me_id && !gone);
        if primary_elsewhere || map.iter().any(|n| n.name.mesh == self.mesh && n.node_id != self.me_id) {
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
        // The entry read has told this admin what the mesh knows of its roles (or no one could).
        records.set_entering(false);
        // A reply proves a live exact birth: the view holds it (until the topology view speaks for
        // it), settled as Ready, in transition as its source held it (a Leaving
        // member that answers is alive and still leaving). A settled node that did not answer
        // stays in the map as not yet reached; one in transition is left to its own last digest.
        for n in &report.reached {
            records.publish(n.as_node(n.transition.unwrap_or(NodeStatus::Ready), provider_kind));
        }
        for (n, _) in report.not_reached.iter().filter(|(n, _)| n.transition.is_none()) {
            records.publish(n.as_node(NodeStatus::PendingReconnect, provider_kind));
        }
        // A node not reached is unheard, so the mesh-primary's tick watches it: its tickle, then
        // the provider's exact exited verdict, then node recovery only on proof (mesh-recovery.md step 6).
        let _ = slot.set(report);
    })
}

/// What this admin answers an entry pull with.
pub(crate) struct EntryState {
    pub(crate) name: PathName,
    pub(crate) provider: ProviderKind,
    pub(crate) accepted: Arc<AcceptedStore>,
    pub(crate) builds: Arc<dyn BuildStateAdapter>,
    pub(crate) topology_view: TopologyView,
    /// The rafka-time this admin adopted: a join is answered with it, never with the OS clock.
    pub(crate) time: rdm_transport::clock::RafkaTime,
    /// What this admin signs with: its cert policy rides every entry answer (fabric-certs.md
    /// §Tracking: "carried to every later admin in its entry pull").
    pub(crate) issuer: Arc<crate::certs::CertIssuer>,
}

impl EntryState {
    /// What this admin holds now: the answer to a join (its control state and statuses; the
    /// topology is read with `GetTopology`).
    pub(crate) async fn answer(&self) -> Result<crate::wire::JoinAnswer, String> {
        let e = self;
        let me = e.topology_view.digest_view.held_path_name().unwrap_or_else(|| e.name.clone());
        // An admin that holds no rafka-time has none to hand a joiner: the join is not completed,
        // and the refusal says why.
        let rafka_time_ms = e.time.try_now_ms().ok_or_else(|| format!("{me}: this admin holds no rafka-time to answer a join with"))?;
        // The fabric control state a new admin fetches before it may be Ready: the answering admin's
        // current Build (R-BI3) and the attempt it holds of it, the floor a joiner's own copy of that
        // Build's attempt facts must reach before it is Ready.
        // A store that cannot be read is no answer: the join is refused naming it, never answered as
        // an admin that holds no Build or no Fabric.
        let current = e.builds.current().await.map_err(|err| format!("{me}: its builds store cannot be read for the current Build: {err}"))?;
        let build = current.map(|b| crate::wire::BuildFloor { build_id: b.build_id, attempt: b.attempt });
        let fabric = e.accepted.record().await.map_err(|err| format!("{me}: its fabric.storage cannot be read for the Fabric identity: {err}"))?;
        let control = crate::wire::JoinControl { cert_policy: Some(e.issuer.policy()), provider: e.provider, fabric, build, rafka_time_ms, member_cert: Vec::new(), name: None, config: None };
        let me_str = me.to_string();
        Ok(crate::wire::JoinAnswer { served_by: me_str.clone(), control, statuses: e.topology_view.status_frames(&me_str) })
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

    /// CONTRACT: docs/architecture/lifecycles/fabric-create.md:14 "Caller / executor | A person starts the first node-admin with no mesh and no durable topology"
    /// docs/architecture/lifecycles/fabric-create.md:15 "Entry | No fabric exists; Day 0 is the first node-admin and its own mesh, nothing else."
    #[test]
    fn day0_entry_accepts_only_clean_unseeded_start_and_self_applies_pending_gate() {
        let store_dir = crate::test_support::tempdir("day0-entry-store");
        let data_dir = crate::test_support::tempdir("day0-entry-data");
        let cfg = AdminConfig::from_env(|k| match k {
            "RDM_STORE_DIR" => Some(store_dir.display().to_string()),
            "RDM_DATA_DIR" => Some(data_dir.display().to_string()),
            _ => None,
        })
        .expect("clean Day-0 environment parses");

        assert_eq!(cfg.fabric, "fabric1");
        assert_eq!(cfg.mesh, "mesh1");
        assert!(!cfg.fabric_id_named);
        assert!(cfg.adopt.is_none());
        assert!(cfg.mesh_id.is_none());
        assert!(cfg.seeds.is_empty());
        assert!(!cfg.mesh_primary && !cfg.fabric_primary);
        assert_eq!(refuse_contradictory_start(&cfg, None), Ok(()));

        for held in [HOLDS_FABRIC, "a mesh record", "a durable topology"] {
            let err = refuse_contradictory_start(&cfg, Some(held)).expect_err("existing store state must refuse Day 0");
            assert!(err.contains(held), "refusal must name {held}: {err}");
        }

        let mut named_fabric = cfg.clone();
        named_fabric.fabric_id_named = true;
        let err = refuse_contradictory_start(&named_fabric, Some(HOLDS_FABRIC)).expect_err("named existing fabric must refuse Day 0");
        assert!(err.contains("Day 0 creates a fabric and never adopts one"), "{err}");

        let mut with_seed = cfg.clone();
        with_seed.seeds = vec![("n".into(), "127.0.0.1:9000".parse().unwrap())];
        let err = refuse_contradictory_start(&with_seed, None).expect_err("Day 0 with seed must be refused");
        assert!(err.contains("Day 0 names no mesh and no seed"), "{err}");

        let mut mesh_only_recovery = cfg.clone();
        mesh_only_recovery.mesh_primary = true;
        let err = refuse_contradictory_start(&mesh_only_recovery, None).expect_err("mesh_primary without fabric_primary must be refused");
        assert!(err.contains("RDM_MESH_PRIMARY without RDM_FABRIC_PRIMARY"), "{err}");

        let me: PathName = "mesh1.admin.1".parse().unwrap();
        assert_eq!(pending_gate(&me, "mesh1", true, false, false), PendingGate::SelfApply);
        assert_eq!(pending_gate(&me, "mesh1", true, true, false), PendingGate::Clear);
        assert!(matches!(pending_gate(&me, "mesh1", false, false, false), PendingGate::Blocked(_)));
    }
}
