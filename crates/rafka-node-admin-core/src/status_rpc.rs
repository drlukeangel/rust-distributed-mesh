//! The lifecycle/status declarations node-admin applies as an authority (i143.e6.s7;
//! node-rpc.md §38; fabric-mesh-ops.md §3).
//!
//! A declaration reaches this admin over Node RPC (`Status`, op `0x1B`). The sender is never
//! read from the request: it is the authenticated peer, resolved to a birth in this admin's own
//! view. The admin applies a declaration only when it holds the seat the declaration needs and
//! the sender is the subject (or the subject's primary), idempotently on the natural key, forward
//! along the legal order, and answers what happened. `Applied` means the declared state is held
//! in this admin's entity state and, for a node, written to `nodes.storage` as the subject's row.
//!
//! Gossip is untouched: the view's `status` stays what membership says; what the authority
//! applied is the view's `declared`.

use crate::model::{FabricId, IncarnationId, MeshId, NodeId, NodeKind, EndpointId};
use crate::topology::Topology;
use rafka_node_rpc::{PeerContext, ServerBuilder};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::status::{transition, FabricEvent, MeshState, NodeState, NotAuthority, Status, StatusReply, StatusRequest, Transition};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock, RwLock};

/// What this admin applied, by natural key.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Declared {
    /// Per birth `(node id, incarnation)`: the greatest state it declared.
    pub nodes: std::collections::HashMap<(NodeId, IncarnationId), NodeState>,
    /// Per birth held in `nodes`: the path.name it declared at.
    pub names: std::collections::HashMap<(NodeId, IncarnationId), crate::model::PathName>,
    /// Per mesh id.
    pub meshes: std::collections::HashMap<MeshId, MeshState>,
    /// Per fabric id: the events applied, in order, each once.
    pub fabric: std::collections::HashMap<FabricId, Vec<String>>,
    /// Per mesh name: the latest `ReadyForTraffic` report this admin received as the fabric-primary,
    /// from the mesh's primary and the exact birth that sent it. Memory of this process's tenure; it is
    /// never stored or rehydrated, because a report is evidence only while it is fresh.
    pub reports: BTreeMap<String, MeshReport>,
}

/// A mesh primary's round-complete report, as the fabric-primary received it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshReport {
    /// The birth of the node-admin that reported.
    pub incarnation: IncarnationId,
    /// When this admin received it.
    pub at: std::time::Instant,
}

impl Declared {
    /// The state the birth `(node_id, incarnation)` declared, when it declared one.
    pub fn node(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<NodeState> {
        self.nodes.get(&(node_id.clone(), incarnation.clone())).copied()
    }
}

impl Declared {
    /// What an admin applied before it last stopped, folded from the rows it put: the greatest
    /// state per Mesh, each Fabric event once, and the greatest declared state per birth. A reader
    /// folds; nothing is written back.
    pub async fn rehydrate(status: &dyn crate::status_storage::StatusStorage) -> Result<Self, String> {
        let mut declared = Self { meshes: status.mesh_statuses().await.map_err(|e| format!("status.storage: {e}"))?, ..Self::default() };
        for e in status.fabric_events().await.map_err(|e| format!("status.storage: {e}"))? {
            declared.fabric.entry(e.fabric_id).or_default().push(e.event);
        }
        let rows = status.node_declared().await.map_err(|e| format!("status.storage: {e}"))?;
        declared.names = crate::status_storage::declared_names(&rows);
        declared.nodes = crate::status_storage::fold_declared(rows);
        Ok(declared)
    }
}

/// The status rows of this admin's own mesh, which it folds into what it applied when it takes
/// the mesh's seat.
pub struct SeatFold {
    storage: Arc<dyn crate::status_storage::StatusStorage>,
    mesh: String,
    me: NodeId,
}

impl SeatFold {
    /// The rows of `mesh` in `storage`, folded by the admin born as `me`.
    pub fn new(storage: Arc<dyn crate::status_storage::StatusStorage>, mesh: String, me: NodeId) -> Self {
        Self { storage, mesh, me }
    }
}

/// What the service reads and writes: this admin's identity, its current view, what it applied,
/// and the rows it writes.
pub struct StatusAuthority {
    /// This admin's `path.name`: what it is named by in spans and refusals.
    pub me: crate::model::PathName,
    /// The node id this admin was born with: how it finds its own row and its seats in a view
    /// ([`Topology::own`]), never by `path.name`.
    pub node_id: NodeId,
    /// The fabric's minted id.
    pub fabric_id: FabricId,
    /// This admin's observed topology.
    pub topology: Arc<tokio::sync::RwLock<Topology>>,
    /// What this admin applied.
    pub declared: Arc<Mutex<Declared>>,
    /// The nodes store: the node rows a config update puts (`UpdateNodeConfig`).
    pub nodes_storage: Arc<dyn crate::storage::NodesStorage>,
    /// Where an applied Mesh status, Fabric event or node declared state is a keyed row, put before
    /// it is answered.
    pub status_storage: Arc<dyn crate::status_storage::StatusStorage>,
    /// The mesh ids this admin holds, by mesh name.
    pub mesh_ids: Arc<dyn Fn() -> BTreeMap<String, MeshId> + Send + Sync>,
    /// This node's re-publish of its presence (its peers handed to its mesh channel again, its
    /// digest published), filled once it has joined: what a node-admin's status kick asks of it.
    pub republish: Republish,
    /// The commands this admin sent (as an owning mesh-admin) and awaits a completion call for.
    pub commands: Arc<crate::node_commands::CommandBook>,
    /// This admin as the subject of `drain-node` / `stop-node` from its own mesh-admin; filled once
    /// its digest and server exist.
    pub own: Arc<OnceLock<Arc<crate::node_self::NodeSelf>>>,
    /// What serves `LeaveMesh` (as a leaving mesh's primary) and `MeshLeave` (as the
    /// fabric-primary); filled once the runner exists.
    pub leaver: crate::mesh_leave::LeaverSlot,
    /// Testkit knob: hold the next reply past the caller's bound after applying (acceptance 2:
    /// apply + reply loss is `Indeterminate`, and the retry is `AlreadyApplied`).
    pub hold_next_reply: Arc<std::sync::atomic::AtomicBool>,
    /// Wakes this admin's declarer: a view this authority changed, and a down op's receipt.
    pub wake: Arc<crate::status_declare::DeclareWake>,
    /// This admin's rounds (commit-state, open-traffic); filled once the admin holds the handles
    /// they act through.
    pub rounds: Arc<OnceLock<Arc<crate::fabric_rounds::FabricRounds>>>,
    /// The fabric-primary's `node.config.update` door; filled once the admin's node-RPC client exists.
    pub configs: crate::node_config::NodeConfigsSlot,
    /// This admin as the subject of `ApplyNodeConfig`: it holds the config and publishes its digest
    /// carrying it; filled once its digest exists.
    pub configure: Configure,
    /// Where this admin's own `hydrate_before_ready` stands (the cell its start writes): while it is
    /// Blocked, a probe naming this admin is answered `Blocked` carrying its blocker.
    pub hydration: Hydrating,
    /// This admin as the subject of `ApplyNodeName`: it takes the name with no traffic in flight
    /// and publishes its digest carrying it; filled once its digest exists.
    pub rename: Rename,
    /// This admin's view taking a node's row its `node.update` moved (R13): the view lists the birth
    /// at the row's name on every projection from now on. Filled with the view.
    pub row_moved: RowMoved,
}

/// What an `Applied` decision changes once its row is acknowledged. A decision never mutates
/// `Declared`: the effect is committed only after the put returned `Ok`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// A node's declared-state row: the state its exact birth declared.
    Node(crate::status_storage::NodeDeclaredRow, NodeState),
    /// A mesh and the state applied to it.
    Mesh(MeshId, MeshState),
    /// A fabric event applied.
    Fabric(FabricId, String),
}

/// The decision, pure over a view and what was applied: what the admin does with a declaration
/// from `sender`. It reads `declared` and returns the effect to commit; it writes nothing.
pub fn decide(auth: &StatusAuthority, view: &Topology, declared: &Declared, sender: Option<&crate::model::Node>, req: &StatusRequest) -> (StatusReply, Option<Effect>) {
    let me = match view.own(&auth.node_id) {
        Some(n) => n,
        None => return (StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "a view holding this admin".into() } }, None),
    };
    let Some(sender) = sender else {
        return (StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: "unknown peer".into() } }, None);
    };
    match req {
        StatusRequest::DeclareNodeState { node_id, incarnation, state } => {
            if sender.node_id != *node_id {
                return (StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: sender.name.to_string() } }, None);
            }
            // The receiver's seat: a node-admin declares to the fabric-primary, every other kind
            // to its mesh's primary.
            let needed = if sender.kind == NodeKind::NodeAdmin { me.is_fabric_primary } else { me.is_primary && me.mesh == sender.mesh };
            if !needed {
                let seat = if sender.kind == NodeKind::NodeAdmin { "fabric-primary" } else { "mesh-primary" };
                return (StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: format!("{seat} of {}", sender.mesh) } }, None);
            }
            // The exact birth: a declaration for an incarnation the view no longer holds is stale.
            match sender.incarnation_id.as_ref() {
                Some(held) if held == incarnation => {}
                Some(held) => return (StatusReply::RejectedStaleIncarnation { held: held.clone() }, None),
                None => return (StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown }, None),
            }
            let current = declared.node(&sender.node_id, incarnation);
            match transition(current, *state) {
                Transition::AlreadyApplied => (StatusReply::AlreadyApplied, None),
                Transition::Backward { current } => (StatusReply::RejectedInvalidNodeTransition { current }, None),
                Transition::Apply => {
                    let row = crate::status_storage::NodeDeclaredRow { node_id: sender.node_id.clone(), incarnation_id: incarnation.clone(), name: sender.name.clone(), state: *state };
                    (StatusReply::Applied, Some(Effect::Node(row, *state)))
                }
            }
        }
        StatusRequest::DeclareMeshState { mesh_id, state } => {
            if !me.is_fabric_primary {
                return (StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "fabric-primary".into() } }, None);
            }
            // The sender's Mesh id, as this holder knows it: the ids its own Build created, else the
            // id the Mesh's members carry in the view. The fabric seat moves to whichever admin has
            // the lowest node id, which may hold a Mesh it never created.
            let ids = (auth.mesh_ids)();
            let senders_mesh = ids.get(&sender.mesh).or_else(|| view.meshes.iter().find(|m| m.name == sender.mesh).and_then(|m| m.id.as_ref()));
            if !(sender.kind == NodeKind::NodeAdmin && sender.is_primary && senders_mesh == Some(mesh_id)) {
                return (StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: sender.name.to_string() } }, None);
            }
            apply_mesh(declared, mesh_id, *state)
        }
        // Downward exact-node operations reach the subject itself (`self_subject`); an authority
        // that is not the subject is not the receiver they name.
        StatusRequest::ApplyNodeState { .. } | StatusRequest::ProbeNodeState { .. } | StatusRequest::ApplyNodeConfig { .. } | StatusRequest::ApplyNodeName { .. } => {
            (StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "the subject node itself".into() } }, None)
        }
        // Commands and completions are answered before the decision (`StatusAuthority::apply`).
        StatusRequest::DrainNode { .. } | StatusRequest::StopNode { .. } | StatusRequest::StartNode { .. } | StatusRequest::NodeDrained { .. } | StatusRequest::NodeLeft { .. } | StatusRequest::LeaveMesh { .. } | StatusRequest::MeshLeave { .. } | StatusRequest::CommitState { .. } | StatusRequest::StateCommitted { .. } | StatusRequest::OpenTraffic { .. } | StatusRequest::TrafficOpened { .. } | StatusRequest::UpdateNodeConfig { .. } | StatusRequest::UpdateNode { .. } => {
            (StatusReply::NotReady { reason: format!("{} reached the declaration decision, which does not decide {}", auth.me, req.op()) }, None)
        }
        StatusRequest::ApplyMeshState { mesh_id, mesh_name, state } => {
            if !sender.is_fabric_primary {
                return (StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: sender.name.to_string() } }, None);
            }
            if me.mesh != *mesh_name {
                return (StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: format!("an admin of {mesh_name}") } }, None);
            }
            // Never a replacement mesh id to satisfy the operation.
            if let Some(held) = (auth.mesh_ids)().get(mesh_name) {
                if held != mesh_id {
                    return (StatusReply::RejectedStaleMesh { held: held.clone() }, None);
                }
            }
            apply_mesh(declared, mesh_id, *state)
        }
        StatusRequest::ApplyFabricEvent { fabric_id, event } => {
            if !sender.is_fabric_primary {
                return (StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: sender.name.to_string() } }, None);
            }
            if !me.is_primary {
                return (StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "mesh-primary".into() } }, None);
            }
            if *fabric_id != auth.fabric_id {
                return (StatusReply::RejectedStaleFabric { held: auth.fabric_id.clone() }, None);
            }
            let key = event_key(event);
            if declared.fabric.get(fabric_id).is_some_and(|events| events.contains(&key)) {
                return (StatusReply::AlreadyApplied, None);
            }
            (StatusReply::Applied, Some(Effect::Fabric(fabric_id.clone(), key)))
        }
    }
}

fn event_key(e: &FabricEvent) -> String {
    match e {
        FabricEvent::ReadyForTraffic => "ready-for-traffic".into(),
        FabricEvent::ShutdownInitiated { initiated_by } => format!("shutdown-initiated:{initiated_by}"),
    }
}

/// One commit of a Mesh status at its writer: decided over what is held, the keyed row put and
/// acknowledged, then held as applied and answered. A refused put answers `NotReady` naming the
/// store and holds nothing, so the same commit again is decided afresh; the commit decides again
/// over what is held after the put, so two commits of one natural key racing answer `Applied` once.
pub(crate) async fn commit_mesh_state(storage: &dyn crate::status_storage::StatusStorage, declared: &Mutex<Declared>, mesh_id: &MeshId, state: MeshState) -> StatusReply {
    let (decided, effect) = apply_mesh(&declared.lock().unwrap(), mesh_id, state);
    if effect.is_none() {
        return decided;
    }
    if let Err(e) = storage.put_mesh_status(&crate::status_storage::MeshStatusRow { mesh_id: mesh_id.clone(), state }).await {
        return StatusReply::NotReady { reason: format!("status.storage refused the Mesh status row {mesh_id}-{state:?}: {e}") };
    }
    let mut declared = declared.lock().unwrap();
    match apply_mesh(&declared, mesh_id, state) {
        (StatusReply::Applied, _) => {
            declared.meshes.insert(mesh_id.clone(), state);
            StatusReply::Applied
        }
        (other, _) => other,
    }
}

fn apply_mesh(declared: &Declared, mesh_id: &MeshId, state: MeshState) -> (StatusReply, Option<Effect>) {
    match transition(declared.meshes.get(mesh_id).copied(), state) {
        Transition::AlreadyApplied => (StatusReply::AlreadyApplied, None),
        Transition::Backward { current } => (StatusReply::RejectedInvalidMeshTransition { current }, None),
        Transition::Apply => (StatusReply::Applied, Some(Effect::Mesh(mesh_id.clone(), state))),
    }
}

impl StatusAuthority {
    /// The node a call's authenticated endpoint is, from the view this authority decides from: a
    /// birth the view holds, or `None` for a stranger (answered `SenderNotSubject`, "unknown
    /// peer"). A birth this admin admitted over `JoinNode` is in the view when its join is
    /// answered (`join::JoinDoor::known`), so it is never a stranger.
    pub async fn sender_of(&self, peer: &EndpointId) -> Option<crate::model::Node> {
        self.topology.read().await.births().find(|n| n.endpoint_id.as_ref() == Some(peer)).cloned()
    }

    /// The one door every declaration goes through, whoever the sender is: a peer over Node RPC,
    /// or this admin itself when its view names it as the authority. Decide over the view; an
    /// `Applied` node state is durable (the subject's row) before it is answered, and the live
    /// view shows it at once; one span per decision names op, sender, outcome and whether the
    /// receiver holds its mesh's seat (accepting a Pending hand-off is not an election).
    pub async fn apply(&self, sender: Option<crate::model::Node>, req: &StatusRequest) -> StatusReply {
        let view = self.topology.read().await.clone();
        if let (Some(from), Some(rounds)) = (sender.as_ref(), self.rounds.get()) {
            rounds.heard_from(&from.node_id);
        }
        match req {
            StatusRequest::NodeDrained { .. } => return self.accept_completion(sender.as_ref(), req),
            StatusRequest::NodeLeft { .. } => {
                return StatusReply::NotReady { reason: format!("{}: node-left is not a call: a stopped birth reports left on the reply of its stop-node call", self.me) };
            }
            StatusRequest::DrainNode { .. } | StatusRequest::StopNode { .. } | StatusRequest::StartNode { .. } => return self.serve_command(&view, sender.as_ref(), req).await,
            StatusRequest::LeaveMesh { .. } | StatusRequest::MeshLeave { .. } => {
                let Some(leaver) = self.leaver.get().cloned() else {
                    return StatusReply::NotReady { reason: format!("{} is still starting: it cannot serve {} yet", self.me, req.op()) };
                };
                let reply = if matches!(req, StatusRequest::LeaveMesh { .. }) { leaver.leave_mesh(sender.as_ref(), req).await } else { leaver.mesh_leave(sender.as_ref(), req).await };
                tracing::info_span!("rdm.node_admin.status.update.via-command", node = %self.me, op = req.op(), sender = %sender.as_ref().map(|n| n.name.to_string()).unwrap_or_default(), outcome = reply.name(), "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("a mesh-leave call naming this admin was decided"));
                return reply;
            }
            StatusRequest::CommitState { .. } | StatusRequest::StateCommitted { .. } | StatusRequest::OpenTraffic { .. } | StatusRequest::TrafficOpened { .. } => {
                return match self.rounds.get() {
                    Some(rounds) => rounds.serve(&view, sender.as_ref(), req, self.own.get()).await,
                    None => StatusReply::NotReady { reason: format!("{} has not yet built its rounds; {} is refused", self.me, req.op()) },
                };
            }
            StatusRequest::UpdateNode { .. } => {
                // A stopped node is told nothing: only a live birth's apply needs the node-RPC client.
                return crate::node_update::update(&self.me, &self.node_id, &self.topology, &*self.nodes_storage, self.configs.get().map(|c| c.client()), &self.row_moved, req).await;
            }
            StatusRequest::UpdateNodeConfig { .. } => {
                return match self.configs.get() {
                    Some(configs) => configs.update(&self.me, &self.node_id, &view, &*self.nodes_storage, req).await,
                    None => StatusReply::NotReady { reason: format!("{} has no node-RPC client yet; {} is refused", self.me, req.op()) },
                };
            }
            _ => {}
        }
        // A downward exact-node operation naming this admin itself (a probe, an apply) is
        // answered by the subject, through the same door.
        if let Some(me) = view.own(&self.node_id) {
            if let Some(reply) = self_subject(me, sender.as_ref(), req, &self.republish, &self.configure, &self.rename, &self.hydration, &self.wake).await {
                return reply;
            }
        }
        let receiver_is_primary = view.own(&self.node_id).is_some_and(|n| n.is_primary);
        let (reply, effect) = {
            let declared = self.declared.lock().unwrap();
            decide(self, &view, &declared, sender.as_ref(), req)
        };
        let reply = match effect {
            Some(effect) => self.persist_then_commit(effect).await,
            None => reply,
        };
        // A `ReadyForTraffic` Mesh declaration the authority took (applied now or already applied) is
        // the mesh primary's round-complete report: the primary sends it only when its checklist is
        // complete. Whichever it was, the report is what this admin received now from that exact birth.
        if let (StatusRequest::DeclareMeshState { state: MeshState::ReadyForTraffic, .. }, StatusReply::Applied | StatusReply::AlreadyApplied, Some(from)) = (req, &reply, sender.as_ref()) {
            if let Some(incarnation) = from.incarnation_id.clone() {
                self.declared.lock().unwrap().reports.insert(from.mesh.clone(), MeshReport { incarnation, at: std::time::Instant::now() });
            }
        }
        tracing::info_span!(
            "rdm.node_admin.status.update.via-declaration",
            node = %self.me,
            op = req.op(),
            sender = %sender.as_ref().map(|n| n.name.to_string()).unwrap_or_default(),
            outcome = reply.name(),
            detail = %detail(&reply),
            receiver_is_primary,
        )
        .in_scope(|| tracing::info!("declaration decided by the authority"));
        reply
    }
}

impl StatusAuthority {
    /// A start begins `birth`'s next cycle (node-start.md): what it declared before its stop
    /// (`Leaving`) is no longer where its next declarations are measured from, so `Pending` and
    /// `ReadyForTraffic` run forward again from the start. The cleared state is a row put before it
    /// is held, as every applied state is.
    pub async fn begin_cycle(&self, node: &crate::model::Node, birth: &IncarnationId) {
        let held = self.declared.lock().unwrap().node(&node.node_id, birth);
        let Some(state) = held else { return };
        let outcome = match self.status_storage.remove_node_declared(&node.node_id, birth).await {
            Ok(()) => {
                let mut declared = self.declared.lock().unwrap();
                declared.nodes.remove(&(node.node_id.clone(), birth.clone()));
                declared.names.remove(&(node.node_id.clone(), birth.clone()));
                "begun"
            }
            Err(_) => "refused",
        };
        tracing::info_span!("rdm.node_admin.status.update.via-cycle-begun", node = %node.name, node_id = %node.node_id, incarnation_id = %birth.0, held = ?state, outcome, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("a start begins the birth's next cycle: its declared state is measured from Pending again"));
    }

    /// The declared-state rows of every birth this admin holds that `book` says is superseded: the
    /// book holds a later incarnation of the node (a restart), holds the node departed
    /// (node.deleted), or holds the birth's path.name under another node_id (a recovery's new node,
    /// node-recovery.md). Each such birth's keys are tombstoned, blind, and then forgotten in memory.
    /// A birth the book holds at the same incarnation keeps its rows, however long it is silent, and
    /// so does a birth the book does not hold at all whose path no other node holds (nothing is
    /// proven of it).
    pub async fn retire_superseded(&self, book: &rafka_mesh_transport::membership::DigestBook) {
        // The births held and their paths, read under the lock and the lock released before any await.
        let held: Vec<((NodeId, IncarnationId), Option<crate::model::PathName>)> = {
            let declared = self.declared.lock().unwrap();
            declared.nodes.keys().map(|k| (k.clone(), declared.names.get(k).cloned())).collect()
        };
        // Who the book holds at each path.name now.
        let at_path: std::collections::HashMap<crate::model::PathName, NodeId> = book.all().into_iter().map(|d| (d.node.name, d.node.node_id)).collect();
        for ((node_id, incarnation), name) in held {
            let (why, by) = match book.get(node_id.as_str()) {
                Some((d, _)) if d.node.incarnation != incarnation => ("new-incarnation", d.node.incarnation.0.clone()),
                Some(_) => continue,
                None if book.is_departed(node_id.as_str()) => ("node-deleted", String::new()),
                None => match name.as_ref().and_then(|n| at_path.get(n)) {
                    Some(other) if *other != node_id => ("new-node", other.to_string()),
                    _ => continue,
                },
            };
            let (outcome, error) = match self.status_storage.remove_node_declared(&node_id, &incarnation).await {
                Ok(()) => {
                    let mut declared = self.declared.lock().unwrap();
                    declared.nodes.remove(&(node_id.clone(), incarnation.clone()));
                    declared.names.remove(&(node_id.clone(), incarnation.clone()));
                    ("retired", String::new())
                }
                Err(e) => ("refused", e.to_string()),
            };
            let span = match why {
                "new-incarnation" => tracing::info_span!("rdm.node_admin.status.remove.via-new-incarnation", node = %self.me, node_id = %node_id, incarnation_id = %incarnation.0, held_incarnation_id = %by, outcome, error = %error, "otel.kind" = "internal"),
                "new-node" => tracing::info_span!("rdm.node_admin.status.remove.via-new-node", node = %self.me, node_id = %node_id, incarnation_id = %incarnation.0, path = %name.as_ref().map(ToString::to_string).unwrap_or_default(), held_node_id = %by, outcome, error = %error, "otel.kind" = "internal"),
                _ => tracing::info_span!("rdm.node_admin.status.remove.via-node-deleted", node = %self.me, node_id = %node_id, incarnation_id = %incarnation.0, outcome, error = %error, "otel.kind" = "internal"),
            };
            span.in_scope(|| tracing::info!("a superseded birth's declared-state keys are tombstoned: nothing of that birth is folded back"));
        }
    }

    /// A birth's `node-drained` completion call at the admin that commanded it (node-left is the
    /// reply of `stop-node`, never a call): it resolves the open command it names, by `(build_id, attempt, operation, node_id,
    /// incarnation)`. The sender must be the subject. A completion with no open command is
    /// refused `NotReady` naming the mismatch, never counted toward another operation.
    fn accept_completion(&self, sender: Option<&crate::model::Node>, req: &StatusRequest) -> StatusReply {
        crate::node_commands::accept_completion(&self.commands, &self.me.to_string(), sender.map(|n| (&n.node_id, n.incarnation_id.as_ref(), n.name.to_string())).as_ref().map(|(a, b, c)| (*a, *b, c.as_str())), req)
    }

    /// `drain-node` / `stop-node` naming this admin: served by the subject itself, from another
    /// node-admin (its owning mesh-admin).
    async fn serve_command(&self, view: &Topology, sender: Option<&crate::model::Node>, req: &StatusRequest) -> StatusReply {
        let (StatusRequest::DrainNode { node_id, incarnation, .. } | StatusRequest::StopNode { node_id, incarnation, .. } | StatusRequest::StartNode { node_id, incarnation, .. }) = req else { unreachable!("serve_command is called for commands only") };
        let me = view.own(&self.node_id);
        let reply = match (me, sender, self.own.get()) {
            (Some(me), _, _) if me.node_id != *node_id => StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "the subject node itself".into() } },
            (Some(me), _, _) if me.incarnation_id.as_ref().is_some_and(|held| held != incarnation) => StatusReply::RejectedStaleIncarnation { held: me.incarnation_id.clone().expect("checked") },
            (_, Some(from), Some(own)) if from.kind == NodeKind::NodeAdmin && from.node_id != self.node_id => {
                let spare = from.endpoint_id.as_ref().and_then(|e| e.0.parse::<iroh::PublicKey>().ok());
                own.serve(&from.node_id, spare, req).await.expect("a command")
            }
            (_, Some(from), None) => StatusReply::NotReady { reason: format!("{} has not yet joined its mesh; {} from {} is refused", self.me, req.op(), from.name) },
            (_, other, _) => StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: other.map(|n| n.name.to_string()).unwrap_or_else(|| "unknown peer".into()) } },
        };
        tracing::info_span!("rdm.node_admin.status.update.via-command", node = %self.me, op = req.op(), sender = %sender.map(|n| n.name.to_string()).unwrap_or_default(), outcome = reply.name(), "otel.kind" = "internal")
            .in_scope(|| tracing::info!("a node command naming this admin was decided"));
        reply
    }

    /// An `Applied` decision: the fact's row is put and acknowledged, and only then is it held as
    /// applied and answered. A refused put answers `NotReady` naming the store and holds nothing,
    /// so the same declaration again is decided afresh. The commit decides again over what is
    /// held now, so two declarations of one natural key racing each other answer `Applied` once.
    async fn persist_then_commit(&self, effect: Effect) -> StatusReply {
        if let Effect::Mesh(mesh_id, state) = &effect {
            return commit_mesh_state(&*self.status_storage, &self.declared, mesh_id, *state).await;
        }
        let put = match &effect {
            Effect::Node(row, _) => self.status_storage.put_node_declared(row).await.map_err(|e| format!("status.storage refused the node declared-state row: {e}")),
            Effect::Mesh(..) => unreachable!("a Mesh status is committed by commit_mesh_state above"),
            Effect::Fabric(fabric_id, event) => {
                self.status_storage.put_fabric_event(&crate::status_storage::FabricEventRow { fabric_id: fabric_id.clone(), event: event.clone() }).await.map_err(|e| format!("status.storage refused the Fabric event row: {e}"))
            }
        };
        if let Err(reason) = put {
            return StatusReply::NotReady { reason };
        }
        let reply = {
            let mut declared = self.declared.lock().unwrap();
            match &effect {
                Effect::Node(row, state) => match transition(declared.node(&row.node_id, &row.incarnation_id), *state) {
                    Transition::Apply => {
                        declared.nodes.insert((row.node_id.clone(), row.incarnation_id.clone()), *state);
                        declared.names.insert((row.node_id.clone(), row.incarnation_id.clone()), row.name.clone());
                        StatusReply::Applied
                    }
                    Transition::AlreadyApplied => StatusReply::AlreadyApplied,
                    Transition::Backward { current } => StatusReply::RejectedInvalidNodeTransition { current },
                },
                Effect::Mesh(..) => unreachable!("a Mesh status is committed by commit_mesh_state above"),
                Effect::Fabric(fabric_id, event) => {
                    let events = declared.fabric.entry(fabric_id.clone()).or_default();
                    if events.contains(event) {
                        StatusReply::AlreadyApplied
                    } else {
                        events.push(event.clone());
                        StatusReply::Applied
                    }
                }
            }
        };
        if let (StatusReply::Applied, Effect::Node(row, _)) = (&reply, &effect) {
            let mut t = self.topology.write().await;
            if let Some(n) = t.nodes.iter_mut().find(|n| n.node_id == row.node_id && n.incarnation_id.as_ref() == Some(&row.incarnation_id)) {
                n.declared = Some(format!("{:?}", row.state));
            }
            drop(t);
            self.wake.poke();
        }
        reply
    }

    /// Initial fabric bootstrap: the Day-0 root has no upstream authority to hand it its mesh's
    /// Pending, so it applies it to itself (e4.s11 "single-admin recovery root"). Only Pending, only
    /// for its own mesh; the seat check does not apply because no seat exists yet. The same span
    /// as every decision, with the sender named as itself.
    pub(crate) async fn self_apply_mesh_pending(&self, mesh_id: &MeshId) -> StatusReply {
        let receiver_is_primary = self.topology.read().await.own(&self.node_id).is_some_and(|n| n.is_primary);
        let (decided, effect) = apply_mesh(&self.declared.lock().unwrap(), mesh_id, MeshState::Pending);
        let reply = match effect {
            Some(effect) => self.persist_then_commit(effect).await,
            None => decided,
        };
        tracing::info_span!(
            "rdm.node_admin.status.update.via-declaration",
            node = %self.me,
            op = "apply-mesh-state",
            sender = %self.me,
            outcome = reply.name(),
            detail = "self-applied: initial fabric bootstrap, no upstream authority",
            receiver_is_primary,
        )
        .in_scope(|| tracing::info!("the Day-0 root applies its own mesh's Pending"));
        reply
    }
}

/// Serve `Status` on this admin. `authority` is filled once the admin holds a view; until then a
/// declaration is `NotReady` by name.
/// A node's re-publish of its presence, filled once it has joined its mesh.
pub(crate) type Republish = Arc<OnceLock<Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>>>;

/// Where a node's own `hydrate_before_ready` stands: `None` until its start registered a hook.
pub type Hydrating = Arc<Mutex<Option<crate::app_hydration::HydrationHandle>>>;

/// This node holding the config it is told and publishing its digest carrying it (`ApplyNodeConfig`).
pub type Configure = Arc<OnceLock<Arc<dyn Fn(BTreeMap<String, String>) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>>>;

/// This node deciding a name its mesh-primary's row gives it (`ApplyNodeName`): the name it held
/// and the decision, the name taken and published only on `Applied`.
pub type Rename = Arc<OnceLock<Arc<dyn Fn(rafka_mesh_entity::PathName) -> std::pin::Pin<Box<dyn std::future::Future<Output = (rafka_mesh_entity::PathName, crate::node_name::NameApply)> + Send>> + Send + Sync>>>;

/// This mesh-primary's view taking a row `node.update` moved: `(from, node_id, incarnation, to)`.
pub type RowMoved = Arc<OnceLock<Arc<dyn Fn(rafka_mesh_entity::PathName, NodeId, rafka_mesh_entity::IncarnationId, rafka_mesh_entity::PathName) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>>>;

/// What a node is now, in the declaration vocabulary.
pub fn node_state_of(s: crate::model::NodeStatus) -> NodeState {
    use crate::model::NodeStatus as S;
    match s {
        S::Pending => NodeState::Pending,
        S::ReadyForTraffic => NodeState::ReadyForTraffic,
        S::Draining => NodeState::Draining,
        // A birth under restart is leaving; the frozen status vocabulary has no Restarting.
        S::Leaving | S::Restarting | S::PendingReconnect | S::Dead | S::Deleted => NodeState::Leaving,
    }
}


/// The downward exact-node operations, answered by the subject itself. `Some(reply)` when `req`
/// names this node and comes from a fabric member other than itself (a probe) or from a node-admin
/// other than itself (an apply); `None` for every other request.
///
/// The tickle: ask the exact birth to reassert itself. Protocol name: `ProbeNodeState`. The node
/// re-publishes its presence and answers its current state; no transition. While its
/// `hydrate_before_ready` hook is Blocked it answers `Blocked` with the hook's blocker and
/// re-publishes nothing. `ApplyNodeState` is not served: a drain is `drain-node` and a stop is `stop-node`.
async fn self_subject(me: &crate::model::Node, sender: Option<&crate::model::Node>, req: &StatusRequest, republish: &Republish, configure: &Configure, rename: &Rename, hydration: &Hydrating, wake: &crate::status_declare::DeclareWake) -> Option<StatusReply> {
    let (node_id, incarnation) = match req {
        StatusRequest::ProbeNodeState { node_id, incarnation } | StatusRequest::ApplyNodeState { node_id, incarnation, .. } | StatusRequest::ApplyNodeConfig { node_id, incarnation, .. } | StatusRequest::ApplyNodeName { node_id, incarnation, .. } => (node_id, incarnation),
        _ => return None,
    };
    // A probe changes nothing: it asks this node to say it is here, so any fabric member other than
    // the subject may ask, and a node-admin's probe from another mesh arrives through a carrier (a
    // forward carries no origin identity: the target authenticates the carrier, node-rpc.md 36.1).
    // An apply changes the node: only a node-admin may ask (the fabric-primary's config reaches a
    // node of another mesh through that mesh's primary, which is the sender this node authenticates).
    let sender_may = |s: &crate::model::Node| s.node_id != me.node_id && (matches!(req, StatusRequest::ProbeNodeState { .. }) || s.kind == NodeKind::NodeAdmin);
    if me.node_id != *node_id || !sender.is_some_and(sender_may) {
        return None;
    }
    let Some(held) = me.incarnation_id.as_ref() else {
        return Some(StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown });
    };
    if held != incarnation {
        return Some(StatusReply::RejectedStaleIncarnation { held: held.clone() });
    }
    let sender_name = sender.map(|n| n.name.to_string()).unwrap_or_default();
    match req {
        StatusRequest::ProbeNodeState { .. } => {
            // The hook's blocker as it stood when the probe arrived, before the probe addresses
            // this admin (a hook blocked on its authority's word runs again on it).
            let blocker = hydration.lock().unwrap().as_ref().and_then(|h| h.state().probe_blocker());
            // The authority addressed this admin (R-S3's down op): every declaration owed to it is
            // sent again, whatever its last outcome.
            if let Some(from) = sender {
                wake.addressed_by(&from.node_id);
            }
            // A parked admin presents nothing: it says it is parked.
            if crate::node_self::parking().is_parked() {
                tracing::info_span!("rdm.node_admin.status.update.via-probe", node = %me.name, sender = %sender_name, state = "parked", "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("probed by a node-admin: parked, nothing re-published"));
                return Some(StatusReply::CurrentParked { node_id: me.node_id.clone(), incarnation: held.clone() });
            }
            // node-start.md step 3: while its hook is Blocked, the probe served this exact birth and
            // found an active readiness blocker; it answers Blocked with the hook's own reason and
            // retry event, before any republish, so a polled blocked birth republishes nothing.
            if let Some((reason, retry_on)) = blocker {
                let on = retry_on.name();
                let reply = StatusReply::blocked(me.node_id.clone(), held.clone(), &reason, retry_on);
                tracing::info_span!("rdm.node_admin.status.update.via-probe", node = %me.name, sender = %sender_name, state = "blocked", blocker = %reason, retry_on = on, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("probed by a node-admin: the hook's blocker answered Blocked, nothing re-published"));
                return Some(reply);
            }
            if let Some(republish) = republish.get() {
                republish().await;
            }
            let state = node_state_of(me.status);
            tracing::info_span!("rdm.node_admin.status.update.via-probe", node = %me.name, sender = %sender_name, state = ?state, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("probed by a node-admin: presence re-published, current state answered"));
            Some(StatusReply::Current { node_id: me.node_id.clone(), incarnation: held.clone(), state })
        }
        StatusRequest::ApplyNodeState { state, .. } => {
            // Not a drain command and not a stop: the only drain path is drain-node. The reply
            // carries the state this node holds; the span names why.
            let current = node_state_of(me.status);
            tracing::info_span!("rdm.node_admin.status.reject.via-apply-node-state", node = %me.name, sender = %sender_name, requested = ?state, current = ?current, reason = "drain-node is the only drain path and stop-node the only stop; apply-node-state is served for no state", "otel.kind" = "internal")
                .in_scope(|| tracing::info!("apply-node-state refused"));
            Some(StatusReply::RejectedInvalidNodeTransition { current })
        }
        StatusRequest::ApplyNodeConfig { config, .. } => {
            // ops-naming "Node data": the node applies the config and carries it in its next digest.
            let Some(configure) = configure.get() else {
                return Some(StatusReply::NotReady { reason: format!("{}: its digest does not exist yet; the config is not held", me.name) });
            };
            configure(config.clone()).await;
            tracing::info_span!("rdm.node_admin.node.update.via-apply-node-config", node = %me.name, sender = %sender_name, keys = config.len(), outcome = "applied", "otel.kind" = "internal")
                .in_scope(|| tracing::info!("the fabric-primary's config is held and rides this admin's digest"));
            Some(StatusReply::Applied)
        }
        StatusRequest::ApplyNodeName { name, .. } => {
            // R12: the node owns its name and takes the row's name only with no traffic in flight.
            let asked = match name.parse::<rafka_mesh_entity::PathName>() {
                Ok(n) => n,
                Err(e) => {
                    tracing::info_span!("rdm.node_admin.node.reject.via-apply-node-name", node = %me.name, sender = %sender_name, to = %name, reason = %format!("the name {name:?} is not a path.name: {e}"), "otel.kind" = "internal")
                        .in_scope(|| tracing::info!("apply-node-name refused: the name does not parse"));
                    return Some(StatusReply::Malformed { kind: rafka_node_rpc_contract::outcome::MalformedKind::Corrupt });
                }
            };
            let Some(rename) = rename.get() else {
                return Some(StatusReply::NotReady { reason: format!("{}: its digest does not exist yet; the name {asked} is not taken", me.name) });
            };
            let (held, decision) = rename(asked.clone()).await;
            let in_flight = match decision {
                crate::node_name::NameApply::Busy { in_flight } => in_flight,
                _ => 0,
            };
            tracing::info_span!("rdm.node_admin.node.update.via-apply-node-name", node = %held, sender = %sender_name, from = %held, to = %asked, in_flight, outcome = decision.name(), "otel.kind" = "internal")
                .in_scope(|| tracing::info!("the mesh-primary's name for this admin was decided"));
            Some(crate::node_name::reply(&held, &asked, &decision))
        }
        _ => None,
    }
}

/// Serve `Status` on this admin. `authority` is filled once the admin holds a view; until then a
/// declaration is `NotReady` by name.
pub fn serve(b: ServerBuilder, authority: Arc<OnceLock<Arc<StatusAuthority>>>) -> ServerBuilder {
    b.serve::<Status, _, _>(OpOwner::Product("rdm".into()), move |peer: PeerContext, req: StatusRequest| {
        let authority = authority.clone();
        async move {
            let Some(auth) = authority.get().cloned() else {
                return Ok(StatusReply::NotReady { reason: "this admin holds no view yet".into() });
            };
            let peer_id = EndpointId(peer.endpoint_id.to_string());
            let sender = auth.sender_of(&peer_id).await;
            let reply = auth.apply(sender, &req).await;
            if auth.hold_next_reply.swap(false, std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
            Ok(reply)
        }
    })
}

fn detail(r: &StatusReply) -> String {
    match r {
        StatusReply::RejectedNotAuthority { why } => why.as_str().to_string(),
        StatusReply::RejectedStaleIncarnation { held } => held.to_string(),
        StatusReply::RejectedStaleMesh { held } => held.to_string(),
        StatusReply::RejectedStaleFabric { held } => held.to_string(),
        StatusReply::RejectedInvalidNodeTransition { current } => format!("{current:?}"),
        StatusReply::RejectedInvalidMeshTransition { current } => format!("{current:?}"),
        _ => String::new(),
    }
}

/// Shared handles, for the admin's wiring.
pub type AuthoritySlot = Arc<OnceLock<Arc<StatusAuthority>>>;

#[allow(dead_code)]
fn _assert_send_sync() {
    fn f<T: Send + Sync>() {}
    f::<StatusAuthority>();
    let _ = RwLock::new(());
}

#[cfg(test)]
mod tests {
    use super::*;
    use rafka_node_rpc_contract::status::StatusReply as R;

    fn node(name: &str) -> crate::model::Node {
        let mut n = crate::model::Node::allocated(name.parse().unwrap());
        n.incarnation_id = Some(IncarnationId::mint());
        n
    }

    /// The silence status the mesh primary stored for `node` at `incarnation`, as a reader folds it.
    async fn stored_silence(status: &dyn crate::status_storage::StatusStorage, node: &crate::model::Node) -> Option<crate::model::NodeStatus> {
        status.node_silences().await.unwrap().into_iter().find(|c| c.node_id == node.node_id && Some(&c.incarnation_id) == node.incarnation_id.as_ref()).and_then(|c| c.status)
    }

    /// CONTRACT (#3051, CLAUDE.md read-modify-write ban): a node's declared state and the silence
    /// status its mesh primary holds are two facts, each its own keyed row with its own writer. A
    /// declaration the authority applies, and a start beginning the node's next cycle, put only the
    /// declared fact: neither erases the `PendingReconnect` the mesh primary put for the same birth.
    #[tokio::test]
    async fn a_declaration_and_a_cycle_begin_leave_the_mesh_primarys_silence_status_standing() {
        let mut admin = node("mesh1.admin.1");
        admin.mesh = "mesh1".into();
        admin.kind = NodeKind::NodeAdmin;
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let mut rpc = node("mesh1.rpc.1");
        rpc.mesh = "mesh1".into();
        rpc.kind = NodeKind::RpcNode;
        rpc.endpoint_id = Some(EndpointId("rpc-endpoint".into()));
        rpc.transport_addr = Some("127.0.0.1:1".parse().unwrap());
        let topology = Arc::new(tokio::sync::RwLock::new(Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::ReadyForTraffic, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![admin.clone(), rpc.clone()],
        }));
        let status: Arc<crate::status_storage::MemoryStatusStorage> = Arc::new(Default::default());
        let authority = StatusAuthority {
            me: admin.name.clone(),
            node_id: admin.node_id.clone(),
            fabric_id: topology.read().await.fabric.id.clone(),
            topology: topology.clone(),
            declared: Arc::new(Mutex::new(Declared::default())),
            status_storage: status.clone(),
            mesh_ids: Arc::new(BTreeMap::new),
            republish: Arc::new(OnceLock::new()),
            commands: Arc::new(crate::node_commands::CommandBook::default()),
            own: Arc::new(OnceLock::new()),
            leaver: Arc::new(OnceLock::new()),
            hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wake: Default::default(),
            rounds: Default::default(),
            nodes_storage: Arc::new(crate::storage::MemoryNodesStorage::default()),
            configs: Default::default(),
            configure: Default::default(),
            hydration: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
        };
        crate::offline::put_silence_status(&*status, &rpc, Some(crate::model::NodeStatus::PendingReconnect)).await.unwrap();
        let declare = StatusRequest::DeclareNodeState { node_id: rpc.node_id.clone(), incarnation: rpc.incarnation_id.clone().unwrap(), state: NodeState::ReadyForTraffic };
        assert_eq!(authority.apply(Some(rpc.clone()), &declare).await, R::Applied);
        assert_eq!(stored_silence(&*status, &rpc).await, Some(crate::model::NodeStatus::PendingReconnect), "the authority's declared-state put erased the mesh primary's silence status");
        authority.begin_cycle(&rpc, rpc.incarnation_id.as_ref().unwrap()).await;
        assert_eq!(stored_silence(&*status, &rpc).await, Some(crate::model::NodeStatus::PendingReconnect), "a cycle begin erased the mesh primary's silence status");
    }

    /// A status store whose `Leaving` declared-state put returns only once the `ReadyForTraffic`
    /// put for the same birth has landed: two declarations decided against the same held state,
    /// their puts landing Leaving then ReadyForTraffic.
    #[derive(Default)]
    struct RacingPuts {
        inner: crate::status_storage::MemoryStatusStorage,
        ready_landed: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl crate::status_storage::StatusStorage for RacingPuts {
        async fn put_mesh_status(&self, row: &crate::status_storage::MeshStatusRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_mesh_status(row).await
        }
        async fn mesh_statuses(&self) -> Result<std::collections::HashMap<MeshId, MeshState>, crate::record_store::StorageError> {
            self.inner.mesh_statuses().await
        }
        async fn put_fabric_event(&self, row: &crate::status_storage::FabricEventRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_fabric_event(row).await
        }
        async fn fabric_events(&self) -> Result<Vec<crate::status_storage::FabricEventRow>, crate::record_store::StorageError> {
            self.inner.fabric_events().await
        }
        async fn put_node_declared(&self, row: &crate::status_storage::NodeDeclaredRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_declared(row).await?;
            match row.state {
                NodeState::Leaving => self.ready_landed.notified().await,
                NodeState::ReadyForTraffic => self.ready_landed.notify_one(),
                _ => {}
            }
            Ok(())
        }
        async fn remove_node_declared(&self, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<(), crate::record_store::StorageError> {
            self.inner.remove_node_declared(node_id, incarnation_id).await
        }
        async fn node_declared(&self) -> Result<Vec<crate::status_storage::NodeDeclaredRow>, crate::record_store::StorageError> {
            self.inner.node_declared().await
        }
        async fn put_node_silence(&self, row: &crate::status_storage::NodeSilenceRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_silence(row).await
        }
        async fn node_silences(&self) -> Result<Vec<crate::status_storage::NodeSilenceRow>, crate::record_store::StorageError> {
            self.inner.node_silences().await
        }
        async fn put_node_exit(&self, row: &crate::status_storage::NodeExitRow) -> Result<(), crate::record_store::StorageError> {
            self.inner.put_node_exit(row).await
        }
        async fn node_exit(&self, node_id: &NodeId, incarnation_id: &IncarnationId) -> Result<Option<crate::status_storage::NodeExitRow>, crate::record_store::StorageError> {
            self.inner.node_exit(node_id, incarnation_id).await
        }
    }

    /// CONTRACT (#3051, CLAUDE.md read-modify-write ban): two declarations of one birth decided
    /// against the same held state both put their row before either is committed. Whatever order
    /// the puts land in, what a restarted authority folds from the rows is the state it held in
    /// memory: a node's declared state never goes backward across a restart.
    #[tokio::test]
    async fn two_racing_declarations_rehydrate_to_the_state_held_and_never_backward() {
        let mut admin = node("mesh1.admin.1");
        admin.mesh = "mesh1".into();
        admin.kind = NodeKind::NodeAdmin;
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let mut rpc = node("mesh1.rpc.1");
        rpc.mesh = "mesh1".into();
        rpc.kind = NodeKind::RpcNode;
        let topology = Arc::new(tokio::sync::RwLock::new(Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::ReadyForTraffic, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![admin.clone(), rpc.clone()],
        }));
        let status: Arc<RacingPuts> = Arc::new(Default::default());
        let authority = StatusAuthority {
            me: admin.name.clone(),
            node_id: admin.node_id.clone(),
            fabric_id: topology.read().await.fabric.id.clone(),
            topology: topology.clone(),
            declared: Arc::new(Mutex::new(Declared::default())),
            status_storage: status.clone(),
            mesh_ids: Arc::new(BTreeMap::new),
            republish: Arc::new(OnceLock::new()),
            commands: Arc::new(crate::node_commands::CommandBook::default()),
            own: Arc::new(OnceLock::new()),
            leaver: Arc::new(OnceLock::new()),
            hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wake: Default::default(),
            rounds: Default::default(),
            nodes_storage: Arc::new(crate::storage::MemoryNodesStorage::default()),
            configs: Default::default(),
            configure: Default::default(),
            hydration: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
        };
        let birth = rpc.incarnation_id.clone().unwrap();
        let declare = |state| StatusRequest::DeclareNodeState { node_id: rpc.node_id.clone(), incarnation: birth.clone(), state };
        let (leaving, ready) = (declare(NodeState::Leaving), declare(NodeState::ReadyForTraffic));
        let (a, b) = tokio::join!(authority.apply(Some(rpc.clone()), &leaving), authority.apply(Some(rpc.clone()), &ready));
        assert_eq!((a, b), (R::Applied, R::Applied), "both decided forward from the same held state");
        let held = authority.declared.lock().unwrap().node(&rpc.node_id, &birth);
        assert_eq!(held, Some(NodeState::Leaving), "memory holds the greater state");
        let rehydrated = Declared::rehydrate(&*status).await.unwrap();
        assert_eq!(rehydrated.node(&rpc.node_id, &birth), held, "a restarted authority folds the rows back to a state behind the one it held");
    }

    /// CONTRACT (#2803 detection): a fabric primary probes ANOTHER mesh's node-admin through a
    /// member of that mesh (Forward carries no origin identity, so the node-admin authenticates the
    /// member). The probe is answered with the node-admin's current state; the same sender cannot
    /// apply a state to it, and the node-admin itself is never its own sender.
    #[tokio::test]
    async fn a_probe_carried_by_a_member_is_answered_and_an_apply_from_it_is_not() {
        let admin = node("mesh2.admin.1");
        let carrier = node("mesh2.rpc.1");
        let (node_id, incarnation) = (admin.node_id.clone(), admin.incarnation_id.clone().unwrap());
        let republish = Republish::default();
        let probe = StatusRequest::ProbeNodeState { node_id: node_id.clone(), incarnation: incarnation.clone() };
        let answered = self_subject(&admin, Some(&carrier), &probe, &republish, &Configure::default(), &Rename::default(), &Hydrating::default(), &crate::status_declare::DeclareWake::default()).await;
        assert!(matches!(answered, Some(R::Current { .. })), "{answered:?}");
        let apply = StatusRequest::ApplyNodeState { node_id, incarnation, state: NodeState::Draining };
        assert!(self_subject(&admin, Some(&carrier), &apply, &republish, &Configure::default(), &Rename::default(), &Hydrating::default(), &crate::status_declare::DeclareWake::default()).await.is_none(), "an apply from a member is not the subject's to answer");
        assert!(self_subject(&admin, Some(&admin), &probe, &republish, &Configure::default(), &Rename::default(), &Hydrating::default(), &crate::status_declare::DeclareWake::default()).await.is_none(), "a node-admin is not its own sender");
        assert!(self_subject(&admin, None, &probe, &republish, &Configure::default(), &Rename::default(), &Hydrating::default(), &crate::status_declare::DeclareWake::default()).await.is_none(), "an unresolved peer is answered by no one");
    }

    /// The status authority of the admin born as `me`, deciding over `view`.
    fn authority_of(me: &crate::model::Node, view: Topology) -> StatusAuthority {
        StatusAuthority {
            me: me.name.clone(),
            node_id: me.node_id.clone(),
            fabric_id: view.fabric.id.clone(),
            topology: Arc::new(tokio::sync::RwLock::new(view)),
            declared: Default::default(),
            nodes_storage: Arc::new(crate::storage::MemoryNodesStorage::default()),
            status_storage: Arc::new(crate::status_storage::MemoryStatusStorage::default()),
            mesh_ids: Arc::new(BTreeMap::new),
            republish: Default::default(),
            commands: Default::default(),
            own: Default::default(),
            leaver: Default::default(),
            hold_next_reply: Default::default(),
            wake: Default::default(),
            rounds: Default::default(),
            configs: Default::default(),
            configure: Default::default(),
            hydration: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
        }
    }

    /// CONTRACT (R-RC1, node-replace.md "Successor | New node.id"): a replace or a recovery keeps the
    /// path.name and mints a new node id. The view holds one row per path.name (`admin::project_at`
    /// keys it by `PathName`), so until the retired birth's digest goes non-live or departs, the row
    /// at this admin's path can be the retired birth's, live and seated, and this admin's own row is
    /// absent. The authority finds itself by the node id it was born with: it decides nothing from
    /// the retired birth's seat and refuses naming the true reason, "a view holding this admin"; a
    /// drain-node naming its node id goes on to the subject's own door (`NodeSelf::serve`, which
    /// checks the exact birth itself), here not yet filled. What must NOT happen: a member's
    /// declaration `Applied` on the retired birth's mesh-primary seat, or a drain-node naming this
    /// admin refused as "not the subject node itself".
    #[tokio::test]
    async fn an_authority_whose_view_holds_its_retired_birth_at_its_path_answers_as_itself() {
        let mut retired = node("mesh1.admin.1");
        retired.kind = NodeKind::NodeAdmin;
        retired.mesh = "mesh1".into();
        retired.status = crate::model::NodeStatus::Draining;
        retired.is_primary = true;
        retired.is_fabric_primary = true;
        let mut reborn = node("mesh1.admin.1");
        reborn.kind = NodeKind::NodeAdmin;
        reborn.mesh = "mesh1".into();
        let mut member = node("mesh1.rpc.1");
        member.kind = NodeKind::RpcNode;
        member.mesh = "mesh1".into();
        member.status = crate::model::NodeStatus::ReadyForTraffic;
        let mut owner = node("mesh1.admin.2");
        owner.kind = NodeKind::NodeAdmin;
        owner.mesh = "mesh1".into();
        owner.status = crate::model::NodeStatus::ReadyForTraffic;
        let view = Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::ReadyForTraffic, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![retired.clone(), member.clone(), owner.clone()],
        };
        assert_ne!(retired.node_id, reborn.node_id, "a replace mints a new node id");
        assert_eq!(view.cohort_primary("mesh1", NodeKind::NodeAdmin).map(|n| n.node_id.clone()), Some(retired.node_id.clone()), "the retired birth holds mesh1's seat in this view");
        let auth = authority_of(&reborn, view.clone());
        let holding_me = |r: &StatusReply| matches!(r, R::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed } } if needed == "a view holding this admin");

        let declare = StatusRequest::DeclareNodeState { node_id: member.node_id.clone(), incarnation: member.incarnation_id.clone().unwrap(), state: NodeState::ReadyForTraffic };
        let (reply, effect) = decide(&auth, &view, &Declared::default(), Some(&member), &declare);
        assert!(holding_me(&reply) && effect.is_none(), "the retired birth's seat is not this admin's: {reply:?}");

        let drain = StatusRequest::DrainNode { node_id: reborn.node_id.clone(), incarnation: reborn.incarnation_id.clone().unwrap(), build_id: "bld_1".into(), attempt: 1, operation: "drain-node:mesh1.admin.1".into() };
        let reply = auth.apply(Some(owner.clone()), &drain).await;
        assert!(matches!(&reply, R::NotReady { reason } if reason.contains("has not yet joined its mesh")), "a drain-node naming this admin's node id reaches the subject's own door: {reply:?}");
    }

    fn digest_of(n: &crate::model::Node, incarnation: IncarnationId) -> rafka_mesh_entity::MeshDigest {
        rafka_mesh_entity::MeshDigest {
            fabric_id: FabricId::mint(),
            node: rafka_mesh_entity::MeshNode {
                node_id: n.node_id.clone(),
                name: n.name.clone(),
                endpoint_id: EndpointId(format!("key-{}", n.name)),
                transport_addr: "127.0.0.1:41000".parse().unwrap(),
                incarnation,
                supersedes: None,
                runtime: None,
            },
            status: rafka_mesh_entity::MemberStatus::ReadyForTraffic,
            admin_api_base: None,
            emitted_at_rafka_ms: 0,
            digest_seq: 1,
            mesh_id: None,
            in_flight: None,
            config: BTreeMap::new(),
            load: None,
            gossip: None,
            data_dir: None,
        }
    }

    /// CONTRACT (#3051, node-recovery.md "Steps": node.delete of the exited instance, then node.create
    /// with a new node.id; node-restart: a new incarnation of the same node.id): a birth's declared
    /// state is a fact about that birth only. Once the book holds a later incarnation of the node, or
    /// holds the node departed (node.deleted), the authority tombstones the old birth's declared-state
    /// keys, blind, and forgets them; a birth the book still holds at the same incarnation (silent or
    /// not) keeps its rows. Must NOT happen: an old birth's rows on disk forever, folded back on every
    /// restart of the authority.
    #[tokio::test]
    async fn a_superseded_or_deleted_birth_leaves_no_declared_state_row_behind() {
        let mut admin = node("mesh1.admin.1");
        admin.mesh = "mesh1".into();
        admin.kind = NodeKind::NodeAdmin;
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let member = |name: &str| {
            let mut n = node(name);
            n.mesh = "mesh1".into();
            n.kind = NodeKind::RpcNode;
            n.endpoint_id = Some(EndpointId(format!("{name}-endpoint")));
            n.transport_addr = Some("127.0.0.1:1".parse().unwrap());
            n
        };
        let (restarted, deleted, standing) = (member("mesh1.rpc.1"), member("mesh1.rpc.2"), member("mesh1.rpc.3"));
        let topology = Arc::new(tokio::sync::RwLock::new(Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::ReadyForTraffic, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![admin.clone(), restarted.clone(), deleted.clone(), standing.clone()],
        }));
        let status: Arc<crate::status_storage::MemoryStatusStorage> = Arc::new(Default::default());
        let authority = StatusAuthority {
            me: admin.name.clone(),
            node_id: admin.node_id.clone(),
            fabric_id: topology.read().await.fabric.id.clone(),
            topology: topology.clone(),
            declared: Arc::new(Mutex::new(Declared::default())),
            status_storage: status.clone(),
            mesh_ids: Arc::new(BTreeMap::new),
            republish: Arc::new(OnceLock::new()),
            commands: Arc::new(crate::node_commands::CommandBook::default()),
            own: Arc::new(OnceLock::new()),
            leaver: Arc::new(OnceLock::new()),
            hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wake: Default::default(),
            rounds: Default::default(),
            nodes_storage: Arc::new(crate::storage::MemoryNodesStorage::default()),
            configs: Default::default(),
            configure: Default::default(),
            hydration: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
        };
        for n in [&restarted, &deleted, &standing] {
            let declare = StatusRequest::DeclareNodeState { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap(), state: NodeState::ReadyForTraffic };
            assert_eq!(authority.apply(Some(n.clone()), &declare).await, R::Applied, "{}", n.name);
        }
        let book = rafka_mesh_transport::membership::DigestBook::default();
        // rpc.1 restarted: the book holds its next incarnation.
        book.record(digest_of(&restarted, IncarnationId::mint()));
        // rpc.2 deleted: its exact birth departed.
        book.record(digest_of(&deleted, deleted.incarnation_id.clone().unwrap()));
        book.depart(rafka_mesh_entity::LifecycleOp {
            build_id: "build1".into(),
            attempt: 1,
            operation: "recover-node:mesh1.rpc.2".into(),
            node_id: deleted.node_id.clone(),
            incarnation: deleted.incarnation_id.clone().unwrap(),
            name: deleted.name.clone(),
            event_at_rafka_ms: 0,
        });
        // rpc.3 is held at the birth that declared (silent or not, it is not superseded).
        book.record(digest_of(&standing, standing.incarnation_id.clone().unwrap()));
        let capture = crate::span_capture::Capture::new("i143-3051-superseded-rows");
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            authority.retire_superseded(&book).await;
        }
        let by_incarnation = capture.named("rdm.node_admin.status.remove.via-new-incarnation");
        let by_deletion = capture.named("rdm.node_admin.status.remove.via-node-deleted");
        for s in by_incarnation.iter().chain(&by_deletion) {
            eprintln!("span {} trace_id={} {:?}", s.name, s.trace_id, s.attributes);
        }
        assert_eq!(by_incarnation.iter().map(|s| (s.attributes["node_id"].clone(), s.attributes["outcome"].clone())).collect::<Vec<_>>(), vec![(restarted.node_id.to_string(), "retired".to_string())], "the restarted node's old birth is retired for its new incarnation");
        assert_eq!(by_deletion.iter().map(|s| (s.attributes["node_id"].clone(), s.attributes["outcome"].clone())).collect::<Vec<_>>(), vec![(deleted.node_id.to_string(), "retired".to_string())], "the deleted node's birth is retired for its deletion");
        use crate::status_storage::StatusStorage as _;
        let rows: Vec<String> = status.node_declared().await.unwrap().into_iter().map(|r| r.node_id.to_string()).collect();
        assert_eq!(rows, vec![standing.node_id.to_string()], "only the birth the book still holds keeps its declared-state row");
        let held = authority.declared.lock().unwrap().clone();
        assert_eq!(held.node(&restarted.node_id, restarted.incarnation_id.as_ref().unwrap()), None, "the restarted node's old birth is forgotten");
        assert_eq!(held.node(&deleted.node_id, deleted.incarnation_id.as_ref().unwrap()), None, "the deleted birth is forgotten");
        assert_eq!(held.node(&standing.node_id, standing.incarnation_id.as_ref().unwrap()), Some(NodeState::ReadyForTraffic), "the held birth keeps its state");
        assert_eq!(Declared::rehydrate(&*status).await.unwrap().nodes.len(), 1, "a restarted authority folds only the held birth back");
    }

    /// CONTRACT (node-recovery.md "Steps": "node.create at the same path.name", "New: node.id"): a
    /// recovered birth is superseded by a NEW node at its path.name. Once the book holds that path
    /// under another node_id, the exited birth's declared-state rows are tombstoned (via-new-node),
    /// whether or not the book still holds the exited birth's departure (that is held only
    /// DEPARTED_RETENTION; an admin that rehydrates later holds none).
    /// Must NOT happen: the exited birth's rows folded back for ever because its node_id is gone.
    #[tokio::test]
    async fn a_birth_whose_path_the_book_holds_under_a_new_node_leaves_no_declared_state_row_behind() {
        let mut admin = node("mesh1.admin.1");
        admin.mesh = "mesh1".into();
        admin.kind = NodeKind::NodeAdmin;
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        let mut exited = node("mesh1.rpc.1");
        exited.mesh = "mesh1".into();
        exited.kind = NodeKind::RpcNode;
        exited.endpoint_id = Some(EndpointId("mesh1.rpc.1-endpoint".into()));
        exited.transport_addr = Some("127.0.0.1:1".parse().unwrap());
        let topology = Arc::new(tokio::sync::RwLock::new(Topology {
            fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::ReadyForTraffic, provider: crate::model::ProviderKind::Process },
            meshes: Vec::new(),
            nodes: vec![admin.clone(), exited.clone()],
        }));
        let status: Arc<crate::status_storage::MemoryStatusStorage> = Arc::new(Default::default());
        let authority = StatusAuthority {
            me: admin.name.clone(),
            node_id: admin.node_id.clone(),
            fabric_id: topology.read().await.fabric.id.clone(),
            topology: topology.clone(),
            declared: Arc::new(Mutex::new(Declared::default())),
            status_storage: status.clone(),
            mesh_ids: Arc::new(BTreeMap::new),
            republish: Arc::new(OnceLock::new()),
            commands: Arc::new(crate::node_commands::CommandBook::default()),
            own: Arc::new(OnceLock::new()),
            leaver: Arc::new(OnceLock::new()),
            hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            wake: Default::default(),
            rounds: Default::default(),
            nodes_storage: Arc::new(crate::storage::MemoryNodesStorage::default()),
            configs: Default::default(),
            configure: Default::default(),
            hydration: Default::default(),
            rename: Default::default(),
            row_moved: Default::default(),
        };
        let declare = StatusRequest::DeclareNodeState { node_id: exited.node_id.clone(), incarnation: exited.incarnation_id.clone().unwrap(), state: NodeState::ReadyForTraffic };
        assert_eq!(authority.apply(Some(exited.clone()), &declare).await, R::Applied);
        // The recovery created a new node at the path; the exited birth's departure is no longer held.
        let mut recovered = exited.clone();
        recovered.node_id = NodeId::mint();
        recovered.incarnation_id = Some(IncarnationId::mint());
        let book = rafka_mesh_transport::membership::DigestBook::default();
        book.record(digest_of(&recovered, recovered.incarnation_id.clone().unwrap()));
        let capture = crate::span_capture::Capture::new("i143-3051-recovered-rows");
        {
            let _g = tracing::dispatcher::set_default(&capture.dispatch);
            authority.retire_superseded(&book).await;
        }
        let by_new_node = capture.named("rdm.node_admin.status.remove.via-new-node");
        for s in &by_new_node {
            eprintln!("span {} trace_id={} {:?}", s.name, s.trace_id, s.attributes);
        }
        assert_eq!(
            by_new_node.iter().map(|s| (s.attributes["node_id"].clone(), s.attributes["held_node_id"].clone(), s.attributes["outcome"].clone())).collect::<Vec<_>>(),
            vec![(exited.node_id.to_string(), recovered.node_id.to_string(), "retired".to_string())],
            "the exited birth is retired for the new node at its path"
        );
        use crate::status_storage::StatusStorage as _;
        assert!(status.node_declared().await.unwrap().is_empty(), "no declared-state row of the exited birth is left");
        assert_eq!(authority.declared.lock().unwrap().node(&exited.node_id, exited.incarnation_id.as_ref().unwrap()), None, "the exited birth is forgotten");
        assert!(Declared::rehydrate(&*status).await.unwrap().nodes.is_empty(), "a restarted authority folds nothing of it back");
    }
}
