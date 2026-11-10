//! The node commands the primary above a node runs for a caller (node-stop.md, node-start.md,
//! node-connections-delete.md; Q342, D40). A caller sends NodeCommand 0x22
//! `StopNode`, `StartNode` or `DeleteConnections { scope }` to the node's mesh-primary (the fabric-primary for a mesh-primary). The primary takes the node's work turn (one node command, repair or
//! Build step on a node at a time), refusing the call `Busy` at once while another holds it (Q413),
//! admits it from its atlas (`ControlPlane::admit_node_command`), and moves a primary's roles first
//! by node.primary.update (R-MT1). A node-admin's command whose roles moved is answered
//! `PrimaryUpdated`, naming the new mesh-primary, and the caller sends it there (Q413). Otherwise the
//! primary runs the same stop or
//! start body a Build's step runs (`AdminRunner::standalone`, `AdminRunner::start_stopped`), with no
//! Build: nothing is opened, read or receipted, and each step is recorded on the node record from the
//! node's frames. Those frames go on to the caller as the primary records them: two one-hop streams,
//! never a forwarded one.

use super::runner::AdminRunner;
use crate::control_plane::{ActionKind, CommandRefusal, ControlPlane};
use crate::model::{IncarnationId, NodeId};
use crate::node_self::CommandFrames;
use rdm_core::node_command::{NodeCommand, NodeCommandReply, NodeCommandRequest};
use rdm_core::streaming::{FrameKind, StreamingProtocol};
use std::sync::Arc;
use tracing::Instrument;

/// The primary's `NodeCommandService`: this admin's runner and its control plane's atlas.
pub(crate) struct NodeCommandService {
    pub(crate) runner: Arc<AdminRunner>,
    pub(crate) control: Arc<ControlPlane>,
}

#[async_trait::async_trait]
impl crate::node_commands::PrimaryCommands for NodeCommandService {
    async fn run(&self, caller: Option<NodeId>, req: NodeCommandRequest, mut frames: Box<dyn CommandFrames>) -> NodeCommandReply {
        let (node_id, incarnation, operation, kind) = match &req {
            NodeCommandRequest::StopNode { node_id, incarnation, operation } => (node_id.clone(), incarnation.clone(), operation.clone(), ActionKind::Stop),
            NodeCommandRequest::StartNode { node_id, incarnation, operation } => (node_id.clone(), incarnation.clone(), operation.clone(), ActionKind::Start),
            // The request carries no operation: the command's identity is minted here, keyed on the node.id.
            // A scope that names nothing the node can cut is refused by name, `Rejected` with reason
            // `rejected-connection-scope`, nothing sent (node-connections-delete.md "Wire").
            NodeCommandRequest::DeleteConnections { node_id, incarnation, scope } => {
                if let Some(reason) = scope.refusal() {
                    return rdm_core::reply::node_command::rejected("rejected-connection-scope", format!("node {node_id} ({}): {reason}", incarnation.as_str()));
                }
                (node_id.clone(), incarnation.clone(), rdm_core::OperationId::node("node.connection.delete", node_id).in_occurrence(rdm_core::OperationOccurrenceId::mint()), ActionKind::DeleteConnections)
            }
            other => return rdm_core::reply::node_command::rejected("receiver-not-primary", format!("{} is no caller's node command: the primary above a node runs node.stop, node.start and node.connection.delete", other.op())),
        };
        let me = self.runner.held_name();
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-node-command",
            node_path = %me.path, node_name = %me.name,
            command = req.op(),
            subject_id = %node_id, incarnation_id = %incarnation.as_str(),
            subject_path = tracing::field::Empty, subject_name = tracing::field::Empty,
            caller = %caller.as_ref().map(|c| c.to_string()).unwrap_or_default(),
            operation = %operation.action, operation_occurrence = ?operation.occurrence,
            new_primary = tracing::field::Empty,
            outcome = tracing::field::Empty, detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let reply = self.run_command(&req, &node_id, &incarnation, &operation, kind, &mut frames).instrument(span.clone()).await;
        span.record("outcome", reply.name());
        if let NodeCommandReply::Rejected { detail, .. } | NodeCommandReply::Failed { reason: detail, .. } | NodeCommandReply::Busy { reason: detail } = &reply {
            span.record("detail", detail.as_str());
        }
        match &reply {
            NodeCommandReply::Busy { .. } => span.in_scope(|| tracing::info!("a caller's node command was refused Busy: its primary runs another command or step on the node, and the caller's Send sends it again (Q413)")),
            NodeCommandReply::PrimaryUpdated { .. } => span.in_scope(|| tracing::info!("a caller's node command moved the node's roles and was answered PrimaryUpdated: the caller sends it to the new mesh-primary (Q413)")),
            _ => span.in_scope(|| tracing::info!("a caller's node command was run by the primary above the node, never a Build")),
        }
        reply
    }
}

/// What a caller's node command has sent on so far.
#[derive(Default)]
struct Sent {
    /// The node's acceptance went on: every later frame is progress, and a failure is `Failed`.
    accepted: bool,
    /// The node's terminal frame, held until the run ends.
    terminal: Option<NodeCommandReply>,
    /// The node's refusal before it accepted.
    refusal: Option<NodeCommandReply>,
    /// The caller's stream broke.
    gone: bool,
}

impl Sent {
    /// Take one frame the primary recorded: its acceptance and progress go on at once; its terminal
    /// and a refusal are held for the run's end.
    async fn take(&mut self, frame: NodeCommandReply, frames: &mut Box<dyn CommandFrames>) {
        match NodeCommand::frame_kind(&frame) {
            FrameKind::Started if !self.accepted => {
                self.accepted = true;
                self.gone |= frames.accepted(frame).await.is_err();
            }
            FrameKind::Started => {}
            FrameKind::Data if self.accepted => self.gone |= frames.progress(frame).await.is_err(),
            FrameKind::Data => {}
            FrameKind::Terminal => self.terminal = Some(frame),
            FrameKind::Refusal(_) => self.refusal = Some(frame),
        }
    }
}

/// Run `body` while each frame the primary records for the command goes on to the caller: its
/// acceptance and progress at once, its terminal and a refusal held for the end ([`Sent::take`]).
async fn forward<T>(mut recorded: tokio::sync::mpsc::UnboundedReceiver<NodeCommandReply>, body: impl std::future::Future<Output = T>, frames: &mut Box<dyn CommandFrames>) -> (T, Sent) {
    tokio::pin!(body);
    let mut sent = Sent::default();
    let ended = loop {
        tokio::select! {
            Some(frame) = recorded.recv() => sent.take(frame, frames).await,
            r = &mut body => break r,
        }
    };
    while let Ok(frame) = recorded.try_recv() {
        sent.take(frame, frames).await;
    }
    (ended, sent)
}

impl NodeCommandService {
    async fn run_command(&self, req: &NodeCommandRequest, node_id: &NodeId, incarnation: &IncarnationId, operation: &rdm_core::OperationId, kind: ActionKind, frames: &mut Box<dyn CommandFrames>) -> NodeCommandReply {
        // Work on one node is serialized at its primary (node-recovery.md step 1): this command runs
        // before or after another command, a repair or a Build's step on the node, never beside it. A
        // caller's command naming a node whose turn is held is refused `Busy` at once, before anything
        // is admitted or any role moves, and the caller's `Send` sends it again (Q413): never a second
        // live call waiting beside the first.
        let Some(_turn) = self.runner.try_node_work(node_id) else {
            // A command admission refuses by name is answered that refusal, never `Busy`: sent again,
            // it would be refused the same.
            return match self.control.admit_node_command(node_id, incarnation, kind).await {
                Err(CommandRefusal { reason: "rejected-stale-incarnation", held: Some(held), .. }) => rdm_core::reply::node_command::rejected_stale_incarnation(held),
                Err(r) => rdm_core::reply::node_command::rejected(r.reason, r.detail),
                Ok(_) => rdm_core::reply::node_command::busy(format!("node {node_id}: its primary {} runs another command or step on it; {} is refused, and its caller's Send sends it again", self.runner.node_id, req.op())),
            };
        };
        let start = kind == ActionKind::Start;
        let named = match req {
            NodeCommandRequest::DeleteConnections { scope: scope @ rdm_core::status::ConnectionScope::Connections(_), .. } => Some(scope),
            _ => None,
        };
        let (path, pin) = match self.control.admit_node_command(node_id, incarnation, kind).await {
            Ok(admitted) => admitted,
            Err(CommandRefusal { reason: "rejected-stale-incarnation", held: Some(held), .. }) => return rdm_core::reply::node_command::rejected_stale_incarnation(held),
            Err(r) => return rdm_core::reply::node_command::rejected(r.reason, r.detail),
        };
        tracing::Span::current().record("subject_path", path.path.as_str()).record("subject_name", path.name.as_str());
        // Admitted: the command is in flight on the node until this run returns (node-investigation.md:
        // "A node-admin's atlas is its topology and its in-flight commands"; Q439). The atlas's row of the
        // node carries it as accepted, cleared before the terminal reaches the caller.
        let _in_flight = self.control.atlas.command_in_flight(node_id, incarnation, operation);
        // A primary's roles move first by node.primary.update (R-MT1; node-primary-update.md §1,
        // node-connections-delete.md step 0). A node-admin's command is then answered `PrimaryUpdated`,
        // naming its mesh-primary as it stands after the update, the new primary the update's
        // node.primary.updated names, and the caller sends it there (step 6; Q413). A delete of named
        // connections moves no role: step 0 is `All`'s alone.
        if !start && named.is_none() {
            // The roles the node holds as this admin's atlas holds them (Q355): an update this admin
            // commanded is answered before gossip carries it here.
            let mut view = self.runner.topology.read().await.clone();
            self.control.atlas.primaries(&mut view);
            if let Some(m) = crate::executor::node_role_move(&pin, false, &view) {
                let successor = match self.move_roles(&m, operation).await {
                    Ok(successor) => successor,
                    Err(end) => return rdm_core::reply::node_command::rejected("primary-not-updated", format!("node {node_id}: node.primary.update before its {} did not complete: {end:?}", req.op())),
                };
                if m.mesh_role || m.fabric_role {
                    return self.primary_updated(req, &pin.mesh, node_id, successor).await;
                }
            }
        }
        let Some(status_service) = self.runner.observer.status_service.get().cloned() else {
            return rdm_core::reply::node_command::not_ready(format!("{}: this admin holds no StatusService yet; {} is refused", self.runner.node_id, req.op()));
        };
        let (_caller, recorded) = status_service.commands.caller_stream(node_id, incarnation);
        if let Some(scope) = named {
            // The named connections: the carrier the `All` body reaches, with these ids. It sets no
            // node-wide status (node-connections-delete.md "`Connections([id, …])`").
            let (end, sent) = forward(recorded, status_service.carry_connections_delete(node_id, incarnation, scope), frames).await;
            use crate::status_rpc::CarryEnd;
            return match end {
                CarryEnd::Deleted { closed, not_open, held, receipt } => sent.terminal.unwrap_or(NodeCommandReply::ConnectionsDeleted { closed, not_open, held, receipt }),
                CarryEnd::Refused(reply) | CarryEnd::RefusedByNode(reply) => reply,
                end @ (CarryEnd::Lost { .. } | CarryEnd::Incomplete { .. }) if sent.accepted => rdm_core::reply::node_command::failed(req.op().into(), end.detail(), end.code()),
                end => rdm_core::reply::node_command::rejected("not-sent", format!("{} of node {node_id} was not admitted by the node: {}", req.op(), end.detail())),
            };
        }
        let run = async {
            match kind {
                ActionKind::Start => self.runner.start_stopped(&path, &pin, operation).await,
                ActionKind::DeleteConnections => self.runner.standalone(&path, &pin, false, operation).await,
                _ => self.runner.standalone(&path, &pin, true, operation).await,
            }
        };
        let (ended, sent) = forward(recorded, run, frames).await;
        self.runner.refresh_view().await;
        match ended {
            Ok(()) => {
                if !sent.accepted {
                    // A start an earlier command completed answers its terminal with no acceptance frame:
                    // the stream still opens with the birth's acceptance.
                    let _ = frames.accepted(NodeCommandReply::CommandAccepted { node_id: node_id.clone(), incarnation: incarnation.clone() }).await;
                }
                match (sent.terminal, kind) {
                    (Some(terminal), _) => terminal,
                    (None, ActionKind::Start) => NodeCommandReply::NodeStarted,
                    (None, ActionKind::DeleteConnections) => NodeCommandReply::ConnectionsDeleted { closed: Vec::new(), not_open: Vec::new(), held: Vec::new(), receipt: None },
                    (None, _) => NodeCommandReply::NodeStopped,
                }
            }
            Err(end) if sent.accepted => {
                let code = match &end {
                    crate::executor::RunEnd::Failed { code, .. } => *code,
                    crate::executor::RunEnd::Incomplete(_) => rdm_core::outcome::Code::Unavailable,
                };
                rdm_core::reply::node_command::failed(req.op().into(), end.reason().to_string(), code)
            }
            Err(end) => sent.refusal.unwrap_or_else(|| rdm_core::reply::node_command::rejected("not-sent", format!("{} of node {node_id} was not admitted by the node: {}", req.op(), end.reason()))),
        }
    }

    /// node.primary.update of the roles `m` names before a stop of the primary that holds them, as the
    /// fabric-primary's drive moves them before a Build's step (`RoleGate::update_primaries`): a
    /// mesh-primary or another type's primary commanded from above; this admin's own roles moved by
    /// itself, having nothing above it. A node command belongs to no Build: the update names none.
    /// Answers the new primary the update's node.primary.updated names, when it moved a role.
    async fn move_roles(&self, m: &crate::executor::RoleMove, operation: &rdm_core::OperationId) -> Result<Option<NodeId>, crate::build_drive::RoleMoveEnd> {
        use crate::election::Moving;
        use crate::primary_update::update_operation;
        use rdm_core::PrimaryRole;
        let primary_update = &self.runner.primary_update;
        let build = rdm_core::LifecycleOp::NO_BUILD;
        if m.primary.node_id != primary_update.node_id {
            if m.mesh_role || m.node_type_role {
                let role = if m.node_type_role { PrimaryRole::of_type(&m.primary.r#type) } else { PrimaryRole::MeshPrimary };
                return Ok(Some(primary_update.command_update_primary(&m.primary, build, &update_operation(role, operation)).await?));
            }
            return Ok(None);
        }
        let (moving, role) = match (m.mesh_role, m.fabric_role) {
            (true, true) => (Moving::BothRoles, PrimaryRole::MeshPrimary),
            (true, false) => (Moving::MeshRole, PrimaryRole::MeshPrimary),
            (false, true) => (Moving::FabricRoleOutside, PrimaryRole::FabricPrimary),
            (false, false) => return Ok(None),
        };
        Ok(Some(primary_update.update_own_primaries(moving, build, &update_operation(role, operation), m.successor.as_ref()).await?))
    }

    /// The caller's command `req` on the node-admin `node_id`, its roles moved, answered
    /// `PrimaryUpdated` naming its commander as it stands after the update (commands go down: the
    /// primary above the node runs it): the new primary the update's node.primary.updated names
    /// (`successor`), by which it is stopped (node-stop.md "Primary", node-primary-update.md step 6);
    /// the mesh-primary of `mesh` this admin's atlas holds (Q355), when the answer names none. The
    /// caller sends the command there itself: two one-hop calls, never one held across both (Q413).
    async fn primary_updated(&self, req: &NodeCommandRequest, mesh: &str, node_id: &NodeId, successor: Option<NodeId>) -> NodeCommandReply {
        self.runner.refresh_view().await;
        let to = match successor {
            Some(s) => Some(s),
            None => {
                let mut view = self.runner.topology.read().await.clone();
                self.control.atlas.primaries(&mut view);
                view.node_type_primary(mesh, rdm_core::NODE_ADMIN).map(|p| p.node_id.clone())
            }
        }
        .filter(|p| p != node_id);
        let Some(to) = to else {
            return rdm_core::reply::node_command::rejected("no-secondary", format!("node {node_id}: its roles moved, and this view names no mesh-primary of mesh {mesh} to run its {}", req.op()));
        };
        tracing::Span::current().record("new_primary", to.to_string().as_str());
        rdm_core::reply::node_command::primary_updated(to)
    }
}
