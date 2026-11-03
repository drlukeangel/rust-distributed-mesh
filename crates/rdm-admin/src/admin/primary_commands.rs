//! The node commands the primary above a node runs for a caller (node-stop.md, node-start.md,
//! node-connections-delete.md; Q342, D40). A caller sends NodeCommand 0x22
//! `StopNode`, `StartNode` or `DeleteConnections { scope }` to the node's mesh-primary (the fabric-primary for a mesh-primary). The primary admits it from its atlas (`ControlPlane::
//! admit_node_command`), takes the node's work turn (one node command, repair or Build step on a node
//! at a time), moves a primary's roles first by node.primary.update (R-MT1), and runs the same stop or
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

/// The primary's node command door: this admin's runner and its control plane's atlas.
pub(crate) struct PrimaryCommandDoor {
    pub(crate) runner: Arc<AdminRunner>,
    pub(crate) control: Arc<ControlPlane>,
}

#[async_trait::async_trait]
impl crate::node_commands::PrimaryCommands for PrimaryCommandDoor {
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
            handed_to = tracing::field::Empty,
            outcome = tracing::field::Empty, detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let reply = self.run_command(&req, &node_id, &incarnation, &operation, kind, &mut frames).instrument(span.clone()).await;
        span.record("outcome", reply.name());
        if let NodeCommandReply::Rejected { detail, .. } | NodeCommandReply::Failed { reason: detail, .. } = &reply {
            span.record("detail", detail.as_str());
        }
        span.in_scope(|| tracing::info!("a caller's node command was run by the primary above the node, never a Build"));
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

impl PrimaryCommandDoor {
    async fn run_command(&self, req: &NodeCommandRequest, node_id: &NodeId, incarnation: &IncarnationId, operation: &rdm_core::OperationId, kind: ActionKind, frames: &mut Box<dyn CommandFrames>) -> NodeCommandReply {
        // Work on one node is serialized at its primary (node-recovery.md "Execution"): this command runs
        // before or after a repair or a Build's step on the node, never beside it.
        let _turn = self.runner.node_work(node_id).await;
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
        // A primary's roles move first by node.primary.update (R-MT1; node-primary-update.md §1,
        // node-connections-delete.md step 0). A node-admin's command is then its mesh's new mesh-primary's,
        // which the caller's command goes on to. A delete of named connections moves no role: step 0 is
        // `All`'s alone.
        if !start && named.is_none() {
            let op = match kind {
                ActionKind::DeleteConnections => crate::build::BuildOperation::DeleteConnections { r#type: pin.r#type.clone(), mesh: pin.mesh.clone(), path: path.path.clone(), name: path.name.clone(), node_id: pin.node_id.clone(), from_incarnation: pin.incarnation.clone() },
                _ => crate::build::BuildOperation::StopNode { r#type: pin.r#type.clone(), mesh: pin.mesh.clone(), path: path.path.clone(), name: path.name.clone(), node_id: pin.node_id.clone(), from_incarnation: pin.incarnation.clone() },
            };
            let view = self.runner.topology.read().await.clone();
            if let Some(m) = crate::executor::role_move(&op, &view) {
                if let Err(end) = self.move_roles(&m, operation).await {
                    return rdm_core::reply::node_command::rejected("primary-not-updated", format!("node {node_id}: node.primary.update before its {} did not complete: {end:?}", req.op()));
                }
                if m.mesh_role || m.fabric_role {
                    return self.hand_on(req, &pin.mesh, node_id, frames).await;
                }
            }
        }
        let Some(door) = self.runner.observer.status_door.get().cloned() else {
            return rdm_core::reply::node_command::not_ready(format!("{}: this admin holds no Status door yet; {} is refused", self.runner.node_id, req.op()));
        };
        let (_caller, recorded) = door.commands.caller_stream(node_id, incarnation);
        if let Some(scope) = named {
            // The named connections: the carrier the `All` body reaches, with these ids. It sets no
            // node-wide status (node-connections-delete.md "`Connections([id, …])`").
            let (end, sent) = forward(recorded, door.carry_connections_delete(node_id, incarnation, scope), frames).await;
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
                ActionKind::Start => self.runner.start_stopped(None, rdm_core::LifecycleOp::NO_ATTEMPT, &path, &pin, operation).await,
                ActionKind::DeleteConnections => self.runner.standalone(None, rdm_core::LifecycleOp::NO_ATTEMPT, &path, &pin, false, operation).await,
                _ => self.runner.standalone(None, rdm_core::LifecycleOp::NO_ATTEMPT, &path, &pin, true, operation).await,
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
    async fn move_roles(&self, m: &crate::executor::RoleMove, operation: &rdm_core::OperationId) -> Result<(), crate::build_drive::RoleMoveEnd> {
        use crate::election::Moving;
        use crate::fabric_handover::update_operation;
        use rdm_core::PrimaryRole;
        let door = &self.runner.handover;
        let build = rdm_core::LifecycleOp::NO_BUILD;
        let run = rdm_core::LifecycleOp::NO_ATTEMPT;
        if m.primary.node_id != door.node_id {
            if m.mesh_role || m.node_type_role {
                let role = if m.node_type_role { PrimaryRole::of_type(&m.primary.r#type) } else { PrimaryRole::MeshPrimary };
                door.command_update_primary(&m.primary, build, run, &update_operation(role, operation)).await?;
            }
            return Ok(());
        }
        let (moving, role) = match (m.mesh_role, m.fabric_role) {
            (true, true) => (Moving::BothRoles, PrimaryRole::MeshPrimary),
            (true, false) => (Moving::MeshRole, PrimaryRole::MeshPrimary),
            (false, true) => (Moving::FabricRoleOutside, PrimaryRole::FabricPrimary),
            (false, false) => return Ok(()),
        };
        door.update_own_primaries(moving, build, run, &update_operation(role, operation), m.successor.as_ref()).await?;
        Ok(())
    }

    /// The caller's command `req` on the node-admin `node_id`, its roles moved, sent on to its mesh
    /// `mesh`'s mesh-primary now (commands go down: the primary above the node runs it), whose frames go
    /// on to the caller as they arrive.
    async fn hand_on(&self, req: &NodeCommandRequest, mesh: &str, node_id: &NodeId, frames: &mut Box<dyn CommandFrames>) -> NodeCommandReply {
        use rdm_transport::stream::StreamItem;
        self.runner.refresh_view().await;
        let to = self.runner.topology.read().await.node_type_primary(mesh, rdm_core::NODE_ADMIN).map(|p| p.node_id.clone()).filter(|p| p != node_id);
        let Some(to) = to else {
            return rdm_core::reply::node_command::rejected("no-secondary", format!("node {node_id}: its roles moved, and this view names no mesh-primary of mesh {mesh} to run its stop"));
        };
        tracing::Span::current().record("handed_to", to.to_string().as_str());
        let opts = rdm_transport::CallOptions { send: rdm_transport::Send::with_time_limit(self.runner.observer.commands.connections_delete_reply() + rdm_transport::TRANSPORT_LIMIT), ..Default::default() };
        let mut stream = match self.runner.node_rpc.client.call_stream::<NodeCommand>(&rdm_transport::NodeTarget::ExactNode(to.clone()), req, &opts).await {
            Ok((stream, _)) => stream,
            Err((rdm_core::outcome::RpcOutcome::Reply(r), _)) => return r.into_value(),
            Err((other, _)) => return rdm_core::reply::node_command::rejected("not-sent", format!("node {node_id}: its stop could not reach its mesh's mesh-primary {to}: {}", other.name())),
        };
        let mut sent = Sent::default();
        while let Some(item) = stream.next().await {
            let frame = match item {
                StreamItem::Frame(_, frame) => frame,
                StreamItem::Failed(f) if sent.accepted => return rdm_core::reply::node_command::failed(req.op().into(), format!("the stream from mesh-primary {to} broke: {f:?}"), f.code()),
                StreamItem::Failed(f) => return rdm_core::reply::node_command::rejected("not-sent", format!("the stream from mesh-primary {to} broke before acceptance: {f:?}")),
            };
            stream.accepted().await;
            sent.take(frame, frames).await;
            if let Some(end) = sent.terminal.take().or_else(|| sent.refusal.take()) {
                return end;
            }
        }
        rdm_core::reply::node_command::failed(req.op().into(), format!("the stream from mesh-primary {to} ended without its terminal"), rdm_core::outcome::Code::Unknown)
    }
}
