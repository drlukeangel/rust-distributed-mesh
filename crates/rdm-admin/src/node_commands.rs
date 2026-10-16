//! The open node commands (`stop-node`, `start-node`) the owning mesh-admin has sent and awaits a
//! completion for (node-stop.md, node-start.md; node-rpc-envelope.md, "Node stop command pair").
//!
//! A command is sent to one exact birth. `stop-node` is answered `Stopped` once the birth is
//! stopped, and that reply is node-left: no upward node-left call exists (R-ON1). `DrainNode` and
//! `NodeDrained` are reserved positions (R-CR4), refused by name ([`refuse_reserved`]). A completion is matched to
//! the open command by `(build_id, attempt, operation, node_id, incarnation)`. A completion with no open command is
//! refused by name; a repeat of an accepted completion is `AlreadyApplied`. This is the RPC
//! completion inbox of the executing admin: it reads no gossip and writes nothing durable (the
//! pipeline's step receipt is the durable record).

use crate::deployment::pipeline::{CommandAdmission, CommandContext, Completion};
use crate::model::{IncarnationId, Node, NodeId};
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget};
use rdm_core::status::{StatusReply, StatusRequest};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::watch;

/// Which of the two commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeCommand {
    /// `node.connections.delete { All }`; its reply is `node.connections.deleted`.
    DeleteConnections,
    /// `stop-node`; completes with `node-left`, which is the reply of the call.
    Stop,
    /// `start-node`; completes with `node-started`, which is the reply of the call.
    Start,
}

impl NodeCommand {
    /// The operation prefix of the command's operation id.
    pub fn operation_prefix(self) -> &'static str {
        match self {
            Self::DeleteConnections => "node.connections.delete",
            Self::Stop => "stop-node",
            Self::Start => "start-node",
        }
    }
    /// The completion's name.
    pub fn completion(self) -> &'static str {
        match self {
            Self::DeleteConnections => "node.connections.deleted",
            Self::Stop => "node-left",
            Self::Start => "node-started",
        }
    }
}

/// The natural key of a command and its completion.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommandKey {
    /// The Build the operation belongs to.
    pub build_id: String,
    /// The Build attempt that holds it.
    pub attempt: u32,
    /// `node.stop:<path>` or `node.start:<path>`.
    pub operation: String,
    /// The exact birth commanded.
    pub node_id: NodeId,
    /// Its incarnation.
    pub incarnation: IncarnationId,
}

/// What accepting a completion found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Accepted {
    /// The first completion of an open command.
    First,
    /// A repeat of a completion already accepted.
    Again,
    /// No command is open under that key: the first field that differs from the command held
    /// for that birth, as `(field, expected, reported)`.
    NoOpenCommand {
        /// The field that differs.
        field: String,
        /// What the receiver holds for it.
        expected: String,
        /// What the completion reported.
        reported: String,
    },
}

/// The commands this admin has sent and not forgotten, and the bound of the
/// `node.connections.delete { All }` a commanded birth runs (this admin's own,
/// `AdminConfig::connections_delete_deadline`, which every birth it launches carries).
pub struct CommandBook {
    open: Mutex<HashMap<CommandKey, watch::Sender<bool>>>,
    connections_delete_deadline: Duration,
}

/// A book whose commanded births run their `node.connections.delete { All }` under the launch default.
impl Default for CommandBook {
    fn default() -> Self {
        Self::new(rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE)
    }
}

impl CommandBook {
    /// A book whose commanded births run their `node.connections.delete { All }` under `connections_delete_deadline`.
    pub fn new(connections_delete_deadline: Duration) -> Self {
        Self { open: Mutex::new(HashMap::new()), connections_delete_deadline }
    }

    /// How long a command whose birth runs a `node.connections.delete { All }` (the command itself,
    /// or a stop) waits for its reply: the bound the birth runs it under, and 10 s for the rest of the step.
    pub fn connections_delete_reply(&self) -> Duration {
        self.connections_delete_deadline + Duration::from_secs(10)
    }

    /// Open `key` before the command is sent (so a completion that races the reply finds it) and
    /// return what resolves when the completion is accepted. Opening again is the same command.
    pub fn open(&self, key: CommandKey) -> watch::Receiver<bool> {
        self.open.lock().unwrap().entry(key).or_insert_with(|| watch::channel(false).0).subscribe()
    }

    /// Accept the completion for `key`.
    pub fn complete(&self, key: &CommandKey) -> Accepted {
        let open = self.open.lock().unwrap();
        match open.get(key) {
            Some(tx) => {
                if tx.send_replace(true) {
                    Accepted::Again
                } else {
                    Accepted::First
                }
            }
            None => {
                // The command held for this birth that the completion most resembles: the one
                // under the same operation, else any. The first differing field is the answer.
                let mut held: Vec<&CommandKey> = open.keys().filter(|k| k.node_id == key.node_id && k.incarnation == key.incarnation).collect();
                held.sort_by_key(|k| (k.operation != key.operation, k.operation.clone(), k.build_id.clone(), k.attempt));
                match held.first() {
                    None => Accepted::NoOpenCommand { field: "command".into(), expected: "an open stop-node or start-node for this birth".into(), reported: format!("{} (build {}, attempt {})", key.operation, key.build_id, key.attempt) },
                    Some(k) if k.build_id != key.build_id => Accepted::NoOpenCommand { field: "build_id".into(), expected: k.build_id.clone(), reported: key.build_id.clone() },
                    Some(k) if k.attempt != key.attempt => Accepted::NoOpenCommand { field: "attempt".into(), expected: k.attempt.to_string(), reported: key.attempt.to_string() },
                    Some(k) => Accepted::NoOpenCommand { field: "operation".into(), expected: k.operation.clone(), reported: key.operation.clone() },
                }
            }
        }
    }

    /// Whether a command is open under `key` (the completion has not been forgotten).
    pub fn is_open(&self, key: &CommandKey) -> bool {
        self.open.lock().unwrap().contains_key(key)
    }

    /// Whether any stop command is open for this exact birth.
    pub fn stop_open_for(&self, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
        self.open.lock().unwrap().keys().any(|k| &k.node_id == node_id && &k.incarnation == incarnation && k.operation.starts_with("node.stop:"))
    }

    /// Forget the command: its step receipt is the record.
    pub fn close(&self, key: &CommandKey) {
        self.open.lock().unwrap().remove(key);
    }
}

/// Why a command was never sent to its birth, when the cause is the birth's reachability
/// (node-tickle.md step 1): the connect was refused, it timed out or failed on the wire, or the
/// birth's endpoint did not resolve. A cause local to the sender (a request that does not encode, a
/// budget, an admission block, a frame reset) is none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unreachable {
    /// `connect-refused`, `connect-timed-out`, `connect-failed` or `endpoint-unresolved`.
    pub reason: &'static str,
    /// The transport's own account.
    pub detail: String,
}

/// The reachability cause of `cause`, or `None` for a cause local to the sender.
pub fn unreachable_of(cause: &rdm_core::outcome::NotSentReason) -> Option<Unreachable> {
    use rdm_core::outcome::NotSentReason as R;
    let reason = match cause {
        R::Connection(e) if e.starts_with("request does not encode") => return None,
        R::Connection(e) if e.contains("refused") => "connect-refused",
        R::Connection(e) if e.contains("timed out") => "connect-timed-out",
        R::Connection(_) => "connect-failed",
        R::Deadline => "connect-timed-out",
        R::Resolve(_) | R::NoActiveRoute => "endpoint-unresolved",
        R::StreamBudget | R::FrameNotSent | R::AdmissionBlocked(_) | R::Carried(_) | R::CarrierEdgeLost(_) | R::CarrierNoBudget { .. } | R::NotForwardable { .. } => return None,
    };
    Some(Unreachable { reason, detail: format!("{cause:?}") })
}

/// Send `cmd` to the exact birth `node` through `client` at `target`, opening the command before
/// the send so a completion that races the reply finds it. The outcome is admission, not
/// completion.
pub async fn send_command(client: &NodeRpcClient, commands: &CommandBook, target: &NodeTarget, node: &Node, cmd: NodeCommand, ctx: &CommandContext) -> CommandAdmission {
    send_command_reaching(client, commands, target, node, cmd, ctx).await.0
}

/// [`send_command`], with the reachability cause of a command that was never sent ([`unreachable_of`]).
pub async fn send_command_reaching(client: &NodeRpcClient, commands: &CommandBook, target: &NodeTarget, node: &Node, cmd: NodeCommand, ctx: &CommandContext) -> (CommandAdmission, Option<Unreachable>) {
    let Some(incarnation) = node.incarnation_id.clone() else {
        return (CommandAdmission::NotSent { reason: format!("{} has no known birth to command", node.name) }, None);
    };
    let build_id = match rdm_client::BuildId::parse(&ctx.build_id) {
        Ok(build_id) => build_id,
        Err(e) => return (CommandAdmission::NotSent { reason: format!("{}: the command names Build {:?}, which is not a Crockford id: {e}", node.name, ctx.build_id) }, None),
    };
    let _ = commands.open(CommandKey { build_id: ctx.build_id.clone(), attempt: ctx.attempt, operation: ctx.operation.clone(), node_id: node.node_id.clone(), incarnation: incarnation.clone() });
    let (node_id, attempt) = (node.node_id.clone(), ctx.attempt);
    let mut unreachable = None;
    let admission = match cmd {
        // The executor's own status authority carries it (status_rpc.rs `carry_delete_connections`):
        // see `AdminObserver::send_command`; a client alone has no mesh-primary to set the status.
        NodeCommand::DeleteConnections => {
            let req = StatusRequest::DeleteConnections { node_id: node_id.clone(), incarnation: incarnation.clone(), scope: rdm_core::status::ConnectionScope::All };
            let opts = CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: commands.connections_delete_reply() }, ..Default::default() };
            crate::deployment::pipeline::command_admission(&client.call::<rdm_core::status::Status>(target, &req, &opts).await.0)
        }
        NodeCommand::Stop => {
            let birth = rdm_client::ExactBirth { target: target.clone(), node_id, incarnation };
            let stop = rdm_client::StopContext::new(build_id, attempt, node.name.clone());
            let opts = CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: commands.connections_delete_reply() }, ..Default::default() };
            let end = rdm_client::NodeRpc::new(client).stop(&birth, &stop, &opts).await;
            unreachable = not_sent_cause(&end);
            admission_of(end)
        }
        NodeCommand::Start => {
            let birth = rdm_client::ExactBirth { target: target.clone(), node_id, incarnation };
            let start = rdm_client::StartContext::new(build_id, attempt, node.name.clone());
            let opts = CallOptions { budget: rdm_transport::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(60) }, ..Default::default() };
            let end = rdm_client::NodeRpc::new(client).start(&birth, &start, &opts).await;
            unreachable = not_sent_cause(&end);
            admission_of(end)
        }
    };
    // The reply of a stop is its completion. The connection it rode is closed now: the node cut every
    // other one, so its start reaches the parked node on a fresh dial.
    if matches!(admission, CommandAdmission::Stopped { .. }) {
        if let Some(peer) = node.endpoint_id.as_ref().and_then(|e| e.0.parse::<iroh::PublicKey>().ok()) {
            client.close_pooled_to(&peer, "node.stopped");
        }
        let _ = commands.complete(&CommandKey { build_id: ctx.build_id.clone(), attempt: ctx.attempt, operation: ctx.operation.clone(), node_id: node.node_id.clone(), incarnation: node.incarnation_id.clone().expect("checked above") });
    }
    // A command not admitted names why on its span (the reply, or why no reply came), as its step's
    // receipt does: the refusal is read where it happened.
    let detail = match &admission {
        CommandAdmission::Refused { .. } | CommandAdmission::NotSent { .. } | CommandAdmission::Indeterminate { .. } | CommandAdmission::StartFailed { .. } => format!("{admission:?}"),
        _ => String::new(),
    };
    tracing::info_span!("rdm.node_admin.node.update.via-command-sent", node = %node.name, command = cmd.operation_prefix(), operation = %ctx.operation, build_id = %ctx.build_id, attempt = ctx.attempt, admission = admission.name(), detail = %detail, "otel.kind" = "internal")
        .in_scope(|| tracing::info!("the command was sent to the exact birth"));
    (admission, unreachable)
}

/// The reachability cause of a call that ended never sent.
fn not_sent_cause(end: &Result<StatusReply, rdm_client::CallEnd>) -> Option<Unreachable> {
    match end {
        Err(rdm_client::CallEnd::NotSent { cause, .. }) => unreachable_of(cause),
        _ => None,
    }
}

/// The arm a `node.stop` or `node.start` object call ended as: only `Applied` and `AlreadyApplied` admit it; every
/// other reply is `Refused` by its name, an unsent call `NotSent`, an unanswered one `Indeterminate`.
fn admission_of(r: Result<StatusReply, rdm_client::CallEnd>) -> CommandAdmission {
    use rdm_client::CallEnd;
    match r {
        Ok(StatusReply::Applied) => CommandAdmission::Admitted {},
        Ok(StatusReply::AlreadyApplied) => CommandAdmission::AlreadyAdmitted {},
        Ok(StatusReply::Stopped { receipt }) => CommandAdmission::Stopped { receipt },
        Ok(StatusReply::Started) => CommandAdmission::Started {},
        Ok(StatusReply::StartFailed { step, reason }) => CommandAdmission::StartFailed { step, reason },
        Ok(other) => CommandAdmission::Refused { reply: format!("{}: {other:?}", other.name()) },
        Err(CallEnd::NotSent { reason, .. }) => CommandAdmission::NotSent { reason },
        Err(CallEnd::Indeterminate { reason }) => CommandAdmission::Indeterminate { reason },
        Err(CallEnd::Unserved { op }) => CommandAdmission::Refused { reply: format!("unserved op {op:#04x}") },
        Err(CallEnd::RejectedStale { target_node_id }) => CommandAdmission::Refused { reply: format!("stale target {target_node_id}") },
        Err(other) => CommandAdmission::Refused { reply: other.to_string() },
    }
}

/// The completion of the command `ctx` names for `node`, received or not: the span every completion
/// a step reads writes, whether a wait received it or the command's own reply carried it (a stop's
/// `Stopped`, node-stop.md).
pub fn completion_span(node: &Node, cmd: NodeCommand, ctx: &CommandContext, received: bool) {
    tracing::info_span!("rdm.node_admin.node.update.via-completion-awaited", node = %node.name, completion = cmd.completion(), operation = %ctx.operation, build_id = %ctx.build_id, attempt = ctx.attempt, received = received, "otel.kind" = "internal")
        .in_scope(|| tracing::info!("the completion was awaited"));
}

/// Wait up to `within` for the completion of the command `ctx` names for `node`: its
/// call, or the `Stopped` reply of its `stop-node` call (node-left).
pub async fn await_completion(commands: &CommandBook, node: &Node, cmd: NodeCommand, ctx: &CommandContext, within: Duration) -> Completion {
    let Some(incarnation) = node.incarnation_id.clone() else {
        return Completion::NotAwaited { admission: format!("{} has no known birth", node.name) };
    };
    let key = CommandKey { build_id: ctx.build_id.clone(), attempt: ctx.attempt, operation: ctx.operation.clone(), node_id: node.node_id.clone(), incarnation };
    let mut rx = commands.open(key);
    let got = tokio::time::timeout(within, rx.wait_for(|done| *done)).await.is_ok_and(|r| r.is_ok());
    completion_span(node, cmd, ctx, got);
    if got {
        Completion::Received {}
    } else {
        Completion::Deadline {}
    }
}

pub use rdm_node::node_self::{
    refuse_node_left, refuse_reserved, NODE_LEFT_IS_NEVER_A_CALL, RETIRED_PAIR_IS_RESERVED,
};

#[cfg(test)]
mod tests {
    use super::*;

    fn key(op: &str) -> CommandKey {
        CommandKey { build_id: "66h8qrqvaajk".into(), attempt: 1, operation: op.into(), node_id: NodeId::mint(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap() }
    }

    /// CONTRACT (node-tickle.md step 1; i144 R9): a command never sent because its birth refused or
    /// did not answer the connect, or because its endpoint did not resolve, is a reachability failure
    /// named by its cause; a cause local to the sender (a request that does not encode, a stream
    /// budget, a frame reset, an admission block, a protocol that is not forwardable) is none, and
    /// marks nothing.
    // @feature: node-lifecycle
    #[test]
    fn a_reachability_not_sent_is_named_and_a_local_not_sent_marks_nothing() {
        use rdm_core::outcome::{NotSentReason as R, ResolveFailure};
        let named = |r: R| unreachable_of(&r).map(|u| u.reason);
        assert_eq!(named(R::Connection("timed out".into())), Some("connect-timed-out"));
        assert_eq!(named(R::Connection("Failed(\"connection refused\")".into())), Some("connect-refused"));
        assert_eq!(named(R::Connection("connection lost".into())), Some("connect-failed"));
        assert_eq!(named(R::Deadline), Some("connect-timed-out"));
        assert_eq!(named(R::Resolve(ResolveFailure::Unknown)), Some("endpoint-unresolved"));
        assert_eq!(named(R::NoActiveRoute), Some("endpoint-unresolved"));
        for local in [R::Connection("request does not encode: too long".into()), R::StreamBudget, R::FrameNotSent, R::AdmissionBlocked("all".into()), R::NotForwardable { op: 0x1b }, R::CarrierNoBudget { remaining_ms: 1, reserve_ms: 2 }] {
            assert_eq!(unreachable_of(&local), None, "{local:?} is local to the sender");
        }
    }

    /// CONTRACT: a completion resolves its open command once; a repeat is a repeat; a completion
    /// for a command that is not open names the first field that differs from the held command.
    #[tokio::test]
    async fn a_completion_resolves_only_its_open_command() {
        let book = CommandBook::default();
        let k = key("node.start:mesh1.rpc.1");
        let mut rx = book.open(k.clone());
        assert!(!*rx.borrow());
        assert_eq!(book.complete(&k), Accepted::First);
        rx.wait_for(|v| *v).await.unwrap();
        assert_eq!(book.complete(&k), Accepted::Again);
        let other = CommandKey { operation: "node.stop:mesh1.rpc.1".into(), ..k.clone() };
        assert_eq!(book.complete(&other), Accepted::NoOpenCommand { field: "operation".into(), expected: "node.start:mesh1.rpc.1".into(), reported: "node.stop:mesh1.rpc.1".into() });
        let attempt = CommandKey { attempt: 2, ..k.clone() };
        assert_eq!(book.complete(&attempt), Accepted::NoOpenCommand { field: "attempt".into(), expected: "1".into(), reported: "2".into() });
        let stranger = CommandKey { node_id: NodeId::mint(), ..k.clone() };
        assert!(matches!(book.complete(&stranger), Accepted::NoOpenCommand { field, .. } if field == "command"));
    }
}
