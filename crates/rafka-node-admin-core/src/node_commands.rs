//! The open node commands (`drain-node`, `stop-node`) the owning mesh-admin has sent and awaits a
//! completion for (node-drain.md, node-stop.md; node-rpc-envelope.md, "Node drain and stop
//! command pairs").
//!
//! A command is sent to one exact birth; the birth answers `Applied` on admission and, later, calls
//! `NodeDrained` / `NodeLeft` back. The completion is matched to the open command by
//! `(build_id, attempt, operation, node_id, incarnation)`. A completion with no open command is
//! refused by name; a repeat of an accepted completion is `AlreadyApplied`. This is the RPC
//! completion inbox of the executing admin: it reads no gossip and writes nothing durable (the
//! pipeline's step receipt is the durable record).

use crate::deployment::pipeline::{command_admission, CommandAdmission, CommandContext, Completion};
use crate::model::{IncarnationId, Node, NodeId};
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget};
use rafka_node_rpc_contract::status::{NotAuthority, Status, StatusReply, StatusRequest};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::watch;

/// Which of the two commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeCommand {
    /// `drain-node`; completes with `node-drained`.
    Drain,
    /// `stop-node`; completes with `node-left`.
    Stop,
}

impl NodeCommand {
    /// The operation prefix of the command's operation id.
    pub fn operation_prefix(self) -> &'static str {
        match self {
            Self::Drain => "drain-node",
            Self::Stop => "stop-node",
        }
    }
    /// The completion's name.
    pub fn completion(self) -> &'static str {
        match self {
            Self::Drain => "node-drained",
            Self::Stop => "node-left",
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
    /// `drain-node:<path>` or `stop-node:<path>`.
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
    /// No command is open under that key; the open ones are named.
    NoOpenCommand {
        /// The operations open for the same node.
        open_for_node: Vec<String>,
    },
}

/// The commands this admin has sent and not forgotten.
#[derive(Default)]
pub struct CommandBook {
    open: Mutex<HashMap<CommandKey, watch::Sender<bool>>>,
}

impl CommandBook {
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
            None => Accepted::NoOpenCommand { open_for_node: open.keys().filter(|k| k.node_id == key.node_id).map(|k| format!("{}@{}/{}", k.operation, k.build_id, k.attempt)).collect() },
        }
    }

    /// Whether a command is open under `key` (the completion has not been forgotten).
    pub fn is_open(&self, key: &CommandKey) -> bool {
        self.open.lock().unwrap().contains_key(key)
    }

    /// Whether any stop command is open for this exact birth.
    pub fn stop_open_for(&self, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
        self.open.lock().unwrap().keys().any(|k| &k.node_id == node_id && &k.incarnation == incarnation && k.operation.starts_with("stop-node:"))
    }

    /// Forget the command: its step receipt is the record.
    pub fn close(&self, key: &CommandKey) {
        self.open.lock().unwrap().remove(key);
    }
}

/// Send `cmd` to the exact birth `node` through `client` at `target`, opening the command before
/// the send so a completion that races the reply finds it. The outcome is admission, not
/// completion.
pub async fn send_command(client: &NodeRpcClient, commands: &CommandBook, target: &NodeTarget, node: &Node, cmd: NodeCommand, ctx: &CommandContext) -> CommandAdmission {
    let Some(incarnation) = node.incarnation_id.clone() else {
        return CommandAdmission::NotSent { reason: format!("{} has no known birth to command", node.name) };
    };
    let _ = commands.open(CommandKey { build_id: ctx.build_id.clone(), attempt: ctx.attempt, operation: ctx.operation.clone(), node_id: node.node_id.clone(), incarnation: incarnation.clone() });
    let (node_id, build_id, attempt, operation) = (node.node_id.clone(), ctx.build_id.clone(), ctx.attempt, ctx.operation.clone());
    let req = match cmd {
        NodeCommand::Drain => StatusRequest::DrainNode { node_id, incarnation, build_id, attempt, operation },
        NodeCommand::Stop => StatusRequest::StopNode { node_id, incarnation, build_id, attempt, operation },
    };
    let (out, _) = client.call::<Status>(target, &req, &CallOptions::default()).await;
    let admission = command_admission(&out);
    tracing::info_span!("rdm.node_admin.node.update.via-command-sent", node = %node.name, command = cmd.operation_prefix(), operation = %ctx.operation, build_id = %ctx.build_id, attempt = ctx.attempt, admission = admission.name(), "otel.kind" = "internal")
        .in_scope(|| tracing::info!("the command was sent to the exact birth"));
    admission
}

/// Wait up to `within` for the completion call of the command `ctx` names for `node`.
pub async fn await_completion(commands: &CommandBook, node: &Node, cmd: NodeCommand, ctx: &CommandContext, within: Duration) -> Completion {
    let Some(incarnation) = node.incarnation_id.clone() else {
        return Completion::NotAwaited { admission: format!("{} has no known birth", node.name) };
    };
    let key = CommandKey { build_id: ctx.build_id.clone(), attempt: ctx.attempt, operation: ctx.operation.clone(), node_id: node.node_id.clone(), incarnation };
    let mut rx = commands.open(key);
    let got = tokio::time::timeout(within, rx.wait_for(|done| *done)).await.is_ok_and(|r| r.is_ok());
    tracing::info_span!("rdm.node_admin.node.update.via-completion-awaited", node = %node.name, completion = cmd.completion(), operation = %ctx.operation, build_id = %ctx.build_id, attempt = ctx.attempt, received = got, "otel.kind" = "internal")
        .in_scope(|| tracing::info!("the completion call was awaited"));
    if got {
        Completion::Received
    } else {
        Completion::Deadline
    }
}

/// A birth's completion call (`node-drained`, `node-left`) at the admin `receiver` that commanded
/// it: it resolves the open command it names. `sender` is the resolved caller `(node id,
/// incarnation, path.name)`; it must be the subject. A completion with no open command is
/// refused `NotReady` naming the mismatch, never counted toward another operation.
pub fn accept_completion(commands: &CommandBook, receiver: &str, sender: Option<(&NodeId, Option<&IncarnationId>, &str)>, req: &StatusRequest) -> StatusReply {
    let (node_id, incarnation, build_id, attempt, operation) = match req {
        StatusRequest::NodeDrained { node_id, incarnation, build_id, attempt, operation } | StatusRequest::NodeLeft { node_id, incarnation, build_id, attempt, operation } => (node_id, incarnation, build_id, attempt, operation),
        _ => unreachable!("accept_completion is called for completions only"),
    };
    let reply = match sender {
        Some((from, held, _)) if from == node_id && held == Some(incarnation) => {
            let key = CommandKey { build_id: build_id.clone(), attempt: *attempt, operation: operation.clone(), node_id: node_id.clone(), incarnation: incarnation.clone() };
            match commands.complete(&key) {
                Accepted::First => StatusReply::Applied,
                Accepted::Again => StatusReply::AlreadyApplied,
                Accepted::NoOpenCommand { open_for_node } => StatusReply::NotReady {
                    reason: format!("{receiver} holds no open command {operation} (build {build_id}, attempt {attempt}) for {node_id} incarnation {}; open for that node: [{}]", incarnation.0, open_for_node.join(", ")),
                },
            }
        }
        other => StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: other.map(|(_, _, n)| n.to_string()).unwrap_or_else(|| "unknown peer".into()) } },
    };
    tracing::info_span!(
        "rdm.node_admin.status.update.via-completion-accepted",
        node = %receiver,
        op = req.op(),
        operation = %operation,
        build_id = %build_id,
        attempt = *attempt,
        subject = %node_id,
        incarnation_id = %incarnation.0,
        sender = %sender.map(|(_, _, n)| n).unwrap_or_default(),
        outcome = reply.name(),
        "otel.kind" = "internal"
    )
    .in_scope(|| tracing::info!("a completion call was decided by the commanding admin"));
    reply
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(op: &str) -> CommandKey {
        CommandKey { build_id: "bld_1".into(), attempt: 1, operation: op.into(), node_id: NodeId::mint(), incarnation: IncarnationId("i".into()) }
    }

    /// CONTRACT: a completion resolves its open command once; a repeat is a repeat; a completion
    /// for a command that is not open is refused and names the commands open for that node.
    #[tokio::test]
    async fn a_completion_resolves_only_its_open_command() {
        let book = CommandBook::default();
        let k = key("drain-node:mesh1.rpc.1");
        let mut rx = book.open(k.clone());
        assert!(!*rx.borrow());
        assert_eq!(book.complete(&k), Accepted::First);
        rx.wait_for(|v| *v).await.unwrap();
        assert_eq!(book.complete(&k), Accepted::Again);
        let other = CommandKey { operation: "stop-node:mesh1.rpc.1".into(), ..k.clone() };
        assert_eq!(book.complete(&other), Accepted::NoOpenCommand { open_for_node: vec!["drain-node:mesh1.rpc.1@bld_1/1".into()] });
    }
}
