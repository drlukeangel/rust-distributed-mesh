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

use crate::deployment::pipeline::{CommandAdmission, CommandContext, Completion};
use crate::model::{IncarnationId, Node, NodeId};
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget};
use rafka_node_rpc_contract::status::{NotAuthority, StatusReply, StatusRequest};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::watch;

/// Which of the two commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeCommand {
    /// `drain-node`; completes with `node-drained`.
    Drain,
    /// `stop-node`; completes with `node-left`, which is the reply of the call.
    Stop,
    /// `start-node`; completes with `node-started`, which is the reply of the call.
    Start,
}

impl NodeCommand {
    /// The operation prefix of the command's operation id.
    pub fn operation_prefix(self) -> &'static str {
        match self {
            Self::Drain => "drain-node",
            Self::Stop => "stop-node",
            Self::Start => "start-node",
        }
    }
    /// The completion's name.
    pub fn completion(self) -> &'static str {
        match self {
            Self::Drain => "node-drained",
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
            None => {
                // The command held for this birth that the completion most resembles: the one
                // under the same operation, else any. The first differing field is the answer.
                let mut held: Vec<&CommandKey> = open.keys().filter(|k| k.node_id == key.node_id && k.incarnation == key.incarnation).collect();
                held.sort_by_key(|k| (k.operation != key.operation, k.operation.clone(), k.build_id.clone(), k.attempt));
                match held.first() {
                    None => Accepted::NoOpenCommand { field: "command".into(), expected: "an open drain-node or stop-node for this birth".into(), reported: format!("{} (build {}, attempt {})", key.operation, key.build_id, key.attempt) },
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
    let (node_id, build_id, attempt) = (node.node_id.clone(), ctx.build_id.clone(), ctx.attempt);
    let admission = match cmd {
        // `node.drain` is the typed object: it builds `DrainNode` and ends the call as the transport did.
        NodeCommand::Drain => {
            let birth = rafka_node_admin_client::ExactBirth { target: target.clone(), node_id, incarnation };
            let drain = rafka_node_admin_client::DrainContext::new(rafka_node_admin_client::BuildId(build_id), attempt, node.name.clone());
            admission_of(rafka_node_admin_client::NodeRpc::new(client).drain(&birth, &drain, &CallOptions::default()).await)
        }
        // `node.stop` and `node.start` are held until the birth has parked or rejoined, and answer
        // `Stopped` or `Started` on that call's own reply: the reply is the completion.
        NodeCommand::Stop => {
            let birth = rafka_node_admin_client::ExactBirth { target: target.clone(), node_id, incarnation };
            let stop = rafka_node_admin_client::StopContext::new(rafka_node_admin_client::BuildId(build_id), attempt, node.name.clone());
            let opts = CallOptions { budget: rafka_node_rpc::Budget::Split { send: Duration::from_secs(5), reply: crate::node_self::drain_deadline_from_env() + Duration::from_secs(10) }, ..Default::default() };
            admission_of(rafka_node_admin_client::NodeRpc::new(client).stop(&birth, &stop, &opts).await)
        }
        NodeCommand::Start => {
            let birth = rafka_node_admin_client::ExactBirth { target: target.clone(), node_id, incarnation };
            let start = rafka_node_admin_client::StartContext::new(rafka_node_admin_client::BuildId(build_id), attempt, node.name.clone());
            let opts = CallOptions { budget: rafka_node_rpc::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(60) }, ..Default::default() };
            admission_of(rafka_node_admin_client::NodeRpc::new(client).start(&birth, &start, &opts).await)
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
    tracing::info_span!("rdm.node_admin.node.update.via-command-sent", node = %node.name, command = cmd.operation_prefix(), operation = %ctx.operation, build_id = %ctx.build_id, attempt = ctx.attempt, admission = admission.name(), "otel.kind" = "internal")
        .in_scope(|| tracing::info!("the command was sent to the exact birth"));
    admission
}

/// The arm a `node.drain`, `node.stop` or `node.start` object call ended as: only `Applied` and `AlreadyApplied` admit it; every
/// other reply is `Refused` by its name, an unsent call `NotSent`, an unanswered one `Indeterminate`.
fn admission_of(r: Result<StatusReply, rafka_node_admin_client::CallEnd>) -> CommandAdmission {
    use rafka_node_admin_client::CallEnd;
    match r {
        Ok(StatusReply::Applied) => CommandAdmission::Admitted,
        Ok(StatusReply::AlreadyApplied) => CommandAdmission::AlreadyAdmitted,
        Ok(StatusReply::Stopped { receipt }) => CommandAdmission::Stopped { receipt },
        Ok(StatusReply::Started) => CommandAdmission::Started,
        Ok(StatusReply::StartFailed { step, reason }) => CommandAdmission::StartFailed { step, reason },
        Ok(other) => CommandAdmission::Refused { reply: format!("{}: {other:?}", other.name()) },
        Err(CallEnd::NotSent { reason }) => CommandAdmission::NotSent { reason },
        Err(CallEnd::Indeterminate { reason }) => CommandAdmission::Indeterminate { reason },
        Err(CallEnd::Unserved { op }) => CommandAdmission::Refused { reply: format!("unserved op {op:#04x}") },
        Err(CallEnd::RejectedStale { target_node_id }) => CommandAdmission::Refused { reply: format!("stale target {target_node_id}") },
        Err(other) => CommandAdmission::Refused { reply: other.to_string() },
    }
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
        Some((from, held, _)) if from == node_id => match held {
            Some(held) if held != incarnation => StatusReply::RejectedStaleIncarnation { held: held.clone() },
            Some(_) => {
                let key = CommandKey { build_id: build_id.clone(), attempt: *attempt, operation: operation.clone(), node_id: node_id.clone(), incarnation: incarnation.clone() };
                match commands.complete(&key) {
                    Accepted::First => StatusReply::Applied,
                    Accepted::Again => StatusReply::AlreadyApplied,
                    Accepted::NoOpenCommand { field, expected, reported } => StatusReply::RejectedUnmatchedCompletion { field, expected, reported },
                }
            }
            None => StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown },
        },
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
    /// for a command that is not open names the first field that differs from the held command.
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
        assert_eq!(book.complete(&other), Accepted::NoOpenCommand { field: "operation".into(), expected: "drain-node:mesh1.rpc.1".into(), reported: "stop-node:mesh1.rpc.1".into() });
        let attempt = CommandKey { attempt: 2, ..k.clone() };
        assert_eq!(book.complete(&attempt), Accepted::NoOpenCommand { field: "attempt".into(), expected: "1".into(), reported: "2".into() });
        let stranger = CommandKey { node_id: NodeId::mint(), ..k.clone() };
        assert!(matches!(book.complete(&stranger), Accepted::NoOpenCommand { field, .. } if field == "command"));
    }

    /// CONTRACT: a completion is answered by the one fact that is wrong. The subject's own
    /// completion for its open command is `Applied` (a repeat `AlreadyApplied`); one reporting an
    /// incarnation other than the sender's is `RejectedStaleIncarnation` with the held one; one
    /// naming no open command is `RejectedUnmatchedCompletion` naming the differing field; one from
    /// another birth is `RejectedNotAuthority`. NotReady stands for none of them.
    #[test]
    fn a_completion_is_answered_by_the_fact_that_is_wrong() {
        let book = CommandBook::default();
        let k = key("drain-node:mesh1.rpc.1");
        let _ = book.open(k.clone());
        let req = |attempt: u32, incarnation: &IncarnationId| StatusRequest::NodeDrained { node_id: k.node_id.clone(), incarnation: incarnation.clone(), build_id: k.build_id.clone(), attempt, operation: k.operation.clone() };
        fn sender_of<'a>(id: &'a NodeId, inc: &'a IncarnationId) -> Option<(&'a NodeId, Option<&'a IncarnationId>, &'static str)> {
            Some((id, Some(inc), "mesh1.rpc.1"))
        }

        assert_eq!(accept_completion(&book, "mesh1.admin.1", sender_of(&k.node_id, &k.incarnation), &req(1, &k.incarnation)), StatusReply::Applied);
        assert_eq!(accept_completion(&book, "mesh1.admin.1", sender_of(&k.node_id, &k.incarnation), &req(1, &k.incarnation)), StatusReply::AlreadyApplied);
        let newer = IncarnationId("newer".into());
        assert_eq!(accept_completion(&book, "mesh1.admin.1", sender_of(&k.node_id, &newer), &req(1, &k.incarnation)), StatusReply::RejectedStaleIncarnation { held: newer.clone() });
        assert_eq!(
            accept_completion(&book, "mesh1.admin.1", sender_of(&k.node_id, &k.incarnation), &req(2, &k.incarnation)),
            StatusReply::RejectedUnmatchedCompletion { field: "attempt".into(), expected: "1".into(), reported: "2".into() }
        );
        let other = NodeId::mint();
        assert!(matches!(accept_completion(&book, "mesh1.admin.1", Some((&other, Some(&k.incarnation), "mesh1.rpc.2")), &req(1, &k.incarnation)), StatusReply::RejectedNotAuthority { .. }));
    }
}
