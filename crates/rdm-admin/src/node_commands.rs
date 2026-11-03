//! The open node commands (`stop-node`, `start-node`) the owning mesh-admin has sent and awaits a
//! completion for (node-stop.md, node-start.md; node-rpc-envelope.md, "Node stop command pair").
//!
//! A command is sent to one exact birth. `stop-node` is answered `Stopped` once the birth is
//! stopped, and that reply is node-left: no upward node-left call exists (R-ON1). `Reserved6` and
//! `Reserved7` are reserved positions (R-CR4), refused by name ([`refuse_reserved`]). A completion is matched to
//! the open command by `(operation, node_id, incarnation)`: a node command is no Build's (Q342). A completion with no open command is
//! refused by name; a repeat of an accepted completion is `AlreadyApplied`. This is the RPC
//! completion inbox of the executing admin: it reads no gossip and writes nothing durable (the
//! pipeline's step receipt is the durable record).

use crate::deployment::pipeline::{CommandContext, Completion};
use crate::model::{IncarnationId, Node, NodeId};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// Which of the two commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeCommand {
    /// `node.connection.delete { All }`; its reply is `node.connection.deleted`.
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
            Self::DeleteConnections => "node.connection.delete",
            Self::Stop => "stop-node",
            Self::Start => "start-node",
        }
    }
    /// The completion's name.
    pub fn completion(self) -> &'static str {
        match self {
            Self::DeleteConnections => "node.connection.deleted",
            Self::Stop => "node-left",
            Self::Start => "node-started",
        }
    }
}

/// The natural key of a command and its completion.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommandKey {
    /// `node.stop:<node_id>` or `node.start:<node_id>`.
    pub operation: rdm_core::OperationId,
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
/// `node.connection.delete { All }` a commanded birth runs (this admin's own,
/// `AdminConfig::connections_delete_deadline`, which every birth it launches carries).
pub struct CommandView {
    /// Each open command: what resolves its completion, and the instant it was first sent.
    open: Mutex<HashMap<CommandKey, (watch::Sender<bool>, Instant)>>,
    connections_delete_deadline: Duration,
    /// Whether this admin, as a mesh-primary, replied `blocked` to a storage change's `mesh.block`
    /// (storage.md §2 "Changing it", step 1): it issues no new command until it is unblocked.
    blocked: watch::Sender<bool>,
    /// Whether this admin's rows are held: from its `blocked` until its `mesh.unblock` has moved it onto
    /// the provider the change decided ([`Self::release_rows`]), nothing of its mesh writes a row (step 2:
    /// the copy holds every line of the old provider; i149 R8).
    rows_held: watch::Sender<bool>,
    /// Held by a row writer while it writes (the rows writer's sweep, a node's declared-state row):
    /// `mesh.block` waits for a write in progress, so none lands after it replied `blocked`.
    row_writes: tokio::sync::Mutex<()>,
    /// The caller's stream each frame of a command to a birth is sent on to, while the primary above it
    /// runs a caller's node command on it ([`Self::caller_stream`]; node-stop.md "Upward": two one-hop streams).
    caller_streams: Mutex<HashMap<(NodeId, IncarnationId), tokio::sync::mpsc::UnboundedSender<rdm_core::node_command::NodeCommandReply>>>,
}

/// A caller's node command's hold on the frames of its birth ([`CommandView::caller_stream`]): released when
/// the command ends, however it ends.
pub struct CallerStream {
    commands: std::sync::Arc<CommandView>,
    birth: (NodeId, IncarnationId),
}

impl Drop for CallerStream {
    fn drop(&mut self) {
        self.commands.caller_streams.lock().unwrap().remove(&self.birth);
    }
}

/// A tracked command: open from its send until its call ends, its caller's cancellation included.
pub struct CommandCall<'a> {
    commands: &'a CommandView,
    key: CommandKey,
}

impl Drop for CommandCall<'_> {
    fn drop(&mut self) {
        self.commands.close(&self.key);
    }
}

/// A view whose commanded births run their `node.connection.delete { All }` under the launch default.
impl Default for CommandView {
    fn default() -> Self {
        Self::new(rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE)
    }
}

impl CommandView {
    /// A view whose commanded births run their `node.connection.delete { All }` under `connections_delete_deadline`.
    pub fn new(connections_delete_deadline: Duration) -> Self {
        Self { open: Mutex::new(HashMap::new()), connections_delete_deadline, blocked: watch::channel(false).0, rows_held: watch::channel(false).0, row_writes: tokio::sync::Mutex::new(()), caller_streams: Mutex::new(HashMap::new()) }
    }

    /// Send every frame of the commands this admin carries to the birth `(node_id, incarnation)` on to
    /// the receiver answered here, until the returned hold is dropped: the frames of the caller's node
    /// command the primary above the node runs (node-stop.md "Upward"). Work on one node is serialized
    /// at its primary, so one caller holds a birth's frames at a time.
    pub fn caller_stream(self: &std::sync::Arc<Self>, node_id: &NodeId, incarnation: &IncarnationId) -> (CallerStream, tokio::sync::mpsc::UnboundedReceiver<rdm_core::node_command::NodeCommandReply>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let birth = (node_id.clone(), incarnation.clone());
        self.caller_streams.lock().unwrap().insert(birth.clone(), tx);
        (CallerStream { commands: self.clone(), birth }, rx)
    }

    /// The frame `frame` of a command to the birth `(node_id, incarnation)`, recorded by this admin:
    /// sent on to the caller whose node command holds the birth's frames, if one does.
    pub fn on_to_caller(&self, node_id: &NodeId, incarnation: &IncarnationId, frame: &rdm_core::node_command::NodeCommandReply) {
        if let Some(tx) = self.caller_streams.lock().unwrap().get(&(node_id.clone(), incarnation.clone())) {
            let _ = tx.send(frame.clone());
        }
    }

    /// `mesh.block`: from here no new command is issued and no row is written; resolves once every
    /// command in flight has completed and a rows sweep in progress has ended (storage.md §2 "Changing
    /// it", steps 1 and 2). Answers how many commands were in flight at the block.
    pub async fn block(&self) -> usize {
        self.blocked.send_replace(true);
        self.rows_held.send_replace(true);
        let in_flight = self.open.lock().unwrap().len();
        while !self.open.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        drop(self.row_writes.lock().await);
        in_flight
    }

    /// A row writer's turn to write: `None` while this admin's rows are held (storage.md §2 step 2;
    /// i149 R8).
    pub async fn row_writes(&self) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        let turn = self.row_writes.lock().await;
        (!*self.rows_held.borrow()).then_some(turn)
    }

    /// `mesh.unblock`, this admin moved onto the provider the change decided: rows are written again,
    /// on that provider, while commands stay held until [`Self::unblock`] (storage.md §2 step 4).
    pub fn release_rows(&self) {
        self.rows_held.send_replace(false);
    }

    /// `mesh.unblock`: commands are issued and rows written again.
    pub fn unblock(&self) {
        self.rows_held.send_replace(false);
        self.blocked.send_replace(false);
    }

    /// Resolves once this admin's rows are written again: at once, or once its `mesh.unblock` moved it.
    pub async fn rows_released(&self) {
        let _ = self.rows_held.subscribe().wait_for(|held| !*held).await;
    }

    /// Whether this admin replied `blocked` and is not unblocked yet.
    pub fn is_blocked(&self) -> bool {
        *self.blocked.borrow()
    }

    /// Resolves once this admin may issue a new command: at once, or at its unblock.
    pub async fn admitted(&self) {
        let _ = self.blocked.subscribe().wait_for(|b| !*b).await;
    }

    /// How long a command whose birth runs a `node.connection.delete { All }` (the command itself,
    /// or a stop) waits for its reply: the bound the birth runs it under, and 10 s for the rest of the step.
    pub fn connections_delete_reply(&self) -> Duration {
        self.connections_delete_deadline + Duration::from_secs(10)
    }

    /// Track a call: its command is open until the call ends, however it ends, a cancelled caller's
    /// included.
    pub fn track(&self, key: CommandKey) -> CommandCall<'_> {
        let _ = self.open(key.clone());
        CommandCall { commands: self, key }
    }

    /// Open `key` before the command is sent (so a completion that races the reply finds it) and
    /// return what resolves when the completion is accepted. Opening again is the same command.
    pub fn open(&self, key: CommandKey) -> watch::Receiver<bool> {
        self.open.lock().unwrap().entry(key).or_insert_with(|| (watch::channel(false).0, Instant::now())).0.subscribe()
    }

    /// Accept the completion for `key`.
    pub fn complete(&self, key: &CommandKey) -> Accepted {
        let open = self.open.lock().unwrap();
        match open.get(key) {
            Some((tx, _)) => {
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
                held.sort_by_key(|k| (k.operation != key.operation, k.operation.clone()));
                match held.first() {
                    None => Accepted::NoOpenCommand { field: "command".into(), expected: "an open stop-node or start-node for this birth".into(), reported: format!("{:?}", key.operation) },
                    Some(k) => Accepted::NoOpenCommand { field: "operation".into(), expected: format!("{:?}", k.operation), reported: format!("{:?}", key.operation) },
                }
            }
        }
    }

    /// Whether a command is open under `key` (the completion has not been forgotten).
    pub fn is_open(&self, key: &CommandKey) -> bool {
        self.open.lock().unwrap().contains_key(key)
    }

    /// Whether any command (a stop, a start) is open for this exact birth: its lifecycle is that
    /// command's until the completion.
    pub fn open_for(&self, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
        self.open.lock().unwrap().keys().any(|k| &k.node_id == node_id && &k.incarnation == incarnation)
    }

    /// Whether any stop command is open for this exact birth.
    pub fn stop_open_for(&self, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
        self.open.lock().unwrap().keys().any(|k| &k.node_id == node_id && &k.incarnation == incarnation && k.operation.action == "node.stop")
    }

    /// Forget the command, however it ended: its step receipt is the record. A call to the birth that
    /// ended with its attempts spent is the spent call's mark at the primary above it (node-rpc.md §43;
    /// Q332, Q338), never this view's.
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
        R::StreamBudget | R::FrameNotSent | R::AdmissionBlocked(_) | R::Carried(_) | R::ConnectionLost(_) | R::ConnectionNoBudget { .. } | R::NotForwardable { .. } => return None,
    };
    Some(Unreachable { reason, detail: format!("{cause:?}") })
}


/// The completion of the command `ctx` names for `node`, received or not: the span every completion
/// a step reads writes, whether a wait received it or the command's own reply carried it (a stop's
/// `Stopped`, node-stop.md).
pub fn completion_span(node: &Node, cmd: NodeCommand, ctx: &CommandContext, received: bool) {
    tracing::info_span!("rdm.node_admin.node.update.via-completion-awaited", node_path = %node.path, node_name = %node.name, completion = cmd.completion(), operation = %ctx.operation.action, operation_subject = ?ctx.operation.subject, build_id = %ctx.build_id, received = received, "otel.kind" = "internal")
        .in_scope(|| tracing::info!("the completion was awaited"));
}

/// Wait up to `within` for the completion of the command `ctx` names for `node`: its
/// call, or the `Stopped` reply of its `stop-node` call (node-left).
pub async fn await_completion(commands: &CommandView, node: &Node, cmd: NodeCommand, ctx: &CommandContext, within: Duration) -> Completion {
    let Some(incarnation) = node.incarnation_id.clone() else {
        return Completion::NotAwaited { admission: format!("{} has no known birth", node.node_id) };
    };
    let key = CommandKey { operation: ctx.operation.clone(), node_id: node.node_id.clone(), incarnation };
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

/// The node commands the primary above a node runs for a caller (node-stop.md, node-start.md: never a
/// Build or part of one, Q342; D40): a caller's NodeCommand 0x22 `StopNode` or `StartNode` reaches the
/// node's mesh-primary (the fabric-primary for a mesh-primary), which admits it, runs it on the node and
/// sends the node's frames on to the caller.
#[async_trait::async_trait]
pub trait PrimaryCommands: Send + Sync {
    /// Run the caller's `req` on the node it names, `caller` the calling birth as this admin resolved
    /// it: every frame before the terminal goes on `frames`, and the terminal (or the refusal, before
    /// anything ran) is the answer.
    async fn run(&self, caller: Option<NodeId>, req: rdm_core::node_command::NodeCommandRequest, frames: Box<dyn crate::node_self::CommandFrames>) -> rdm_core::node_command::NodeCommandReply;
}

/// The primary's node command door, set once the admin's runner is built.
pub type PrimaryCommandsSlot = std::sync::Arc<std::sync::OnceLock<std::sync::Arc<dyn PrimaryCommands>>>;

#[cfg(test)]
mod tests {
    use super::*;

    fn key(op: &str) -> CommandKey {
        CommandKey { operation: rdm_core::OperationId::unscoped(op), node_id: NodeId::mint(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("i")).unwrap() }
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
        for local in [R::Connection("request does not encode: too long".into()), R::StreamBudget, R::FrameNotSent, R::AdmissionBlocked("all".into()), R::NotForwardable { op: 0x1b }, R::ConnectionNoBudget { remaining_ms: 1, reserve_ms: 2 }] {
            assert_eq!(unreachable_of(&local), None, "{local:?} is local to the sender");
        }
    }

    /// CONTRACT: a completion resolves its open command once; a repeat is a repeat; a completion
    /// for a command that is not open names the first field that differs from the held command.
    #[tokio::test]
    async fn a_completion_resolves_only_its_open_command() {
        let commands = CommandView::default();
        let k = key("node.start");
        let mut rx = commands.open(k.clone());
        assert!(!*rx.borrow());
        assert_eq!(commands.complete(&k), Accepted::First);
        rx.wait_for(|v| *v).await.unwrap();
        assert_eq!(commands.complete(&k), Accepted::Again);
        let other = CommandKey { operation: rdm_core::OperationId::node("node.stop", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "1").unwrap())), ..k.clone() };
        assert_eq!(commands.complete(&other), Accepted::NoOpenCommand { field: "operation".into(), expected: format!("{:?}", k.operation), reported: format!("{:?}", other.operation) });
        let stranger = CommandKey { node_id: NodeId::mint(), ..k.clone() };
        assert!(matches!(commands.complete(&stranger), Accepted::NoOpenCommand { field, .. } if field == "command"));
    }

    // @feature: node-records
    /// CONTRACT: a tracked call's command is open while the call runs and closed when it ends, its
    /// caller cancelled included. Must NOT happen: a cancelled call holding command admission forever.
    #[tokio::test]
    async fn a_cancelled_tracked_call_closes_its_command() {
        let commands = std::sync::Arc::new(CommandView::default());
        let k = key("storage-block:change");
        let began = std::sync::Arc::new(tokio::sync::Notify::new());
        let (c, request, n) = (commands.clone(), k.clone(), began.clone());
        let task = tokio::spawn(async move {
            let _call = c.track(request);
            n.notify_one();
            std::future::pending::<()>().await;
        });
        began.notified().await;
        assert!(commands.is_open(&k));
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(!commands.is_open(&k), "a cancelled call cannot hold command admission forever");
    }

}
