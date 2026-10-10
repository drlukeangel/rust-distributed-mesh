//! The single calls of the `node` family that travel over node-RPC today, and the one way any
//! object call ends.
//!
//! Each object builds today's request (`node.drain` → `StatusRequest::DrainNode`, `node.declare`
//! → `DeclareNodeState`, `node.apply` → `ApplyNodeState`, `node.get` of one exact birth →
//! `ProbeNodeState`, `node.topology.get` → `GetTopology` on op `0x1E`), sends it with the
//! node-RPC client and returns the typed reply. A caller never builds the request.

use crate::names::NodeOp;
use crate::{BuildId, ClientError};
use rafka_mesh_entity::{IncarnationId, NodeId, PathName};
use rafka_node_rpc::stream::StreamItem;
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::status::{NodeState, Status, StatusReply, StatusRequest};
use rafka_node_rpc_contract::streaming::FrameKind;
use rafka_node_rpc_contract::topology::{SourceVersion, Topology, TopologyReply, TopologyRequest};

/// How an object call ended when it did not produce its typed reply.
///
/// `NotSent` and `Indeterminate` are the commit-cut outcomes of `node-rpc-envelope.md`: before
/// the cut the request provably reached no handler; after it, nothing proves whether it ran. A
/// broken stream is one of the two, never an inferred step failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallEnd {
    /// The request provably never reached a handler.
    NotSent {
        /// Why, as the transport named it.
        reason: String,
    },
    /// The request committed and no stronger proof exists that it did or did not run.
    Indeterminate {
        /// Why no stronger proof exists.
        reason: String,
    },
    /// The receiver does not serve the op; nothing was dispatched.
    Unserved {
        /// The op tag.
        op: u8,
    },
    /// The request named a node the receiver is not; nothing was dispatched.
    RejectedStale {
        /// The node the caller addressed.
        target_node_id: String,
    },
    /// The control API refused the call, with its status and named reason.
    Refused {
        /// The HTTP status of the refusal.
        status: u16,
        /// The named reason.
        error: String,
        /// The detail given with the reason.
        detail: String,
    },
    /// The typed object exists and its request is valid, and no op carries it today.
    NotBackedToday {
        /// The call.
        op: NodeOp,
        /// What carries it once it is backed.
        why: &'static str,
    },
    /// A Build receipt names an operation or step this client does not recognise. A reader
    /// that does not recognise a value refuses it by name.
    UnrecognisedReceipt {
        /// The receipt's operation key.
        operation: String,
        /// The receipt's step.
        step: String,
    },
}

impl std::fmt::Display for CallEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotSent { reason } => write!(f, "not sent: {reason}"),
            Self::Indeterminate { reason } => write!(f, "indeterminate: {reason}"),
            Self::Unserved { op } => write!(f, "unserved op {op:#04x}"),
            Self::RejectedStale { target_node_id } => write!(f, "stale target {target_node_id}"),
            Self::Refused { status, error, detail } => write!(f, "refused ({status} {error}): {detail}"),
            Self::NotBackedToday { op, why } => write!(f, "{} is not backed by an op today: {why}", op.name()),
            Self::UnrecognisedReceipt { operation, step } => write!(f, "a receipt names operation {operation} step {step}, which this client does not recognise"),
        }
    }
}

impl std::error::Error for CallEnd {}

impl CallEnd {
    /// The span outcome word of this ending.
    pub fn outcome(&self) -> &'static str {
        match self {
            Self::NotSent { .. } => "not-sent",
            Self::Indeterminate { .. } => "indeterminate",
            Self::Unserved { .. } => "unserved",
            Self::RejectedStale { .. } => "rejected-stale",
            Self::Refused { .. } => "refused",
            Self::NotBackedToday { .. } => "not-backed-today",
            Self::UnrecognisedReceipt { .. } => "unrecognised-receipt",
        }
    }
}

impl From<ClientError> for CallEnd {
    fn from(e: ClientError) -> Self {
        match e {
            ClientError::Refused { status, error, detail } => Self::Refused { status, error, detail },
            ClientError::Transport { url, reason, request_written: false } => Self::NotSent { reason: format!("{url}: {reason}") },
            ClientError::Transport { url, reason, request_written: true } => Self::Indeterminate { reason: format!("{url}: {reason}") },
        }
    }
}

fn ended<R>(out: RpcOutcome<R>) -> Result<R, CallEnd> {
    match out {
        RpcOutcome::Reply(r) => Ok(r.into_value()),
        RpcOutcome::NotSent(n) => Err(CallEnd::NotSent { reason: format!("{:?}", n.reason()) }),
        RpcOutcome::Unserved(u) => Err(CallEnd::Unserved { op: u.op() }),
        RpcOutcome::RejectedStale(r) => Err(CallEnd::RejectedStale { target_node_id: r.target_node_id().to_string() }),
        RpcOutcome::Indeterminate(i) => Err(CallEnd::Indeterminate { reason: format!("{:?}", i.reason()) }),
    }
}

/// The exact birth a node-RPC call is for: where to reach it and which node and incarnation it
/// must be. A call that names the wrong birth is refused by the receiver, never applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactBirth {
    /// Where the call goes.
    pub target: NodeTarget,
    /// The node the call is about.
    pub node_id: NodeId,
    /// The incarnation of that birth.
    pub incarnation: IncarnationId,
}

/// The Build, attempt and node a `node.drain` belongs to. The operation key
/// (`drain-node:<path.name>`) is derived here and never written by a caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainContext {
    build_id: BuildId,
    attempt: u32,
    node: PathName,
}

impl DrainContext {
    /// The drain of `node` that attempt `attempt` of Build `build_id` holds.
    pub fn new(build_id: BuildId, attempt: u32, node: PathName) -> Self {
        Self { build_id, attempt, node }
    }

    /// The operation key the Build records the drain under.
    pub fn operation(&self) -> String {
        format!("drain-node:{}", self.node)
    }
}

/// The node-RPC half of the `node` objects: single calls to one exact birth.
pub struct NodeRpc<'a> {
    client: &'a NodeRpcClient,
}

impl<'a> NodeRpc<'a> {
    /// The objects over `client`.
    pub fn new(client: &'a NodeRpcClient) -> Self {
        Self { client }
    }

    async fn status(&self, op: NodeOp, birth: &ExactBirth, req: StatusRequest, opts: &CallOptions) -> Result<StatusReply, CallEnd> {
        let span = op.span();
        let _ = &birth.node_id;
        let (out, _) = tracing::Instrument::instrument(self.client.call::<Status>(&birth.target, &req, opts), span.clone()).await;
        let r = ended(out);
        span.record("outcome", match &r {
            Ok(reply) => reply.name(),
            Err(e) => e.outcome(),
        });
        span.in_scope(|| tracing::info!(op = op.name(), "an object call ended"));
        r
    }

    /// `node.drain`: tell the exact birth to refuse new work and finish what it holds, and keep
    /// running. `Applied` and `AlreadyApplied` admit the command; completion arrives later as
    /// the birth's `node-drained` call.
    pub async fn drain(&self, birth: &ExactBirth, ctx: &DrainContext, opts: &CallOptions) -> Result<StatusReply, CallEnd> {
        let req = StatusRequest::DrainNode {
            node_id: birth.node_id.clone(),
            incarnation: birth.incarnation.clone(),
            build_id: ctx.build_id.0.clone(),
            attempt: ctx.attempt,
            operation: ctx.operation(),
        };
        self.status(NodeOp::Drain, birth, req, opts).await
    }

    /// `node.declare`: the birth tells its authority the status it committed.
    pub async fn declare(&self, birth: &ExactBirth, state: NodeState, opts: &CallOptions) -> Result<StatusReply, CallEnd> {
        let req = StatusRequest::DeclareNodeState { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), state };
        self.status(NodeOp::Declare, birth, req, opts).await
    }

    /// `node.apply`: the authority sets the exact birth's status.
    pub async fn apply(&self, birth: &ExactBirth, state: NodeState, opts: &CallOptions) -> Result<StatusReply, CallEnd> {
        let req = StatusRequest::ApplyNodeState { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), state };
        self.status(NodeOp::Apply, birth, req, opts).await
    }

    /// `node.get` of one exact birth: it reasserts its presence and answers its current state.
    /// No transition.
    pub async fn get(&self, birth: &ExactBirth, opts: &CallOptions) -> Result<StatusReply, CallEnd> {
        let req = StatusRequest::ProbeNodeState { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone() };
        self.status(NodeOp::Get, birth, req, opts).await
    }

    /// `node.topology.get`: the topology the node at `target` holds, `mesh` or every mesh, as the
    /// frames of the read in order. A stream that breaks before its terminal is `Indeterminate`.
    pub async fn topology_get(&self, target: &NodeTarget, mesh: Option<String>, since: Option<SourceVersion>, opts: &CallOptions) -> Result<Vec<TopologyReply>, CallEnd> {
        let span = NodeOp::TopologyGet.span();
        let r = tracing::Instrument::instrument(self.read_topology(target, TopologyRequest::GetTopology { mesh, since }, opts), span.clone()).await;
        span.record("outcome", match &r {
            Ok(_) => "read",
            Err(e) => e.outcome(),
        });
        span.in_scope(|| tracing::info!(op = NodeOp::TopologyGet.name(), "an object call ended"));
        r
    }

    async fn read_topology(&self, target: &NodeTarget, req: TopologyRequest, opts: &CallOptions) -> Result<Vec<TopologyReply>, CallEnd> {
        let (mut stream, _) = match self.client.call_stream::<Topology>(target, &req, opts).await {
            Ok(s) => s,
            Err((out, _)) => {
                return match ended(out) {
                    Err(e) => Err(e),
                    Ok(refusal) => Ok(vec![refusal]),
                }
            }
        };
        let mut frames = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                StreamItem::Frame(kind, frame) => {
                    frames.push(frame);
                    if kind == FrameKind::Terminal {
                        break;
                    }
                }
                StreamItem::Failed(f) => return Err(CallEnd::Indeterminate { reason: format!("{f:?}") }),
            }
        }
        Ok(frames)
    }
}
