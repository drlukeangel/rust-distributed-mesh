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
use rafka_node_rpc_contract::status::{Status, StatusReply, StatusRequest};
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
    /// The receiver refused the call by name; nothing was accepted. `reason` is the refusal's name
    /// (`not-fabric-primary`, `build-in-progress`, `unknown-node`, ...) and `detail` what it carries.
    Rejected {
        /// The refusal's name.
        reason: String,
        /// What the refusal says.
        detail: String,
    },
    /// Another Build is still reconciling: one accepted topology, one Build in flight. `current` is
    /// that Build; a caller that must go after it follows it to its end by re-submitting its id.
    BuildInProgress {
        /// The Build in flight.
        current: BuildId,
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
            Self::Rejected { reason, detail } => write!(f, "rejected ({reason}): {detail}"),
            Self::BuildInProgress { current } => write!(f, "Build {current} is still reconciling; one Build at a time"),
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
            Self::Rejected { .. } => "rejected",
            Self::BuildInProgress { .. } => "build-in-progress",
            Self::NotBackedToday { .. } => "not-backed-today",
            Self::UnrecognisedReceipt { .. } => "unrecognised-receipt",
        }
    }
}

impl CallEnd {
    /// The name the node-RPC outcome algebra gives this ending (`NotSent`, `Indeterminate`, …), for
    /// the callers whose span attributes already carry it.
    pub fn rpc_name(&self) -> &'static str {
        match self {
            Self::NotSent { .. } => "NotSent",
            Self::Indeterminate { .. } => "Indeterminate",
            Self::Unserved { .. } => "Unserved",
            Self::RejectedStale { .. } => "RejectedStale",
            other => other.outcome(),
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

pub(crate) fn ended<R>(out: RpcOutcome<R>) -> Result<R, CallEnd> {
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

/// The membership one node holds of a mesh, read through `node.topology.get`: the births it holds
/// and the open overlays and retained departures it knows. What a peer reports of another node.
#[derive(Debug, Clone, Default)]
pub struct HeldView {
    /// The births the node holds.
    pub members: Vec<rafka_mesh_entity::MeshDigest>,
    /// The open lifecycle overlays (restart, stop, delete) the node holds.
    pub in_flight: Vec<rafka_mesh_entity::LifecycleOp>,
    /// The departures the node retains.
    pub departed: Vec<rafka_mesh_entity::LifecycleOp>,
}

impl HeldView {
    /// The birth the node holds of `node_id`, if it holds one.
    pub fn birth(&self, node_id: &NodeId) -> Option<&rafka_mesh_entity::MeshDigest> {
        self.members.iter().find(|d| &d.node.node_id == node_id)
    }

    /// The overlay the node holds for `node_id` whose operation starts with `prefix`
    /// (`stop-node:`, `restart-node:`), if it holds one.
    pub fn overlay(&self, node_id: &NodeId, prefix: &str) -> Option<&rafka_mesh_entity::LifecycleOp> {
        self.in_flight.iter().find(|o| &o.node_id == node_id && o.operation.starts_with(prefix))
    }
}

/// The Build, attempt and node a `node.stop` belongs to. The operation key (`stop-node:<path.name>`)
/// is derived here and never written by a caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopContext {
    build_id: BuildId,
    attempt: u32,
    node: PathName,
}

impl StopContext {
    /// The stop of `node` that attempt `attempt` of Build `build_id` holds.
    pub fn new(build_id: BuildId, attempt: u32, node: PathName) -> Self {
        Self { build_id, attempt, node }
    }

    /// The operation key the Build records the stop under.
    pub fn operation(&self) -> String {
        format!("stop-node:{}", self.node)
    }
}

/// The Build, attempt and node a `node.start` belongs to. The operation key (`start-node:<path.name>`)
/// is derived here and never written by a caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartContext {
    build_id: BuildId,
    attempt: u32,
    node: PathName,
}

impl StartContext {
    /// The start of `node` that attempt `attempt` of Build `build_id` holds.
    pub fn new(build_id: BuildId, attempt: u32, node: PathName) -> Self {
        Self { build_id, attempt, node }
    }

    /// The operation key the Build records the start under.
    pub fn operation(&self) -> String {
        format!("start-node:{}", self.node)
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

    /// `node.stop`: tell the exact birth to drain, cut its mesh connections and park. The call is
    /// held until the birth is parked; the reply is `Stopped` with the drain's receipt, on the stop
    /// call's own stream. A birth already parked answers the same.
    pub async fn stop(&self, birth: &ExactBirth, ctx: &StopContext, opts: &CallOptions) -> Result<StatusReply, CallEnd> {
        let req = StatusRequest::StopNode {
            node_id: birth.node_id.clone(),
            incarnation: birth.incarnation.clone(),
            build_id: ctx.build_id.0.clone(),
            attempt: ctx.attempt,
            operation: ctx.operation(),
        };
        self.status(NodeOp::Stop, birth, req, opts).await
    }

    /// `node.start`: tell the parked exact birth to rejoin as itself. The call is held until the
    /// birth has rejoined; the reply is `Started`, or `StartFailed` naming the step that failed.
    pub async fn start(&self, birth: &ExactBirth, ctx: &StartContext, opts: &CallOptions) -> Result<StatusReply, CallEnd> {
        let req = StatusRequest::StartNode {
            node_id: birth.node_id.clone(),
            incarnation: birth.incarnation.clone(),
            build_id: ctx.build_id.0.clone(),
            attempt: ctx.attempt,
            operation: ctx.operation(),
        };
        self.status(NodeOp::Start, birth, req, opts).await
    }

    /// `node.get` of one exact birth: it reasserts its presence and answers its current state.
    /// No transition.
    pub async fn get(&self, birth: &ExactBirth, opts: &CallOptions) -> Result<StatusReply, CallEnd> {
        let req = StatusRequest::ProbeNodeState { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone() };
        self.status(NodeOp::Get, birth, req, opts).await
    }

    /// `node.get` of what the node at `target` holds of `mesh`: its held births and overlays, read
    /// through `node.topology.get`. A refusal the target answers is the call's end, named.
    pub async fn held_view(&self, target: &NodeTarget, mesh: &str, opts: &CallOptions) -> Result<HeldView, CallEnd> {
        let frames = self.topology_get(target, Some(mesh.to_string()), None, opts).await?;
        let mut view = HeldView::default();
        for f in frames {
            match f {
                TopologyReply::Snapshot { digests, in_flight, departed, .. } => {
                    view.members.extend(digests.into_iter().map(rafka_mesh_entity::MeshDigest::from));
                    view.in_flight.extend(in_flight);
                    view.departed.extend(departed);
                }
                TopologyReply::Started | TopologyReply::End { .. } | TopologyReply::Unchanged { .. } | TopologyReply::Seats { .. } | TopologyReply::RafkaTime { .. } => {}
                other => return Err(CallEnd::Indeterminate { reason: format!("{} answered {other:?} to the topology read", target_name(target)) }),
            }
        }
        Ok(view)
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
