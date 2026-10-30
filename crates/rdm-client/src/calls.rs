//! The single calls of the `node` family that travel over node-RPC today, and the one way any
//! object call ends.
//!
//! Each object builds today's request (`node.stop` → `NodeCommandRequest::StopNode`, `node.declare`
//! → `DeclareNodeState`, `node.apply` → `ApplyNodeState`, `node.get` of one exact birth →
//! `ProbeNodeState`, `node.topology.get` → `GetTopology` on op `0x1E`), sends it with the
//! node-RPC client and returns the typed reply. A caller never builds the request.

use rdm_core::names::NodeOp;
use crate::{BuildId, ClientError};
use rdm_core::{IncarnationId, NodeId, PathName};
use rdm_transport::stream::StreamItem;
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget, RouteChoice};
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{Status, StatusReply, StatusRequest};
use rdm_core::streaming::FrameKind;
use rdm_core::topology::{SourceVersion, Topology, TopologyReply, TopologyRequest};

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
        /// The transport's own reason, typed: what a caller reads to tell a reachability failure
        /// (the target refused or did not answer the connect, or its endpoint did not resolve) from a
        /// local one.
        cause: rdm_core::outcome::NotSentReason,
    },
    /// The request committed and no stronger proof exists that it did or did not run.
    Indeterminate {
        /// The standard code captured from the typed delivery outcome.
        code: rdm_core::outcome::Code,
        /// Why no stronger proof exists.
        reason: String,
    },
    /// The receiver does not serve the op; nothing was dispatched.
    Unserved {
        /// The opcode.
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
    /// A read of one entity the answering node does not hold (`node.get { id }`: `NOT_FOUND`, Q165).
    NotFound {
        /// The read.
        read: &'static str,
        /// What was asked for and who answered.
        detail: String,
    },
}

impl std::fmt::Display for CallEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotSent { reason, .. } => write!(f, "not sent: {reason}"),
            Self::Indeterminate { reason, .. } => write!(f, "indeterminate: {reason}"),
            Self::Unserved { op } => write!(f, "unserved op {op:#04x}"),
            Self::RejectedStale { target_node_id } => write!(f, "stale target {target_node_id}"),
            Self::Refused { status, error, detail } => write!(f, "refused ({status} {error}): {detail}"),
            Self::Rejected { reason, detail } => write!(f, "rejected ({reason}): {detail}"),
            Self::BuildInProgress { current } => write!(f, "Build {current} is still reconciling; one Build at a time"),
            Self::NotBackedToday { op, why } => write!(f, "{} is not backed by an op today: {why}", op.name()),
            Self::UnrecognisedReceipt { operation, step } => write!(f, "a receipt names operation {operation} step {step}, which this client does not recognise"),
            Self::NotFound { read, detail } => write!(f, "{read}: not found: {detail}"),
        }
    }
}

impl std::error::Error for CallEnd {}

impl CallEnd {
    /// Standard code selected from this typed ending, never its explanation.
    pub fn code(&self) -> rdm_core::outcome::Code {
        use rdm_core::outcome::Code;
        match self {
            Self::NotSent { cause, .. } => cause.code(),
            Self::Indeterminate { code, .. } => *code,
            Self::Unserved { .. } | Self::NotBackedToday { .. } => Code::Unimplemented,
            Self::RejectedStale { .. } => Code::Aborted,
            Self::UnrecognisedReceipt { .. } => Code::DataLoss,
            Self::NotFound { .. } => Code::NotFound,
            Self::Refused { .. } | Self::Rejected { .. } | Self::BuildInProgress { .. } => Code::FailedPrecondition,
        }
    }
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
            Self::NotFound { .. } => "not-found",
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
            ClientError::Transport { url, reason, request_written: false } => {
                let reason = format!("{url}: {reason}");
                Self::NotSent { cause: rdm_core::outcome::NotSentReason::Connection(reason.clone()), reason }
            }
            ClientError::Transport { url, reason, request_written: true } => Self::Indeterminate { code: rdm_core::outcome::Code::Unknown, reason: format!("{url}: {reason}") },
        }
    }
}

fn target_name(t: &NodeTarget) -> String {
    format!("{t:?}")
}

pub(crate) fn ended<R>(out: RpcOutcome<R>) -> Result<R, CallEnd> {
    match out {
        RpcOutcome::Reply(r) => Ok(r.into_value()),
        RpcOutcome::NotSent(n) => Err(CallEnd::NotSent { reason: format!("{:?}", n.reason()), cause: n.reason().clone() }),
        RpcOutcome::Unserved(u) => Err(CallEnd::Unserved { op: u.op() }),
        RpcOutcome::RejectedStale(r) => Err(CallEnd::RejectedStale { target_node_id: r.target_node_id().to_string() }),
        RpcOutcome::Indeterminate(i) => Err(CallEnd::Indeterminate { code: i.code(), reason: format!("{:?}", i.reason()) }),
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

/// The topology view one node holds of a mesh, read through `node.topology.get`: the births it holds
/// and the open overlays and retained departures it knows. What a peer reports of another node.
#[derive(Debug, Clone, Default)]
pub struct HeldView {
    /// The births the node holds.
    pub members: Vec<rdm_core::MeshDigest>,
    /// The open lifecycle overlays (stop, delete) the node holds.
    pub in_flight: Vec<rdm_core::LifecycleOp>,
    /// The departures the node retains.
    pub deleted: Vec<rdm_core::LifecycleOp>,
}

impl HeldView {
    /// The birth the node holds of `node_id`, if it holds one.
    pub fn birth(&self, node_id: &NodeId) -> Option<&rdm_core::MeshDigest> {
        self.members.iter().find(|d| &d.node.node_id == node_id)
    }

    /// The overlay the node holds for `node_id` whose operation starts with `prefix`
    /// (`node.stop:`, `node.delete:`), if it holds one.
    pub fn overlay(&self, node_id: &NodeId, prefix: &str) -> Option<&rdm_core::LifecycleOp> {
        self.in_flight.iter().find(|o| &o.node_id == node_id && o.operation.starts_with(prefix))
    }
}

/// The Build, attempt and node a `node.stop` belongs to. The operation key (`node.stop:<path.name>`)
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
        format!("node.stop:{}", self.node)
    }
}

/// The Build, attempt and node a `node.start` belongs to. The operation key (`node.start:<path.name>`)
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
        format!("node.start:{}", self.node)
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

    /// `node.stop`: tell the exact birth to delete its connections, cut its transport and stop. The
    /// command stream reports acceptance, the All closure's receipt as Data, then NodeStopped.
    /// A birth already stopped reports its held closure and completion on the new invocation.
    pub async fn stop(&self, birth: &ExactBirth, ctx: &StopContext, opts: &CallOptions) -> Result<rdm_transport::stream::ReplyStream<rdm_core::node_command::NodeCommand>, RpcOutcome<rdm_core::node_command::NodeCommandReply>> {
        let req = rdm_core::node_command::NodeCommandRequest::StopNode {
            node_id: birth.node_id.clone(),
            incarnation: birth.incarnation.clone(),
            build_id: ctx.build_id.to_string(),
            attempt: ctx.attempt,
            operation: ctx.operation(),
        };
        match self.client.call_stream::<rdm_core::node_command::NodeCommand>(&birth.target, &req, opts).await {
            Ok((stream, _)) => Ok(stream),
            Err((out, _)) => Err(out),
        }
    }

    /// `node.start`: tell the stopped exact birth to rejoin as itself. The call is held until the
    /// birth reports NodeJoined after joining, then NodeStarted after every gate, or Failed with
    /// its step, reason and code. This is only the exact-node hop, never the caller's Build carrier.
    pub async fn start(&self, birth: &ExactBirth, ctx: &StartContext, opts: &CallOptions) -> Result<rdm_transport::stream::ReplyStream<rdm_core::node_command::NodeCommand>, RpcOutcome<rdm_core::node_command::NodeCommandReply>> {
        let req = rdm_core::node_command::NodeCommandRequest::StartNode {
            node_id: birth.node_id.clone(),
            incarnation: birth.incarnation.clone(),
            build_id: ctx.build_id.to_string(),
            attempt: ctx.attempt,
            operation: ctx.operation(),
        };
        match self.client.call_stream::<rdm_core::node_command::NodeCommand>(&birth.target, &req, opts).await {
            Ok((stream, _)) => Ok(stream),
            Err((out, _)) => Err(out),
        }
    }

    /// `node.config.update`, the caller's single call to the fabric-primary `receiver`
    /// (node-config-update.md step 1): put the birth's whole config. The fabric-primary writes the map
    /// to the node in its atlas and sends the command to the node's mesh-primary, the writer
    /// of that mesh's rows. `route` is the caller's choice: `Direct`, or `ViaPeer` through a carrier
    /// (Forward `0x1A`) when the receiver is not directly reachable, executed exactly as chosen
    /// (`NodeRpcClient::call_routed`). `ConfigUpdated` is `node.config.updated`: the mesh-primary wrote
    /// the node's row and the birth applied the config. A caller that is not a node-admin may send it to
    /// any node-admin: one that is not the fabric-primary carries it up and hands the answer back
    /// ([`Self::own_config_update`]). A node-admin's call at an admin holding neither the fabric-primary
    /// nor the node's mesh-primary is refused `RejectedNotPrimary`, naming the fabric-primary.
    pub async fn config_update(&self, receiver: &NodeId, route: &RouteChoice, birth: &ExactBirth, config: std::collections::BTreeMap<String, String>, opts: &CallOptions) -> Result<StatusReply, CallEnd> {
        let req = StatusRequest::UpdateNodeConfig { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), config };
        let span = NodeOp::ConfigUpdate.span();
        let (out, _, _) = tracing::Instrument::instrument(self.client.call_routed::<Status>(receiver, route, &req, opts), span.clone()).await;
        let r = ended(out);
        span.record("outcome", match &r {
            Ok(reply) => reply.name(),
            Err(e) => e.outcome(),
        });
        span.in_scope(|| tracing::info!(op = NodeOp::ConfigUpdate.name(), route = route.token(), "an object call ended"));
        r
    }

    /// A node's own `node.config.update` (node-config-update.md, a node's own update, step 1; Q235), sent
    /// to `mesh_primary`, the mesh-primary its topology view names: put the whole config of `birth`, the
    /// node itself. The mesh-primary writes the row and its atlas and owes the row up to the
    /// fabric-primary; the node is sent no `ApplyNodeConfig`. `opts` as the caller's (`Budget::Owed`:
    /// owed until it answers). `ConfigUpdated` is `node.config.updated`, carrying the row as written
    /// (its config and revision); a refusal by name carries the row as it stands where the mesh-primary
    /// holds one (`ConfigRowRefused`).
    pub async fn own_config_update(&self, mesh_primary: &NodeTarget, birth: &ExactBirth, config: std::collections::BTreeMap<String, String>, opts: &CallOptions) -> Result<StatusReply, CallEnd> {
        let req = StatusRequest::UpdateNodeConfig { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), config };
        let span = NodeOp::ConfigUpdate.span();
        let (out, _) = tracing::Instrument::instrument(self.client.call::<Status>(mesh_primary, &req, opts), span.clone()).await;
        let r = ended(out);
        span.record("outcome", match &r {
            Ok(reply) => reply.name(),
            Err(e) => e.outcome(),
        });
        span.in_scope(|| tracing::info!(op = NodeOp::ConfigUpdate.name(), route = "mesh-primary", "an object call ended"));
        r
    }

    /// The mesh-primary's call to the exact birth of its mesh, direct: hold this whole config, the row's
    /// as written at `revision` (node-config-update.md, a caller's update, step 3: it installs the map
    /// over an older revision).
    pub async fn config_apply(&self, birth: &ExactBirth, config: std::collections::BTreeMap<String, String>, revision: u64, opts: &CallOptions) -> Result<StatusReply, CallEnd> {
        let req = StatusRequest::ApplyNodeConfig { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), config, revision };
        self.status(NodeOp::ConfigUpdate, birth, req, opts).await
    }

    /// `node.tag.create` of `labels` on the exact birth, the caller's single call to the node's
    /// mesh-primary `mesh_primary` (R-TG1; node-rpc-envelope.md node.tags): it writes the node's row
    /// carrying them and puts each label's key of the tags index, and answers `TagsCreated` naming
    /// any label the node already carried. `route` as `Self::node_update`.
    pub async fn tags_create(&self, mesh_primary: &NodeId, route: &RouteChoice, birth: &ExactBirth, labels: &std::collections::BTreeSet<String>, opts: &CallOptions) -> Result<StatusReply, CallEnd> {
        let req = StatusRequest::CreateTags { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), labels: labels.clone() };
        self.row_writer_call(NodeOp::TagsCreate, mesh_primary, route, &req, opts).await
    }

    /// `node.tag.delete` of `scope` on the exact birth, as [`Self::tags_create`]: the row written
    /// without the labels and each removed label's key of the tags index tombstoned; answered
    /// `TagsDeleted` naming any named label the node did not carry.
    pub async fn tags_delete(&self, mesh_primary: &NodeId, route: &RouteChoice, birth: &ExactBirth, scope: &rdm_core::status::TagScope, opts: &CallOptions) -> Result<StatusReply, CallEnd> {
        let req = StatusRequest::DeleteTags { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), scope: scope.clone() };
        self.row_writer_call(NodeOp::TagsDelete, mesh_primary, route, &req, opts).await
    }

    /// One call of `op` to the row writer `mesh_primary` over `route`, its span saying how it ended.
    async fn row_writer_call(&self, op: NodeOp, mesh_primary: &NodeId, route: &RouteChoice, req: &StatusRequest, opts: &CallOptions) -> Result<StatusReply, CallEnd> {
        let span = op.span();
        let (out, _, _) = tracing::Instrument::instrument(self.client.call_routed::<Status>(mesh_primary, route, req, opts), span.clone()).await;
        let r = ended(out);
        span.record("outcome", match &r {
            Ok(reply) => reply.name(),
            Err(e) => e.outcome(),
        });
        span.in_scope(|| tracing::info!(op = op.name(), route = route.token(), "an object call ended"));
        r
    }

    /// `node.name.update` of the exact birth, the caller's single call to the node's mesh-primary
    /// `mesh_primary` (proposal node-properties-own-commands.md §2; R13, R-CR1): `name` is the name
    /// only, never its path. The mesh-primary validates from its held record, claims the name,
    /// moves the row, tells a live birth `ApplyNodeName` and answers `NameUpdated` after
    /// the birth answered; a stopped birth is told nothing and the call completes on the row. `route`
    /// is the caller's choice: `Direct` to the mesh-primary, or `ViaPeer` through a carrier (Forward
    /// `0x1A`), executed exactly as chosen (`NodeRpcClient::call_routed`).
    pub async fn name_update(&self, mesh_primary: &NodeId, route: &RouteChoice, birth: &ExactBirth, name: &str, opts: &CallOptions) -> Result<StatusReply, CallEnd> {
        let req = StatusRequest::UpdateName { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), name: name.to_string() };
        self.row_writer_call(NodeOp::NameUpdate, mesh_primary, route, &req, opts).await
    }

    /// `node.connection.delete` of the exact birth's connections named by `scope`, the caller's single
    /// call to the node's mesh-primary `mesh_primary` (R-CR2, R-CR3): it carries the cut to the exact
    /// birth and holds the birth's `node.connections.deleted` for its rename precondition, answering the
    /// birth's reply. `route` is executed exactly as chosen (`NodeRpcClient::call_routed`).
    pub async fn connections_delete(&self, mesh_primary: &NodeId, route: &RouteChoice, birth: &ExactBirth, scope: &rdm_core::status::ConnectionScope, opts: &CallOptions) -> Result<StatusReply, CallEnd> {
        let req = StatusRequest::DeleteConnections { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), scope: scope.clone() };
        let span = NodeOp::ConnectionsDelete.span();
        let (out, _, _) = tracing::Instrument::instrument(self.client.call_routed::<Status>(mesh_primary, route, &req, opts), span.clone()).await;
        let r = ended(out);
        span.record("outcome", match &r {
            Ok(reply) => reply.name(),
            Err(e) => e.outcome(),
        });
        span.in_scope(|| tracing::info!(op = NodeOp::ConnectionsDelete.name(), route = route.token(), scope = scope.name(), "an object call ended"));
        r
    }

    /// The mesh-primary's call to the exact birth, after it wrote the renamed row: take this name and carry
    /// it in every digest from now on (`ApplyNodeName`, R12/R13). `Applied` once the birth holds and
    /// published it, `AlreadyApplied` for the name it holds, `BusyHoldingConnections` naming each
    /// connection it holds while it holds any (R-CR1). `route` is executed exactly as chosen
    /// (`NodeRpcClient::call_routed`).
    pub async fn name_apply(&self, birth: &ExactBirth, route: &RouteChoice, name: &rdm_core::PathName, opts: &CallOptions) -> Result<StatusReply, CallEnd> {
        let req = StatusRequest::ApplyNodeName { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone(), name: name.to_string() };
        let span = NodeOp::NameUpdate.span();
        let (out, _, _) = tracing::Instrument::instrument(self.client.call_routed::<Status>(&birth.node_id, route, &req, opts), span.clone()).await;
        let r = ended(out);
        span.record("outcome", match &r {
            Ok(reply) => reply.name(),
            Err(e) => e.outcome(),
        });
        span.in_scope(|| tracing::info!(op = NodeOp::NameUpdate.name(), route = route.token(), "an object call ended"));
        r
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
                TopologyReply::Snapshot { digests, in_flight, deleted, .. } => {
                    view.members.extend(digests.into_iter().map(rdm_core::MeshDigest::from));
                    view.in_flight.extend(in_flight);
                    view.deleted.extend(deleted);
                }
                TopologyReply::Started | TopologyReply::End { .. } | TopologyReply::Unchanged { .. } | TopologyReply::Primaries { .. } | TopologyReply::RafkaTime { .. } => {}
                other => return Err(CallEnd::Indeterminate { code: <Topology as rdm_core::protocol::NodeProtocol>::classify_reply(&other).code(), reason: format!("{} answered {other:?} to the topology read", target_name(target)) }),
            }
        }
        Ok(view)
    }

    /// A plural read on op `0x23` (Q285; node-rpc-envelope.md "Read, op `0x23`"): every row of
    /// `entity` the node at `target` answers for `filter`, collected from the stream in its order. The
    /// read succeeds only on a valid `End`: the rows collected number `End.rows` across `End.frames`
    /// data frames. A refusal (`RejectedFilter` naming the pair, `ReadOverBound` naming the row), a cut
    /// or a deadline is a failed read; no partial list is ever returned.
    pub async fn read(&self, target: &NodeTarget, entity: rdm_core::filter::Entity, filter: &rdm_core::filter::Filter, opts: &CallOptions) -> Result<rdm_core::read::Rows, CallEnd> {
        use rdm_core::read::{ReadRequest, Rows};
        let Some(rows) = Rows::empty(entity) else {
            return Err(CallEnd::Rejected { reason: "rejected-filter".into(), detail: format!("{} is GetConnections (with its node and incarnation), never Get", entity.read()) });
        };
        let req = ReadRequest::Get { entity, filter: filter.clone() };
        self.read_stream(target, req, rows, opts).await
    }

    /// `node.connection.get { node }` on op `0x23` (Q285): every connection the exact birth `birth`
    /// answers for `filter`, collected from the stream in its order under [`Self::read`]'s rules,
    /// `node` and `complete` taken from the frames (all frames agree, or the read fails).
    pub async fn connections_get(&self, birth: &ExactBirth, filter: &rdm_core::filter::Filter, opts: &CallOptions) -> Result<rdm_core::views::ConnectionsView, CallEnd> {
        use rdm_core::read::{ReadRequest, Rows};
        let req = ReadRequest::GetConnections { node: birth.node_id.clone(), incarnation: birth.incarnation.clone(), filter: filter.clone() };
        let empty = Rows::Connections(rdm_core::views::ConnectionsView { node: String::new(), complete: false, connections: Vec::new() });
        match self.read_stream(&birth.target, req, empty, opts).await? {
            Rows::Connections(view) if !view.node.is_empty() => Ok(view),
            _ => Err(CallEnd::Indeterminate { code: rdm_core::outcome::Code::DataLoss, reason: "the node.connection.get stream ended with no Connections frame".into() }),
        }
    }

    async fn read_stream(&self, target: &NodeTarget, req: rdm_core::read::ReadRequest, mut rows: rdm_core::read::Rows, opts: &CallOptions) -> Result<rdm_core::read::Rows, CallEnd> {
        use rdm_core::read::{Read, ReadReply};
        let op = req.op();
        let refused = |r: ReadReply| {
            let detail = match &r {
                ReadReply::RejectedFilter { reason } => format!("{target:?}: {reason}"),
                ReadReply::ReadOverBound { read, row, bytes, max } => format!("{target:?}: {read}'s row {row} encodes to {bytes} bytes, past the {max}-byte frame"),
                ReadReply::RejectedSourceFailed { source, op, cause } => format!("{target:?}: {source} {op}: {cause}"),
                ReadReply::NotReady { reason } => format!("{target:?}: {reason}"),
                other => format!("{target:?} answered {op} with {other:?}"),
            };
            CallEnd::Rejected { reason: r.name().into(), detail }
        };
        // A read of the target's answer, recorded nowhere here: reading it again is its recovery.
        let mut stream = match self.client.call_stream::<Read>(target, &req, opts).await {
            Ok((s, _)) => s.read_only(),
            Err((RpcOutcome::Reply(r), _)) => return Err(refused(r.into_value())),
            Err((out, _)) => return Err(ended(out).err().unwrap_or_else(|| CallEnd::Indeterminate { code: rdm_core::outcome::Code::Unknown, reason: "the read ended with no stream and no refusal".into() })),
        };
        let mut frames = 0u32;
        while let Some(item) = stream.next().await {
            match item {
                StreamItem::Frame(FrameKind::Refusal(_), r) => return Err(refused(r)),
                StreamItem::Frame(_, ReadReply::Started { .. }) => {}
                StreamItem::Frame(FrameKind::Data, frame) => {
                    if !rows.take(frame) {
                        return Err(CallEnd::Indeterminate { code: rdm_core::outcome::Code::DataLoss, reason: format!("a data frame of another entity in the {op} stream") });
                    }
                    frames += 1;
                }
                StreamItem::Frame(_, ReadReply::End { rows: n, frames: f }) => {
                    if n as usize != rows.len() || f != frames {
                        return Err(CallEnd::Indeterminate { code: rdm_core::outcome::Code::DataLoss, reason: format!("the {op} End counts {n} rows in {f} frames; {} rows arrived in {frames}", rows.len()) });
                    }
                    return Ok(rows);
                }
                StreamItem::Frame(_, other) => return Err(refused(other)),
                StreamItem::Failed(f) => return Err(CallEnd::Indeterminate { code: f.code(), reason: format!("the {op} stream failed before its End: {f:?}") }),
            }
        }
        Err(CallEnd::Indeterminate { code: rdm_core::outcome::Code::Unavailable, reason: format!("the {op} stream ended after {} rows in {frames} frames without its End", rows.len()) })
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
        // A read of the target's view, recorded nowhere here: reading a frame is its recovery.
        let (mut stream, _) = match self.client.call_stream::<Topology>(target, &req, opts).await.map(|(s, e)| (s.read_only(), e)) {
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
                StreamItem::Failed(f) => return Err(CallEnd::Indeterminate { code: f.code(), reason: format!("{f:?}") }),
            }
        }
        Ok(frames)
    }

    /// `node.primary.update` (`0x21`, `lifecycles/node-primary-update.md`): command the mesh-primary
    /// to move its primary (and the fabric-primary with it when it is both) to the successor the
    /// election names in its own view.
    pub async fn primary_update(&self, _fabric: &str, primary: &PathName, build_id: &BuildId, attempt: u32, opts: &CallOptions) -> Result<rdm_core::handover::NodePrimaryUpdateReply, CallEnd> {
        use rdm_core::handover::{NodePrimaryUpdate, NodePrimaryUpdateRequest};
        let req = NodePrimaryUpdateRequest::UpdatePrimary {
            node_type: primary.node_type().to_string(),
            build_id: build_id.to_string(),
            run: attempt,
            operation: format!("node.primary.update:{build_id}:{attempt}:{primary}"),
            successor: None,
        };
        let span = NodeOp::PrimaryUpdate.span();
        let (out, _) = tracing::Instrument::instrument(self.client.call::<NodePrimaryUpdate>(&NodeTarget::CurrentPath(primary.clone()), &req, opts), span.clone()).await;
        let r = ended(out);
        span.record("outcome", match &r {
            Ok(_) => "ok",
            Err(e) => e.outcome(),
        });
        span.in_scope(|| tracing::info!(op = NodeOp::PrimaryUpdate.name(), "an object call ended"));
        r
    }

    /// `node.fabric-primary.update` (`0x21`, `lifecycles/node-primary-update.md` "The fabric-primary"):
    /// command the fabric-primary to move the fabric-primary: its own mesh first, so both
    /// move together when a same-mesh node-admin is Ready, else another mesh's mesh-primary.
    pub async fn fabric_primary_update(&self, _fabric: &str, primary: &PathName, build_id: &BuildId, attempt: u32, opts: &CallOptions) -> Result<rdm_core::handover::NodePrimaryUpdateReply, CallEnd> {
        use rdm_core::handover::{NodePrimaryUpdate, NodePrimaryUpdateReply, NodePrimaryUpdateRequest};
        let req = NodePrimaryUpdateRequest::UpdatePrimary {
            node_type: rdm_core::handover::FABRIC.to_string(),
            build_id: build_id.to_string(),
            run: attempt,
            operation: format!("node.fabric-primary.update:{build_id}:{attempt}:{primary}"),
            successor: None,
        };
        let span = NodeOp::FabricPrimaryUpdate.span();
        let (out, _) = tracing::Instrument::instrument(self.client.call::<NodePrimaryUpdate>(&NodeTarget::CurrentPath(primary.clone()), &req, opts), span.clone()).await;
        let r: Result<NodePrimaryUpdateReply, CallEnd> = ended(out);
        span.record("outcome", match &r {
            Ok(_) => "ok",
            Err(e) => e.outcome(),
        });
        span.in_scope(|| tracing::info!(op = NodeOp::FabricPrimaryUpdate.name(), "an object call ended"));
        r
    }
}

/// A plural read's `rows` (op `0x23`, Q285) packed, in order, into data frames that each encode within
/// the reply frame (`rdm_transport::chunking::pack_in_order`): what a server streams between `Started`
/// and `End`. A row that fits no frame alone is `ReadOverBound` naming it; nothing is packed then.
pub fn read_frames(rows: Vec<rdm_core::read::Row>, read: &str) -> Result<Vec<rdm_core::read::ReadReply>, rdm_core::read::ReadReply> {
    use rdm_core::protocol::NodeProtocol as _;
    use rdm_core::read::{Read, ReadReply, Row};
    let max = Read::MAX_REPLY_FRAME_BYTES;
    let (packed, over) = rdm_transport::chunking::pack_in_order(rows, |batch: &[Row]| {
        let bytes = Read::encode_reply(&Row::frame(batch.to_vec())).map(|b| b.len()).unwrap_or(usize::MAX);
        if bytes <= max {
            Ok(bytes::Bytes::new())
        } else {
            Err((batch.first().map(Row::name).unwrap_or_default(), bytes))
        }
    });
    if let Some((row, bytes)) = over.into_iter().next() {
        return Err(ReadReply::ReadOverBound { read: read.to_string(), row, bytes: bytes as u64, max: max as u64 });
    }
    Ok(packed.into_iter().map(|(batch, _)| Row::frame(batch)).collect())
}

#[cfg(test)]
mod outcome_code_cells {
    use super::*;
    use rdm_core::protocol::NodeProtocol;
    use rdm_core::outcome::{Code, PreCommit, RequestFinished, IndeterminateReason};
    #[test]
    fn object_adapters_preserve_the_original_typed_delivery_code() {
        for cause in [IndeterminateReason::ReplyLost("same explanation".into()), IndeterminateReason::ReplyDeadline, IndeterminateReason::ReceiverGone("same explanation".into())] {
            let code = cause.code();
            let committed = PreCommit::begin(Status::OP).commit(RequestFinished::after_complete_write(1,1).unwrap());
            let end = ended::<StatusReply>(committed.indeterminate(cause)).unwrap_err();
            assert_eq!(end.code(), code);
        }
        assert_eq!(Code::DeadlineExceeded.number(),4);
    }
}

