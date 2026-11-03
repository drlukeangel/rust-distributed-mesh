//! The `node`, `mesh`, `fabric` and `build` objects.
//!
//! Workflows are submitted to the fabric-primary through the Build family (op `0x20`,
//! `build.create`) and answered with a reply stream ([`WorkflowStream`]) read from the call itself.
//! The `build` objects are that family's calls. The reads are Status calls (op `0x1B`), one rule
//! (Q167): one read per entity, plural, each taking one [`Filter`] (`(property, values)` pairs, AND
//! across pairs, any-of within) and answering a list, empty when nothing matches. `fabric.get`,
//! `mesh.get` and `node.get` go to the fabric-primary the carrier starts at, which answers from
//! what it holds, and `node.connection.get { node }` to the exact birth whose connections it reads. A
//! read the node refuses ends `Rejected`, naming the refusal (`rejected-filter` naming the pair). Calls whose request is valid but that no op carries
//! yet end `NotBackedToday`, naming why; no request is invented for them.

use rdm_core::read::Rows;
use crate::build_stream::{Build, BuildCarrier, BuildStream};
use rdm_core::names::{BuildOp, FabricOp, MeshOp, NodeOp};
use crate::workflow::{WorkflowKind, WorkflowStream};
use crate::{Accepted, AtlasView, BuildId, CallEnd, ConnectionsView, ExactBirth, FabricView, MeshView, NodeView};
use rdm_core::{NodeId, PathName};
use rdm_transport::{CallOptions, NodeTarget};
use rdm_core::build::BuildChange;
use rdm_core::filter::{Entity, Filter};
use rdm_core::status::{Status, StatusReply, StatusRequest};

/// A read on op `0x1B` to `target`: the reply the node answered, or how the call ended.
async fn read(carrier: &BuildCarrier, target: &NodeTarget, req: &StatusRequest) -> Result<StatusReply, CallEnd> {
    crate::calls::ended(carrier.client().call::<Status>(target, req, &CallOptions::default()).await.0)
}

/// A reply other than the read's answer: the refusal, named, with what it carries.
fn refused(target: &NodeTarget, op: &str, reply: StatusReply) -> CallEnd {
    // A refusal naming its own rule is that rule's.
    if let StatusReply::RejectedTopologyStorageProvider { reason, detail } = reply {
        return CallEnd::Rejected { reason, detail: format!("{target:?}: {detail}") };
    }
    let detail = match &reply {
        StatusReply::RejectedFilter { reason } => format!("{target:?}: {reason}"),
        StatusReply::ReadOverBound { read, bytes, max } => format!("{target:?}: the {read} answer encodes to {bytes} bytes, past the {max}-byte Status reply bound"),
        other => format!("{target:?} answered {op} with {other:?}"),
    };
    CallEnd::Rejected { reason: reply.name().into(), detail }
}

/// Record how a read ended on its span.
fn ended_read<T>(span: &tracing::Span, op: &'static str, r: &Result<T, CallEnd>) {
    span.record("outcome", match r {
        Ok(_) => "read",
        Err(e) => e.outcome(),
    });
    span.in_scope(|| tracing::info!(op, "an object call ended"));
}

/// Await a typed fact of one admin's view: `read` it at half the gossip interval, so a fact that
/// spreads by gossip is read as it lands, until `holds`. `kind` and `fact` name the fact and `admin` the
/// admin asked. An admin that does not report it within `within` ends the call `Indeterminate`, naming
/// the fact, the admin and the last thing it reported (`report` of the last view read) or how its last
/// read ended. `Fabrics::fact` is this body, and so is a caller's that supplies the read of a fact whose type
/// this crate cannot name (the Build's records, read from the fabric's store root). `Peers::fact` polls on its own
/// loop and is not this body.
pub async fn await_fact<T, Fut>(
    kind: &str,
    admin: &str,
    fact: &str,
    within: std::time::Duration,
    mut read: impl FnMut() -> Fut,
    holds: impl Fn(&T) -> bool,
    report: impl Fn(&T) -> String,
) -> Result<T, CallEnd>
where
    Fut: std::future::Future<Output = Result<T, CallEnd>>,
{
    let started = std::time::Instant::now();
    let every = rdm_core::cadence::gossip_interval() / 2;
    loop {
        let last = match read().await {
            Ok(view) if holds(&view) => return Ok(view),
            Ok(view) => report(&view),
            Err(e) => e.to_string(),
        };
        if started.elapsed() >= within {
            return Err(CallEnd::Indeterminate { code: rdm_core::outcome::Code::DeadlineExceeded, reason: format!("{admin} never reported the {kind} fact {fact} within {within:?}; its last report was: {last}") });
        }
        tokio::time::sleep(every).await;
    }
}

/// A create or a rename into a `(path, name)` another node holds, stopped or live: a name is held by
/// exactly one node until that node is renamed or deleted (ops-naming "Identity";
/// node-rpc-envelope.md "Meta and config"). The row writer reads the row at the name's key and
/// claims it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameHeld {
    /// The name claimed.
    pub path: String,
    /// The name, beside its path.
    pub name: String,
    /// The node that holds it.
    pub by: NodeId,
}

impl NameHeld {
    /// The `(path, name)` lookup index key of path and name.
    pub fn location(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.path.clone(), name: self.name.clone() } }
}

impl std::fmt::Display for NameHeld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "path {:?}, name {:?} is held by node {}: it is released only when that node is renamed or deleted", self.path, self.name, self.by)
    }
}

impl std::error::Error for NameHeld {}

/// A tags call's labels refused before anything is sent: its span says so, and the reply is the
/// `RejectedTags` the mesh-primary would answer.
fn refused_before_sending(op: NodeOp, reason: &str) -> StatusReply {
    let span = op.span();
    span.record("outcome", "rejected-tags");
    span.in_scope(|| tracing::info!(op = op.name(), reason = %reason, "the labels are not a node's tags; nothing is sent"));
    rdm_core::reply::status::bounded_rejected_tags(reason)
}

/// The `node` objects: workflows through the fabric-primary's Build family, reads as Status calls.
#[derive(Debug, Clone)]
pub struct Nodes {
    carrier: BuildCarrier,
}

impl Nodes {
    /// The objects over `carrier`: workflows and view reads start at its fabric-primary.
    pub fn new(carrier: BuildCarrier) -> Self {
        Self { carrier }
    }

    /// Submit a workflow call ([`workflow`]).
    async fn workflow(&self, kind: WorkflowKind, change: BuildChange) -> Result<WorkflowStream, CallEnd> {
        workflow(&self.carrier, kind, change).await
    }

    /// Re-submit the workflow `kind` of the call that opened `accepted`: never a second Build. A
    /// complete Build answers `AlreadyApplied` with its frames and terminal; a running one streams
    /// from where it is; a failed one takes its next attempt.
    pub async fn resume(&self, kind: WorkflowKind, accepted: &Accepted) -> Result<WorkflowStream, CallEnd> {
        let span = kind.span();
        match tracing::Instrument::instrument(self.carrier.resubmit(&accepted.build_id, accepted.attempt), span.clone()).await {
            Ok(inner) => Ok(WorkflowStream::new(inner, kind, span)),
            Err(end) => {
                span.record("outcome", end.outcome());
                span.in_scope(|| tracing::info!(op = kind.name(), reason = %end, "the workflow re-submit was not accepted"));
                Err(end)
            }
        }
    }

    /// `node.get` (`query.md` §1): every node row the fabric-primary holds that matches every pair of
    /// `filter`, by `(path, name)`; the empty filter is the fabric's topology, and nothing matching is the
    /// empty list (`[id: x]` is one node or none).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rdm_client::{birth_of_line, Filter, NodeView, Nodes, Peers};
    ///
    /// async fn ready_brokers(admin_stdout_line: &str) -> Result<Vec<NodeView>, String> {
    ///     let birth = birth_of_line(admin_stdout_line).ok_or("not a birth line")??;
    ///     let peers = Peers::at(&birth).await?;
    ///     let nodes = Nodes::new(peers.carrier_at(birth.name));
    ///     nodes.get(&Filter::new().eq("type", ["broker"]).eq("status", ["ready"])).await.map_err(|e| e.to_string())
    /// }
    /// ```
    pub async fn get(&self, filter: &Filter) -> Result<Vec<NodeView>, CallEnd> {
        let target = self.carrier.target().clone();
        read_rows(&self.carrier, &target, NodeOp::Get.span(), NodeOp::Get.name(), Entity::Nodes, filter, |r| match r {
            Rows::Nodes(nodes) => Some(nodes),
            _ => None,
        })
        .await
    }

    /// `node.name.update`: rename the exact birth `birth` to `name`, the name only (never its path), a
    /// single call to the node's mesh-primary `mesh_primary` over `route` (proposal
    /// node-properties-own-commands.md §2; [`crate::NodeRpc::name_update`]). The mesh-primary decides
    /// everything from its own held record; a name outside the name grammar is refused here,
    /// `RejectedName`, as the mesh-primary would refuse it, and nothing is sent.
    pub async fn name_update(&self, birth: &ExactBirth, mesh_primary: &NodeId, route: &rdm_transport::RouteChoice, name: &rdm_core::PathName) -> Result<StatusReply, CallEnd> {
        if let Err(reason) = rdm_core::PathName::new(name.path.clone(), name.name.clone()) {
            let span = NodeOp::NameUpdate.span();
            span.record("outcome", "rejected-name");
            span.in_scope(|| tracing::info!(op = NodeOp::NameUpdate.name(), reason = %reason, "the name is outside the name grammar; nothing is sent"));
            return Ok(rdm_core::reply::status::bounded_rejected_name(&reason.to_string()));
        }
        crate::NodeRpc::new(self.carrier.client()).name_update(mesh_primary, route, birth, name, &rdm_transport::CallOptions::default()).await
    }

    /// `node.tag.create`: add `labels` to the tags of the exact birth `birth`, a single call to the
    /// node's mesh-primary `mesh_primary` over `route` ([`crate::NodeRpc::tags_create`]). Labels
    /// outside the label grammar, an empty list or more than a node carries are refused here,
    /// `RejectedTags`, and nothing is sent.
    pub async fn tags_create(&self, birth: &ExactBirth, mesh_primary: &NodeId, route: &rdm_transport::RouteChoice, labels: &std::collections::BTreeSet<String>) -> Result<StatusReply, CallEnd> {
        if let Some(reason) = rdm_core::status::create_tags_refusal(labels) {
            return Ok(refused_before_sending(NodeOp::TagsCreate, &reason));
        }
        crate::NodeRpc::new(self.carrier.client()).tags_create(mesh_primary, route, birth, labels, &rdm_transport::CallOptions::default()).await
    }

    /// `node.tag.delete`: remove every label (`All`) or the named ones from the exact birth `birth`,
    /// as [`Self::tags_create`]; a `Labels` scope naming nothing or a non-label is refused here,
    /// `RejectedTags`, and nothing is sent.
    pub async fn tags_delete(&self, birth: &ExactBirth, mesh_primary: &NodeId, route: &rdm_transport::RouteChoice, scope: &rdm_core::status::TagScope) -> Result<StatusReply, CallEnd> {
        if let Some(reason) = scope.refusal() {
            return Ok(refused_before_sending(NodeOp::TagsDelete, &reason));
        }
        crate::NodeRpc::new(self.carrier.client()).tags_delete(mesh_primary, route, birth, scope, &rdm_transport::CallOptions::default()).await
    }

    /// `node.config.get`: no request of its own (Q167 R-RD4: a property's read is its row's field);
    /// [`Self::get`] answers the whole node, its config included.
    pub fn config_get(&self) -> CallEnd {
        CallEnd::NotBackedToday { op: NodeOp::ConfigGet, why: "node.config.get has no request of its own; node.get answers the node's config" }
    }

    /// `node.create` of `nodes`, each whole by its Nodes schema, in meshes already live: one Build,
    /// `build.create { Change(Create { meshes: [], nodes }) }` (build.md §1). The request names only the
    /// nodes it adds. The stream is `Started`, then the step events of each create, ending `Complete` or
    /// `Failed { step, reason }`.
    pub async fn create(&self, nodes: Vec<rdm_core::build::Nodes>) -> Result<WorkflowStream, CallEnd> {
        workflow(&self.carrier, WorkflowKind::Create, BuildChange::Create { meshes: Vec::new(), nodes }).await
    }

    /// `node.stop` of `node` (`lifecycles/node-stop.md`): delete its connections, leave its mesh and stop, keeping its `node.id`, `incarnation`, `(path, name)` and data dir.
    /// A node command, never a Build (Q342): NodeCommand 0x22 `StopNode` to the node's mesh-primary (the
    /// fabric-primary for a mesh-primary), which runs it (D40). The stream is `node.connections.deleted`
    /// and `node.stopped`, ending `Complete` or `Failed { step, reason }`; a stop the primary refuses
    /// before it runs is the call's `Rejected`, by name (`node-not-live`, `no-secondary`).
    pub async fn stop(&self, node: &PathName) -> Result<WorkflowStream, CallEnd> {
        self.node_command(WorkflowKind::Stop(node.clone()), node, None).await
    }

    /// `node.start` of the stopped `node`: the same process rejoins as itself (same node id,
    /// incarnation, endpoint key and port). A node command, never a Build (Q342), as [`Self::stop`]:
    /// NodeCommand 0x22 `StartNode` to the node's mesh-primary. The stream is `node.joined` and
    /// `node.started`, ending `Complete` or `Failed { step, reason }`.
    pub async fn start(&self, node: &PathName) -> Result<WorkflowStream, CallEnd> {
        self.node_command(WorkflowKind::Start(node.clone()), node, None).await
    }

    /// `node.connection.delete { scope }` of `node` (`lifecycles/node-connections-delete.md`): a node
    /// command, never a Build (Q342), NodeCommand 0x22 `DeleteConnections { scope }` to the node's
    /// mesh-primary (the fabric-primary for a mesh-primary), directly (R-CR2, R-CR3, D18), which for
    /// `All` moves a primary's roles first, then commands the exact birth. `All` is graceful closure of
    /// every open connection; `Connections` cuts exactly the named ones. The process keeps running. The
    /// stream is `node.connections.deleted`, ending `Complete`, and [`WorkflowStream::completion`] holds
    /// the door's `ConnectionsDeleted` (what was closed, not open and held); a refusal is the call's
    /// `Rejected`, by name. A scope that names nothing to cut is refused here as the primary's door
    /// refuses it, `rejected-connection-scope`, and nothing is sent.
    pub async fn delete_connections(&self, node: &PathName, scope: &rdm_core::status::ConnectionScope) -> Result<WorkflowStream, CallEnd> {
        if let Some(reason) = scope.refusal() {
            let span = NodeOp::ConnectionsDelete.span();
            span.record("outcome", "rejected-connection-scope");
            span.in_scope(|| tracing::info!(op = NodeOp::ConnectionsDelete.name(), reason = %reason, "the scope names nothing to cut; nothing is sent"));
            return Err(CallEnd::Rejected { reason: "rejected-connection-scope".into(), detail: format!("path {:?}, name {:?}: {reason}", node.path, node.name) });
        }
        self.node_command(WorkflowKind::DeleteConnections(node.clone()), node, Some(scope)).await
    }

    /// `node.mesh.leave` of `node` (`lifecycles/node-mesh-leave.md`): a node command, never a Build
    /// (Q342), NodeCommand 0x22 `LeaveMesh` to the node's mesh-primary, directly, sent after the
    /// node's connections are deleted ([`Self::delete_connections`]); the mesh-primary holds the node's
    /// `node.mesh.left` for its rename. The process keeps running. The stream is `node.mesh.left`,
    /// ending `Complete`; a leave the primary refuses before it runs is the call's `Rejected`, by name.
    pub async fn leave_mesh(&self, node: &PathName) -> Result<WorkflowStream, CallEnd> {
        self.node_command(WorkflowKind::LeaveMesh(node.clone()), node, None).await
    }

    /// The node command `kind` of the node at `node`, sent to the primary above it (D40): its exact
    /// birth and that primary read with `node.get` from the node the carrier starts at, then one direct
    /// NodeCommand 0x22 call, `scope` a connections delete's. A command the primary refuses before it
    /// runs ends here, by name.
    async fn node_command(&self, kind: WorkflowKind, node: &PathName, scope: Option<&rdm_core::status::ConnectionScope>) -> Result<WorkflowStream, CallEnd> {
        let span = kind.span();
        let call = async {
            let (birth, primary) = self.primary_above(node).await?;
            let target = ExactBirth { target: rdm_transport::NodeTarget::ExactNode(primary.clone()), ..birth };
            let rpc = crate::NodeRpc::new(self.carrier.client());
            let opts = rdm_transport::CallOptions::default();
            // The command's identity: its operation, keyed on the node.id, with an occurrence minted once
            // for this command (every resend of it carries it): each stop and each start of one node is
            // its own command, never a repeat of the last.
            let step = match kind {
                WorkflowKind::Start(_) => rdm_core::names::NodeStep::Start,
                WorkflowKind::LeaveMesh(_) => rdm_core::names::NodeStep::MeshLeave,
                _ => rdm_core::names::NodeStep::Stop,
            };
            let operation = rdm_core::OperationId::node(step.name(), &target.node_id).in_occurrence(rdm_core::OperationOccurrenceId::mint());
            let sent = match &kind {
                WorkflowKind::Start(_) => rpc.start(&target, &operation, &opts).await,
                WorkflowKind::LeaveMesh(_) => {
                    let req = rdm_core::node_command::NodeCommandRequest::LeaveMesh { node_id: target.node_id.clone(), incarnation: target.incarnation.clone(), operation: operation.clone() };
                    self.carrier.client().call_stream::<rdm_core::node_command::NodeCommand>(&target.target, &req, &opts).await.map(|(stream, _)| stream).map_err(|(out, _)| out)
                }
                WorkflowKind::DeleteConnections(_) => {
                    let scope = scope.cloned().unwrap_or(rdm_core::status::ConnectionScope::All);
                    let req = rdm_core::node_command::NodeCommandRequest::DeleteConnections { node_id: target.node_id.clone(), incarnation: target.incarnation.clone(), scope };
                    self.carrier.client().call_stream::<rdm_core::node_command::NodeCommand>(&target.target, &req, &opts).await.map(|(stream, _)| stream).map_err(|(out, _)| out)
                }
                _ => rpc.stop(&target, &operation, &opts).await,
            };
            // A refusal, as the Build carrier names it: the reason before the refusal's own text.
            let refused = |reply: rdm_core::node_command::NodeCommandReply| match reply {
                rdm_core::node_command::NodeCommandReply::Rejected { reason, detail } => CallEnd::Rejected { detail: format!("{reason}: {detail}"), reason },
                other => CallEnd::Rejected { reason: other.name().into(), detail: format!("node {primary} refused {} of node {}: {other:?}", kind.name(), target.node_id) },
            };
            let mut stream = sent.map_err(|out| match crate::calls::ended(out) {
                Ok(reply) => refused(reply),
                Err(end) => end,
            })?;
            // The primary's answer is the stream's first frame: a refusal before the command ran is the
            // call's end, by name; anything else is the stream's first frame.
            use rdm_core::streaming::StreamingProtocol as _;
            let first = match stream.next().await {
                Some(rdm_transport::stream::StreamItem::Frame(_, f)) => f,
                Some(rdm_transport::stream::StreamItem::Failed(f)) => return Err(CallEnd::Indeterminate { code: f.code(), reason: format!("the node command's stream to {primary} broke before its first frame: {f:?}") }),
                None => return Err(CallEnd::Indeterminate { code: rdm_core::outcome::Code::Unknown, reason: format!("the node command's stream to {primary} ended before its first frame") }),
            };
            stream.accepted().await;
            if matches!(rdm_core::node_command::NodeCommand::frame_kind(&first), rdm_core::streaming::FrameKind::Refusal(_)) {
                return Err(refused(first));
            }
            Ok((stream, first))
        };
        match tracing::Instrument::instrument(call, span.clone()).await {
            Ok((inner, first)) => Ok(WorkflowStream::of_command(inner, first, kind, span)),
            Err(end) => {
                span.record("outcome", end.outcome());
                span.in_scope(|| tracing::info!(op = kind.name(), reason = %end, "the node command was not run"));
                Err(end)
            }
        }
    }

    /// The exact birth at `node` and the primary above it (D40): its mesh-primary, or the
    /// fabric-primary for a mesh-primary, read with `node.get`. No node at `node` is `unknown-node`.
    async fn primary_above(&self, node: &PathName) -> Result<(ExactBirth, NodeId), CallEnd> {
        let found = self.get(&Filter::new().eq("path", [node.path.as_str()]).eq("name", [node.name.as_str()])).await?;
        let Some(view) = found.into_iter().next() else {
            return Err(CallEnd::Rejected { reason: "unknown-node".into(), detail: format!("node.get names no node at path {:?}, name {:?}", node.path, node.name) });
        };
        let Some(incarnation) = view.incarnation_id.clone() else {
            return Err(CallEnd::Rejected { reason: "unknown-node".into(), detail: format!("node {} at path {:?}, name {:?} has no known birth", view.node_id, node.path, node.name) });
        };
        let mesh_primary = rdm_core::is_node_admin(&view.r#type) && view.is_primary;
        let above = if mesh_primary { Filter::new().eq("is_fabric_primary", ["true"]) } else { Filter::new().eq("mesh", [view.mesh.as_str()]).eq("type", [rdm_core::meta::NODE_ADMIN]).eq("is_primary", ["true"]) };
        let primary = self.get(&above).await?.into_iter().next().map(|p| p.node_id).ok_or_else(|| CallEnd::Rejected {
            reason: "no-primary".into(),
            detail: format!("node.get names no {} above node {} at path {:?}, name {:?}", if mesh_primary { "fabric-primary" } else { "mesh-primary" }, view.node_id, node.path, node.name),
        })?;
        Ok((ExactBirth { target: rdm_transport::NodeTarget::ExactNode(view.node_id.clone()), node_id: view.node_id, incarnation }, primary))
    }

    /// `node.delete` of the nodes `node_ids`, each by its node.id (`lifecycles/node-delete.md`): one
    /// Build, `build.create { Change(Delete { meshes: [], node_ids }) }` (build.md §1). Each composes
    /// `node.stop` when its process is live, terminates its exact runtime through the provider, and
    /// releases its node.id and path and name. A node.id already deleted or replaced is refused by name.
    /// The stream is `Started`, every `node.stop` event of a live node, and `node.deleted`, ending
    /// `Complete` or `Failed { step, reason }`.
    pub async fn delete(&self, node_ids: Vec<NodeId>) -> Result<WorkflowStream, CallEnd> {
        workflow(&self.carrier, WorkflowKind::Delete(node_ids.clone()), BuildChange::Delete { meshes: Vec::new(), node_ids }).await
    }

    /// `node.primary.update` (`0x21`, `lifecycles/node-primary-update.md`): command the mesh-primary
    /// to move its mesh-primary role (and the fabric-primary role with it when it is both)
    /// to its same-mesh successor.
    pub async fn primary_update(&self, fabric: &str, primary: &NodeId, build_id: &BuildId, attempt: u32) -> Result<rdm_core::handover::NodePrimaryUpdateReply, CallEnd> {
        crate::NodeRpc::new(self.carrier.client()).primary_update(fabric, primary, build_id, attempt, &CallOptions::default()).await
    }

    /// `node.fabric-primary.update` (`0x21`, `lifecycles/node-primary-update.md` "The fabric-primary"): command the
    /// fabric-primary to move the fabric-primary role (step 1 same-mesh successor
    /// first, step 2 another Ready mesh-primary when no same-mesh successor is Ready).
    pub async fn fabric_primary_update(&self, fabric: &str, primary: &NodeId, build_id: &BuildId, attempt: u32) -> Result<rdm_core::handover::NodePrimaryUpdateReply, CallEnd> {
        crate::NodeRpc::new(self.carrier.client()).fabric_primary_update(fabric, primary, build_id, attempt, &CallOptions::default()).await
    }
}

/// A plural read (Q167) on op `0x23` (Q285) to `target` under `span`: every row of `entity` the node
/// answers for `filter`, collected from the stream and taken as `answer`'s list, or how it ended. A
/// filter the entity's schema refuses is refused here, `rejected-filter` naming the pair, as the node
/// would refuse it, and nothing is sent.
async fn read_rows<T>(
    carrier: &BuildCarrier,
    target: &NodeTarget,
    span: tracing::Span,
    op: &'static str,
    entity: Entity,
    filter: &Filter,
    answer: impl FnOnce(Rows) -> Option<Vec<T>>,
) -> Result<Vec<T>, CallEnd> {
    span.record("properties", filter.properties().as_str());
    if let Some(reason) = filter.refusal(entity) {
        let r = Err(refused(target, op, rdm_core::reply::status::bounded_rejected_filter(&reason)));
        ended_read(&span, op, &r);
        return r;
    }
    let call = async {
        let rows = crate::NodeRpc::new(carrier.client()).read(target, entity, filter, &CallOptions::default()).await?;
        answer(rows).ok_or_else(|| CallEnd::Indeterminate { code: rdm_core::outcome::Code::DataLoss, reason: format!("{target:?} answered {op} with another entity's rows") })
    };
    let r = tracing::Instrument::instrument(call, span.clone()).await;
    ended_read(&span, op, &r);
    r
}

/// Submit a workflow call through `carrier`. Its span opens here and closes with the returned stream;
/// a call the fabric-primary did not accept ends here, with the span saying how.
async fn workflow(carrier: &BuildCarrier, kind: WorkflowKind, change: BuildChange) -> Result<WorkflowStream, CallEnd> {
    let span = kind.span();
    match tracing::Instrument::instrument(carrier.create(change), span.clone()).await {
        Ok(inner) => Ok(WorkflowStream::new(inner, kind, span)),
        Err(end) => {
            span.record("outcome", end.outcome());
            span.in_scope(|| tracing::info!(op = kind.name(), reason = %end, "the workflow call was not accepted"));
            Err(end)
        }
    }
}

/// The `mesh` objects: `mesh.create` and `mesh.delete` through the fabric-primary's Build family, and
/// reads on op `0x23` to the fabric-primary the carrier starts at.
#[derive(Debug, Clone)]
pub struct Meshes {
    carrier: BuildCarrier,
}

impl Meshes {
    /// The objects over `carrier`.
    pub fn new(carrier: BuildCarrier) -> Self {
        Self { carrier }
    }

    /// `mesh.create` of `meshes` with `nodes`, each node whole by its Nodes schema: one Build,
    /// `build.create { Change(Create { meshes, nodes }) }` (build.md §1). Each new mesh is created with
    /// its first node-admin, which `nodes` names; a new mesh with none is refused by name. The stream is
    /// `Started`, then the step events of each create, ending `Complete` or `Failed { step, reason }`.
    pub async fn create(&self, meshes: Vec<rdm_core::build::Meshes>, nodes: Vec<rdm_core::build::Nodes>) -> Result<WorkflowStream, CallEnd> {
        workflow(&self.carrier, WorkflowKind::Create, BuildChange::Create { meshes, nodes }).await
    }

    /// `mesh.delete` of the meshes `names` (lifecycles/mesh-delete.md): one Build,
    /// `build.create { Change(Delete { meshes: names, node_ids: [] }) }` (build.md §1).
    pub async fn delete(&self, names: Vec<String>) -> Result<BuildStream, CallEnd> {
        self.carrier.create(BuildChange::Delete { meshes: names, node_ids: Vec::new() }).await
    }

    /// `mesh.get` (Q167): every mesh the fabric-primary holds that matches every pair of `filter`, by
    /// name; a mesh's nodes are `node.get [mesh: name]`.
    pub async fn get(&self, filter: &Filter) -> Result<Vec<MeshView>, CallEnd> {
        let target = self.carrier.target().clone();
        read_rows(&self.carrier, &target, MeshOp::Get.span(), MeshOp::Get.name(), Entity::Meshes, filter, |r| match r {
            Rows::Meshes(meshes) => Some(meshes),
            _ => None,
        })
        .await
    }
}

/// The `fabric` objects: reads on op `0x23` to the fabric-primary the carrier starts at.
#[derive(Debug, Clone)]
pub struct Fabrics {
    carrier: BuildCarrier,
}

impl Fabrics {
    /// The objects over `carrier`.
    pub fn new(carrier: BuildCarrier) -> Self {
        Self { carrier }
    }

    /// `fabric.get` (Q167): the fabric the fabric-primary holds, when it matches every pair of
    /// `filter`: its id, name, state, provider, fabric-primary and meshes; its nodes are `node.get`.
    pub async fn get(&self, filter: &Filter) -> Result<Vec<FabricView>, CallEnd> {
        let target = self.carrier.target().clone();
        read_rows(&self.carrier, &target, FabricOp::Get.span(), FabricOp::Get.name(), Entity::Fabrics, filter, |r| match r {
            Rows::Fabrics(fabrics) => Some(fabrics),
            _ => None,
        })
        .await
    }

    /// The fabric the fabric-primary holds (`fabric.get` with no pair): the one row it answers.
    async fn the_fabric(&self) -> Result<FabricView, CallEnd> {
        let target = self.carrier.target().clone();
        self.get(&Filter::new()).await?.into_iter().next().ok_or_else(|| CallEnd::NotFound { read: FabricOp::Get.name(), detail: format!("{target:?} answered fabric.get with no fabric") })
    }

    /// `fabric.atlas.get` (docs/plans/proposal/fabric-atlas.md R-FA4): the fabric-primary's
    /// atlas, one row per node of the current Build's topology: the birth its node row names
    /// current, its lifecycle as the ops left it, and the operation accepted on it whose attempt has
    /// not completed. `fabric.get` is unchanged by it.
    pub async fn atlas_get(&self) -> Result<AtlasView, CallEnd> {
        let span = FabricOp::AtlasGet.span();
        let target = self.carrier.target().clone();
        let call = async {
            match read(&self.carrier, &target, &StatusRequest::GetAtlas).await? {
                StatusReply::Atlas { atlas } => Ok(atlas),
                other => Err(refused(&target, FabricOp::AtlasGet.name(), other)),
            }
        };
        let r = tracing::Instrument::instrument(call, span.clone()).await;
        ended_read(&span, FabricOp::AtlasGet.name(), &r);
        r
    }

    /// `fabric.root.update` (fabric-ca-renew.md "fabric.root.update, the caller op"): the new root's
    /// `secret` to the fabric-primary, which holds it beside the old one and answers the phase its
    /// roots row is at (`minted`); refused by name while a rotation is in flight
    /// (`rotation-in-progress`) and for a root already trusted (`root-already-trusted`).
    pub async fn update_root(&self, secret: rdm_core::status::RootSecretHex) -> Result<rdm_core::status::RootsPhase, CallEnd> {
        let span = FabricOp::RootUpdate.span();
        let target = self.carrier.target().clone();
        let call = async {
            match read(&self.carrier, &target, &StatusRequest::UpdateRoot { secret }).await? {
                StatusReply::RootUpdated { phase } => Ok(phase),
                other => Err(refused(&target, FabricOp::RootUpdate.name(), other)),
            }
        };
        let r = tracing::Instrument::instrument(call, span.clone()).await;
        ended_read(&span, FabricOp::RootUpdate.name(), &r);
        r
    }

    /// `fabric.storage.update` (storage.md §2 "Changing it"): the fabric's new
    /// `topology_storage_provider` to the fabric-primary, which starts the storage change, its own
    /// workflow, and answers the change's id once its `rdm.storage.started` event is written; refused
    /// by name while a Build or another change is in progress (`build-in-progress`) and for a provider
    /// its wiring does not register or cannot open. The change's end is its `rdm.storage.ended` event.
    pub async fn update_storage(&self, topology_storage_provider: &str) -> Result<rdm_core::ChangeId, CallEnd> {
        let span = FabricOp::StorageUpdate.span();
        let target = self.carrier.target().clone();
        let call = async {
            match read(&self.carrier, &target, &StatusRequest::UpdateStorage { topology_storage_provider: topology_storage_provider.to_string() }).await? {
                StatusReply::StorageUpdated { change_id } => Ok(change_id),
                other => Err(refused(&target, FabricOp::StorageUpdate.name(), other)),
            }
        };
        let r = tracing::Instrument::instrument(call, span.clone()).await;
        ended_read(&span, FabricOp::StorageUpdate.name(), &r);
        r
    }

    /// Await the typed fact `holds` of the fabric as the admin the carrier starts at reports it
    /// (`fabric.get`), read at half the gossip interval so a fact that spreads by gossip is read as it
    /// lands. `fact` names it. An admin that does not report it within `within` ends the call
    /// `Indeterminate`, naming the fact, the admin and the last thing it reported or how its last read
    /// ended.
    pub async fn fact(&self, fact: &str, within: std::time::Duration, holds: impl Fn(&FabricView) -> bool) -> Result<FabricView, CallEnd> {
        let report = |view: &FabricView| format!("status {:?}, fabric-primary {}, {} meshes", view.status, view.fabric_primary.as_ref().map(|p| p.to_string()).unwrap_or_else(|| "none".into()), view.meshes.len());
        await_fact("fabric", &format!("{:?}", self.carrier.target()), fact, within, || self.the_fabric(), holds, report).await
    }
}

/// The `connections` objects: `node.connection.get { node }` (Q167 R-RD7, Q285), a read on op `0x23`
/// to the exact birth whose connections log holds the connections, the node the read's address.
#[derive(Debug, Clone)]
pub struct Connections {
    carrier: BuildCarrier,
}

impl Connections {
    /// The objects over `carrier`.
    pub fn new(carrier: BuildCarrier) -> Self {
        Self { carrier }
    }

    /// `node.connection.get { node }`: the connections the exact birth `birth` holds as its own that match
    /// every pair of `filter` (`id`, `destination`, `kind`, `state`, …), under the name it holds, each
    /// named by the minted id `node.connection.delete` names it by.
    pub async fn get(&self, birth: &ExactBirth, filter: &Filter) -> Result<ConnectionsView, CallEnd> {
        let span = NodeOp::ConnectionsGet.span();
        let op = NodeOp::ConnectionsGet.name();
        span.record("properties", filter.properties().as_str());
        if let Some(reason) = filter.refusal(Entity::Connections) {
            let r = Err(refused(&birth.target, op, rdm_core::reply::status::bounded_rejected_filter(&reason)));
            ended_read(&span, op, &r);
            return r;
        }
        let rpc = crate::NodeRpc::new(self.carrier.client());
        let opts = CallOptions::default();
        let r = tracing::Instrument::instrument(rpc.connections_get(birth, filter, &opts), span.clone()).await;
        ended_read(&span, op, &r);
        r
    }
}

/// The `build` objects: the calls of the fabric-primary's Build family.
#[derive(Debug, Clone)]
pub struct Builds {
    carrier: BuildCarrier,
}

impl Builds {
    /// The objects over `carrier`.
    pub fn new(carrier: BuildCarrier) -> Self {
        Self { carrier }
    }

    async fn call<T>(&self, op: BuildOp, call: impl std::future::Future<Output = Result<T, CallEnd>>) -> Result<T, CallEnd> {
        let span = op.span();
        let r = tracing::Instrument::instrument(call, span.clone()).await;
        span.record("outcome", match &r {
            Ok(_) => "ok",
            Err(e) => e.outcome(),
        });
        span.in_scope(|| tracing::info!(op = op.name(), "a build call ended"));
        r
    }

    /// `build.get` (Q167 R-RD5): every Build the fabric-primary holds whose header matches every pair
    /// of `filter` (`id`, `current`, `phase`, `attempt`, `executor`), newest first, each whole: header,
    /// topology (`nodes`) and receipts (`steps`), the current one marked. `[id: x]` is one Build, the
    /// empty list when the fabric-primary holds none of that id; `[current: true]` the current Build.
    pub async fn get(&self, filter: &Filter) -> Result<Vec<Build>, CallEnd> {
        let span = BuildOp::Get.span();
        span.record("properties", filter.properties().as_str());
        let r = tracing::Instrument::instrument(self.carrier.builds(filter), span.clone()).await;
        span.record("outcome", match &r {
            Ok(_) => "ok",
            Err(e) => e.outcome(),
        });
        span.in_scope(|| tracing::info!(op = BuildOp::Get.name(), "a build call ended"));
        r
    }

    /// `build.delete`: drop a finished Build from history.
    pub async fn delete(&self, id: &BuildId) -> Result<(), CallEnd> {
        self.call(BuildOp::Delete, self.carrier.delete(id)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT: each op and each step event has one canonical name and one span, a literal of five
    /// segments (telemetry-patterns.md §8): the op `node.connection.delete` spans
    /// `rdm.node_admin.connection.delete.via-call`, a workflow `…via-workflow` (or
    /// `node.update.via-<op>-workflow` for stop and start), and an event's frame
    /// `…via-<event>-frame`. Every op and event opens exactly the span this table names.
    #[test]
    fn one_name_per_op_opens_its_one_literal_span() {
        use rdm_core::names::NodeEvent;
        let (_g, spans) = capture_spans();
        let ops: [(tracing::Span, &str); 16] = [
            (NodeOp::Get.span(), "rdm.node_admin.node.resolve.via-call"),
            (NodeOp::NameUpdate.span(), "rdm.node_admin.name.update.via-call"),
            (NodeOp::TopologyGet.span(), "rdm.node_admin.topology.resolve.via-call"),
            (NodeOp::ConnectionsGet.span(), "rdm.node_admin.connection.resolve.via-call"),
            (NodeOp::ConnectionsDelete.span(), "rdm.node_admin.connection.delete.via-call"),
            (NodeOp::ConfigGet.span(), "rdm.node_admin.config.resolve.via-call"),
            (NodeOp::ConfigUpdate.span(), "rdm.node_admin.config.update.via-call"),
            (NodeOp::TagsCreate.span(), "rdm.node_admin.tag.create.via-call"),
            (NodeOp::TagsDelete.span(), "rdm.node_admin.tag.delete.via-call"),
            (NodeOp::Create.span(), "rdm.node_admin.node.create.via-workflow"),
            (NodeOp::Stop.span(), "rdm.node_admin.node.update.via-stop-workflow"),
            (NodeOp::Start.span(), "rdm.node_admin.node.update.via-start-workflow"),
            (NodeOp::Delete.span(), "rdm.node_admin.node.delete.via-workflow"),
            (NodeOp::MeshLeave.span(), "rdm.node_admin.node.remove.via-mesh-leave-call"),
            (NodeOp::PrimaryUpdate.span(), "rdm.node_admin.primary.update.via-call"),
            (NodeOp::FabricPrimaryUpdate.span(), "rdm.node_admin.primary.update.via-fabric-primary-call"),
        ];
        let events: [(tracing::Span, &str); 8] = [
            (NodeEvent::Created.span(), "rdm.node_admin.node.create.via-created-frame"),
            (NodeEvent::Joined.span(), "rdm.node_admin.node.update.via-joined-frame"),
            (NodeEvent::Started.span(), "rdm.node_admin.node.update.via-started-frame"),
            (NodeEvent::ConnectionsDeleted.span(), "rdm.node_admin.connection.delete.via-deleted-frame"),
            (NodeEvent::Stopped.span(), "rdm.node_admin.node.update.via-stopped-frame"),
            (NodeEvent::Deleted.span(), "rdm.node_admin.node.delete.via-deleted-frame"),
            (NodeEvent::Updated.span(), "rdm.node_admin.node.update.via-updated-frame"),
            (NodeEvent::MeshLeft.span(), "rdm.node_admin.node.remove.via-mesh-left-frame"),
        ];
        let reads: [(tracing::Span, &str); 6] = [
            (BuildOp::Create.span(), "rdm.node_admin.build.create.via-call"),
            (BuildOp::Get.span(), "rdm.node_admin.build.resolve.via-call"),
            (BuildOp::Delete.span(), "rdm.node_admin.build.delete.via-call"),
            (MeshOp::Get.span(), "rdm.node_admin.mesh.resolve.via-call"),
            (FabricOp::Get.span(), "rdm.node_admin.fabric.resolve.via-call"),
            (FabricOp::AtlasGet.span(), "rdm.node_admin.fabric.resolve.via-atlas-call"),
        ];
        let want: Vec<&str> = ops.iter().chain(&events).chain(&reads).map(|(_, n)| *n).collect();
        drop((ops, events, reads));
        let names = spans.lock().unwrap().clone();
        assert_eq!(names, want, "each op and event opens exactly its one span, in order");
    }

    /// A subscriber that records the name of every span created while the guard lives.
    fn capture_spans() -> (tracing::subscriber::DefaultGuard, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        use tracing::span;
        struct Names(std::sync::Arc<std::sync::Mutex<Vec<String>>>);
        impl tracing::Subscriber for Names {
            fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
                true
            }
            fn new_span(&self, a: &span::Attributes<'_>) -> span::Id {
                let mut n = self.0.lock().unwrap();
                n.push(a.metadata().name().to_string());
                span::Id::from_u64(n.len() as u64)
            }
            fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
            fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
            fn event(&self, _: &tracing::Event<'_>) {}
            fn enter(&self, _: &span::Id) {}
            fn exit(&self, _: &span::Id) {}
        }
        let names = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        (tracing::subscriber::set_default(Names(names.clone())), names)
    }
}
