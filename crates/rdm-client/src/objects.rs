//! The `node`, `mesh`, `fabric` and `build` objects.
//!
//! Workflows are submitted to the fabric-primary through the Build family (op `0x20`,
//! `build.create`) and answered with a reply stream ([`WorkflowStream`]) read from the call itself.
//! The `build` objects are that family's calls. The reads are Status calls (op `0x1B`), one rule
//! (Q165): `<entity>.get` returns one, `<parent>.<children>.get` the list. `node.get { id }`,
//! `mesh.get { name }`, `mesh.nodes.get { tags? }`, `fabric.get`, `fabric.meshes.get` and
//! `fabric.nodes.get { tags? }` go to the fabric-primary the carrier starts at, which answers from its
//! view, and `node.connections.get` to the exact birth whose connections it reads. A read the node
//! refuses ends `Rejected`, naming the refusal. Calls whose request is valid but that no op carries
//! yet end `NotBackedToday`, naming why; no request is invented for them.

use crate::build_stream::{BuildCarrier, BuildReceipts};
use rdm_core::names::{BuildOp, FabricOp, MeshOp, NodeOp};
use crate::workflow::{WorkflowKind, WorkflowStream};
use crate::{Accepted, BuildId, CallEnd, ConnectionsView, ExactBirth, FabricView, MeshView, NodeView};
use rdm_core::{NodeId, PathName};
use rdm_transport::{CallOptions, NodeTarget};
use rdm_core::build::BuildChange;
use rdm_core::status::{NodeSelect, Status, StatusReply, StatusRequest};

/// A read on op `0x1B` to `target`: the reply the node answered, or how the call ended.
async fn read(carrier: &BuildCarrier, target: &NodeTarget, req: &StatusRequest) -> Result<StatusReply, CallEnd> {
    crate::calls::ended(carrier.client().call::<Status>(target, req, &CallOptions::default()).await.0)
}

/// A reply other than the read's answer: the refusal, named, with what it carries.
fn refused(target: &NodeTarget, op: &str, reply: StatusReply) -> CallEnd {
    let detail = match &reply {
        StatusReply::UnknownMesh { mesh } => format!("{target:?} holds no mesh {mesh}"),
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
/// this crate cannot name (a node-admin's own Build log over `FetchBuildFacts`). `Peers::fact` polls on its own
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

pub use rdm_core::status::TagFilter;

/// A create or a rename into a `path.name` another node holds, stopped or live: a name is held by
/// exactly one node until that node is renamed or deleted (ops-naming "Identity";
/// node-rpc-envelope.md "Meta and config"). The row writer reads the row at the name's key and
/// claims it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameHeld {
    /// The name claimed.
    pub name: PathName,
    /// The node that holds it.
    pub by: NodeId,
}

impl std::fmt::Display for NameHeld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} is held by node {}: it is released only when that node is renamed or deleted", self.name, self.by)
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

    /// Submit a workflow call. Its span opens here and closes with the returned stream; a call the
    /// fabric-primary did not accept ends here, with the span saying how.
    async fn workflow(&self, kind: WorkflowKind, change: BuildChange) -> Result<WorkflowStream, CallEnd> {
        let span = kind.span();
        match tracing::Instrument::instrument(self.carrier.create(change), span.clone()).await {
            Ok(inner) => Ok(WorkflowStream::new(inner, kind, span)),
            Err(end) => {
                span.record("outcome", end.outcome());
                span.in_scope(|| tracing::info!(op = kind.name(), reason = %end, "the workflow call was not accepted"));
                Err(end)
            }
        }
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

    /// `node.get { id }`: the one node with the minted id `id`, as the fabric-primary's view holds
    /// it; a node it does not hold ends [`CallEnd::NotFound`] (`NOT_FOUND`, Q165).
    pub async fn get(&self, id: &NodeId) -> Result<NodeView, CallEnd> {
        let span = NodeOp::Get.span();
        let target = NodeTarget::CurrentPath(self.carrier.fabric_primary().clone());
        let call = async {
            let nodes = nodes_read(&self.carrier, &target, NodeOp::Get.name(), NodeSelect::Id(id.clone())).await?;
            nodes.into_iter().find(|n| &n.node_id == id).ok_or_else(|| CallEnd::NotFound { read: NodeOp::Get.name(), detail: format!("{target:?} holds no node {id}") })
        };
        let r = tracing::Instrument::instrument(call, span.clone()).await;
        ended_read(&span, NodeOp::Get.name(), &r);
        r
    }

    /// `node.name.update`: rename the exact birth `birth` to `name`, the name only (never its path), a
    /// single call to the node's mesh-primary `mesh_primary` over `route` (proposal
    /// node-properties-own-commands.md §2; [`crate::NodeRpc::name_update`]). The mesh-primary decides
    /// everything from its own held record; a name outside the name grammar is refused here,
    /// `RejectedName`, as the mesh-primary would refuse it, and nothing is sent.
    pub async fn name_update(&self, birth: &ExactBirth, mesh_primary: &NodeId, route: &rdm_transport::RouteChoice, name: &str) -> Result<StatusReply, CallEnd> {
        if let Some(reason) = rdm_core::status::name_refusal(name) {
            let span = NodeOp::NameUpdate.span();
            span.record("outcome", "rejected-name");
            span.in_scope(|| tracing::info!(op = NodeOp::NameUpdate.name(), reason = %reason, "the name is outside the name grammar; nothing is sent"));
            return Ok(rdm_core::reply::status::bounded_rejected_name(&reason));
        }
        crate::NodeRpc::new(self.carrier.client()).name_update(mesh_primary, route, birth, name, &rdm_transport::CallOptions::default()).await
    }

    /// `node.tags.create`: add `labels` to the tags of the exact birth `birth`, a single call to the
    /// node's mesh-primary `mesh_primary` over `route` ([`crate::NodeRpc::tags_create`]). Labels
    /// outside the label grammar, an empty list or more than a node carries are refused here,
    /// `RejectedTags`, and nothing is sent.
    pub async fn tags_create(&self, birth: &ExactBirth, mesh_primary: &NodeId, route: &rdm_transport::RouteChoice, labels: &std::collections::BTreeSet<String>) -> Result<StatusReply, CallEnd> {
        if let Some(reason) = rdm_core::status::create_tags_refusal(labels) {
            return Ok(refused_before_sending(NodeOp::TagsCreate, &reason));
        }
        crate::NodeRpc::new(self.carrier.client()).tags_create(mesh_primary, route, birth, labels, &rdm_transport::CallOptions::default()).await
    }

    /// `node.tags.delete`: remove every label (`All`) or the named ones from the exact birth `birth`,
    /// as [`Self::tags_create`]; a `Labels` scope naming nothing or a non-label is refused here,
    /// `RejectedTags`, and nothing is sent.
    pub async fn tags_delete(&self, birth: &ExactBirth, mesh_primary: &NodeId, route: &rdm_transport::RouteChoice, scope: &rdm_core::status::TagScope) -> Result<StatusReply, CallEnd> {
        if let Some(reason) = scope.refusal() {
            return Ok(refused_before_sending(NodeOp::TagsDelete, &reason));
        }
        crate::NodeRpc::new(self.carrier.client()).tags_delete(mesh_primary, route, birth, scope, &rdm_transport::CallOptions::default()).await
    }

    /// `node.connections.get`: the connections the exact birth `birth` holds, under the name it
    /// holds, each named by the minted id [`Self::connections_delete`] names it by.
    pub async fn connections_get(&self, birth: &ExactBirth) -> Result<ConnectionsView, CallEnd> {
        let span = NodeOp::ConnectionsGet.span();
        let req = StatusRequest::GetConnections { node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone() };
        let call = async {
            match read(&self.carrier, &birth.target, &req).await? {
                StatusReply::Connections { connections } => Ok(connections),
                other => Err(refused(&birth.target, NodeOp::ConnectionsGet.name(), other)),
            }
        };
        let r = tracing::Instrument::instrument(call, span.clone()).await;
        ended_read(&span, NodeOp::ConnectionsGet.name(), &r);
        r
    }

    /// `node.connections.delete`: the hard cut of the connections of the exact birth `birth`,
    /// `scope` `All` (the stop's cut) or `Connections` naming open entries by id, a single call to the
    /// node's mesh-primary `mesh_primary` over `route` (R-CR2, R-CR3; [`crate::NodeRpc::connections_delete`]).
    /// A scope that names nothing to cut is refused here by name, `RejectedConnectionScope`, and
    /// nothing is sent.
    pub async fn connections_delete(&self, birth: &ExactBirth, mesh_primary: &NodeId, route: &rdm_transport::RouteChoice, scope: &rdm_core::status::ConnectionScope) -> Result<rdm_core::status::StatusReply, CallEnd> {
        if let Some(reason) = scope.refusal() {
            let span = NodeOp::ConnectionsDelete.span();
            span.record("outcome", "rejected-connection-scope");
            span.in_scope(|| tracing::info!(op = NodeOp::ConnectionsDelete.name(), reason = %reason, "the scope names nothing to cut; nothing is sent"));
            return Ok(rdm_core::reply::status::rejected_connection_scope(format!("node {} ({}): {reason}", birth.node_id, birth.incarnation.as_str())));
        }
        crate::NodeRpc::new(self.carrier.client()).connections_delete(mesh_primary, route, birth, scope, &rdm_transport::CallOptions::default()).await
    }

    /// `node.config.get`: no request of its own (node-rpc-envelope.md: its dedicated request mapping is
    /// open); [`Self::get`] answers the whole node, its config included.
    pub fn config_get(&self) -> CallEnd {
        CallEnd::NotBackedToday { op: NodeOp::ConfigGet, why: "node.config.get has no request of its own; node.get answers the node's config" }
    }

    /// `node.create` of the paths `nodes` adds to the topology Build `based_on` accepted, each named by
    /// its create spec, the node's properties flat, the executable included (Q166; build.md §1): `nodes` is the complete topology, `build.create { Change(Reconcile { based_on, nodes }) }`.
    /// The stream is `Started`, then the step events of the create, ending `Complete` or
    /// `Failed { step, reason }`.
    pub async fn create_named(&self, based_on: BuildId, nodes: Vec<rdm_core::meta::NodeSpec>) -> Result<WorkflowStream, CallEnd> {
        self.workflow(WorkflowKind::Create, BuildChange::Reconcile { based_on, nodes }).await
    }

    /// `node.stop` of `node`.
    pub async fn stop(&self, node: &PathName) -> Result<WorkflowStream, CallEnd> {
        self.workflow(WorkflowKind::Stop(node.clone()), BuildChange::Stop { node: node.clone() }).await
    }

    /// `node.start` of the stopped `node`: the same process rejoins as itself (same node id,
    /// incarnation, endpoint key and port). The stream is `Started`, then `node.joined` and
    /// `node.started`, ending `Complete` or `Failed { step, reason }`.
    pub async fn start(&self, node: &PathName) -> Result<WorkflowStream, CallEnd> {
        self.workflow(WorkflowKind::Start(node.clone()), BuildChange::Start { node: node.clone() }).await
    }

    /// `node.restart` of `node`.
    pub async fn restart(&self, node: &PathName) -> Result<WorkflowStream, CallEnd> {
        self.workflow(WorkflowKind::Restart(node.clone()), BuildChange::Restart { node: node.clone() }).await
    }

    /// `node.connections.delete { All }` of `node` as a Build (node-connections-delete.md step 0: a
    /// seat holder's seats move first). The stream is `Started`, then `node.connections.deleted`,
    /// ending `Complete`; the process keeps running.
    pub async fn delete_connections(&self, node: &PathName) -> Result<WorkflowStream, CallEnd> {
        self.workflow(WorkflowKind::DeleteConnections(node.clone()), BuildChange::DeleteConnections { node: node.clone() }).await
    }

    /// `node.delete` of `node`.
    pub async fn delete(&self, node: &PathName) -> Result<WorkflowStream, CallEnd> {
        self.workflow(WorkflowKind::Delete(node.clone()), BuildChange::RemoveNode { node: node.clone() }).await
    }

    /// A recipe: one Build of node primitives (`BuildChange::Recipe`), each step its op's own
    /// workflow, run in order by RDM (build.md §1; lifecycles/node-replace.md). The caller owns the
    /// recipe. The stream is `Started`, then every step's events in step order, ending `Complete`
    /// or `Failed { step, reason }` naming the step that failed; a resume never runs a completed step.
    pub async fn recipe(&self, steps: Vec<rdm_core::build::RecipeStep>) -> Result<WorkflowStream, CallEnd> {
        self.workflow(WorkflowKind::Recipe(steps.clone()), BuildChange::Recipe { steps }).await
    }

    /// `node.primary.update` (`0x21`, `lifecycles/node-primary-update.md` C1): command the holder at
    /// `holder` to move its mesh-primary seat (and the fabric-primary seat with it when it holds both)
    /// to its same-mesh successor.
    pub async fn primary_update(&self, fabric: &str, holder: &PathName, build_id: &BuildId, attempt: u32) -> Result<rdm_core::handover::NodePrimaryUpdateReply, CallEnd> {
        crate::NodeRpc::new(self.carrier.client()).primary_update(fabric, holder, build_id, attempt, &CallOptions::default()).await
    }

    /// `node.fabric-primary.update` (`0x21`, `lifecycles/node-primary-update.md` C2): command the
    /// fabric-primary holder at `holder` to move the fabric-primary seat (step 1 same-mesh successor
    /// first, step 2 outside mesh primary when no same-mesh successor is ReadyForTraffic).
    pub async fn fabric_primary_update(&self, fabric: &str, holder: &PathName, build_id: &BuildId, attempt: u32) -> Result<rdm_core::handover::NodePrimaryUpdateReply, CallEnd> {
        crate::NodeRpc::new(self.carrier.client()).fabric_primary_update(fabric, holder, build_id, attempt, &CallOptions::default()).await
    }
}

/// A node read on `GetNodes` (`node.get`, `mesh.nodes.get`, `fabric.nodes.get`) to `target`: the
/// nodes it answered. A tag filter no node can match is refused here, by name, and nothing is sent.
async fn nodes_read(carrier: &BuildCarrier, target: &NodeTarget, op: &str, select: NodeSelect) -> Result<Vec<NodeView>, CallEnd> {
    if let NodeSelect::Tags(filter) | NodeSelect::MeshTags(_, filter) = &select {
        if let Some(reason) = filter.refusal() {
            return Err(refused(target, op, rdm_core::reply::status::bounded_rejected_tags(&reason)));
        }
    }
    match read(carrier, target, &StatusRequest::GetNodes { select }).await? {
        StatusReply::Nodes { nodes } => Ok(nodes),
        other => Err(refused(target, op, other)),
    }
}

/// A mesh read on `GetMeshes` (`mesh.get`, `fabric.meshes.get`) to `target`: the meshes it answered.
async fn meshes_read(carrier: &BuildCarrier, target: &NodeTarget, op: &str, mesh: Option<String>) -> Result<Vec<MeshView>, CallEnd> {
    match read(carrier, target, &StatusRequest::GetMeshes { mesh }).await? {
        StatusReply::Meshes { meshes } => Ok(meshes),
        other => Err(refused(target, op, other)),
    }
}

/// The `mesh` objects: reads as Status calls to the fabric-primary the carrier starts at.
#[derive(Debug, Clone)]
pub struct Meshes {
    carrier: BuildCarrier,
}

impl Meshes {
    /// The objects over `carrier`.
    pub fn new(carrier: BuildCarrier) -> Self {
        Self { carrier }
    }

    /// `mesh.get { name }`: the one mesh with the name (or minted id) `name`, as the fabric-primary's
    /// view holds it. A mesh it does not hold ends `Rejected` with `unknown-mesh`, naming the mesh.
    pub async fn get(&self, name: &str) -> Result<MeshView, CallEnd> {
        let span = MeshOp::Get.span();
        let target = NodeTarget::CurrentPath(self.carrier.fabric_primary().clone());
        let call = async {
            let meshes = meshes_read(&self.carrier, &target, MeshOp::Get.name(), Some(name.to_string())).await?;
            meshes.into_iter().next().ok_or_else(|| refused(&target, MeshOp::Get.name(), rdm_core::reply::status::unknown_mesh(name.to_string())))
        };
        let r = tracing::Instrument::instrument(call, span.clone()).await;
        ended_read(&span, MeshOp::Get.name(), &r);
        r
    }

    /// `mesh.nodes.get { tags? }`: every node of the mesh `mesh`, as the fabric-primary reads them
    /// from the rows, or, given `tags`, every node of it carrying the labels the filter names (all of
    /// them, or any of them; R-TG1), read through the tags index (Q165).
    pub async fn nodes(&self, mesh: &str, tags: Option<&TagFilter>) -> Result<Vec<NodeView>, CallEnd> {
        let span = MeshOp::NodesGet.span();
        let target = NodeTarget::CurrentPath(self.carrier.fabric_primary().clone());
        let select = match tags {
            None => NodeSelect::Mesh(mesh.to_string()),
            Some(filter) => NodeSelect::MeshTags(mesh.to_string(), filter.clone()),
        };
        let r = tracing::Instrument::instrument(nodes_read(&self.carrier, &target, MeshOp::NodesGet.name(), select), span.clone()).await;
        ended_read(&span, MeshOp::NodesGet.name(), &r);
        r
    }
}

/// The `fabric` objects: reads as Status calls to the fabric-primary the carrier starts at.
#[derive(Debug, Clone)]
pub struct Fabrics {
    carrier: BuildCarrier,
}

impl Fabrics {
    /// The objects over `carrier`.
    pub fn new(carrier: BuildCarrier) -> Self {
        Self { carrier }
    }

    /// `fabric.get`: the fabric as the fabric-primary's view holds it.
    pub async fn get(&self) -> Result<FabricView, CallEnd> {
        let span = FabricOp::Get.span();
        let target = NodeTarget::CurrentPath(self.carrier.fabric_primary().clone());
        let call = async {
            match read(&self.carrier, &target, &StatusRequest::GetFabric).await? {
                StatusReply::Fabric { fabric } => Ok(fabric),
                other => Err(refused(&target, FabricOp::Get.name(), other)),
            }
        };
        let r = tracing::Instrument::instrument(call, span.clone()).await;
        ended_read(&span, FabricOp::Get.name(), &r);
        r
    }

    /// `fabric.meshes.get`: every mesh of the fabric, as the fabric-primary's view holds them (Q165).
    pub async fn meshes(&self) -> Result<Vec<MeshView>, CallEnd> {
        let span = FabricOp::MeshesGet.span();
        let target = NodeTarget::CurrentPath(self.carrier.fabric_primary().clone());
        let r = tracing::Instrument::instrument(meshes_read(&self.carrier, &target, FabricOp::MeshesGet.name(), None), span.clone()).await;
        ended_read(&span, FabricOp::MeshesGet.name(), &r);
        r
    }

    /// `fabric.nodes.get { tags? }`: every node of every mesh, the fabric topology, as the
    /// fabric-primary reads them from the rows (the mesh selector's answer over every mesh), each with
    /// its status, config, tags and the executable its row records; given `tags`, every node of the
    /// fabric carrying the labels the filter names, read through the tags index (Q165).
    pub async fn nodes(&self, tags: Option<&TagFilter>) -> Result<Vec<NodeView>, CallEnd> {
        let span = FabricOp::NodesGet.span();
        let target = NodeTarget::CurrentPath(self.carrier.fabric_primary().clone());
        let select = match tags {
            None => NodeSelect::Fabric,
            Some(filter) => NodeSelect::Tags(filter.clone()),
        };
        let r = tracing::Instrument::instrument(nodes_read(&self.carrier, &target, FabricOp::NodesGet.name(), select), span.clone()).await;
        ended_read(&span, FabricOp::NodesGet.name(), &r);
        r
    }

    /// Await the typed fact `holds` of the fabric as the admin the carrier starts at reports it
    /// (`fabric.get`), read at half the gossip interval so a fact that spreads by gossip is read as it
    /// lands. `fact` names it. An admin that does not report it within `within` ends the call
    /// `Indeterminate`, naming the fact, the admin and the last thing it reported or how its last read
    /// ended.
    pub async fn fact(&self, fact: &str, within: std::time::Duration, holds: impl Fn(&FabricView) -> bool) -> Result<FabricView, CallEnd> {
        let report = |view: &FabricView| format!("status {:?}, fabric-primary {}, {} meshes", view.status, view.fabric_primary.as_ref().map(|p| p.to_string()).unwrap_or_else(|| "none".into()), view.meshes.len());
        await_fact("fabric", &self.carrier.fabric_primary().to_string(), fact, within, || self.get(), holds, report).await
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

    /// `build.get`: the Build with its attempts and step receipts, for reattachment after a cut.
    pub async fn get(&self, id: &BuildId) -> Result<BuildReceipts, CallEnd> {
        self.call(BuildOp::Get, self.carrier.get(id)).await
    }

    /// `build.delete`: drop a finished Build from history.
    pub async fn delete(&self, id: &BuildId) -> Result<(), CallEnd> {
        self.call(BuildOp::Delete, self.carrier.delete(id)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT: each op has one canonical name, rendered the same way everywhere: the name is
    /// `node.<verb>`, the span of a single call is `rdm.node_admin.<name>.via-call` and of a
    /// workflow `…via-workflow`, and an event's frame span is `…<event>.via-reply-frame`.
    #[test]
    fn one_name_per_op_renders_the_same_in_the_name_and_the_span() {
        use rdm_core::names::NodeEvent;
        let (_g, spans) = capture_spans();
        for op in [NodeOp::Get, NodeOp::NameUpdate, NodeOp::TopologyGet, NodeOp::ConnectionsGet, NodeOp::ConnectionsDelete, NodeOp::ConfigGet, NodeOp::ConfigUpdate, NodeOp::TagsCreate, NodeOp::TagsDelete, NodeOp::Create, NodeOp::Stop, NodeOp::Start, NodeOp::Restart, NodeOp::Delete, NodeOp::PrimaryUpdate, NodeOp::FabricPrimaryUpdate] {
            drop(op.span());
        }
        for e in [NodeEvent::Created, NodeEvent::Joined, NodeEvent::Started, NodeEvent::ConnectionsDeleted, NodeEvent::Stopped, NodeEvent::Deleted, NodeEvent::Restarting, NodeEvent::Restarted] {
            drop(e.span());
        }
        for b in [BuildOp::Create, BuildOp::Get, BuildOp::Delete] {
            drop(b.span());
        }
        drop(MeshOp::Get.span());
        drop(FabricOp::Get.span());
        let names = spans.lock().unwrap().clone();
        for op in [NodeOp::Get, NodeOp::NameUpdate, NodeOp::TopologyGet, NodeOp::ConnectionsGet, NodeOp::ConnectionsDelete, NodeOp::ConfigGet, NodeOp::ConfigUpdate, NodeOp::TagsCreate, NodeOp::TagsDelete, NodeOp::Create, NodeOp::Stop, NodeOp::Start, NodeOp::Restart, NodeOp::Delete, NodeOp::PrimaryUpdate, NodeOp::FabricPrimaryUpdate] {
            let via = if op.is_workflow() { "via-workflow" } else { "via-call" };
            let want = format!("rdm.node_admin.{}.{via}", op.name());
            assert!(names.contains(&want), "{want} missing from {names:?}");
        }
        for e in [NodeEvent::Created, NodeEvent::Joined, NodeEvent::Started, NodeEvent::ConnectionsDeleted, NodeEvent::Stopped, NodeEvent::Deleted, NodeEvent::Restarting, NodeEvent::Restarted] {
            let want = format!("rdm.node_admin.{}.via-reply-frame", e.name());
            assert!(names.contains(&want), "{want} missing from {names:?}");
        }
        for b in [BuildOp::Create, BuildOp::Get, BuildOp::Delete] {
            let want = format!("rdm.node_admin.{}.via-call", b.name());
            assert!(names.contains(&want), "{want} missing from {names:?}");
        }
        for want in [format!("rdm.node_admin.{}.via-call", MeshOp::Get.name()), format!("rdm.node_admin.{}.via-call", FabricOp::Get.name())] {
            assert!(names.contains(&want), "{want} missing from {names:?}");
        }
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
