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

use crate::build_stream::{Build, BuildCarrier};
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

    /// `node.get` (`query.md` §1): every node row the fabric-primary holds that matches every pair of
    /// `filter`, by `path.name`; the empty filter is the fabric's topology, and nothing matching is the
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
    ///     nodes.get(&Filter::new().eq("kind", ["broker"]).eq("status", ["ready"])).await.map_err(|e| e.to_string())
    /// }
    /// ```
    pub async fn get(&self, filter: &Filter) -> Result<Vec<NodeView>, CallEnd> {
        let target = NodeTarget::CurrentPath(self.carrier.fabric_primary().clone());
        read_list(&self.carrier, &target, NodeOp::Get.span(), NodeOp::Get.name(), Entity::Nodes, None, filter, |r| match r {
            StatusReply::Nodes { nodes } => Ok(nodes),
            other => Err(other),
        })
        .await
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

    /// `node.config.get`: no request of its own (Q167 R-RD4: a property's read is its row's field);
    /// [`Self::get`] answers the whole node, its config included.
    pub fn config_get(&self) -> CallEnd {
        CallEnd::NotBackedToday { op: NodeOp::ConfigGet, why: "node.config.get has no request of its own; node.get answers the node's config" }
    }

    /// `node.create` of the paths `nodes` adds to the topology Build `based_on` accepted, each named by
    /// its create spec, the node's properties flat, the executable included (Q166; build.md §1): `nodes` is the complete topology, `build.create { Change(Reconcile { based_on, nodes }) }`.
    /// The stream is `Started`, then the step events of the create, ending `Complete` or
    /// `Failed { step, reason }`.
    pub async fn create_named(&self, based_on: BuildId, nodes: Vec<rdm_core::meta::NodeSpec>) -> Result<WorkflowStream, CallEnd> {
        self.workflow(WorkflowKind::Create, BuildChange::Reconcile { topology_storage_provider: None, based_on, nodes }).await
    }

    /// `node.stop` of `node` (`lifecycles/node-stop.md`): delete its connections, leave its mesh and stop, keeping its `node.id`, `incarnation`, `path.name` and data dir.
    /// The stream is `Started`, then `node.connections.deleted` and `node.stopped`, ending `Complete` or `Failed { step, reason }`.
    pub async fn stop(&self, node: &PathName) -> Result<WorkflowStream, CallEnd> {
        self.workflow(WorkflowKind::Stop(node.clone()), BuildChange::Stop { node: node.clone() }).await
    }

    /// `node.start` of the stopped `node`: the same process rejoins as itself (same node id,
    /// incarnation, endpoint key and port). The stream is `Started`, then `node.joined` and
    /// `node.started`, ending `Complete` or `Failed { step, reason }`.
    pub async fn start(&self, node: &PathName) -> Result<WorkflowStream, CallEnd> {
        self.workflow(WorkflowKind::Start(node.clone()), BuildChange::Start { node: node.clone() }).await
    }

    /// `node.connections.delete { All }` of `node` as a Build (`lifecycles/node-connections-delete.md`: a
    /// target that is a primary runs `node.primary.update` first). The stream is `Started`, then `node.connections.deleted`,
    /// ending `Complete`; the process keeps running.
    pub async fn delete_connections(&self, node: &PathName) -> Result<WorkflowStream, CallEnd> {
        self.workflow(WorkflowKind::DeleteConnections(node.clone()), BuildChange::DeleteConnections { node: node.clone() }).await
    }

    /// `node.delete` of `node` (`lifecycles/node-delete.md`): compose `node.stop` when the process is live, terminate its exact runtime through the provider, and release `node.id` and `path.name`.
    /// The stream is `Started`, every `node.stop` event when live, and `node.deleted`, ending `Complete` or `Failed { step, reason }`.
    pub async fn delete(&self, node: &PathName) -> Result<WorkflowStream, CallEnd> {
        self.workflow(WorkflowKind::Delete(node.clone()), BuildChange::RemoveNode { node: node.clone() }).await
    }

    /// Steps: one Build of node primitives (`BuildChange::Steps`), each step its op's own
    /// workflow, run in order by RDM (build.md §1; lifecycles/node-replace.md). The caller owns the
    /// steps. The stream is `Started`, then every step's events in step order, ending `Complete`
    /// or `Failed { step, reason }` naming the step that failed; a resume never runs a completed step.
    pub async fn steps(&self, steps: Vec<rdm_core::build::Step>) -> Result<WorkflowStream, CallEnd> {
        self.workflow(WorkflowKind::Steps(steps.clone()), BuildChange::Steps { steps }).await
    }

    /// `node.primary.update` (`0x21`, `lifecycles/node-primary-update.md`): command the mesh-primary
    /// to move its mesh-primary role (and the fabric-primary role with it when it is both)
    /// to its same-mesh successor.
    pub async fn primary_update(&self, fabric: &str, holder: &PathName, build_id: &BuildId, attempt: u32) -> Result<rdm_core::handover::NodePrimaryUpdateReply, CallEnd> {
        crate::NodeRpc::new(self.carrier.client()).primary_update(fabric, holder, build_id, attempt, &CallOptions::default()).await
    }

    /// `node.fabric-primary.update` (`0x21`, `lifecycles/node-primary-update.md` "The fabric-primary"): command the
    /// fabric-primary to move the fabric-primary role (step 1 same-mesh successor
    /// first, step 2 another Ready mesh-primary when no same-mesh successor is Ready).
    pub async fn fabric_primary_update(&self, fabric: &str, holder: &PathName, build_id: &BuildId, attempt: u32) -> Result<rdm_core::handover::NodePrimaryUpdateReply, CallEnd> {
        crate::NodeRpc::new(self.carrier.client()).fabric_primary_update(fabric, holder, build_id, attempt, &CallOptions::default()).await
    }
}

/// A read (Q167) to `target`: `Get { entity, node, filter }` under `span`, the list `answer` takes
/// from its reply, or how it ended; a reply that is not the answer (`RejectedFilter` naming the pair,
/// `ReadOverBound`) ends `Rejected`, named. A filter the entity's schema refuses is refused here,
/// `rejected-filter` naming the pair, as the node would refuse it, and nothing is sent.
#[allow(clippy::too_many_arguments)]
async fn read_list<T>(
    carrier: &BuildCarrier,
    target: &NodeTarget,
    span: tracing::Span,
    op: &'static str,
    entity: Entity,
    node: Option<(NodeId, rdm_core::IncarnationId)>,
    filter: &Filter,
    answer: impl FnOnce(StatusReply) -> Result<T, StatusReply>,
) -> Result<T, CallEnd> {
    span.record("properties", filter.properties().as_str());
    if let Some(reason) = filter.refusal(entity) {
        let r = Err(refused(target, op, rdm_core::reply::status::bounded_rejected_filter(&reason)));
        ended_read(&span, op, &r);
        return r;
    }
    let req = StatusRequest::Get { entity, node, filter: filter.clone() };
    let call = async { answer(read(carrier, target, &req).await?).map_err(|other| refused(target, op, other)) };
    let r = tracing::Instrument::instrument(call, span.clone()).await;
    ended_read(&span, op, &r);
    r
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

    /// `mesh.get` (Q167): every mesh the fabric-primary holds that matches every pair of `filter`, by
    /// name; a mesh's nodes are `node.get [mesh: name]`.
    pub async fn get(&self, filter: &Filter) -> Result<Vec<MeshView>, CallEnd> {
        let target = NodeTarget::CurrentPath(self.carrier.fabric_primary().clone());
        read_list(&self.carrier, &target, MeshOp::Get.span(), MeshOp::Get.name(), Entity::Meshes, None, filter, |r| match r {
            StatusReply::Meshes { meshes } => Ok(meshes),
            other => Err(other),
        })
        .await
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

    /// `fabric.get` (Q167): the fabric the fabric-primary holds, when it matches every pair of
    /// `filter`: its id, name, state, provider, fabric-primary and meshes; its nodes are `node.get`.
    pub async fn get(&self, filter: &Filter) -> Result<Vec<FabricView>, CallEnd> {
        let target = NodeTarget::CurrentPath(self.carrier.fabric_primary().clone());
        read_list(&self.carrier, &target, FabricOp::Get.span(), FabricOp::Get.name(), Entity::Fabrics, None, filter, |r| match r {
            StatusReply::Fabrics { fabrics } => Ok(fabrics),
            other => Err(other),
        })
        .await
    }

    /// The fabric the fabric-primary holds (`fabric.get` with no pair): the one row it answers.
    async fn the_fabric(&self) -> Result<FabricView, CallEnd> {
        let target = NodeTarget::CurrentPath(self.carrier.fabric_primary().clone());
        self.get(&Filter::new()).await?.into_iter().next().ok_or_else(|| CallEnd::NotFound { read: FabricOp::Get.name(), detail: format!("{target:?} answered fabric.get with no fabric") })
    }

    /// `fabric.atlas.get` (docs/plans/proposal/fabric-atlas.md R-FA4): the fabric-primary's
    /// atlas, one row per node of the current Build's topology: the birth its node row names
    /// current, its lifecycle as the ops left it, and the operation accepted on it whose attempt has
    /// not completed. `fabric.get` is unchanged by it.
    pub async fn atlas_get(&self) -> Result<AtlasView, CallEnd> {
        let span = FabricOp::AtlasGet.span();
        let target = NodeTarget::CurrentPath(self.carrier.fabric_primary().clone());
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
        let target = NodeTarget::CurrentPath(self.carrier.fabric_primary().clone());
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

    /// Await the typed fact `holds` of the fabric as the admin the carrier starts at reports it
    /// (`fabric.get`), read at half the gossip interval so a fact that spreads by gossip is read as it
    /// lands. `fact` names it. An admin that does not report it within `within` ends the call
    /// `Indeterminate`, naming the fact, the admin and the last thing it reported or how its last read
    /// ended.
    pub async fn fact(&self, fact: &str, within: std::time::Duration, holds: impl Fn(&FabricView) -> bool) -> Result<FabricView, CallEnd> {
        let report = |view: &FabricView| format!("status {:?}, fabric-primary {}, {} meshes", view.status, view.fabric_primary.as_ref().map(|p| p.to_string()).unwrap_or_else(|| "none".into()), view.meshes.len());
        await_fact("fabric", &self.carrier.fabric_primary().to_string(), fact, within, || self.the_fabric(), holds, report).await
    }
}

/// The `connections` objects: `node.connection.get { node }` (Q167 R-RD7), a Status call to the exact
/// birth whose connections log holds the connections, the node the read's address.
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
    /// named by the minted id `node.connections.delete` names it by.
    pub async fn get(&self, birth: &ExactBirth, filter: &Filter) -> Result<ConnectionsView, CallEnd> {
        let node = Some((birth.node_id.clone(), birth.incarnation.clone()));
        read_list(&self.carrier, &birth.target, NodeOp::ConnectionsGet.span(), NodeOp::ConnectionsGet.name(), Entity::Connections, node, filter, |r| match r {
            StatusReply::Connections { connections } => Ok(connections),
            other => Err(other),
        })
        .await
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
    /// segments (telemetry-patterns.md §8): the op `node.connections.delete` spans
    /// `rdm.node_admin.connection.delete.via-call`, a workflow `…via-workflow` (or
    /// `node.update.via-<op>-workflow` for stop and start), and an event's frame
    /// `…via-<event>-frame`. Every op and event opens exactly the span this table names.
    #[test]
    fn one_name_per_op_opens_its_one_literal_span() {
        use rdm_core::names::NodeEvent;
        let (_g, spans) = capture_spans();
        let ops: [(tracing::Span, &str); 15] = [
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
