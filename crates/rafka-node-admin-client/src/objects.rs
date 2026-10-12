//! The `node`, `mesh`, `fabric` and `build` objects.
//!
//! Workflows are submitted to the fabric-primary through the Build family (op `0x20`,
//! `build.create`) and answered with a reply stream ([`WorkflowStream`]) read from the call itself.
//! The `build` objects are that family's calls. The reads are Status calls (op `0x1B`): `node.get`,
//! `mesh.get` and `fabric.get` go to the fabric-primary the carrier starts at, which answers from its
//! view, and `node.connections.get` to the exact birth whose connections it reads. A read the node
//! refuses ends `Rejected`, naming the refusal. Calls whose request is valid but that no op carries
//! yet end `NotBackedToday`, naming why; no request is invented for them.

use crate::build_stream::{BuildCarrier, BuildReceipts, BuildStream};
use crate::names::{BuildOp, FabricOp, MeshOp, NodeOp};
use crate::workflow::{WorkflowKind, WorkflowStream};
use crate::{Accepted, BuildId, CallEnd, ConnectionsView, ExactBirth, FabricDesired, FabricView, MeshDesired, MeshView, NodeStatus, NodeView};
use rafka_mesh_entity::{IncarnationId, NodeId, NodeKind, PathName};
use rafka_node_rpc::{CallOptions, NodeTarget};
use rafka_node_rpc_contract::build::{BuildChange, MeshCounts};
use rafka_node_rpc_contract::status::{NodeSelect, Status, StatusReply, StatusRequest};
use std::collections::BTreeSet;

fn counts(m: &MeshDesired) -> MeshCounts {
    MeshCounts { name: m.name.clone(), node_admin: m.node_admin, rpc_node: m.rpc_node, broker: m.broker, gateway: m.gateway, compute: m.compute }
}

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
    let every = rafka_mesh_entity::cadence::gossip_interval() / 2;
    loop {
        let last = match read().await {
            Ok(view) if holds(&view) => return Ok(view),
            Ok(view) => report(&view),
            Err(e) => e.to_string(),
        };
        if started.elapsed() >= within {
            return Err(CallEnd::Indeterminate { reason: format!("{admin} never reported the {kind} fact {fact} within {within:?}; its last report was: {last}") });
        }
        tokio::time::sleep(every).await;
    }
}

/// What `node.create` is given: the mesh to add a node to and the node's kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeSpec {
    /// The mesh the node joins.
    pub mesh: String,
    /// The node's kind.
    pub kind: NodeKind,
}

pub use rafka_node_rpc_contract::status::TagFilter;

/// What `node.get` selects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeSelector {
    /// The node with this minted id.
    Id(NodeId),
    /// Every node of the mesh.
    Mesh(String),
    /// Every node carrying the tag labels the filter names: all of them, or any of them (R-TG1).
    Tags(TagFilter),
}

/// A field of a node's meta that RDM owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetaField {
    /// `node.id`: never copied, never put.
    NodeId,
    /// `incarnation`: never copied, never put.
    Incarnation,
    /// `endpoint`: never copied, never put.
    Endpoint,
    /// `transport_addr`: never copied, never put.
    TransportAddr,
    /// `exe`: never copied, never put.
    Exe,
    /// The runtime handle: never copied, never put.
    Runtime,
    /// `status`: moves only through the actions and workflows.
    Status,
    /// `path` (`<mesh>.<kind>`): copied on replace, never put.
    Path,
    /// `name`: changes only while the node is stopped.
    Name,
    /// `mesh`: copied on replace, never put.
    Mesh,
    /// `kind`: copied on replace, never put.
    Kind,
}

impl MetaField {
    /// The field's name.
    pub const fn name(self) -> &'static str {
        match self {
            Self::NodeId => "node.id",
            Self::Incarnation => "incarnation",
            Self::Endpoint => "endpoint",
            Self::TransportAddr => "transport_addr",
            Self::Exe => "exe",
            Self::Runtime => "runtime handle",
            Self::Status => "status",
            Self::Path => "path",
            Self::Name => "name",
            Self::Mesh => "mesh",
            Self::Kind => "kind",
        }
    }
}

/// A `node.update` body differs from what RDM holds in a field RDM owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaFieldIsRdmOwned {
    /// The field.
    pub field: MetaField,
}

impl std::fmt::Display for MetaFieldIsRdmOwned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node.update: {} is owned by RDM and differs from what RDM holds", self.field.name())
    }
}

impl std::error::Error for MetaFieldIsRdmOwned {}

/// A create or a rename into a `path.name` another node holds, parked or live: a name is held by
/// exactly one node until that node is renamed or deleted (ops-naming "Identity";
/// node-rpc-envelope.md "Meta and config"). The claim is insert-and-fail on the name's key.
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

/// Everything RDM stores about a node: the whole entity `node.update` takes and sends back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeMeta {
    /// RDM-owned, never copied and never put.
    pub node_id: NodeId,
    /// RDM-owned, never copied and never put.
    pub incarnation: Option<IncarnationId>,
    /// RDM-owned, never copied and never put.
    pub endpoint: Option<String>,
    /// RDM-owned, never copied and never put.
    pub transport_addr: Option<std::net::SocketAddr>,
    /// RDM-owned, never copied and never put: the executable's identity.
    pub exe: Option<String>,
    /// RDM-owned, never copied and never put: the runtime handle, the deployment that runs the birth.
    pub runtime: Option<String>,
    /// RDM-owned, never copied and never put.
    pub status: NodeStatus,
    /// RDM-owned, copied on replace: the node's `path.name`.
    pub name: PathName,
    /// RDM-owned, copied on replace.
    pub mesh: String,
    /// RDM-owned, copied on replace.
    pub kind: NodeKind,
    /// Caller-owned, copied on replace and settable by put: the node's OpenTelemetry debug level.
    pub otel_debug: bool,
    /// Caller-owned, copied on recover and replace and settable by put: the node's tag labels
    /// (R-TG1), a set a put replaces whole.
    pub tags: BTreeSet<String>,
}

impl NodeMeta {
    /// The successor `self` with the caller-owned group of `predecessor` copied onto it: the copy
    /// step of recover and replace. Nothing RDM owns is copied, the name least of all: the
    /// successor's `path.name` and identity come from its own create spec.
    pub fn with_caller_owned_of(&self, predecessor: &NodeMeta) -> NodeMeta {
        NodeMeta { otel_debug: predecessor.otel_debug, tags: predecessor.tags.clone(), ..self.clone() }
    }

    /// `node.update`'s validation from the held record, in the documented order
    /// (node-rpc-envelope.md "Meta and config"; R13 step 2 and the first half of step 3): every
    /// RDM-owned field equals the held value, else `MetaFieldIsRdmOwned { field }`; a changed name
    /// requires the node quiet, that is stopped, or drained with `node.connections.deleted` (R-CR1a);
    /// a node that can carry traffic keeps its name whatever its name. The only renames are a node to or
    /// from its `.old` form and to or from its `.new` form (node-replace.md); `.old` to `.new` and a
    /// new ordinal are refused naming the name.
    pub fn check_update(&self, held: &NodeMeta, traffic: Traffic) -> Result<(), MetaFieldIsRdmOwned> {
        let refuse = |field| Err(MetaFieldIsRdmOwned { field });
        if self.node_id != held.node_id {
            return refuse(MetaField::NodeId);
        }
        if self.incarnation != held.incarnation {
            return refuse(MetaField::Incarnation);
        }
        if self.endpoint != held.endpoint {
            return refuse(MetaField::Endpoint);
        }
        if self.transport_addr != held.transport_addr {
            return refuse(MetaField::TransportAddr);
        }
        if self.exe != held.exe {
            return refuse(MetaField::Exe);
        }
        if self.runtime != held.runtime {
            return refuse(MetaField::Runtime);
        }
        if self.status != held.status {
            return refuse(MetaField::Status);
        }
        if self.mesh != held.mesh {
            return refuse(MetaField::Mesh);
        }
        if self.kind != held.kind {
            return refuse(MetaField::Kind);
        }
        if (self.name.mesh.as_str(), self.name.kind) != (held.name.mesh.as_str(), held.name.kind) {
            return refuse(MetaField::Path);
        }
        if self.name != held.name {
            // One side is the bare name and the other its `.old` or `.new` form.
            let suffixed = |n: &PathName| n.old || n.new;
            let one_suffix_toggles = self.name.base() == held.name.base() && suffixed(&self.name) != suffixed(&held.name);
            let quiet = match traffic {
                Traffic::Stopped | Traffic::ConnectionsDeleted => true,
                Traffic::Routed => false,
            };
            if !(one_suffix_toggles && quiet) {
                return refuse(MetaField::Name);
            }
        }
        Ok(())
    }
}

/// Whether the node a `node.update` names can carry traffic, as its mesh-primary holds it: a name
/// changes only when it is not ready-for-traffic and holds no connection (R-CR1a, R-CR2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Traffic {
    /// The held status is stopped.
    Stopped,
    /// The node was drained and its mesh-primary holds its `node.connections.deleted` of every
    /// connection.
    ConnectionsDeleted,
    /// Anything else: the node can carry traffic.
    Routed,
}

/// What `build.create` is given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildSpec {
    /// Reconcile the whole fabric to these meshes.
    Fabric(FabricDesired),
    /// A named, fixed set of meshes for the fabric named `fabric`.
    Preset {
        /// The preset.
        preset: BuildPreset,
        /// The fabric it reconciles.
        fabric: String,
    },
    /// A new mesh with these counts, beside the meshes the fabric holds.
    CreateMesh(MeshDesired),
}

/// A preset `build.create` expands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildPreset {
    /// The demo set: `mesh1` with 2 node-admins and 3 rpc nodes (the shape the demo's bootstrap
    /// reconciles to today).
    Demo,
}

impl BuildPreset {
    /// The meshes the preset reconciles a fabric named `fabric` to.
    pub fn desired(self, fabric: &str) -> FabricDesired {
        match self {
            Self::Demo => FabricDesired { fabric: fabric.to_string(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 3)])] },
        }
    }
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
        let span = kind.op().span();
        match tracing::Instrument::instrument(self.carrier.create(change), span.clone()).await {
            Ok(inner) => Ok(WorkflowStream::new(inner, kind, span)),
            Err(end) => {
                span.record("outcome", end.outcome());
                span.in_scope(|| tracing::info!(op = kind.op().name(), reason = %end, "the workflow call was not accepted"));
                Err(end)
            }
        }
    }

    /// Re-submit the workflow `kind` of the call that opened `accepted`: never a second Build. A
    /// complete Build answers `AlreadyApplied` with its frames and terminal; a running one streams
    /// from where it is; a failed one takes its next attempt.
    pub async fn resume(&self, kind: WorkflowKind, accepted: &Accepted) -> Result<WorkflowStream, CallEnd> {
        let span = kind.op().span();
        match tracing::Instrument::instrument(self.carrier.resubmit(&accepted.build_id, accepted.attempt), span.clone()).await {
            Ok(inner) => Ok(WorkflowStream::new(inner, kind, span)),
            Err(end) => {
                span.record("outcome", end.outcome());
                span.in_scope(|| tracing::info!(op = kind.op().name(), reason = %end, "the workflow re-submit was not accepted"));
                Err(end)
            }
        }
    }

    /// `node.get`: the nodes `selector` selects, as the fabric-primary's view holds them.
    pub async fn get(&self, selector: &NodeSelector) -> Result<Vec<NodeView>, CallEnd> {
        let span = NodeOp::Get.span();
        let r = tracing::Instrument::instrument(self.select(selector), span.clone()).await;
        ended_read(&span, NodeOp::Get.name(), &r);
        r
    }

    async fn select(&self, selector: &NodeSelector) -> Result<Vec<NodeView>, CallEnd> {
        let target = NodeTarget::CurrentPath(self.carrier.fabric_primary().clone());
        let select = match selector {
            NodeSelector::Id(id) => NodeSelect::Id(id.clone()),
            NodeSelector::Mesh(mesh) => NodeSelect::Mesh(mesh.clone()),
            // A filter no node can match is refused here, by name, and nothing is sent.
            NodeSelector::Tags(filter) => match filter.refusal() {
                Some(reason) => return Err(refused(&target, NodeOp::Get.name(), StatusReply::rejected_tags(&reason))),
                None => NodeSelect::Tags(filter.clone()),
            },
        };
        match read(&self.carrier, &target, &StatusRequest::GetNodes { select }).await? {
            StatusReply::Nodes { nodes } => Ok(nodes),
            other => Err(refused(&target, NodeOp::Get.name(), other)),
        }
    }

    /// `node.update`: put the full meta entity `body` over what RDM `held`, a single call to the
    /// node's mesh-primary `mesh_primary` over `route` (R13; [`crate::NodeRpc::node_update`]). The
    /// mesh-primary decides from its own held record whether the node carries traffic; the request
    /// carries the exact birth, the name and the tags, so a body that differs from `held` in a field
    /// RDM owns is refused here, `RejectedMetaFieldIsRdmOwned { field }`, as are tags that are not a
    /// node's tag set, `RejectedTags`, and nothing is sent. A body that changes `otel_debug` ends
    /// `NotBackedToday`: the node row has no field that carries it.
    pub async fn update(&self, held: &NodeMeta, body: &NodeMeta, mesh_primary: &NodeId, route: &rafka_node_rpc::RouteChoice) -> Result<rafka_node_rpc_contract::status::StatusReply, CallEnd> {
        // The name's traffic rule is the mesh-primary's: only the fields the request cannot carry are checked here.
        if let Err(e) = body.check_update(held, crate::Traffic::Stopped) {
            let span = NodeOp::Update.span();
            span.record("outcome", "meta-field-is-rdm-owned");
            span.in_scope(|| tracing::info!(op = NodeOp::Update.name(), field = e.field.name(), "the put changes a field RDM owns; nothing is sent"));
            return Ok(rafka_node_rpc_contract::status::StatusReply::RejectedMetaFieldIsRdmOwned { field: e.field.name().into() });
        }
        if let Some(reason) = rafka_node_rpc_contract::status::tags_refusal(&body.tags) {
            let span = NodeOp::Update.span();
            span.record("outcome", "rejected-tags");
            span.in_scope(|| tracing::info!(op = NodeOp::Update.name(), reason = %reason, "the put's tags are not a node's tag set; nothing is sent"));
            return Ok(rafka_node_rpc_contract::status::StatusReply::rejected_tags(&reason));
        }
        if body.otel_debug != held.otel_debug {
            return Err(CallEnd::NotBackedToday { op: NodeOp::Update, why: "otel_debug: the node row has no field that carries it" });
        }
        let incarnation = held.incarnation.clone().ok_or_else(|| CallEnd::Indeterminate { reason: format!("{} ({}) has no known birth; node.update names an exact birth", held.name, held.node_id) })?;
        let birth = crate::ExactBirth { target: rafka_node_rpc::NodeTarget::ExactNode(held.node_id.clone()), node_id: held.node_id.clone(), incarnation };
        crate::NodeRpc::new(self.carrier.client()).node_update(mesh_primary, route, &birth, &body.name, &body.tags, &rafka_node_rpc::CallOptions::default()).await
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

    /// `node.connections.delete`: the hard cut of the connections of the exact birth `held` names,
    /// `scope` `All` (the stop's cut) or `Connections` naming open entries by id, a single call to the
    /// node's mesh-primary `mesh_primary` over `route` (R-CR2, R-CR3; [`crate::NodeRpc::connections_delete`]).
    /// A scope that names nothing to cut is refused here by name, `RejectedConnectionScope`, and
    /// nothing is sent.
    pub async fn connections_delete(&self, held: &NodeMeta, mesh_primary: &NodeId, route: &rafka_node_rpc::RouteChoice, scope: &rafka_node_rpc_contract::status::ConnectionScope) -> Result<rafka_node_rpc_contract::status::StatusReply, CallEnd> {
        if let Some(reason) = scope.refusal() {
            let span = NodeOp::ConnectionsDelete.span();
            span.record("outcome", "rejected-connection-scope");
            span.in_scope(|| tracing::info!(op = NodeOp::ConnectionsDelete.name(), reason = %reason, "the scope names nothing to cut; nothing is sent"));
            return Ok(rafka_node_rpc_contract::status::StatusReply::RejectedConnectionScope { reason: format!("{}: {reason}", held.name) });
        }
        let incarnation = held.incarnation.clone().ok_or_else(|| CallEnd::Indeterminate { reason: format!("{} ({}) has no known birth; node.connections.delete names an exact birth", held.name, held.node_id) })?;
        let birth = crate::ExactBirth { target: rafka_node_rpc::NodeTarget::ExactNode(held.node_id.clone()), node_id: held.node_id.clone(), incarnation };
        crate::NodeRpc::new(self.carrier.client()).connections_delete(mesh_primary, route, &birth, scope, &rafka_node_rpc::CallOptions::default()).await
    }

    /// `node.config.get`.
    pub fn config_get(&self) -> CallEnd {
        CallEnd::NotBackedToday { op: NodeOp::ConfigGet, why: "no op carries node config" }
    }

    /// `node.create(spec)`: add a node to a mesh. The stream is `Started`, then the step events
    /// of the create, ending `Complete` or `Failed { step, reason }`.
    pub async fn create(&self, spec: &NodeSpec) -> Result<WorkflowStream, CallEnd> {
        self.workflow(WorkflowKind::Create, BuildChange::AddNode { mesh: spec.mesh.clone(), node_kind: spec.kind }).await
    }

    /// `node.stop` of `node`.
    pub async fn stop(&self, node: &PathName) -> Result<WorkflowStream, CallEnd> {
        self.workflow(WorkflowKind::Stop(node.clone()), BuildChange::Stop { node: node.clone() }).await
    }

    /// `node.start` of the parked `node`: the same process rejoins as itself (same node id,
    /// incarnation, endpoint key and port). The stream is `Started`, then `node.joined` and
    /// `node.started`, ending `Complete` or `Failed { step, reason }`.
    pub async fn start(&self, node: &PathName) -> Result<WorkflowStream, CallEnd> {
        self.workflow(WorkflowKind::Start(node.clone()), BuildChange::Start { node: node.clone() }).await
    }

    /// `node.restart` of `node`.
    pub async fn restart(&self, node: &PathName) -> Result<WorkflowStream, CallEnd> {
        self.workflow(WorkflowKind::Restart(node.clone()), BuildChange::Restart { node: node.clone() }).await
    }

    /// `node.delete` of `node`.
    pub async fn delete(&self, node: &PathName) -> Result<WorkflowStream, CallEnd> {
        self.workflow(WorkflowKind::Delete(node.clone()), BuildChange::RemoveNode { node: node.clone() }).await
    }
}

/// What `mesh.get` selects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeshSelector {
    /// The mesh with this minted id or this name.
    Mesh(String),
    /// Every mesh of the fabric.
    Fabric,
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

    /// `mesh.get`: the meshes `selector` selects, as the fabric-primary's view holds them. A mesh it
    /// does not hold ends `Rejected` with `unknown-mesh`, naming the mesh.
    pub async fn get(&self, selector: &MeshSelector) -> Result<Vec<MeshView>, CallEnd> {
        let span = MeshOp::Get.span();
        let mesh = match selector {
            MeshSelector::Mesh(m) => Some(m.clone()),
            MeshSelector::Fabric => None,
        };
        let target = NodeTarget::CurrentPath(self.carrier.fabric_primary().clone());
        let call = async {
            match read(&self.carrier, &target, &StatusRequest::GetMeshes { mesh }).await? {
                StatusReply::Meshes { meshes } => Ok(meshes),
                other => Err(refused(&target, MeshOp::Get.name(), other)),
            }
        };
        let r = tracing::Instrument::instrument(call, span.clone()).await;
        ended_read(&span, MeshOp::Get.name(), &r);
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

    /// `build.create`: accept a Build that reconciles the fabric to `spec` and follow it. A preset
    /// is expanded against the fabric it names.
    pub async fn create(&self, spec: &BuildSpec) -> Result<BuildStream, CallEnd> {
        let desired = match spec {
            BuildSpec::Fabric(d) => d.clone(),
            BuildSpec::Preset { preset, fabric } => preset.desired(fabric),
            BuildSpec::CreateMesh(mesh) => return self.call(BuildOp::Create, self.carrier.create(BuildChange::CreateMesh { mesh: counts(mesh) })).await,
        };
        self.call(BuildOp::Create, self.carrier.create(BuildChange::ReconcileFabric { fabric: desired.fabric.clone(), meshes: desired.meshes.iter().map(counts).collect() })).await
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

    fn held() -> NodeMeta {
        NodeMeta {
            node_id: NodeId::mint(),
            incarnation: Some(IncarnationId::mint()),
            endpoint: Some("ep-1".into()),
            transport_addr: Some("127.0.0.1:4000".parse().unwrap()),
            exe: Some("rpc-node@abc123".into()),
            runtime: Some("dep-1".into()),
            status: NodeStatus::ReadyForTraffic,
            name: "mesh1.rpc.1".parse().unwrap(),
            mesh: "mesh1".into(),
            kind: NodeKind::RpcNode,
            otel_debug: false,
            tags: BTreeSet::new(),
        }
    }

    /// CONTRACT: a put may change only the caller-owned group. Flipping `otel_debug` is accepted; a
    /// body that differs from what RDM holds in any RDM-owned field is refused naming that field.
    #[test]
    fn a_put_changing_a_field_rdm_owns_is_refused_naming_the_field() {
        let held = held();
        assert_eq!(NodeMeta { otel_debug: true, ..held.clone() }.check_update(&held, Traffic::Routed), Ok(()));
        assert_eq!(NodeMeta { tags: ["rabbit".to_string()].into(), ..held.clone() }.check_update(&held, Traffic::Routed), Ok(()), "tags are caller-owned: a put may change them whatever the traffic");
        let cases: Vec<(NodeMeta, MetaField)> = vec![
            (NodeMeta { node_id: NodeId::mint(), ..held.clone() }, MetaField::NodeId),
            (NodeMeta { incarnation: Some(IncarnationId::mint()), ..held.clone() }, MetaField::Incarnation),
            (NodeMeta { endpoint: Some("ep-2".into()), ..held.clone() }, MetaField::Endpoint),
            (NodeMeta { transport_addr: Some("127.0.0.1:4001".parse().unwrap()), ..held.clone() }, MetaField::TransportAddr),
            (NodeMeta { exe: Some("rpc-node@def456".into()), ..held.clone() }, MetaField::Exe),
            (NodeMeta { runtime: Some("dep-2".into()), ..held.clone() }, MetaField::Runtime),
            (NodeMeta { status: NodeStatus::Leaving, ..held.clone() }, MetaField::Status),
            (NodeMeta { mesh: "mesh2".into(), ..held.clone() }, MetaField::Mesh),
            (NodeMeta { kind: NodeKind::Broker, ..held.clone() }, MetaField::Kind),
            (NodeMeta { name: "mesh2.rpc.1".parse().unwrap(), ..held.clone() }, MetaField::Path),
        ];
        for (body, field) in cases {
            assert_eq!(body.check_update(&held, Traffic::Stopped), Err(MetaFieldIsRdmOwned { field }), "{}", field.name());
        }
    }

    /// CONTRACT: a node's name changes only while the node is stopped, and only to or from its
    /// `.old` form: a live node's rename is refused, and so is a new ordinal on a stopped node.
    #[test]
    fn a_name_changes_only_while_stopped_and_only_to_or_from_old() {
        let held = held();
        let old = NodeMeta { name: held.name.renamed(), ..held.clone() };
        assert_eq!(old.check_update(&held, Traffic::Routed), Err(MetaFieldIsRdmOwned { field: MetaField::Name }), "a live node keeps its name");
        assert_eq!(old.check_update(&held, Traffic::Stopped), Ok(()), "a stopped node gives up its name to <name>.old");
        assert_eq!(held.check_update(&old, Traffic::Stopped), Ok(()), "and takes it back");
        let renumbered = NodeMeta { name: PathName { ordinal: 9, ..held.name.clone() }, ..held.clone() };
        assert_eq!(renumbered.check_update(&held, Traffic::Stopped), Err(MetaFieldIsRdmOwned { field: MetaField::Name }), "no rename but .old exists");
    }

    /// CONTRACT (R13 step 3, R-CR1a; node-replace.md rollback 1): a node takes another name only
    /// when quiet, stopped or drained with `node.connections.deleted`, and then only between the bare
    /// name and one suffix; a node that can carry traffic keeps its name, `.new` included; `.old` and
    /// `.new` never swap.
    #[test]
    fn a_name_changes_only_when_quiet_and_only_between_the_bare_name_and_one_suffix() {
        let base = held();
        let new = NodeMeta { name: base.name.successor(), ..base.clone() };
        let old = NodeMeta { name: base.name.renamed(), ..base.clone() };
        let name = |r| r == Err(MetaFieldIsRdmOwned { field: MetaField::Name });
        assert_eq!(base.check_update(&new, Traffic::Stopped), Ok(()), "the stopped successor takes the name");
        assert_eq!(base.check_update(&new, Traffic::ConnectionsDeleted), Ok(()), "the drained successor whose connections were deleted takes the name");
        assert!(name(base.check_update(&new, Traffic::Routed)), "a successor that can carry traffic keeps .new");
        assert_eq!(new.check_update(&base, Traffic::Stopped), Ok(()), "rollback: the stopped successor gives the name back");
        assert!(name(old.check_update(&base, Traffic::Routed)), "the predecessor gives up its name only when quiet");
        assert!(name(new.check_update(&old, Traffic::Stopped)), ".old never becomes .new");
        assert!(name(old.check_update(&new, Traffic::Stopped)), ".new never becomes .old");
    }

    /// CONTRACT: the copy step of recover and replace copies only the caller-owned group: the
    /// successor takes the predecessor's `otel_debug` and keeps every field RDM owns as its own,
    /// its name included (ops-naming "Identity"; node-rpc-envelope.md "Meta and config").
    #[test]
    fn the_copy_step_copies_only_caller_owned_fields_never_the_name() {
        let predecessor = NodeMeta { otel_debug: true, name: held().name.renamed(), ..held() };
        let successor = NodeMeta { node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()), endpoint: Some("ep-9".into()), runtime: Some("dep-9".into()), status: NodeStatus::Pending, ..held() };
        let copied = successor.with_caller_owned_of(&predecessor);
        assert_eq!(copied, NodeMeta { otel_debug: true, ..successor.clone() }, "only otel_debug moves; the name stays the successor's own");
    }

    /// CONTRACT: each op has one canonical name, rendered the same way everywhere: the name is
    /// `node.<verb>`, the span of a single call is `rdm.node_admin.<name>.via-call` and of a
    /// workflow `…via-workflow`, and an event's frame span is `…<event>.via-reply-frame`.
    #[test]
    fn one_name_per_op_renders_the_same_in_the_name_and_the_span() {
        use crate::names::NodeEvent;
        let (_g, spans) = capture_spans();
        for op in [NodeOp::Get, NodeOp::Update, NodeOp::TopologyGet, NodeOp::ConnectionsGet, NodeOp::ConnectionsDelete, NodeOp::ConfigGet, NodeOp::ConfigUpdate, NodeOp::Drain, NodeOp::Create, NodeOp::Stop, NodeOp::Start, NodeOp::Restart, NodeOp::Delete] {
            drop(op.span());
        }
        for e in [NodeEvent::Created, NodeEvent::Joined, NodeEvent::Started, NodeEvent::Draining, NodeEvent::Drained, NodeEvent::ConnectionsDeleted, NodeEvent::Stopped, NodeEvent::Deleted, NodeEvent::Restarting, NodeEvent::Restarted] {
            drop(e.span());
        }
        for b in [BuildOp::Create, BuildOp::Get, BuildOp::Delete] {
            drop(b.span());
        }
        drop(MeshOp::Get.span());
        drop(FabricOp::Get.span());
        let names = spans.lock().unwrap().clone();
        for op in [NodeOp::Get, NodeOp::Update, NodeOp::TopologyGet, NodeOp::ConnectionsGet, NodeOp::ConnectionsDelete, NodeOp::ConfigGet, NodeOp::ConfigUpdate, NodeOp::Drain, NodeOp::Create, NodeOp::Stop, NodeOp::Start, NodeOp::Restart, NodeOp::Delete] {
            let via = if op.is_workflow() { "via-workflow" } else { "via-call" };
            let want = format!("rdm.node_admin.{}.{via}", op.name());
            assert!(names.contains(&want), "{want} missing from {names:?}");
        }
        for e in [NodeEvent::Created, NodeEvent::Joined, NodeEvent::Started, NodeEvent::Draining, NodeEvent::Drained, NodeEvent::ConnectionsDeleted, NodeEvent::Stopped, NodeEvent::Deleted, NodeEvent::Restarting, NodeEvent::Restarted] {
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
