//! The `node` and `build` objects.
//!
//! Workflows are submitted to the fabric-primary through the Build family (op `0x20`,
//! `build.create`) and answered with a reply stream ([`WorkflowStream`]) read from the call itself.
//! The `build` objects are that family's calls. The reads that have no node-RPC op yet (`node.get`,
//! `node.connections.get`) go to one admin's control API. Calls whose request is valid but that no
//! op carries yet end `NotBackedToday`, naming what will carry them; no request is invented for
//! them.

use crate::build_stream::{BuildCarrier, BuildReceipts, BuildStream};
use crate::names::{BuildOp, NodeOp};
use crate::workflow::{WorkflowKind, WorkflowStream};
use crate::{Accepted, BuildId, CallEnd, ConnectionsView, FabricDesired, MeshDesired, NodeAdminClient, NodeStatus, NodeView};
use rafka_mesh_entity::{IncarnationId, NodeId, NodeKind, PathName};
use rafka_node_rpc_contract::build::{BuildChange, MeshCounts};
use std::collections::BTreeSet;

fn counts(m: &MeshDesired) -> MeshCounts {
    MeshCounts { name: m.name.clone(), node_admin: m.node_admin, rpc_node: m.rpc_node, broker: m.broker, gateway: m.gateway, compute: m.compute }
}

/// What `node.create` is given: the mesh to add a node to and the node's kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeSpec {
    /// The mesh the node joins.
    pub mesh: String,
    /// The node's kind.
    pub kind: NodeKind,
}

/// Which tags a `node.get` filter requires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TagFilter {
    /// The node holds every one of these tags.
    All(BTreeSet<String>),
    /// The node holds at least one of these tags.
    Any(BTreeSet<String>),
}

/// What `node.get` selects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeSelector {
    /// The node with this minted id.
    Id(NodeId),
    /// Every node of the mesh.
    Mesh(String),
    /// Every node whose config tags match.
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
}

impl NodeMeta {
    /// Whether putting `self` over `held` changes only what a caller may change. `stopped` is
    /// whether the node is stopped: its name may change only then, and only to or from its
    /// `.old` form.
    pub fn check_put(&self, held: &NodeMeta, stopped: bool) -> Result<(), MetaFieldIsRdmOwned> {
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
            let only_old_toggles = self.name.ordinal == held.name.ordinal && self.name.old != held.name.old;
            if !(stopped && only_old_toggles) {
                return refuse(MetaField::Name);
            }
        }
        Ok(())
    }
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

/// The `node` objects: workflows through the fabric-primary's Build family, reads over one admin's
/// control API.
#[derive(Debug, Clone)]
pub struct Nodes {
    admin: NodeAdminClient,
    carrier: BuildCarrier,
}

impl Nodes {
    /// The objects over `admin` (reads) and `carrier` (workflows).
    pub fn new(admin: NodeAdminClient, carrier: BuildCarrier) -> Self {
        Self { admin, carrier }
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

    /// `node.get`: the nodes `selector` selects.
    pub async fn get(&self, selector: &NodeSelector) -> Result<Vec<NodeView>, CallEnd> {
        let span = NodeOp::Get.span();
        let r = tracing::Instrument::instrument(self.select(selector), span.clone()).await;
        span.record("outcome", match &r {
            Ok(_) => "read",
            Err(e) => e.outcome(),
        });
        span.in_scope(|| tracing::info!(op = NodeOp::Get.name(), "an object call ended"));
        r
    }

    async fn select(&self, selector: &NodeSelector) -> Result<Vec<NodeView>, CallEnd> {
        if matches!(selector, NodeSelector::Tags(_)) {
            return Err(CallEnd::NotBackedToday { op: NodeOp::Get, why: "tags are node config, and no op carries node config yet" });
        }
        let all = self.admin.nodes().await?;
        Ok(match selector {
            NodeSelector::Id(id) => all.into_iter().filter(|n| &n.node_id == id).collect(),
            NodeSelector::Mesh(mesh) => all.into_iter().filter(|n| &n.mesh == mesh).collect(),
            NodeSelector::Tags(_) => unreachable!("refused above"),
        })
    }

    /// `node.update`: put the full meta entity `body` over what RDM `held`. A body that differs in
    /// a field RDM owns is refused `MetaFieldIsRdmOwned`; no op carries the put yet.
    pub fn update(&self, held: &NodeMeta, body: &NodeMeta, stopped: bool) -> Result<CallEnd, MetaFieldIsRdmOwned> {
        let span = NodeOp::Update.span();
        let checked = body.check_put(held, stopped);
        span.record("outcome", match &checked {
            Ok(()) => "not-backed-today",
            Err(_) => "meta-field-is-rdm-owned",
        });
        span.in_scope(|| match &checked {
            Ok(()) => tracing::info!(op = NodeOp::Update.name(), "the put is valid and no op carries it"),
            Err(e) => tracing::info!(op = NodeOp::Update.name(), field = e.field.name(), "the put changes a field RDM owns"),
        });
        checked.map(|()| CallEnd::NotBackedToday { op: NodeOp::Update, why: "the meta put" })
    }

    /// `node.connections.get`: the connection facts the answering admin holds.
    pub async fn connections_get(&self) -> Result<ConnectionsView, CallEnd> {
        let span = NodeOp::ConnectionsGet.span();
        let r = tracing::Instrument::instrument(self.admin.connections(), span.clone()).await.map_err(CallEnd::from);
        span.record("outcome", match &r {
            Ok(_) => "read",
            Err(e) => e.outcome(),
        });
        span.in_scope(|| tracing::info!(op = NodeOp::ConnectionsGet.name(), "an object call ended"));
        r
    }

    /// `node.connections.delete`: the hard cut of a node's mesh connections.
    pub fn connections_delete(&self) -> CallEnd {
        CallEnd::NotBackedToday { op: NodeOp::ConnectionsDelete, why: "no op cuts a node's connections; the cut is a step of the stop workflow" }
    }

    /// `node.config.get`.
    pub fn config_get(&self) -> CallEnd {
        CallEnd::NotBackedToday { op: NodeOp::ConfigGet, why: "no op carries node config" }
    }

    /// `node.config.update`.
    pub fn config_update(&self) -> CallEnd {
        CallEnd::NotBackedToday { op: NodeOp::ConfigUpdate, why: "no op carries node config" }
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

    /// `node.start`: the parked process joins. The steps that start a node run today only inside
    /// `node.create` and `node.restart`.
    pub fn start(&self) -> CallEnd {
        CallEnd::NotBackedToday { op: NodeOp::Start, why: "a stop that parks the process lands with the node lifecycle; start steps run inside create and restart" }
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
            status: NodeStatus::ReadyForTraffic,
            name: "mesh1.rpc.1".parse().unwrap(),
            mesh: "mesh1".into(),
            kind: NodeKind::RpcNode,
            otel_debug: false,
        }
    }

    /// CONTRACT: a put may change only the caller-owned group. Flipping `otel_debug` is accepted; a
    /// body that differs from what RDM holds in any RDM-owned field is refused naming that field.
    #[test]
    fn a_put_changing_a_field_rdm_owns_is_refused_naming_the_field() {
        let held = held();
        assert_eq!(NodeMeta { otel_debug: true, ..held.clone() }.check_put(&held, false), Ok(()));
        let cases: Vec<(NodeMeta, MetaField)> = vec![
            (NodeMeta { node_id: NodeId::mint(), ..held.clone() }, MetaField::NodeId),
            (NodeMeta { incarnation: Some(IncarnationId::mint()), ..held.clone() }, MetaField::Incarnation),
            (NodeMeta { endpoint: Some("ep-2".into()), ..held.clone() }, MetaField::Endpoint),
            (NodeMeta { transport_addr: Some("127.0.0.1:4001".parse().unwrap()), ..held.clone() }, MetaField::TransportAddr),
            (NodeMeta { exe: Some("rpc-node@def456".into()), ..held.clone() }, MetaField::Exe),
            (NodeMeta { status: NodeStatus::Leaving, ..held.clone() }, MetaField::Status),
            (NodeMeta { mesh: "mesh2".into(), ..held.clone() }, MetaField::Mesh),
            (NodeMeta { kind: NodeKind::Broker, ..held.clone() }, MetaField::Kind),
            (NodeMeta { name: "mesh2.rpc.1".parse().unwrap(), ..held.clone() }, MetaField::Path),
        ];
        for (body, field) in cases {
            assert_eq!(body.check_put(&held, true), Err(MetaFieldIsRdmOwned { field }), "{}", field.name());
        }
    }

    /// CONTRACT: a node's name changes only while the node is stopped, and only to or from its
    /// `.old` form: a live node's rename is refused, and so is a new ordinal on a stopped node.
    #[test]
    fn a_name_changes_only_while_stopped_and_only_to_or_from_old() {
        let held = held();
        let old = NodeMeta { name: PathName { old: true, ..held.name.clone() }, ..held.clone() };
        assert_eq!(old.check_put(&held, false), Err(MetaFieldIsRdmOwned { field: MetaField::Name }), "a live node keeps its name");
        assert_eq!(old.check_put(&held, true), Ok(()), "a stopped node gives up its name to <name>.old");
        assert_eq!(held.check_put(&old, true), Ok(()), "and takes it back");
        let renumbered = NodeMeta { name: PathName { ordinal: 9, ..held.name.clone() }, ..held.clone() };
        assert_eq!(renumbered.check_put(&held, true), Err(MetaFieldIsRdmOwned { field: MetaField::Name }), "no rename but .old exists");
    }

    /// CONTRACT: each op has one canonical name, rendered the same way everywhere: the name is
    /// `node.<verb>`, the span of a single call is `rdm.node_admin.<name>.via-call` and of a
    /// workflow `…via-workflow`, and an event's frame span is `…<event>.via-reply-frame`.
    #[test]
    fn one_name_per_op_renders_the_same_in_the_name_and_the_span() {
        use crate::names::NodeEvent;
        let (_g, spans) = capture_spans();
        for op in [NodeOp::Get, NodeOp::Update, NodeOp::Declare, NodeOp::Apply, NodeOp::TopologyGet, NodeOp::ConnectionsGet, NodeOp::ConnectionsDelete, NodeOp::ConfigGet, NodeOp::ConfigUpdate, NodeOp::Drain, NodeOp::Create, NodeOp::Stop, NodeOp::Start, NodeOp::Restart, NodeOp::Delete] {
            drop(op.span());
        }
        for e in [NodeEvent::Created, NodeEvent::Started, NodeEvent::Joined, NodeEvent::Ready, NodeEvent::Draining, NodeEvent::Drained, NodeEvent::ConnectionsDeleted, NodeEvent::Left, NodeEvent::Stopped, NodeEvent::Deleted, NodeEvent::Restarting, NodeEvent::Restarted] {
            drop(e.span());
        }
        for b in [BuildOp::Create, BuildOp::Get, BuildOp::Delete] {
            drop(b.span());
        }
        let names = spans.lock().unwrap().clone();
        for op in [NodeOp::Get, NodeOp::Update, NodeOp::Declare, NodeOp::Apply, NodeOp::TopologyGet, NodeOp::ConnectionsGet, NodeOp::ConnectionsDelete, NodeOp::ConfigGet, NodeOp::ConfigUpdate, NodeOp::Drain, NodeOp::Create, NodeOp::Stop, NodeOp::Start, NodeOp::Restart, NodeOp::Delete] {
            let via = if op.is_workflow() { "via-workflow" } else { "via-call" };
            let want = format!("rdm.node_admin.{}.{via}", op.name());
            assert!(names.contains(&want), "{want} missing from {names:?}");
        }
        for e in [NodeEvent::Created, NodeEvent::Started, NodeEvent::Joined, NodeEvent::Ready, NodeEvent::Draining, NodeEvent::Drained, NodeEvent::ConnectionsDeleted, NodeEvent::Left, NodeEvent::Stopped, NodeEvent::Deleted, NodeEvent::Restarting, NodeEvent::Restarted] {
            let want = format!("rdm.node_admin.{}.via-reply-frame", e.name());
            assert!(names.contains(&want), "{want} missing from {names:?}");
        }
        for b in [BuildOp::Create, BuildOp::Get, BuildOp::Delete] {
            let want = format!("rdm.node_admin.{}.via-call", b.name());
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
