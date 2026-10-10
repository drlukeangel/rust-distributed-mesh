//! The `node` and `build` objects over the control API, for the calls that have no node-RPC op
//! today: the workflows and the Build reads.
//!
//! Workflows are accepted by the control API as a Build (or an attempt of the current Build)
//! and answered with a reply stream ([`WorkflowStream`]). Calls whose request is valid but that
//! no op carries yet end `NotBackedToday`, naming what will carry them; no request is invented
//! for them.

use crate::names::{BuildOp, NodeOp};
use crate::workflow::{WorkflowKind, WorkflowStream};
use crate::{Accepted, BuildId, BuildView, CallEnd, ConnectionsView, FabricDesired, MeshDesired, NodeAdminClient, NodeStatus, NodeView};
use rafka_mesh_entity::{IncarnationId, NodeId, NodeKind, PathName};
use std::collections::BTreeSet;
use std::time::Duration;

/// The default interval at which a reply stream reads its Build.
pub const DEFAULT_POLL: Duration = Duration::from_millis(50);

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
    /// A named, fixed set of meshes.
    Preset(BuildPreset),
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

/// The `node` objects over one admin's control API.
#[derive(Debug, Clone)]
pub struct Nodes {
    admin: NodeAdminClient,
    poll: Duration,
}

impl Nodes {
    /// The objects over `admin`.
    pub fn new(admin: NodeAdminClient) -> Self {
        Self { admin, poll: DEFAULT_POLL }
    }

    /// The same objects with reply streams reading their Build every `poll`.
    pub fn polling_every(mut self, poll: Duration) -> Self {
        self.poll = poll;
        self
    }

    fn stream(&self, kind: WorkflowKind, accepted: Accepted) -> WorkflowStream {
        WorkflowStream::new(self.admin.clone(), kind, accepted, self.poll)
    }

    async fn call<T>(&self, op: NodeOp, call: impl std::future::Future<Output = Result<T, crate::ClientError>>) -> Result<T, CallEnd> {
        let span = op.span();
        let r = tracing::Instrument::instrument(call, span.clone()).await.map_err(CallEnd::from);
        span.record("outcome", match &r {
            Ok(_) => "accepted",
            Err(e) => e.outcome(),
        });
        span.in_scope(|| tracing::info!(op = op.name(), "a workflow call was answered"));
        r
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
        let accepted = self.call(NodeOp::Create, self.admin.spawn(&spec.mesh, spec.kind)).await?;
        Ok(self.stream(WorkflowKind::Create, accepted))
    }

    /// `node.stop` of `node`.
    pub async fn stop(&self, node: &PathName) -> Result<WorkflowStream, CallEnd> {
        let accepted = self.call(NodeOp::Stop, self.admin.stop(node)).await?;
        Ok(self.stream(WorkflowKind::Stop(node.clone()), accepted))
    }

    /// `node.start`: the parked process joins. The steps that start a node run today only inside
    /// `node.create` and `node.restart`.
    pub fn start(&self) -> CallEnd {
        CallEnd::NotBackedToday { op: NodeOp::Start, why: "a stop that parks the process lands with the node lifecycle; start steps run inside create and restart" }
    }

    /// `node.restart` of `node`.
    pub async fn restart(&self, node: &PathName) -> Result<WorkflowStream, CallEnd> {
        let accepted = self.call(NodeOp::Restart, self.admin.restart(node)).await?;
        Ok(self.stream(WorkflowKind::Restart(node.clone()), accepted))
    }

    /// `node.delete` of `node`.
    pub async fn delete(&self, node: &PathName) -> Result<WorkflowStream, CallEnd> {
        let accepted = self.call(NodeOp::Delete, self.admin.remove(node)).await?;
        Ok(self.stream(WorkflowKind::Delete(node.clone()), accepted))
    }
}

/// The `build` objects over one admin's control API.
#[derive(Debug, Clone)]
pub struct Builds {
    admin: NodeAdminClient,
}

impl Builds {
    /// The objects over `admin`.
    pub fn new(admin: NodeAdminClient) -> Self {
        Self { admin }
    }

    async fn call<T>(&self, op: BuildOp, call: impl std::future::Future<Output = Result<T, crate::ClientError>>) -> Result<T, CallEnd> {
        let span = op.span();
        let r = tracing::Instrument::instrument(call, span.clone()).await.map_err(CallEnd::from);
        span.record("outcome", match &r {
            Ok(_) => "ok",
            Err(e) => e.outcome(),
        });
        span.in_scope(|| tracing::info!(op = op.name(), "a build call ended"));
        r
    }

    /// `build.create`: accept a Build that reconciles the fabric to `spec`. A preset is expanded
    /// against the fabric the answering admin reports.
    pub async fn create(&self, spec: &BuildSpec) -> Result<Accepted, CallEnd> {
        let desired = match spec {
            BuildSpec::Fabric(d) => d.clone(),
            BuildSpec::Preset(p) => p.desired(&self.admin.fabric().await.map_err(CallEnd::from)?.name),
        };
        self.call(BuildOp::Create, self.admin.build(&desired)).await
    }

    /// `build.get`: the Build with its attempts and step receipts.
    pub async fn get(&self, id: &BuildId) -> Result<BuildView, CallEnd> {
        self.call(BuildOp::Get, self.admin.build_view(id)).await
    }

    /// `build.delete`: drop a finished Build from history.
    pub async fn delete(&self, id: &BuildId) -> Result<(), CallEnd> {
        self.call(BuildOp::Delete, self.admin.forget(id)).await
    }
}
