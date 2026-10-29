//! The one canonical name of every `node` and `build` op, and of every step event.
//!
//! Each name is written once, here: the Rust variant and the event frame a workflow's stream
//! carries (`node.joined`). Each op and event has one span, a five-segment literal
//! (telemetry-patterns.md §8): its entity is what the op acts on (`node.connection.delete` spans
//! `rdm.node_admin.connection.delete.via-call`), its verb from the whitelist (a `get` is `resolve`).
//! No caller writes an op name as a string.

/// A call on the `node` op family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeOp {
    /// `node.get`: the node rows matching a filter (Q167).
    Get,
    /// `node.name.update`: the node's rename, the name only (proposal node-properties-own-commands.md §2).
    NameUpdate,
    /// `node.topology.get`: the node's held topology.
    TopologyGet,
    /// `node.connection.get { node }`: the exact birth's own connections (Q167).
    ConnectionsGet,
    /// `node.connection.delete`: a hard cut.
    ConnectionsDelete,
    /// `node.config.get`: the app's own attributes.
    ConfigGet,
    /// `node.config.update`.
    ConfigUpdate,
    /// `node.tag.create`: labels added, each its own key of the tags index.
    TagsCreate,
    /// `node.tag.delete`: every label or the named ones removed.
    TagsDelete,
    /// `node.create(spec)`.
    Create,
    /// `node.stop`.
    Stop,
    /// `node.start`.
    Start,
    /// `node.delete`.
    Delete,
    /// `node.primary.update`: move a cohort's or mesh-primary's seat (and the fabric-primary seat
    /// with it when the holder holds both) to its same-mesh successor (`0x21`).
    PrimaryUpdate,
    /// `node.fabric-primary.update`: move the fabric-primary seat (`0x21`, C2 step 1 same-mesh first,
    /// step 2 outside mesh primary when no same-mesh successor is Ready).
    FabricPrimaryUpdate,
}

impl NodeOp {
    /// The canonical name of the call.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Get => "node.get",
            Self::NameUpdate => "node.name.update",
            Self::TopologyGet => "node.topology.get",
            Self::ConnectionsGet => "node.connection.get",
            Self::ConnectionsDelete => "node.connection.delete",
            Self::ConfigGet => "node.config.get",
            Self::ConfigUpdate => "node.config.update",
            Self::TagsCreate => "node.tag.create",
            Self::TagsDelete => "node.tag.delete",
            Self::Create => "node.create",
            Self::Stop => "node.stop",
            Self::Start => "node.start",
            Self::Delete => "node.delete",
            Self::PrimaryUpdate => "node.primary.update",
            Self::FabricPrimaryUpdate => "node.fabric-primary.update",
        }
    }

    /// Whether the call is a workflow: one call that RDM runs as several steps inside one Build.
    pub const fn is_workflow(self) -> bool {
        matches!(self, Self::Create | Self::Stop | Self::Start | Self::Delete)
    }

    /// The span of one call of this op: `rdm.node_admin.<entity>.<verb>.via-call` for a single call,
    /// `…via-workflow` for a workflow (`node.update.via-stop-workflow` and `…via-start-workflow` for
    /// the two that change a node's status). Every op is spelled out so the name stays a literal.
    pub fn span(self) -> tracing::Span {
        match self {
            Self::Get => tracing::info_span!("rdm.node_admin.node.resolve.via-call", op = "node.get", properties = tracing::field::Empty, outcome = tracing::field::Empty),
            Self::NameUpdate => tracing::info_span!("rdm.node_admin.name.update.via-call", op = "node.name.update", outcome = tracing::field::Empty),
            Self::TopologyGet => tracing::info_span!("rdm.node_admin.topology.resolve.via-call", op = "node.topology.get", outcome = tracing::field::Empty),
            Self::ConnectionsGet => tracing::info_span!("rdm.node_admin.connection.resolve.via-call", op = "node.connection.get", properties = tracing::field::Empty, outcome = tracing::field::Empty),
            Self::ConnectionsDelete => tracing::info_span!("rdm.node_admin.connection.delete.via-call", op = "node.connection.delete", outcome = tracing::field::Empty),
            Self::ConfigGet => tracing::info_span!("rdm.node_admin.config.resolve.via-call", op = "node.config.get", outcome = tracing::field::Empty),
            Self::ConfigUpdate => tracing::info_span!("rdm.node_admin.config.update.via-call", op = "node.config.update", outcome = tracing::field::Empty),
            Self::TagsCreate => tracing::info_span!("rdm.node_admin.tag.create.via-call", op = "node.tag.create", outcome = tracing::field::Empty),
            Self::TagsDelete => tracing::info_span!("rdm.node_admin.tag.delete.via-call", op = "node.tag.delete", outcome = tracing::field::Empty),
            Self::Create => tracing::info_span!("rdm.node_admin.node.create.via-workflow", op = "node.create", outcome = tracing::field::Empty, failed_step = tracing::field::Empty, build_id = tracing::field::Empty, attempt = tracing::field::Empty),
            Self::Stop => tracing::info_span!("rdm.node_admin.node.update.via-stop-workflow", op = "node.stop", outcome = tracing::field::Empty, failed_step = tracing::field::Empty, build_id = tracing::field::Empty, attempt = tracing::field::Empty),
            Self::Start => tracing::info_span!("rdm.node_admin.node.update.via-start-workflow", op = "node.start", outcome = tracing::field::Empty, failed_step = tracing::field::Empty, build_id = tracing::field::Empty, attempt = tracing::field::Empty),
            Self::Delete => tracing::info_span!("rdm.node_admin.node.delete.via-workflow", op = "node.delete", outcome = tracing::field::Empty, failed_step = tracing::field::Empty, build_id = tracing::field::Empty, attempt = tracing::field::Empty),
            Self::PrimaryUpdate => tracing::info_span!("rdm.node_admin.primary.update.via-call", op = "node.primary.update", outcome = tracing::field::Empty),
            Self::FabricPrimaryUpdate => tracing::info_span!("rdm.node_admin.primary.update.via-fabric-primary-call", op = "node.fabric-primary.update", outcome = tracing::field::Empty),
        }
    }
}

/// A call on the `mesh` op family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MeshOp {
    /// `mesh.get`: the meshes matching a filter (Q167).
    Get,
}

impl MeshOp {
    /// The canonical name of the call.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Get => "mesh.get",
        }
    }

    /// The span of one call: `rdm.node_admin.mesh.resolve.via-…call`.
    pub fn span(self) -> tracing::Span {
        match self {
            Self::Get => tracing::info_span!("rdm.node_admin.mesh.resolve.via-call", op = "mesh.get", properties = tracing::field::Empty, outcome = tracing::field::Empty),
        }
    }
}

/// A call on the `fabric` op family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FabricOp {
    /// `fabric.get`: the fabric the answering admin holds, when it matches a filter (Q167).
    Get,
    /// `fabric.atlas.get`: the fabric-primary's atlas (fabric-atlas.md R-FA4).
    AtlasGet,
    /// `fabric.root.update`: a new root's secret to the fabric-primary (fabric-ca-renew.md).
    RootUpdate,
}

impl FabricOp {
    /// The canonical name of the call.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Get => "fabric.get",
            Self::AtlasGet => "fabric.atlas.get",
            Self::RootUpdate => "fabric.root.update",
        }
    }

    /// The span of one call: `rdm.node_admin.fabric.resolve.via-…call`.
    pub fn span(self) -> tracing::Span {
        match self {
            Self::Get => tracing::info_span!("rdm.node_admin.fabric.resolve.via-call", op = "fabric.get", properties = tracing::field::Empty, outcome = tracing::field::Empty),
            Self::AtlasGet => tracing::info_span!("rdm.node_admin.fabric.resolve.via-atlas-call", op = "fabric.atlas.get", outcome = tracing::field::Empty),
            Self::RootUpdate => tracing::info_span!("rdm.node_admin.root.update.via-call", op = "fabric.root.update", outcome = tracing::field::Empty),
        }
    }
}

/// A call on the `build` op family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BuildOp {
    /// `build.create`, including a preset.
    Create,
    /// `build.get` (Q167 R-RD5).
    Get,
    /// `build.delete`.
    Delete,
}

impl BuildOp {
    /// The canonical name of the call.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Create => "build.create",
            Self::Get => "build.get",
            Self::Delete => "build.delete",
        }
    }

    /// The span of one call: `rdm.node_admin.build.<verb>.via-call`.
    pub fn span(self) -> tracing::Span {
        match self {
            Self::Create => tracing::info_span!("rdm.node_admin.build.create.via-call", op = "build.create", outcome = tracing::field::Empty),
            Self::Get => tracing::info_span!("rdm.node_admin.build.resolve.via-call", op = "build.get", properties = tracing::field::Empty, outcome = tracing::field::Empty),
            Self::Delete => tracing::info_span!("rdm.node_admin.build.delete.via-call", op = "build.delete", outcome = tracing::field::Empty),
        }
    }
}

/// A step event a workflow's reply stream carries, published when its step has completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeEvent {
    /// `node.created`: the node exists and is started; the last event of a create.
    Created,
    /// `node.joined`: the node's own topology digest reached its authority.
    Joined,
    /// `node.started`: the node reports its hook passed, nothing pending.
    Started,
    /// `node.connection.deleted`: the hard cut.
    ConnectionsDeleted,
    /// `node.stopped`: the node reported it is stopped, on the stop call's own stream.
    Stopped,
    /// `node.deleted`: id and name released.
    Deleted,
    /// `node.updated`: the node's row holds what the update put (lifecycles/node-name-update.md).
    Updated,
    /// `node.mesh.left`: the node left its mesh, and the backbone a node-admin, keeping its endpoint
    /// (lifecycles/node-mesh-leave.md).
    MeshLeft,
}

impl NodeEvent {
    /// The canonical name of the event.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Created => "node.created",
            Self::Joined => "node.joined",
            Self::Started => "node.started",
            Self::ConnectionsDeleted => "node.connection.deleted",
            Self::Stopped => "node.stopped",
            Self::Deleted => "node.deleted",
            Self::Updated => "node.updated",
            Self::MeshLeft => "node.mesh.left",
        }
    }

    /// The span of the frame that carried this event: `rdm.node_admin.<entity>.<verb>.via-<event>-frame`.
    pub fn span(self) -> tracing::Span {
        match self {
            Self::Created => tracing::info_span!("rdm.node_admin.node.create.via-created-frame", event = "node.created"),
            Self::Started => tracing::info_span!("rdm.node_admin.node.update.via-started-frame", event = "node.started"),
            Self::Joined => tracing::info_span!("rdm.node_admin.node.update.via-joined-frame", event = "node.joined"),
            Self::ConnectionsDeleted => tracing::info_span!("rdm.node_admin.connection.delete.via-deleted-frame", event = "node.connection.deleted"),
            Self::Stopped => tracing::info_span!("rdm.node_admin.node.update.via-stopped-frame", event = "node.stopped"),
            Self::Deleted => tracing::info_span!("rdm.node_admin.node.delete.via-deleted-frame", event = "node.deleted"),
            Self::Updated => tracing::info_span!("rdm.node_admin.node.update.via-updated-frame", event = "node.updated"),
            Self::MeshLeft => tracing::info_span!("rdm.node_admin.node.remove.via-mesh-left-frame", event = "node.mesh.left"),
        }
    }
}

/// The workflow step a failure names. A step is a stage of a workflow, named for the call it
/// belongs to, not the Build receipt key that records it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeStep {
    /// `node.create`: identity, storage, network, launch and bind.
    Create,
    /// `node.start`: the published process, before it joins, and its report that its hook passed.
    Start,
    /// `node.join`: the node's digest reaching its authority.
    Join,
    /// `node.connection.delete`.
    ConnectionsDelete,
    /// `node.stop`.
    Stop,
    /// `node.delete`.
    Delete,
    /// `node.update`.
    Update,
    /// `node.mesh.leave`.
    MeshLeave,
}

impl NodeStep {
    /// The canonical name of the step.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Create => "node.create",
            Self::Start => "node.start",
            Self::Join => "node.join",
            Self::ConnectionsDelete => "node.connection.delete",
            Self::Stop => "node.stop",
            Self::Delete => "node.delete",
            Self::Update => "node.update",
            Self::MeshLeave => "node.mesh.leave",
        }
    }
}
