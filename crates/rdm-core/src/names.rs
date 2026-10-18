//! The one canonical name of every `node` and `build` op, and of every step event.
//!
//! Each name is written once, here, and rendered mechanically everywhere else: the Rust
//! variant, the event frame a workflow's stream carries (`node.joined`) and the span
//! (`rdm.node_admin.node.<verb>.via-…`). No caller writes an op name as a string.

/// A call on the `node` op family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeOp {
    /// `node.get`: by id, by mesh, or by tags.
    Get,
    /// `node.name.update`: the node's rename, the name only (proposal node-properties-own-commands.md §2).
    NameUpdate,
    /// `node.topology.get`: the node's held topology.
    TopologyGet,
    /// `node.connections.get`.
    ConnectionsGet,
    /// `node.connections.delete`: a hard cut.
    ConnectionsDelete,
    /// `node.config.get`: the app's own attributes.
    ConfigGet,
    /// `node.config.update`.
    ConfigUpdate,
    /// `node.tags.create`: labels added, each its own key of the tags index.
    TagsCreate,
    /// `node.tags.delete`: every label or the named ones removed.
    TagsDelete,
    /// `node.create(spec)`.
    Create,
    /// `node.stop`.
    Stop,
    /// `node.start`.
    Start,
    /// `node.restart`.
    Restart,
    /// `node.delete`.
    Delete,
    /// `node.primary.update`: move a cohort's or mesh-primary's seat (and the fabric-primary seat
    /// with it when the holder holds both) to its same-mesh successor (`0x21`).
    PrimaryUpdate,
    /// `node.fabric-primary.update`: move the fabric-primary seat (`0x21`, C2 step 1 same-mesh first,
    /// step 2 outside mesh primary when no same-mesh successor is ReadyForTraffic).
    FabricPrimaryUpdate,
}

impl NodeOp {
    /// The canonical name of the call.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Get => "node.get",
            Self::NameUpdate => "node.name.update",
            Self::TopologyGet => "node.topology.get",
            Self::ConnectionsGet => "node.connections.get",
            Self::ConnectionsDelete => "node.connections.delete",
            Self::ConfigGet => "node.config.get",
            Self::ConfigUpdate => "node.config.update",
            Self::TagsCreate => "node.tags.create",
            Self::TagsDelete => "node.tags.delete",
            Self::Create => "node.create",
            Self::Stop => "node.stop",
            Self::Start => "node.start",
            Self::Restart => "node.restart",
            Self::Delete => "node.delete",
            Self::PrimaryUpdate => "node.primary.update",
            Self::FabricPrimaryUpdate => "node.fabric-primary.update",
        }
    }

    /// Whether the call is a workflow: one call that RDM runs as several steps inside one Build.
    pub const fn is_workflow(self) -> bool {
        matches!(self, Self::Create | Self::Stop | Self::Start | Self::Restart | Self::Delete)
    }

    /// The span of one call of this op: `rdm.node_admin.node.<verb>.via-call` for a single call,
    /// `…via-workflow` for a workflow. Every op is spelled out so the name stays a literal.
    pub fn span(self) -> tracing::Span {
        match self {
            Self::Get => tracing::info_span!("rdm.node_admin.node.get.via-call", op = "node.get", outcome = tracing::field::Empty),
            Self::NameUpdate => tracing::info_span!("rdm.node_admin.node.name.update.via-call", op = "node.name.update", outcome = tracing::field::Empty),
            Self::TopologyGet => tracing::info_span!("rdm.node_admin.node.topology.get.via-call", op = "node.topology.get", outcome = tracing::field::Empty),
            Self::ConnectionsGet => tracing::info_span!("rdm.node_admin.node.connections.get.via-call", op = "node.connections.get", outcome = tracing::field::Empty),
            Self::ConnectionsDelete => tracing::info_span!("rdm.node_admin.node.connections.delete.via-call", op = "node.connections.delete", outcome = tracing::field::Empty),
            Self::ConfigGet => tracing::info_span!("rdm.node_admin.node.config.get.via-call", op = "node.config.get", outcome = tracing::field::Empty),
            Self::ConfigUpdate => tracing::info_span!("rdm.node_admin.node.config.update.via-call", op = "node.config.update", outcome = tracing::field::Empty),
            Self::TagsCreate => tracing::info_span!("rdm.node_admin.node.tags.create.via-call", op = "node.tags.create", outcome = tracing::field::Empty),
            Self::TagsDelete => tracing::info_span!("rdm.node_admin.node.tags.delete.via-call", op = "node.tags.delete", outcome = tracing::field::Empty),
            Self::Create => tracing::info_span!("rdm.node_admin.node.create.via-workflow", op = "node.create", outcome = tracing::field::Empty, failed_step = tracing::field::Empty, build_id = tracing::field::Empty, attempt = tracing::field::Empty),
            Self::Stop => tracing::info_span!("rdm.node_admin.node.stop.via-workflow", op = "node.stop", outcome = tracing::field::Empty, failed_step = tracing::field::Empty, build_id = tracing::field::Empty, attempt = tracing::field::Empty),
            Self::Start => tracing::info_span!("rdm.node_admin.node.start.via-workflow", op = "node.start", outcome = tracing::field::Empty, failed_step = tracing::field::Empty, build_id = tracing::field::Empty, attempt = tracing::field::Empty),
            Self::Restart => tracing::info_span!("rdm.node_admin.node.restart.via-workflow", op = "node.restart", outcome = tracing::field::Empty, failed_step = tracing::field::Empty, build_id = tracing::field::Empty, attempt = tracing::field::Empty),
            Self::Delete => tracing::info_span!("rdm.node_admin.node.delete.via-workflow", op = "node.delete", outcome = tracing::field::Empty, failed_step = tracing::field::Empty, build_id = tracing::field::Empty, attempt = tracing::field::Empty),
            Self::PrimaryUpdate => tracing::info_span!("rdm.node_admin.node.primary.update.via-call", op = "node.primary.update", outcome = tracing::field::Empty),
            Self::FabricPrimaryUpdate => tracing::info_span!("rdm.node_admin.node.fabric-primary.update.via-call", op = "node.fabric-primary.update", outcome = tracing::field::Empty),
        }
    }
}

/// A call on the `mesh` op family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MeshOp {
    /// `mesh.get`: one mesh, or every mesh of the fabric.
    Get,
}

impl MeshOp {
    /// The canonical name of the call.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Get => "mesh.get",
        }
    }

    /// The span of one call: `rdm.node_admin.mesh.<verb>.via-call`.
    pub fn span(self) -> tracing::Span {
        match self {
            Self::Get => tracing::info_span!("rdm.node_admin.mesh.get.via-call", op = "mesh.get", outcome = tracing::field::Empty),
        }
    }
}

/// A call on the `fabric` op family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FabricOp {
    /// `fabric.get`: the fabric as the answering admin holds it.
    Get,
}

impl FabricOp {
    /// The canonical name of the call.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Get => "fabric.get",
        }
    }

    /// The span of one call: `rdm.node_admin.fabric.<verb>.via-call`.
    pub fn span(self) -> tracing::Span {
        match self {
            Self::Get => tracing::info_span!("rdm.node_admin.fabric.get.via-call", op = "fabric.get", outcome = tracing::field::Empty),
        }
    }
}

/// A call on the `build` op family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BuildOp {
    /// `build.create`, including a preset.
    Create,
    /// `build.get`.
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
            Self::Get => tracing::info_span!("rdm.node_admin.build.get.via-call", op = "build.get", outcome = tracing::field::Empty),
            Self::Delete => tracing::info_span!("rdm.node_admin.build.delete.via-call", op = "build.delete", outcome = tracing::field::Empty),
        }
    }
}

/// A step event a workflow's reply stream carries, published when its step has completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeEvent {
    /// `node.created`: the node exists and is started; the last event of a create.
    Created,
    /// `node.joined`: the node's own membership digest reached its authority.
    Joined,
    /// `node.started`: the node reports its hook passed, nothing pending.
    Started,
    /// `node.connections.deleted`: the hard cut.
    ConnectionsDeleted,
    /// `node.stopped`: the node reported it is parked, on the stop call's own stream.
    Stopped,
    /// `node.deleted`: id and name released.
    Deleted,
    /// `node.restarting`: the restart began.
    Restarting,
    /// `node.restarted`: the restart finished.
    Restarted,
    /// `node.updated`: the node's row holds what the update put (lifecycles/node-name-update.md).
    Updated,
}

impl NodeEvent {
    /// The canonical name of the event.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Created => "node.created",
            Self::Joined => "node.joined",
            Self::Started => "node.started",
            Self::ConnectionsDeleted => "node.connections.deleted",
            Self::Stopped => "node.stopped",
            Self::Deleted => "node.deleted",
            Self::Restarting => "node.restarting",
            Self::Restarted => "node.restarted",
            Self::Updated => "node.updated",
        }
    }

    /// The span of the frame that carried this event: `rdm.node_admin.node.<event>.via-reply-frame`.
    pub fn span(self) -> tracing::Span {
        match self {
            Self::Created => tracing::info_span!("rdm.node_admin.node.created.via-reply-frame", event = "node.created"),
            Self::Started => tracing::info_span!("rdm.node_admin.node.started.via-reply-frame", event = "node.started"),
            Self::Joined => tracing::info_span!("rdm.node_admin.node.joined.via-reply-frame", event = "node.joined"),
            Self::ConnectionsDeleted => tracing::info_span!("rdm.node_admin.node.connections.deleted.via-reply-frame", event = "node.connections.deleted"),
            Self::Stopped => tracing::info_span!("rdm.node_admin.node.stopped.via-reply-frame", event = "node.stopped"),
            Self::Deleted => tracing::info_span!("rdm.node_admin.node.deleted.via-reply-frame", event = "node.deleted"),
            Self::Restarting => tracing::info_span!("rdm.node_admin.node.restarting.via-reply-frame", event = "node.restarting"),
            Self::Restarted => tracing::info_span!("rdm.node_admin.node.restarted.via-reply-frame", event = "node.restarted"),
            Self::Updated => tracing::info_span!("rdm.node_admin.node.updated.via-reply-frame", event = "node.updated"),
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
    /// `node.connections.delete`.
    ConnectionsDelete,
    /// `node.stop`.
    Stop,
    /// `node.restart`.
    Restart,
    /// `node.delete`.
    Delete,
    /// `node.update`.
    Update,
}

impl NodeStep {
    /// The canonical name of the step.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Create => "node.create",
            Self::Start => "node.start",
            Self::Join => "node.join",
            Self::ConnectionsDelete => "node.connections.delete",
            Self::Stop => "node.stop",
            Self::Restart => "node.restart",
            Self::Delete => "node.delete",
            Self::Update => "node.update",
        }
    }
}
