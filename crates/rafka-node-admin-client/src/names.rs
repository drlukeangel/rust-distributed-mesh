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
    /// `node.update`: the full meta entity, a blind put.
    Update,
    /// `node.declare`: the node tells its authority its own status.
    Declare,
    /// `node.apply`: the authority sets a node's status.
    Apply,
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
    /// `node.drain`: refuse new work, finish in-flight work.
    Drain,
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
}

impl NodeOp {
    /// The canonical name of the call.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Get => "node.get",
            Self::Update => "node.update",
            Self::Declare => "node.declare",
            Self::Apply => "node.apply",
            Self::TopologyGet => "node.topology.get",
            Self::ConnectionsGet => "node.connections.get",
            Self::ConnectionsDelete => "node.connections.delete",
            Self::ConfigGet => "node.config.get",
            Self::ConfigUpdate => "node.config.update",
            Self::Drain => "node.drain",
            Self::Create => "node.create",
            Self::Stop => "node.stop",
            Self::Start => "node.start",
            Self::Restart => "node.restart",
            Self::Delete => "node.delete",
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
            Self::Update => tracing::info_span!("rdm.node_admin.node.update.via-call", op = "node.update", outcome = tracing::field::Empty),
            Self::Declare => tracing::info_span!("rdm.node_admin.node.declare.via-call", op = "node.declare", outcome = tracing::field::Empty),
            Self::Apply => tracing::info_span!("rdm.node_admin.node.apply.via-call", op = "node.apply", outcome = tracing::field::Empty),
            Self::TopologyGet => tracing::info_span!("rdm.node_admin.node.topology.get.via-call", op = "node.topology.get", outcome = tracing::field::Empty),
            Self::ConnectionsGet => tracing::info_span!("rdm.node_admin.node.connections.get.via-call", op = "node.connections.get", outcome = tracing::field::Empty),
            Self::ConnectionsDelete => tracing::info_span!("rdm.node_admin.node.connections.delete.via-call", op = "node.connections.delete", outcome = tracing::field::Empty),
            Self::ConfigGet => tracing::info_span!("rdm.node_admin.node.config.get.via-call", op = "node.config.get", outcome = tracing::field::Empty),
            Self::ConfigUpdate => tracing::info_span!("rdm.node_admin.node.config.update.via-call", op = "node.config.update", outcome = tracing::field::Empty),
            Self::Drain => tracing::info_span!("rdm.node_admin.node.drain.via-call", op = "node.drain", outcome = tracing::field::Empty),
            Self::Create => tracing::info_span!("rdm.node_admin.node.create.via-workflow", op = "node.create", outcome = tracing::field::Empty, failed_step = tracing::field::Empty, build_id = tracing::field::Empty, attempt = tracing::field::Empty),
            Self::Stop => tracing::info_span!("rdm.node_admin.node.stop.via-workflow", op = "node.stop", outcome = tracing::field::Empty, failed_step = tracing::field::Empty, build_id = tracing::field::Empty, attempt = tracing::field::Empty),
            Self::Start => tracing::info_span!("rdm.node_admin.node.start.via-workflow", op = "node.start", outcome = tracing::field::Empty, failed_step = tracing::field::Empty, build_id = tracing::field::Empty, attempt = tracing::field::Empty),
            Self::Restart => tracing::info_span!("rdm.node_admin.node.restart.via-workflow", op = "node.restart", outcome = tracing::field::Empty, failed_step = tracing::field::Empty, build_id = tracing::field::Empty, attempt = tracing::field::Empty),
            Self::Delete => tracing::info_span!("rdm.node_admin.node.delete.via-workflow", op = "node.delete", outcome = tracing::field::Empty, failed_step = tracing::field::Empty, build_id = tracing::field::Empty, attempt = tracing::field::Empty),
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
    /// `node.created`: identity minted, data dir prepared, process launched, port bound.
    Created,
    /// `node.started`: the process is published and is about to join.
    Started,
    /// `node.joined`: the node's own membership digest reached its authority.
    Joined,
    /// `node.ready`: the node reports hydrated, nothing pending.
    Ready,
    /// `node.draining`: the drain started.
    Draining,
    /// `node.drained`: in-flight work finished.
    Drained,
    /// `node.connections.deleted`: the hard cut.
    ConnectionsDeleted,
    /// `node.left`: the node reported it left.
    Left,
    /// `node.stopped`: the process exited, with the provider's proof.
    Stopped,
    /// `node.deleted`: id and name released.
    Deleted,
    /// `node.restarting`: the restart began.
    Restarting,
    /// `node.restarted`: the restart finished.
    Restarted,
}

impl NodeEvent {
    /// The canonical name of the event.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Created => "node.created",
            Self::Started => "node.started",
            Self::Joined => "node.joined",
            Self::Ready => "node.ready",
            Self::Draining => "node.draining",
            Self::Drained => "node.drained",
            Self::ConnectionsDeleted => "node.connections.deleted",
            Self::Left => "node.left",
            Self::Stopped => "node.stopped",
            Self::Deleted => "node.deleted",
            Self::Restarting => "node.restarting",
            Self::Restarted => "node.restarted",
        }
    }

    /// The span of the frame that carried this event: `rdm.node_admin.node.<event>.via-reply-frame`.
    pub fn span(self) -> tracing::Span {
        match self {
            Self::Created => tracing::info_span!("rdm.node_admin.node.created.via-reply-frame", event = "node.created"),
            Self::Started => tracing::info_span!("rdm.node_admin.node.started.via-reply-frame", event = "node.started"),
            Self::Joined => tracing::info_span!("rdm.node_admin.node.joined.via-reply-frame", event = "node.joined"),
            Self::Ready => tracing::info_span!("rdm.node_admin.node.ready.via-reply-frame", event = "node.ready"),
            Self::Draining => tracing::info_span!("rdm.node_admin.node.draining.via-reply-frame", event = "node.draining"),
            Self::Drained => tracing::info_span!("rdm.node_admin.node.drained.via-reply-frame", event = "node.drained"),
            Self::ConnectionsDeleted => tracing::info_span!("rdm.node_admin.node.connections.deleted.via-reply-frame", event = "node.connections.deleted"),
            Self::Left => tracing::info_span!("rdm.node_admin.node.left.via-reply-frame", event = "node.left"),
            Self::Stopped => tracing::info_span!("rdm.node_admin.node.stopped.via-reply-frame", event = "node.stopped"),
            Self::Deleted => tracing::info_span!("rdm.node_admin.node.deleted.via-reply-frame", event = "node.deleted"),
            Self::Restarting => tracing::info_span!("rdm.node_admin.node.restarting.via-reply-frame", event = "node.restarting"),
            Self::Restarted => tracing::info_span!("rdm.node_admin.node.restarted.via-reply-frame", event = "node.restarted"),
        }
    }
}

/// The workflow step a failure names. A step is a stage of a workflow, named for the call it
/// belongs to, not the Build receipt key that records it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeStep {
    /// `node.create`: identity, storage, network, launch and bind.
    Create,
    /// `node.start`: the published process, before it joins.
    Start,
    /// `node.join`: the node's digest reaching its authority.
    Join,
    /// `node.ready`: the node reporting hydrated.
    Ready,
    /// `node.drain`.
    Drain,
    /// `node.stop`.
    Stop,
    /// `node.restart`.
    Restart,
    /// `node.delete`.
    Delete,
}

impl NodeStep {
    /// The canonical name of the step.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Create => "node.create",
            Self::Start => "node.start",
            Self::Join => "node.join",
            Self::Ready => "node.ready",
            Self::Drain => "node.drain",
            Self::Stop => "node.stop",
            Self::Restart => "node.restart",
            Self::Delete => "node.delete",
        }
    }
}
