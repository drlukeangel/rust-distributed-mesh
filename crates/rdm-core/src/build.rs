//! The Build family on op `0x20`, RDM's additive carrier for Builds (node-rpc-envelope.md "Build, op
//! `0x20`, rdm"; R-ON8 = A′).
//!
//! The fabric-primary accepts every Build and dispatches each mesh's section to that mesh's primary.
//! Five calls, one stream shape:
//!
//! ```text
//! Create     caller -> fabric-primary         a submitted change, or a re-submit by build_id
//! AttemptRun fabric-primary -> mesh-primary   the steps (build_id, mesh) owes, start-or-reattach
//! Get        caller -> fabric-primary         the Build and its events, for reattachment after a cut
//! Delete     caller -> fabric-primary         a finished Build leaves history
//! GetBuilds  caller -> fabric-primary         build.get: every Build the filter matches, each whole
//! ```
//!
//! A stream is one refusal, or `Started`, then `Step`/`Blocked`/`Events` frames, then one terminal:
//!
//! ```text
//! Started { build_id, disposition }
//! Step { build_id, mesh, operation, step, result }    after the step's `rdm.build.step` is written
//! Blocked { build_id, mesh, operation, step, reason } non-terminal, never durable, never a step outcome
//! Complete | Failed { step, reason } | FabricPrimaryMoved { fabric_primary }
//!                                                  the terminal of Create (Complete or Failed of AttemptRun)
//! Got | Deleted                                    the terminal of Get / Delete
//! End                                              the terminal of GetBuilds, after per Build its
//!                                                  Topology and Events chunks and its Build header
//! ```
//!
//! Every progress frame carries `(build_id, mesh, operation, step)`; a receiver drops a frame whose
//! key it already holds. A cut stream is `NotSent` before the commit cut and `Indeterminate` after
//! it, never a failed step. The family is not forwardable: it is a stream, and only the
//! fabric-primary accepts.

use crate::context::CallContext;
use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::{DecodeFailure, NodeProtocol};
use crate::streaming::{FrameKind, StreamingProtocol};
use crate::NodeId;
use serde::{Deserialize, Serialize};

/// The Build protocol: Builds are created, driven, read and deleted through the fabric-primary.
pub struct Build;

/// A mesh a [`BuildChange::Create`] adds, by its Meshes schema (node-rpc-envelope.md "Build schema"):
/// the settable field. The record adds its `mesh_id` and state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Meshes {
    /// The mesh's name.
    pub name: String,
}

/// A node a [`BuildChange::Create`] adds, whole, by its Nodes schema (node-rpc-envelope.md "Build
/// schema"): the settable fields, `path` and `name` two strings (R-NR1). The record adds the node's
/// node.id, incarnation, state, endpoint and primaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Nodes {
    /// The node's path.
    pub path: String,
    /// The node's name.
    pub name: String,
    /// The mesh the node is created in.
    pub mesh: String,
    /// The node's type, by its name: a type the fabric does not
    /// declare is refused by name at acceptance.
    pub r#type: String,
    /// The node's `storage_persistence` (storage.md §1); `None`: its type's.
    pub storage_persistence: Option<crate::meta::StoragePersistence>,
    /// The executable the node runs; `None`: its type's. A node with neither is refused by name.
    pub executable: Option<crate::binding::Binding>,
    /// The node's tag labels.
    pub tags: std::collections::BTreeSet<String>,
    /// The application's map; RDM never reads it.
    pub config: std::collections::BTreeMap<String, String>,
}

/// What a caller submits (build.md §1): every external change that adds or removes a node is its own
/// Build, a batch with a result per node or mesh. The caller states only what it adds or removes, never
/// a node it does not touch. A node command or a repair is never a Build (Q342).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildChange {
    /// Add `meshes` and `nodes`, each node whole. A node whose mesh is not yet live creates that mesh
    /// with its first node-admin. Refused by name: a new mesh with no node-admin among the nodes, a node
    /// naming a mesh neither in the request nor live, a path and name already held, an undeclared type,
    /// a node with no executable, its own or its type's.
    ///
    /// Position 0 on the wire.
    Create {
        /// The meshes the Build adds.
        meshes: Vec<Meshes>,
        /// The nodes the Build adds.
        nodes: Vec<Nodes>,
    },
    /// Delete `meshes`, each by name, and the nodes `node_ids`, each by its node.id, never by a path
    /// and name. A node.id already deleted or replaced is refused by name in its own result.
    ///
    /// Position 1 on the wire.
    Delete {
        /// The meshes the Build deletes, by name.
        meshes: Vec<String>,
        /// The nodes the Build deletes, by node.id.
        node_ids: Vec<NodeId>,
    },
    /// Delete the fabric (`fabric.delete`, lifecycles/fabric-delete.md): run `mesh.delete` on each
    /// mesh leaf-inward (the fabric-primary's own mesh last, where the fabric role stays put),
    /// delete each other mesh's final mesh-primary, release the fabric name and report
    /// `fabric.deleted` while the fabric-primary remains alive for its separate `node.delete`.
    ///
    /// Position 2 on the wire.
    DeleteFabric,
}
/// A storage change (storage.md §2 "Changing it"): the fabric-primary's own workflow, never a Build,
/// that moves RDM's stores from the fabric's provider to the one `fabric.storage.update` names,
/// recorded as its `rdm.storage.started` event before its first command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageChange {
    /// The change's durable identity: every command and event of the change carries it.
    pub change_id: crate::ChangeId,
    /// The provider the fabric writes at the change's start: the old provider.
    pub from: String,
    /// The provider the change names: the new provider.
    pub to: String,
}

/// What `Create` asks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildSubmit {
    /// A new change to accept and drive.
    ///
    /// Position 0 on the wire.
    Change(BuildChange),
    /// A re-submit of a Build the caller already holds the id of: never a second Build.
    ///
    /// Position 1 on the wire.
    Resubmit {
        /// The Build.
        build_id: String,
    },
}

/// A Build call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildRequest {
    /// `build.create`: accept `submit` and stream the Build until it ends.
    ///
    /// Position 0 on the wire.
    Create {
        /// What is submitted.
        submit: BuildSubmit,
    },
    /// `build.attempt.run`: run the steps `mesh` owes of `build_id` on the recipient, the mesh's
    /// primary. Start-or-reattach: a repeat of the same `(build_id, mesh)` attaches to the run. It
    /// names the Build and the mesh and never carries a copy of either: the mesh-primary reads the
    /// Build and its events from storage before it plans (Q292).
    ///
    /// Position 1 on the wire.
    AttemptRun {
        /// The Build.
        build_id: String,
        /// The mesh whose section the recipient runs.
        mesh: String,
        /// The dispatch's observability context.
        context: CallContext,
    },
    /// `build.get`: the Build and its events, for reattachment after a cut.
    ///
    /// Position 2 on the wire.
    Get {
        /// The Build.
        build_id: String,
    },
    /// `build.delete`: a finished Build leaves history.
    ///
    /// Position 3 on the wire.
    Delete {
        /// The Build.
        build_id: String,
    },
    /// `build.get` (Q167 R-RD5): every Build the fabric-primary holds whose header matches every pair
    /// of `filter` (`id`, `current`, `phase`), newest first, each whole: its topology in `Topology`
    /// chunks, its events in `Events` chunks, then its `Build` header; then `End`. The current Build
    /// is selected once per read and its header marked.
    ///
    /// Position 4 on the wire.
    GetBuilds {
        /// The pairs the Build header must match.
        filter: crate::filter::Filter,
    },
}

impl BuildRequest {
    /// The request's operation name as it appears in spans and replies.
    pub fn op(&self) -> &'static str {
        match self {
            Self::Create { .. } => "create",
            Self::AttemptRun { .. } => "attempt.run",
            Self::Get { .. } => "get",
            Self::Delete { .. } => "delete",
            Self::GetBuilds { .. } => "build.get",
        }
    }
}

/// How a stream's call came to its Build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Disposition {
    /// A new Build was accepted, or a failed Build's remaining steps were dispatched again.
    ///
    /// Position 0 on the wire.
    Created,
    /// The Build is running: this stream follows it.
    ///
    /// Position 1 on the wire.
    Attached,
    /// The Build is complete: nothing runs, the terminal frame follows.
    ///
    /// Position 2 on the wire.
    AlreadyApplied,
    /// `AttemptRun`: the recipient started the mesh's run.
    ///
    /// Position 3 on the wire.
    Started,
    /// `AttemptRun`: the recipient holds the mesh's run; this stream attached to it.
    ///
    /// Position 4 on the wire.
    Reattached,
}

/// How a step ended, as its `rdm.build.step` event says it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StepResult {
    /// The step completed.
    ///
    /// Position 0 on the wire.
    Complete,
    /// The step failed.
    ///
    /// Position 1 on the wire.
    Failed {
        /// Why it failed.
        reason: String,
        /// The standard code captured where the failure occurred.
        code: crate::outcome::Code,
    },
}

/// Where a Build is in its life, as its events say it (build.md §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildPhase {
    /// Accepted, and no `rdm.build.started` written yet.
    ///
    /// Position 0 on the wire.
    Pending,
    /// `rdm.build.started` written, no `rdm.build.ended`.
    ///
    /// Position 1 on the wire.
    Running,
    /// `rdm.build.ended` `complete`.
    ///
    /// Position 2 on the wire.
    Complete,
    /// `rdm.build.ended` `failed`: the Build could not start (Q346).
    ///
    /// Position 3 on the wire.
    Failed,
    /// `rdm.build.ended` `incomplete`: the Build ran and not every section finished; what ran stands
    /// (Q346).
    ///
    /// Position 4 on the wire.
    Incomplete,
}

impl BuildPhase {
    /// The phase's page name, what a `build.get` pair names it by (`running`).
    pub const fn page_name(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Incomplete => "incomplete",
        }
    }
}

/// A mesh's section that ended short of its end (Q346): the step it stopped at and why. A failed
/// step names itself; a section call that ended short names the call, `build.attempt.run`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnfinishedSection {
    /// The mesh whose section it is.
    pub mesh: String,
    /// The operation the section stopped at.
    pub operation: String,
    /// The step it stopped at.
    pub step: String,
    /// Why, by name.
    pub reason: String,
    /// The standard code captured where it stopped.
    pub code: crate::outcome::Code,
}

/// A Build's header as `build.get` matches and streams it (Q167 R-RD5): its phase with the
/// `current` mark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildHeader {
    /// The Build.
    pub build_id: String,
    /// Where the Build is in its life.
    pub phase: BuildPhase,
    /// Why the Build is where it is.
    pub reason: String,
    /// Why the Build failed, when it did.
    pub last_failure: Option<String>,
    /// Whether this is the current Build, selected once for the read.
    pub current: bool,
}

/// One frame of a Build stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildReply {
    /// The stream begins: the Build it follows.
    ///
    /// Position 0 on the wire.
    Started {
        /// The Build.
        build_id: String,
        /// How the call came to the Build.
        disposition: Disposition,
    },
    /// A step ended. On the inner stream the mesh-primary reports the step it has written as
    /// `rdm.build.step`; on the outer stream the fabric-primary passes it on.
    ///
    /// Position 1 on the wire.
    Step {
        /// The Build.
        build_id: String,
        /// The mesh whose section the step belongs to.
        mesh: String,
        /// The step's operation, `noun.verb:<node_id>` (the event's `op`).
        operation: String,
        /// The step.
        step: String,
        /// How the step ended.
        result: StepResult,
    },
    /// The step in flight is blocked. Non-terminal, in memory only, sent on a new blocker or a
    /// changed reason and never on a timer; never a step outcome.
    ///
    /// Position 2 on the wire.
    Blocked {
        /// The Build.
        build_id: String,
        /// The mesh whose section the step belongs to.
        mesh: String,
        /// The operation the blocked step belongs to.
        operation: String,
        /// The blocked step.
        step: String,
        /// What it waits for.
        reason: String,
    },
    /// Terminal: the Build converged. Steps whose event ended `complete` were read and replayed,
    /// never run again.
    ///
    /// Position 3 on the wire.
    Complete {
        /// The Build.
        build_id: String,
    },
    /// Terminal: the Build could not start (Q346). Nothing follows it on the stream.
    ///
    /// Position 4 on the wire.
    Failed {
        /// The Build.
        build_id: String,
        /// Why it could not start.
        reason: String,
        /// The standard code captured where it could not start.
        code: crate::outcome::Code,
    },
    /// Terminal of `Get`: the Build's header; its events came in `Events` chunks.
    ///
    /// Position 5 on the wire.
    Got {
        /// The Build.
        build_id: String,
        /// Where the Build is in its life.
        phase: BuildPhase,
        /// Why the Build is where it is.
        reason: String,
        /// Why the Build failed, when it did.
        last_failure: Option<String>,
        /// The number of events sent.
        events: u32,
        /// The number of chunks sent.
        chunks: u32,
    },
    /// Terminal of `Delete`.
    ///
    /// Position 6 on the wire.
    Deleted {
        /// The Build removed from history.
        build_id: String,
    },
    /// Refusal: the receiver is not the fabric-primary; it names the one it sees.
    ///
    /// Position 7 on the wire.
    NotFabricPrimary {
        /// The fabric-primary the receiver sees, when it sees one.
        fabric_primary: Option<crate::NodeId>,
    },
    /// Refusal: the recipient of `AttemptRun` is not the mesh's primary.
    ///
    /// Position 8 on the wire.
    NotExecutor {
        /// The mesh's primary the recipient sees.
        named: crate::framing::Birth,
        /// The recipient.
        recipient: crate::framing::Birth,
    },
    /// Refusal: the receiver holds no fact of the Build.
    ///
    /// Position 9 on the wire.
    UnknownBuild {
        /// The Build.
        build_id: String,
    },
    /// Refusal: the change is refused by the topology rules, naming the rule and why.
    ///
    /// Position 10 on the wire.
    Rejected {
        /// The rule's name (`unknown-node`, `unheard-mesh`, ...).
        reason: String,
        /// The rule's detail.
        detail: String,
    },
    /// Refusal: a second topology change while a Build is in progress is refused fabric-wide.
    ///
    /// Position 11 on the wire.
    BuildInProgress {
        /// The Build in flight.
        current_build_id: String,
    },
    /// Refusal: the fabric-primary record the receiver holds names another live birth, so the
    /// receiver dispatches nothing (node-rpc-envelope.md's second `NotFabricPrimary { node, by }`).
    ///
    /// Position 12 on the wire.
    RoleYielded {
        /// The admin whose Build log yielded the role.
        node: NodeId,
        /// The record that moved the role, in words.
        by: String,
    },
    /// Refusal: the Build cannot leave history.
    ///
    /// Position 13 on the wire.
    CannotDelete {
        /// The Build.
        build_id: String,
        /// Why it stays.
        reason: String,
    },
    /// Refusal: the peer the call needed could not be resolved.
    ///
    /// Position 14 on the wire.
    PeerUnresolved {
        /// Why the peer could not be resolved.
        reason: String,
    },
    /// Refusal: the receiver cannot answer yet; `reason` names what it waits for.
    ///
    /// Position 15 on the wire.
    NotReady {
        /// What the receiver waits for.
        reason: String,
    },
    /// Refusal: the receiver is at its admission bound.
    ///
    /// Position 16 on the wire.
    Busy {
        /// Which bound it is at.
        reason: String,
    },
    /// Refusal: the receiver is connections-deleting and takes no new work.
    ///
    /// Position 17 on the wire.
    ConnectionsDeleting {
        /// Why it refuses new work.
        reason: String,
    },
    /// Refusal: the request frame was malformed.
    ///
    /// Position 18 on the wire.
    Malformed {
        /// How the frame was malformed.
        kind: MalformedKind,
    },
    /// Refusal: the caller is not allowed this call.
    ///
    /// Position 19 on the wire.
    Unauthorized {
        /// Why the call is refused.
        reason: String,
    },
    /// Terminal of `Create`: the fabric-primary role moved off the receiver while the Build ran (a
    /// node.fabric-primary.update, op `0x21`). The Build goes on at the new fabric-primary; the caller
    /// re-submits the same `build_id` there. Never `Indeterminate` on a planned move.
    ///
    /// Position 20 on the wire.
    FabricPrimaryMoved {
        /// The Build.
        build_id: String,
        /// The admin that holds the role now, when the receiver's view names one.
        fabric_primary: Option<crate::NodeId>,
    },
    /// A chunk of one Build's topology `GetBuilds` reads: its nodes' create specs, in `(path, name)`
    /// order. A topology no reply frame holds takes several chunks.
    ///
    /// Position 21 on the wire.
    Topology {
        /// The Build.
        build_id: String,
        /// This chunk's index.
        chunk_index: u32,
        /// The number of chunks.
        chunk_count: u32,
        /// The create specs in this chunk.
        nodes: Vec<crate::meta::NodeSpec>,
    },
    /// A chunk of one Build's events, in event.id order: each the events store's CloudEvents
    /// envelope (build.md §5) in its JSON form, since its `data` is a JSON object.
    ///
    /// Position 22 on the wire.
    Events {
        /// The Build.
        build_id: String,
        /// This chunk's index.
        chunk_index: u32,
        /// The number of chunks.
        chunk_count: u32,
        /// The events in this chunk, each one envelope's JSON.
        events: Vec<String>,
    },
    /// The header that closes one Build of a `GetBuilds` stream, after its `Topology` and `Events`
    /// chunks. A data frame: `End` ends the read.
    ///
    /// Position 23 on the wire.
    Build {
        /// The Build.
        build_id: String,
        /// Where the Build is in its life.
        phase: BuildPhase,
        /// Whether this is the current Build, selected once for the read.
        current: bool,
        /// The number of events sent for it.
        events: u32,
        /// The number of `Events` chunks sent for it.
        chunks: u32,
        /// The change as it came in, the Build record's (build.md §1): history only.
        change: BuildChange,
        /// When the Build was accepted, the Build record's: milliseconds since the Unix epoch.
        time: u64,
    },
    /// Terminal of `GetBuilds`: the read is complete. A stream that ends before it is an incomplete
    /// read, never a list.
    ///
    /// Position 24 on the wire.
    End {
        /// The number of Builds streamed.
        builds: u32,
    },
    /// Refusal: a `GetBuilds` pair the Build header's schema refuses (Q167 R-RD2), naming the pair.
    ///
    /// Position 25 on the wire.
    RejectedFilter {
        /// Why, naming the pair.
        reason: String,
    },
    /// Terminal: the Build ran and not every section finished; what ran stands (Q346). On a section's
    /// stream, its mesh's section ended at the one section it names. Nothing follows it.
    ///
    /// Position 26 on the wire.
    Incomplete {
        /// The Build.
        build_id: String,
        /// Each section that ended short of its end.
        sections: Vec<UnfinishedSection>,
    },
}

impl BuildReply {
    /// The reply's name as it appears in spans and evidence.
    pub fn name(&self) -> &'static str {
        <Build as NodeProtocol>::reply_detail(self)
    }

    /// The key `(build_id, mesh, operation, step)` of a progress frame, when it is one.
    pub fn progress_key(&self) -> Option<(&str, &str, &str, &str)> {
        match self {
            Self::Step { build_id, mesh, operation, step, .. } | Self::Blocked { build_id, mesh, operation, step, .. } => Some((build_id, mesh, operation, step)),
            _ => None,
        }
    }
}

impl NodeProtocol for Build {
    const OP: u8 = 0x20;
    const NAME: &'static str = "build";
    /// A change with a fabric's meshes, or a dispatch with its context (a traceparent, a tracestate
    /// of 512 bytes and baggage of 8192 bytes), with room for the tags and lengths. A dispatch names
    /// its Build and mesh and never carries a copy of either (Q292).
    const MAX_REQUEST_FRAME_BYTES: usize = 256 * 1024;
    /// A failed step's reason is the largest field; the sender bounds it below this.
    const MAX_REPLY_FRAME_BYTES: usize = 64 * 1024;
    /// A stream, decided by the fabric-primary itself or not at all.
    const FORWARDABLE: bool = false;
    const REQUEST_VARIANTS: u32 = 5;
    const REPLY_VARIANTS: u32 = 27;

    type Request = BuildRequest;
    type Reply = BuildReply;

    fn decode_request(bytes: &[u8]) -> Result<Self::Request, DecodeFailure> {
        crate::protocol::decode_enum(bytes, Self::REQUEST_VARIANTS)
    }

    fn reply_detail(reply: &Self::Reply) -> &'static str {
        match reply {
            BuildReply::Started { .. } => "started",
            BuildReply::Step { .. } => "step",
            BuildReply::Blocked { .. } => "blocked",
            BuildReply::Complete { .. } => "complete",
            BuildReply::Failed { .. } => "failed",
            BuildReply::Got { .. } => "got",
            BuildReply::Deleted { .. } => "deleted",
            BuildReply::NotFabricPrimary { .. } => "not-fabric-primary",
            BuildReply::NotExecutor { .. } => "not-executor",
            BuildReply::UnknownBuild { .. } => "unknown-build",
            BuildReply::Rejected { .. } => "rejected",
            BuildReply::BuildInProgress { .. } => "build-in-progress",
            BuildReply::RoleYielded { .. } => "role-yielded",
            BuildReply::CannotDelete { .. } => "cannot-delete",
            BuildReply::PeerUnresolved { .. } => "peer-unresolved",
            BuildReply::NotReady { .. } => "not-ready",
            BuildReply::Busy { .. } => "busy",
            BuildReply::ConnectionsDeleting { .. } => "connections-deleting",
            BuildReply::Malformed { .. } => "malformed",
            BuildReply::Unauthorized { .. } => "unauthorized",
            BuildReply::FabricPrimaryMoved { .. } => "fabric-primary-moved",
            BuildReply::Topology { .. } => "topology",
            BuildReply::Events { .. } => "events",
            BuildReply::Build { .. } => "build",
            BuildReply::End { .. } => "end",
            BuildReply::RejectedFilter { .. } => "rejected-filter",
            BuildReply::Incomplete { .. } => "incomplete",
        }
    }

    fn classify_reply(reply: &BuildReply) -> ReplyKind {
        match reply {
            BuildReply::Started { disposition: Disposition::AlreadyApplied, .. } => ReplyKind::new(crate::outcome::Code::AlreadyExists),
            BuildReply::Started { .. } => ReplyKind::Success,
            BuildReply::Step { .. } => ReplyKind::Success,
            BuildReply::Blocked { .. } => ReplyKind::Success,
            BuildReply::Complete { .. } => ReplyKind::Success,
            BuildReply::Failed { code, .. } => ReplyKind::new(*code),
            BuildReply::Got { .. } => ReplyKind::Success,
            BuildReply::Deleted { .. } => ReplyKind::Success,
            BuildReply::NotFabricPrimary { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            BuildReply::NotExecutor { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            BuildReply::UnknownBuild { .. } => ReplyKind::new(crate::outcome::Code::NotFound),
            BuildReply::Rejected { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            BuildReply::BuildInProgress { .. } => ReplyKind::new(crate::outcome::Code::Unavailable),
            BuildReply::RoleYielded { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            BuildReply::CannotDelete { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            BuildReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            BuildReply::NotReady { .. } => ReplyKind::NotReady,
            BuildReply::Busy { .. } => ReplyKind::Busy,
            BuildReply::ConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
            BuildReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            BuildReply::Unauthorized { .. } => ReplyKind::Unauthorized,
            BuildReply::FabricPrimaryMoved { .. } => ReplyKind::Success,
            BuildReply::Topology { .. } => ReplyKind::Success,
            BuildReply::Events { .. } => ReplyKind::Success,
            BuildReply::Build { .. } => ReplyKind::Success,
            BuildReply::End { .. } => ReplyKind::Success,
            BuildReply::RejectedFilter { .. } => ReplyKind::new(crate::outcome::Code::InvalidArgument),
            BuildReply::Incomplete { sections, .. } => ReplyKind::new(sections.first().map(|s| s.code).unwrap_or(crate::outcome::Code::Aborted)),
        }
    }

    fn peer_unresolved(reason: String) -> BuildReply {
        BuildReply::PeerUnresolved { reason }
    }

    fn not_ready(reason: String) -> BuildReply {
        BuildReply::NotReady { reason }
    }

    fn busy(reason: String) -> BuildReply {
        BuildReply::Busy { reason }
    }

    fn connections_deleting(reason: String) -> BuildReply {
        BuildReply::ConnectionsDeleting { reason }
    }

    fn malformed(kind: MalformedKind) -> BuildReply {
        BuildReply::Malformed { kind }
    }

    fn unauthorized(reason: String) -> BuildReply {
        BuildReply::Unauthorized { reason }
    }
}

impl StreamingProtocol for Build {
    const ENDS_WITH_ITS_CONNECTION: bool = true;

    fn frame_kind(frame: &BuildReply) -> FrameKind {
        match frame {
            BuildReply::Started { .. } => FrameKind::Started,
            BuildReply::Step { .. } | BuildReply::Blocked { .. } | BuildReply::Events { .. } | BuildReply::Topology { .. } | BuildReply::Build { .. } => FrameKind::Data,
            BuildReply::Complete { .. } | BuildReply::Failed { .. } | BuildReply::Incomplete { .. } | BuildReply::FabricPrimaryMoved { .. } | BuildReply::Got { .. } | BuildReply::Deleted { .. } | BuildReply::End { .. } => FrameKind::Terminal,
            other => FrameKind::Refusal(Build::classify_reply(other)),
        }
    }
}
