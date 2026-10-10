//! The Build family on op `0x20`, RDM's additive carrier for Builds (node-rpc-envelope.md "Build, op
//! `0x20`, rdm"; R-ON8 = A′).
//!
//! The fabric-primary is the one driver of every Build. Four calls, one stream shape:
//!
//! ```text
//! Create     caller -> fabric-primary   a submitted change, or a re-submit by build_id
//! AttemptRun fabric-primary -> executor one claimed attempt, start-or-reattach on (build_id, attempt)
//! Get        caller -> fabric-primary   the Build and its receipts, for reattachment after a cut
//! Delete     caller -> fabric-primary   a finished Build leaves history
//! ```
//!
//! A stream is one refusal, or `Started`, then `Step`/`Blocked`/`Steps` frames, then one terminal:
//!
//! ```text
//! Started { build_id, attempt, disposition }
//! Step { build_id, attempt, operation, step }      only after the step's Complete receipt is durable
//!                                                   on the executor that wrote it
//! Blocked { build_id, attempt, operation, step, reason }   non-terminal, never durable, never a step outcome
//! Complete | Failed { step, reason } | HandedOff { to }    the terminal of Create / AttemptRun
//! Got | Deleted                                    the terminal of Get / Delete
//! ```
//!
//! Every progress frame carries `(build_id, attempt, operation, step)`; a receiver drops a frame
//! whose key it already holds. A cut stream is `NotSent` before the commit cut and `Indeterminate`
//! after it, never a failed step. The family is not forwardable: it is a stream, and only the
//! fabric-primary accepts.

use crate::context::CallContext;
use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::NodeProtocol;
use crate::streaming::{FrameKind, StreamingProtocol};
use rafka_mesh_entity::{IncarnationId, NodeKind, PathName};
use serde::{Deserialize, Serialize};

/// The Build protocol: Builds are created, driven, read and deleted through the fabric-primary.
pub struct Build;

/// One mesh's counts by kind, as a submitted change names them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshCounts {
    /// The mesh's name.
    pub name: String,
    /// The number of node-admins.
    pub node_admin: u32,
    /// The number of rpc nodes.
    pub rpc_node: u32,
    /// The number of brokers.
    pub broker: u32,
    /// The number of gateways.
    pub gateway: u32,
    /// The number of compute nodes.
    pub compute: u32,
}

/// What a caller submits. The fabric-primary compiles it once against the accepted topology (a
/// topology change opens a new Build) or opens the next attempt of the accepted Build (a restart,
/// replacement, drain or stop fenced to one birth).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildChange {
    /// The whole fabric's meshes and counts.
    ReconcileFabric {
        /// The fabric's name.
        fabric: String,
        /// The desired meshes.
        meshes: Vec<MeshCounts>,
    },
    /// One mesh's counts (grow or shrink).
    ReconcileMesh {
        /// The mesh's desired counts.
        mesh: MeshCounts,
    },
    /// One more node of `node_kind` in `mesh`.
    AddNode {
        /// The mesh to add the node to.
        mesh: String,
        /// The kind of node to add.
        node_kind: NodeKind,
    },
    /// Remove one node.
    RemoveNode {
        /// The node to remove.
        node: PathName,
    },
    /// A new mesh with its counts.
    CreateMesh {
        /// The mesh's desired counts.
        mesh: MeshCounts,
    },
    /// Remove a mesh.
    RemoveMesh {
        /// The mesh to remove.
        mesh: String,
    },
    /// Restart the live birth at `node` (the next attempt of the accepted Build).
    Restart {
        /// The node.
        node: PathName,
    },
    /// Replace the birth at `node` with a new node. `incarnation` names the exact birth to retire
    /// when the view does not hold the node.
    Replace {
        /// The node.
        node: PathName,
        /// The exact birth to retire, for a node the view does not hold.
        incarnation: Option<IncarnationId>,
    },
    /// Drain the live birth at `node` and keep it running.
    Drain {
        /// The node.
        node: PathName,
    },
    /// Stop the live birth at `node` with no implicit drain and no departure.
    Stop {
        /// The node.
        node: PathName,
    },
}

/// What `Create` asks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildSubmit {
    /// A new change to accept and drive.
    Change(BuildChange),
    /// A re-submit of a Build the caller already holds the id of: never a second Build.
    Resubmit {
        /// The Build.
        build_id: String,
        /// The attempt the caller's first call opened: the stream carries the frames of this
        /// attempt and later ones.
        from_attempt: u32,
    },
}

/// A Build call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildRequest {
    /// `build.create`: accept `submit` and stream the Build until it ends.
    Create {
        /// What is submitted.
        submit: BuildSubmit,
    },
    /// `build.attempt.run`: run attempt `attempt` of `build_id`, claimed for `executor`, on the
    /// recipient. Start-or-reattach: a repeat of the same `(build_id, attempt)` attaches to the run.
    AttemptRun {
        /// The Build.
        build_id: String,
        /// The attempt, already won by `executor` on the fabric-primary's log.
        attempt: u32,
        /// The executor the claim names (a node-admin's path.name).
        executor: String,
        /// The attempt's observability context, as the claim returned it.
        context: CallContext,
        /// The Build's intent as the fabric-primary holds it: its acceptance and every attempt it
        /// opened, as postcard frames of the Build topic's message (the packing the topic's own
        /// catch-up uses). The executor absorbs them before it plans, so the run never waits for the
        /// Build topic to deliver what the claim was decided on. Empty when the fabric-primary runs
        /// the attempt itself.
        intent: Vec<Vec<u8>>,
    },
    /// `build.get`: the Build and its receipts, for reattachment after a cut.
    Get {
        /// The Build.
        build_id: String,
    },
    /// `build.delete`: a finished Build leaves history.
    Delete {
        /// The Build.
        build_id: String,
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
        }
    }
}

/// How a stream's call came to its Build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Disposition {
    /// A new Build was accepted, or the next attempt of the accepted Build was opened.
    Created,
    /// The Build is running with a live holder: this stream is that holder's.
    Attached,
    /// The Build had failed or its holder was proven lost: the next attempt runs.
    NextAttempt,
    /// The Build is complete: nothing runs, the terminal frame follows.
    AlreadyApplied,
    /// `AttemptRun`: the recipient started the claimed attempt.
    Started,
    /// `AttemptRun`: the recipient holds the attempt's run; this stream attached to it.
    Reattached,
}

/// How a step ended, as a receipt says it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StepResult {
    /// The step completed.
    Complete,
    /// The step failed.
    Failed {
        /// Why it failed.
        reason: String,
    },
}

/// One step receipt of a Build, as `Get` reads it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepReceipt {
    /// The attempt that wrote it.
    pub attempt: u32,
    /// The operation's idempotency key.
    pub operation: String,
    /// The step.
    pub step: String,
    /// How the step ended.
    pub result: StepResult,
}

/// Where a Build is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildPhase {
    /// No executor runs an attempt.
    Pending,
    /// An executor holds and runs an attempt.
    Running,
    /// Every step is complete.
    Complete,
    /// The last attempt failed.
    Failed,
}

/// One frame of a Build stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildReply {
    /// The stream begins: the Build and the attempt it follows.
    Started {
        /// The Build.
        build_id: String,
        /// The attempt this stream follows (the one the call opened, claimed or attached to).
        attempt: u32,
        /// How the call came to the Build.
        disposition: Disposition,
    },
    /// A step completed: sent only after its Complete receipt is durable on the executor that wrote it.
    Step {
        /// The Build.
        build_id: String,
        /// The attempt.
        attempt: u32,
        /// The operation's idempotency key.
        operation: String,
        /// The step.
        step: String,
    },
    /// The step in flight is blocked. Non-terminal, in memory only, sent on a new blocker or a
    /// changed reason and never on a timer; never a step outcome.
    Blocked {
        /// The Build.
        build_id: String,
        /// The attempt.
        attempt: u32,
        /// The operation the blocked step belongs to.
        operation: String,
        /// The blocked step.
        step: String,
        /// What it waits for.
        reason: String,
    },
    /// A chunk of the receipts `Get` reads.
    Steps {
        /// The Build.
        build_id: String,
        /// This chunk's index.
        chunk_index: u32,
        /// The number of chunks.
        chunk_count: u32,
        /// The receipts in this chunk.
        steps: Vec<StepReceipt>,
    },
    /// Terminal: the Build converged. Completed steps were read and replayed, never run again.
    Complete {
        /// The Build.
        build_id: String,
        /// The attempt that converged it.
        attempt: u32,
    },
    /// Terminal: a step failed. Nothing follows it on the stream.
    Failed {
        /// The Build.
        build_id: String,
        /// The attempt that failed.
        attempt: u32,
        /// The operation the failed step belongs to.
        operation: String,
        /// The step that failed.
        step: String,
        /// The step's own reason.
        reason: String,
    },
    /// Terminal of `AttemptRun`: the run ended at an operation another executor runs. The
    /// fabric-primary claims the next attempt for `to`.
    HandedOff {
        /// The Build.
        build_id: String,
        /// The attempt that ran.
        attempt: u32,
        /// The admin that executes the operation the attempt stopped at.
        to: String,
    },
    /// Terminal of `Get`: the Build's header; the receipts came in `Steps` chunks.
    Got {
        /// The Build.
        build_id: String,
        /// Where the Build is in its life.
        phase: BuildPhase,
        /// The current attempt.
        attempt: u32,
        /// The executor of the current attempt, when one is.
        executor: Option<String>,
        /// Why the current attempt exists.
        reason: String,
        /// Why the last attempt failed, when one did.
        last_failure: Option<String>,
        /// The number of receipts sent.
        steps: u32,
        /// The number of chunks sent.
        chunks: u32,
    },
    /// Terminal of `Delete`.
    Deleted {
        /// The Build removed from history.
        build_id: String,
    },
    /// Refusal: the receiver is not the fabric-primary; it names the one it sees.
    NotFabricPrimary {
        /// The fabric-primary the receiver sees, when it sees one.
        fabric_primary: Option<String>,
    },
    /// Refusal: the recipient is not the executor the claim names.
    NotExecutor {
        /// The executor the claim names.
        named: String,
        /// The recipient.
        recipient: String,
    },
    /// Refusal: the claim is not the current folded attempt of the Build.
    StaleClaim {
        /// The attempt the recipient's fold holds as current.
        held_attempt: u32,
        /// The executor of that attempt, when one is.
        held_executor: Option<String>,
        /// The attempt the call carried.
        carried_attempt: u32,
    },
    /// Refusal: the receiver holds no fact of the Build.
    UnknownBuild {
        /// The Build.
        build_id: String,
    },
    /// Refusal: the change is refused by the topology rules, naming the rule and why.
    Rejected {
        /// The rule's name (`unknown-node`, `unheard-mesh`, ...).
        reason: String,
        /// The rule's detail.
        detail: String,
    },
    /// Refusal: another Build is still reconciling; one accepted topology, one Build in flight.
    BuildInProgress {
        /// The Build in flight.
        current_build_id: String,
    },
    /// Refusal: the attempt number was opened first by another action.
    AttemptTaken {
        /// What holds the number.
        detail: String,
    },
    /// Refusal: the receiver yielded the fabric-primary seat and dispatches nothing.
    Fenced {
        /// The admin that yielded.
        node: String,
        /// Why it yielded.
        by: String,
    },
    /// Refusal: the Build cannot leave history.
    CannotDelete {
        /// The Build.
        build_id: String,
        /// Why it stays.
        reason: String,
    },
    /// Refusal: the peer the call needed could not be resolved.
    PeerUnresolved {
        /// Why the peer could not be resolved.
        reason: String,
    },
    /// Refusal: the receiver cannot answer yet; `reason` names what it waits for.
    NotReady {
        /// What the receiver waits for.
        reason: String,
    },
    /// Refusal: the receiver is at its admission bound.
    Busy {
        /// Which bound it is at.
        reason: String,
    },
    /// Refusal: the receiver is draining and takes no new work.
    Draining {
        /// Why it refuses new work.
        reason: String,
    },
    /// Refusal: the request frame was malformed.
    Malformed {
        /// How the frame was malformed.
        kind: MalformedKind,
    },
    /// Refusal: the caller is not allowed this call.
    Unauthorized {
        /// Why the call is refused.
        reason: String,
    },
}

impl BuildReply {
    /// The reply's name as it appears in spans and evidence.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Started { .. } => "started",
            Self::Step { .. } => "step",
            Self::Blocked { .. } => "blocked",
            Self::Steps { .. } => "steps",
            Self::Complete { .. } => "complete",
            Self::Failed { .. } => "failed",
            Self::HandedOff { .. } => "handed-off",
            Self::Got { .. } => "got",
            Self::Deleted { .. } => "deleted",
            Self::NotFabricPrimary { .. } => "not-fabric-primary",
            Self::NotExecutor { .. } => "not-executor",
            Self::StaleClaim { .. } => "stale-claim",
            Self::UnknownBuild { .. } => "unknown-build",
            Self::Rejected { .. } => "rejected",
            Self::BuildInProgress { .. } => "build-in-progress",
            Self::AttemptTaken { .. } => "attempt-taken",
            Self::Fenced { .. } => "fenced",
            Self::CannotDelete { .. } => "cannot-delete",
            Self::PeerUnresolved { .. } => "peer-unresolved",
            Self::NotReady { .. } => "not-ready",
            Self::Busy { .. } => "busy",
            Self::Draining { .. } => "draining",
            Self::Malformed { .. } => "malformed",
            Self::Unauthorized { .. } => "unauthorized",
        }
    }

    /// The receipt key `(attempt, operation, step)` of a progress frame, when it is one.
    pub fn progress_key(&self) -> Option<(&str, u32, &str, &str)> {
        match self {
            Self::Step { build_id, attempt, operation, step } | Self::Blocked { build_id, attempt, operation, step, .. } => Some((build_id, *attempt, operation, step)),
            _ => None,
        }
    }
}

impl NodeProtocol for Build {
    const OP: u8 = 0x20;
    const NAME: &'static str = "build";
    /// A change with a fabric's meshes, or a claim with its attempt context (a traceparent, a
    /// tracestate of 512 bytes and baggage of 8192 bytes) and the Build's intent frames (each at most
    /// the Build topic's message bound), with room for the tags and lengths.
    const MAX_REQUEST_FRAME_BYTES: usize = 256 * 1024;
    /// A failed step's reason is the largest field; the sender bounds it below this.
    const MAX_REPLY_FRAME_BYTES: usize = 64 * 1024;
    /// A stream, decided by the fabric-primary itself or not at all.
    const FORWARDABLE: bool = false;
    const REQUEST_VARIANTS: u32 = 4;
    const REPLY_VARIANTS: u32 = 24;

    type Request = BuildRequest;
    type Reply = BuildReply;

    fn classify_reply(reply: &BuildReply) -> ReplyKind {
        match reply {
            BuildReply::Started { .. }
            | BuildReply::Step { .. }
            | BuildReply::Blocked { .. }
            | BuildReply::Steps { .. }
            | BuildReply::Complete { .. }
            | BuildReply::Failed { .. }
            | BuildReply::HandedOff { .. }
            | BuildReply::Got { .. }
            | BuildReply::Deleted { .. } => ReplyKind::Success,
            BuildReply::NotFabricPrimary { .. }
            | BuildReply::NotExecutor { .. }
            | BuildReply::StaleClaim { .. }
            | BuildReply::UnknownBuild { .. }
            | BuildReply::Rejected { .. }
            | BuildReply::BuildInProgress { .. }
            | BuildReply::AttemptTaken { .. }
            | BuildReply::Fenced { .. }
            | BuildReply::CannotDelete { .. } => ReplyKind::ProtocolRefusal,
            BuildReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            BuildReply::NotReady { .. } => ReplyKind::NotReady,
            BuildReply::Busy { .. } => ReplyKind::Busy,
            BuildReply::Draining { .. } => ReplyKind::Draining,
            BuildReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            BuildReply::Unauthorized { .. } => ReplyKind::Unauthorized,
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
    fn draining(reason: String) -> BuildReply {
        BuildReply::Draining { reason }
    }
    fn malformed(kind: MalformedKind) -> BuildReply {
        BuildReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> BuildReply {
        BuildReply::Unauthorized { reason }
    }
}

impl StreamingProtocol for Build {
    fn frame_kind(frame: &BuildReply) -> FrameKind {
        match frame {
            BuildReply::Started { .. } => FrameKind::Started,
            BuildReply::Step { .. } | BuildReply::Blocked { .. } | BuildReply::Steps { .. } => FrameKind::Data,
            BuildReply::Complete { .. } | BuildReply::Failed { .. } | BuildReply::HandedOff { .. } | BuildReply::Got { .. } | BuildReply::Deleted { .. } => FrameKind::Terminal,
            other => FrameKind::Refusal(Build::classify_reply(other)),
        }
    }
}
