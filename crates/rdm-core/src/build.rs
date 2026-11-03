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
//! GetBuilds  caller -> fabric-primary   build.get: every Build the filter matches, each whole
//! ```
//!
//! A stream is one refusal, or `Started`, then `Step`/`Blocked`/`Steps` frames, then one terminal:
//!
//! ```text
//! Started { build_id, attempt, disposition }
//! Step { build_id, attempt, operation, step }      only after the step's Complete receipt is durable
//!                                                   on the executor that wrote it
//! Blocked { build_id, attempt, operation, step, reason }   non-terminal, never durable, never a step outcome
//! Complete | Failed { step, reason } | HandedOff { to } | FabricPrimaryMoved { fabric_primary }
//!                                                  the terminal of Create / AttemptRun (FabricPrimaryMoved on
//!                                                  AttemptRun: the executor took the fabric-primary
//!                                                  role during the run and names itself)
//! Got | Deleted                                    the terminal of Get / Delete
//! End                                              the terminal of GetBuilds, after per Build its
//!                                                  Topology and Steps chunks and its Build header
//! ```
//!
//! Every progress frame carries `(build_id, attempt, operation, step)`; a receiver drops a frame
//! whose key it already holds. A cut stream is `NotSent` before the commit cut and `Indeterminate`
//! after it, never a failed step. The family is not forwardable: it is a stream, and only the
//! fabric-primary accepts.

use crate::context::CallContext;
use crate::outcome::{MalformedKind, ReplyKind};
use crate::protocol::{DecodeFailure, NodeProtocol};
use crate::streaming::{FrameKind, StreamingProtocol};
use crate::{NodeId, PathName};
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
        /// The attempt the caller's first call opened: the stream carries the frames of this
        /// attempt and later ones.
        from_attempt: u32,
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
    /// `build.attempt.run`: run attempt `attempt` of `build_id`, claimed for `executor`, on the
    /// recipient. Start-or-reattach: a repeat of the same `(build_id, attempt)` attaches to the run.
    ///
    /// Position 1 on the wire.
    AttemptRun {
        /// The Build.
        build_id: String,
        /// The attempt, already won by `executor` on the fabric-primary's log.
        attempt: u32,
        /// The exact executor birth committed by the claim.
        executor: crate::framing::Birth,
        /// The attempt's observability context, as the claim returned it.
        context: CallContext,
        /// The operations the fabric-primary planned for this run, in plan order: the contiguous
        /// leading run of what is left that the executor executes in the fabric-primary's view, one
        /// JSON-encoded operation each. The executor runs exactly these and never plans the Build
        /// from its own view. Each is idempotent by its natural key.
        operations: Vec<Vec<u8>>,
        /// The executor of the operation that follows this run, when the run is not the whole of
        /// what is left: the run then ends `HandedOff` to it. `None` when the run is the rest of the
        /// plan, and the attempt ends `Complete`.
        hand_off_to: Option<crate::NodeId>,
        /// The attempt's mesh for a drift attempt, numbered `(build_id, mesh, attempt)` (build.md section
        /// 5.3); `None` for a topology attempt (Q118).
        mesh: Option<String>,
    },
    /// `build.get`: the Build and its receipts, for reattachment after a cut.
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
    /// of `filter` (`id`, `current`, `phase`, `attempt`, `executor`), newest first, each whole: its
    /// topology in `Topology` chunks, its receipts in `Steps` chunks, then its `Build` header; then
    /// `End`. The current Build is selected once per read and its header marked. Appended last: the
    /// Build wire is positional.
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
    /// A new Build was accepted, or the next attempt of the accepted Build was opened.
    ///
    /// Position 0 on the wire.
    Created,
    /// The Build is running with a live primary: this stream is that holder's.
    ///
    /// Position 1 on the wire.
    Attached,
    /// The Build had failed or its holder was proven lost: the next attempt runs.
    ///
    /// Position 2 on the wire.
    NextAttempt,
    /// The Build is complete: nothing runs, the terminal frame follows.
    ///
    /// Position 3 on the wire.
    AlreadyApplied,
    /// `AttemptRun`: the recipient started the claimed attempt.
    ///
    /// Position 4 on the wire.
    Started,
    /// `AttemptRun`: the recipient holds the attempt's run; this stream attached to it.
    ///
    /// Position 5 on the wire.
    Reattached,
}

/// How a step ended, as a receipt says it.
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

/// One step receipt of a Build, as `Get` reads it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepReceipt {
    /// The attempt that wrote it.
    pub attempt: u32,
    /// The operation's idempotency key.
    pub operation: crate::OperationId,
    /// The step.
    pub step: String,
    /// How the step ended.
    pub result: StepResult,
}

impl StepReceipt {
    /// Encoded size used to bound reply chunks containing structured operation identities.
    pub fn encoded_len(&self) -> Result<usize, postcard::Error> {
        postcard::to_allocvec(self).map(|bytes| bytes.len())
    }
}

/// Where a Build is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildPhase {
    /// No executor runs an attempt.
    ///
    /// Position 0 on the wire.
    Pending,
    /// An executor holds and runs an attempt.
    ///
    /// Position 1 on the wire.
    Running,
    /// Every step is complete.
    ///
    /// Position 2 on the wire.
    Complete,
    /// The last attempt failed.
    ///
    /// Position 3 on the wire.
    Failed,
}

impl BuildPhase {
    /// The phase's page name, what a `build.get` pair names it by (`running`).
    pub fn page_name(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }
}

/// A Build's header as `build.get` matches and streams it (Q167 R-RD5): `Got`'s fields with the
/// `current` mark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildHeader {
    /// The Build.
    pub build_id: String,
    /// Where the Build is in its life.
    pub phase: BuildPhase,
    /// The current attempt.
    pub attempt: u32,
    /// The executor of the current attempt, when one is.
    pub executor: Option<crate::framing::Birth>,
    /// Why the current attempt exists.
    pub reason: String,
    /// Why the last attempt failed, when one did.
    pub last_failure: Option<String>,
    /// Whether this is the current Build, selected once for the read.
    pub current: bool,
}

/// One frame of a Build stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildReply {
    /// The stream begins: the Build and the attempt it follows.
    ///
    /// Position 0 on the wire.
    Started {
        /// The Build.
        build_id: String,
        /// The attempt this stream follows (the one the call opened, claimed or attached to).
        attempt: u32,
        /// How the call came to the Build.
        disposition: Disposition,
    },
    /// A step completed. On the inner attempt stream it reports the step's result upstream to the
    /// fabric-primary, which writes its receipt; on the outer stream the fabric-primary sends it only
    /// after it recorded the receipt (node-rpc-envelope.md, Frames, durable receipts and reattachment).
    ///
    /// Position 1 on the wire.
    Step {
        /// The Build.
        build_id: String,
        /// The attempt.
        attempt: u32,
        /// The operation's idempotency key.
        operation: crate::OperationId,
        /// The step.
        step: String,
        /// On the inner attempt stream the step's receipt (its output and executor), JSON-encoded, which
        /// the fabric-primary writes; `None` on the outer stream and for a receipt already written.
        receipt: Option<Vec<u8>>,
    },
    /// The step in flight is blocked. Non-terminal, in memory only, sent on a new blocker or a
    /// changed reason and never on a timer; never a step outcome.
    ///
    /// Position 2 on the wire.
    Blocked {
        /// The Build.
        build_id: String,
        /// The attempt.
        attempt: u32,
        /// The operation the blocked step belongs to.
        operation: crate::OperationId,
        /// The blocked step.
        step: String,
        /// What it waits for.
        reason: String,
    },
    /// A chunk of the receipts `Get` reads.
    ///
    /// Position 3 on the wire.
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
    ///
    /// Position 4 on the wire.
    Complete {
        /// The Build.
        build_id: String,
        /// The attempt that converged it.
        attempt: u32,
    },
    /// Terminal: a step failed. Nothing follows it on the stream.
    ///
    /// Position 5 on the wire.
    Failed {
        /// The Build.
        build_id: String,
        /// The attempt that failed.
        attempt: u32,
        /// The operation the failed step belongs to.
        operation: crate::OperationId,
        /// The step that failed.
        step: String,
        /// The step's own reason.
        reason: String,
        /// The standard code captured where the failure occurred.
        code: crate::outcome::Code,
    },
    /// Terminal of `AttemptRun`: the run ended at an operation another executor runs. The
    /// fabric-primary claims the next attempt for `to`.
    ///
    /// Position 6 on the wire.
    HandedOff {
        /// The Build.
        build_id: String,
        /// The attempt that ran.
        attempt: u32,
        /// The admin that executes the operation the attempt stopped at.
        to: crate::NodeId,
    },
    /// Terminal of `Get`: the Build's header; the receipts came in `Steps` chunks.
    ///
    /// Position 7 on the wire.
    Got {
        /// The Build.
        build_id: String,
        /// Where the Build is in its life.
        phase: BuildPhase,
        /// The current attempt.
        attempt: u32,
        /// The executor of the current attempt, when one is.
        executor: Option<crate::framing::Birth>,
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
    ///
    /// Position 8 on the wire.
    Deleted {
        /// The Build removed from history.
        build_id: String,
    },
    /// Refusal: the receiver is not the fabric-primary; it names the one it sees.
    ///
    /// Position 9 on the wire.
    NotFabricPrimary {
        /// The fabric-primary the receiver sees, when it sees one.
        fabric_primary: Option<crate::NodeId>,
    },
    /// Refusal: the recipient is not the executor the claim names.
    ///
    /// Position 10 on the wire.
    NotExecutor {
        /// The executor the claim names.
        named: crate::framing::Birth,
        /// The recipient.
        recipient: crate::framing::Birth,
    },
    /// Refusal: the claim is not the current folded attempt of the Build.
    ///
    /// Position 11 on the wire.
    StaleClaim {
        /// The attempt the recipient's fold holds as current.
        held_attempt: u32,
        /// The executor of that attempt, when one is.
        held_executor: Option<crate::framing::Birth>,
        /// The attempt the call carried.
        carried_attempt: u32,
    },
    /// Refusal: the receiver holds no fact of the Build.
    ///
    /// Position 12 on the wire.
    UnknownBuild {
        /// The Build.
        build_id: String,
    },
    /// Refusal: the change is refused by the topology rules, naming the rule and why.
    ///
    /// Position 13 on the wire.
    Rejected {
        /// The rule's name (`unknown-node`, `unheard-mesh`, ...).
        reason: String,
        /// The rule's detail.
        detail: String,
    },
    /// Refusal: another attempt holds what the request needs (R-BI7: one attempt in flight per
    /// mesh; a second topology change while a topology Build is in progress is refused fabric-wide).
    ///
    /// Position 14 on the wire.
    BuildInProgress {
        /// The Build in flight.
        current_build_id: String,
        /// The mesh the holding attempt acts in, when one is known (a claimed attempt's).
        mesh: Option<String>,
        /// The admin that claimed the holding attempt (the fabric-primary for a topology attempt,
        /// the mesh's mesh-primary for its drift attempt), when one claimed it.
        held_by: Option<crate::NodeId>,
    },
    /// Refusal: the attempt number was opened first by another action.
    ///
    /// Position 15 on the wire.
    AttemptTaken {
        /// What holds the number.
        detail: String,
    },
    /// Refusal: the fabric-primary record the receiver holds names another live birth, so the
    /// receiver dispatches nothing (node-rpc-envelope.md's second `NotFabricPrimary { node, by }`).
    ///
    /// Position 16 on the wire.
    RoleYielded {
        /// The admin whose Build log yielded the role.
        node: NodeId,
        /// The record that moved the role, in words.
        by: String,
    },
    /// Refusal: the Build cannot leave history.
    ///
    /// Position 17 on the wire.
    CannotDelete {
        /// The Build.
        build_id: String,
        /// Why it stays.
        reason: String,
    },
    /// Refusal: the peer the call needed could not be resolved.
    ///
    /// Position 18 on the wire.
    PeerUnresolved {
        /// Why the peer could not be resolved.
        reason: String,
    },
    /// Refusal: the receiver cannot answer yet; `reason` names what it waits for.
    ///
    /// Position 19 on the wire.
    NotReady {
        /// What the receiver waits for.
        reason: String,
    },
    /// Refusal: the receiver is at its admission bound.
    ///
    /// Position 20 on the wire.
    Busy {
        /// Which bound it is at.
        reason: String,
    },
    /// Refusal: the receiver is connections-deleting and takes no new work.
    ///
    /// Position 21 on the wire.
    ConnectionsDeleting {
        /// Why it refuses new work.
        reason: String,
    },
    /// Refusal: the request frame was malformed.
    ///
    /// Position 22 on the wire.
    Malformed {
        /// How the frame was malformed.
        kind: MalformedKind,
    },
    /// Refusal: the caller is not allowed this call.
    ///
    /// Position 23 on the wire.
    Unauthorized {
        /// Why the call is refused.
        reason: String,
    },
    /// Terminal of `Create`: the fabric-primary role moved off the receiver while the Build ran (a
    /// node.fabric-primary.update, op `0x21`). The Build goes on at the new fabric-primary; the caller re-submits the
    /// same `build_id` there. Never `Indeterminate` on a planned move.
    ///
    /// Position 24 on the wire.
    FabricPrimaryMoved {
        /// The Build.
        build_id: String,
        /// The admin (path and name) that holds the role now, when the receiver's view names one.
        fabric_primary: Option<crate::NodeId>,
    },
    /// Refusal: the receiver may not claim on the mesh the request acts in (R-BI7): a topology
    /// Build's attempt is the fabric-primary's, a drift attempt its mesh's mesh-primary's. Both are
    /// named as the receiver sees them.
    ///
    /// Position 25 on the wire.
    NotClaimant {
        /// The mesh the request acts in.
        mesh: Option<String>,
        /// The fabric-primary the receiver sees, when it sees one.
        fabric_primary: Option<crate::NodeId>,
        /// The mesh's mesh-primary the receiver sees, when it sees one.
        mesh_primary: Option<crate::NodeId>,
    },
    /// A chunk of one Build's topology `GetBuilds` reads: its nodes' create specs, in `(path, name)`
    /// order. A topology no reply frame holds takes several chunks.
    ///
    /// Position 26 on the wire.
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
    /// The header that closes one Build of a `GetBuilds` stream, after its `Topology` and `Steps`
    /// chunks: `Got`'s fields with the `current` mark. A data frame: `End` ends the read.
    ///
    /// Position 27 on the wire.
    Build {
        /// The Build.
        build_id: String,
        /// Where the Build is in its life.
        phase: BuildPhase,
        /// The current attempt.
        attempt: u32,
        /// The executor of the current attempt, when one is.
        executor: Option<crate::framing::Birth>,
        /// Why the current attempt exists.
        reason: String,
        /// Why the last attempt failed, when one did.
        last_failure: Option<String>,
        /// Whether this is the current Build, selected once for the read.
        current: bool,
        /// The number of receipts sent for it.
        steps: u32,
        /// The number of `Steps` chunks sent for it.
        chunks: u32,
    },
    /// Terminal of `GetBuilds`: the read is complete. A stream that ends before it is an incomplete
    /// read, never a list.
    ///
    /// Position 28 on the wire.
    End {
        /// The number of Builds streamed.
        builds: u32,
    },
    /// Refusal: a `GetBuilds` pair the Build header's schema refuses (Q167 R-RD2), naming the pair.
    ///
    /// Position 29 on the wire.
    RejectedFilter {
        /// Why, naming the pair.
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
            Self::RoleYielded { .. } => "role-yielded",
            Self::CannotDelete { .. } => "cannot-delete",
            Self::PeerUnresolved { .. } => "peer-unresolved",
            Self::NotReady { .. } => "not-ready",
            Self::Busy { .. } => "busy",
            Self::ConnectionsDeleting { .. } => "connections-deleting",
            Self::Malformed { .. } => "malformed",
            Self::Unauthorized { .. } => "unauthorized",
            Self::FabricPrimaryMoved { .. } => "fabric-primary-moved",
            Self::NotClaimant { .. } => "not-claimant",
            Self::Topology { .. } => "topology",
            Self::Build { .. } => "build",
            Self::End { .. } => "end",
            Self::RejectedFilter { .. } => "rejected-filter",
        }
    }

    /// The receipt key `(attempt, operation, step)` of a progress frame, when it is one.
    pub fn progress_key(&self) -> Option<(&str, u32, &crate::OperationId, &str)> {
        match self {
            Self::Step {
                build_id,
                attempt,
                operation,
                step,
                ..
            }
            | Self::Blocked {
                build_id,
                attempt,
                operation,
                step,
                ..
            } => Some((build_id, *attempt, operation, step)),
            _ => None,
        }
    }
}

impl NodeProtocol for Build {
    const OP: u8 = 0x20;
    const NAME: &'static str = "build";
    /// A change with a fabric's meshes, or a claim with its attempt context (a traceparent, a
    /// tracestate of 512 bytes and baggage of 8192 bytes) and the operations it plans, with room for
    /// the tags and lengths. A claim names its Build and never carries a copy of it (Q292).
    const MAX_REQUEST_FRAME_BYTES: usize = 256 * 1024;
    /// A failed step's reason is the largest field; the sender bounds it below this.
    const MAX_REPLY_FRAME_BYTES: usize = 64 * 1024;
    /// A stream, decided by the fabric-primary itself or not at all.
    const FORWARDABLE: bool = false;
    const REQUEST_VARIANTS: u32 = 5;
    const REPLY_VARIANTS: u32 = 30;

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
            BuildReply::Steps { .. } => "steps",
            BuildReply::Complete { .. } => "complete",
            BuildReply::Failed { .. } => "failed",
            BuildReply::HandedOff { .. } => "handed-off",
            BuildReply::Got { .. } => "got",
            BuildReply::Deleted { .. } => "deleted",
            BuildReply::NotFabricPrimary { .. } => "not-fabric-primary",
            BuildReply::NotExecutor { .. } => "not-executor",
            BuildReply::StaleClaim { .. } => "stale-claim",
            BuildReply::UnknownBuild { .. } => "unknown-build",
            BuildReply::Rejected { .. } => "rejected",
            BuildReply::BuildInProgress { .. } => "build-in-progress",
            BuildReply::AttemptTaken { .. } => "attempt-taken",
            BuildReply::RoleYielded { .. } => "role-yielded",
            BuildReply::CannotDelete { .. } => "cannot-delete",
            BuildReply::PeerUnresolved { .. } => "peer-unresolved",
            BuildReply::NotReady { .. } => "not-ready",
            BuildReply::Busy { .. } => "busy",
            BuildReply::ConnectionsDeleting { .. } => "connections-deleting",
            BuildReply::Malformed { .. } => "malformed",
            BuildReply::Unauthorized { .. } => "unauthorized",
            BuildReply::FabricPrimaryMoved { .. } => "fabric-primary-moved",
            BuildReply::NotClaimant { .. } => "not-claimant",
            BuildReply::Topology { .. } => "topology",
            BuildReply::Build { .. } => "build",
            BuildReply::End { .. } => "end",
            BuildReply::RejectedFilter { .. } => "rejected-filter",
        }
    }

    fn classify_reply(reply: &BuildReply) -> ReplyKind {
        match reply {
            BuildReply::Started { disposition: Disposition::AlreadyApplied, .. } => ReplyKind::new(crate::outcome::Code::AlreadyExists),
            BuildReply::Started { .. } => ReplyKind::Success,
            BuildReply::Step { .. } => ReplyKind::Success,
            BuildReply::Blocked { .. } => ReplyKind::Success,
            BuildReply::Steps { .. } => ReplyKind::Success,
            BuildReply::Complete { .. } => ReplyKind::Success,
            BuildReply::Failed { code, .. } => ReplyKind::new(*code),
            BuildReply::HandedOff { .. } => ReplyKind::Success,
            BuildReply::Got { .. } => ReplyKind::Success,
            BuildReply::Deleted { .. } => ReplyKind::Success,
            BuildReply::NotFabricPrimary { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            BuildReply::NotExecutor { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            BuildReply::StaleClaim { .. } => ReplyKind::new(crate::outcome::Code::Aborted),
            BuildReply::UnknownBuild { .. } => ReplyKind::new(crate::outcome::Code::NotFound),
            BuildReply::Rejected { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            BuildReply::BuildInProgress { .. } => ReplyKind::new(crate::outcome::Code::Unavailable),
            BuildReply::AttemptTaken { .. } => ReplyKind::new(crate::outcome::Code::Aborted),
            BuildReply::RoleYielded { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            BuildReply::CannotDelete { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            BuildReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            BuildReply::NotReady { .. } => ReplyKind::NotReady,
            BuildReply::Busy { .. } => ReplyKind::Busy,
            BuildReply::ConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
            BuildReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            BuildReply::Unauthorized { .. } => ReplyKind::Unauthorized,
            BuildReply::FabricPrimaryMoved { .. } => ReplyKind::Success,
            BuildReply::NotClaimant { .. } => ReplyKind::new(crate::outcome::Code::FailedPrecondition),
            BuildReply::Topology { .. } => ReplyKind::Success,
            BuildReply::Build { .. } => ReplyKind::Success,
            BuildReply::End { .. } => ReplyKind::Success,
            BuildReply::RejectedFilter { .. } => ReplyKind::new(crate::outcome::Code::InvalidArgument),
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
            BuildReply::Step { .. } | BuildReply::Blocked { .. } | BuildReply::Steps { .. } | BuildReply::Topology { .. } | BuildReply::Build { .. } => FrameKind::Data,
            BuildReply::Complete { .. } | BuildReply::Failed { .. } | BuildReply::HandedOff { .. } | BuildReply::FabricPrimaryMoved { .. } | BuildReply::Got { .. } | BuildReply::Deleted { .. } | BuildReply::End { .. } => FrameKind::Terminal,
            other => FrameKind::Refusal(Build::classify_reply(other)),
        }
    }
}
