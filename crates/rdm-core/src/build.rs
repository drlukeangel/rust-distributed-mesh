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
use crate::{IncarnationId, NodeId, PathName};
use serde::{Deserialize, Serialize};

/// The Build protocol: Builds are created, driven, read and deleted through the fabric-primary.
pub struct Build;

/// What a caller submits. The fabric-primary compiles it once against the accepted topology (a
/// topology change opens a new Build) or opens the next attempt of the accepted Build (a
/// connections delete, stop, start, recovery or steps, targeted at the births it acts on).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildChange {
    /// Reserved position: the removed count form `ReconcileFabric` (proposal
    /// executable-bindings-at-create.md §3.10(a)). Never sent; a request at this position decodes as
    /// `UnknownVariant` ([`Build::decode_request`]). Kept so every later change keeps its position.
    ///
    /// Position 0 on the wire.
    Reserved0,
    /// Reserved position: the removed count form `ReconcileMesh` (§3.10(a)), as [`Self::Reserved0`].
    ///
    /// Position 1 on the wire.
    Reserved1,
    /// Reserved position: the removed count form `AddNode` (§3.10(a)), as [`Self::Reserved0`].
    ///
    /// Position 2 on the wire.
    Reserved2,
    /// Remove one node.
    ///
    /// Position 3 on the wire.
    RemoveNode {
        /// The node to remove.
        node: PathName,
    },
    /// Reserved position: the removed count form `CreateMesh` (§3.10(a)), as [`Self::Reserved0`].
    ///
    /// Position 4 on the wire.
    Reserved4,
    /// Remove a mesh.
    ///
    /// Position 5 on the wire.
    RemoveMesh {
        /// The mesh to remove.
        mesh: String,
    },
    /// Reserved position: the removed `node.restart` (Q183; callers run node.stop then node.start).
    /// Never sent; a request at this position decodes as `UnknownVariant` ([`Build::decode_request`]).
    /// Kept so every later change keeps its position.
    ///
    /// Position 6 on the wire.
    Reserved6,
    /// `node.connection.delete { All }` of the live birth at `node`; it keeps running.
    ///
    /// Position 7 on the wire.
    DeleteConnections {
        /// The node.
        node: PathName,
    },
    /// Stop the live birth at `node` with no implicit connections delete and no departure.
    ///
    /// Position 8 on the wire.
    Stop {
        /// The node.
        node: PathName,
    },
    /// Start the stopped birth at `node`: the same process rejoins as itself.
    ///
    /// Position 9 on the wire.
    Start {
        /// The node.
        node: PathName,
    },
    /// Recover the node at `node` whose exact birth `(node_id, incarnation)` its mesh-primary proved
    /// exited (node-recovery.md): node.delete of that instance, then node.create at the same
    /// path.name over the inherited data dir, with a new node.id. Keyed on the exited birth: a
    /// repeat answers `AlreadyApplied` or `BuildInProgress`, never a second recovery.
    ///
    /// Position 10 on the wire.
    Recover {
        /// The node's `path.name`.
        node: PathName,
        /// The exited instance's node.id.
        node_id: NodeId,
        /// The exited instance's incarnation.
        incarnation: IncarnationId,
    },
    /// Steps: one Build whose steps are node primitives, run in order (build.md §1: "Rafka's
    /// replace steps (one Build of node primitives ...)"; lifecycles/node-replace.md).
    /// The caller owns the steps; RDM executes each step as its op's own workflow and records the
    /// step's receipts. A resume never runs a completed step again. The topology is unchanged:
    /// the steps are the next attempt of the accepted Build.
    ///
    /// Position 11 on the wire.
    Steps {
        /// The steps, in the order they run.
        steps: Vec<Step>,
    },
    /// Delete the fabric (`fabric.delete`, lifecycles/fabric-delete.md): run `mesh.delete` on each
    /// mesh leaf-inward (the fabric-primary's own mesh last, where the fabric role stays put),
    /// delete each other mesh's final mesh-primary, release the fabric name and report
    /// `fabric.deleted` while the fabric-primary remains alive for its separate `node.delete`.
    ///
    /// Position 12 on the wire.
    DeleteFabric,
    /// A deleted birth heard again (node-investigation.md: "A node that comes back after it was
    /// re-created is refused ... The node that refused it reports it to the fabric-primary, which
    /// terminates its runtime"): the report, keyed on the exact birth; the fabric-primary runs the
    /// existing `TerminateRuntime` on its runtime (Q110, Q128).
    ///
    /// Position 13 on the wire.
    Terminate {
        /// The `path.name` the birth held.
        node: PathName,
        /// The deleted birth's node.id.
        node_id: NodeId,
        /// The deleted birth's incarnation.
        incarnation: IncarnationId,
    },
    /// Reserved position: the removed per-kind executable-set change (proposal
    /// executable-bindings-at-create.md §1, Q157). Never sent; a request at this position decodes as
    /// `UnknownVariant` ([`Build::decode_request`]). A node's executable is replaced by a
    /// [`Self::Reconcile`] naming it for its path. Kept so every later change keeps its position.
    ///
    /// Position 14 on the wire.
    Reserved14,
    /// The complete topology (proposal executable-bindings-at-create.md rule 1): every node of the
    /// fabric, each by its create spec ([`crate::meta::NodeSpec`]: its `path.name` and its properties,
    /// flat, Q166), the executable included. A node left out is
    /// removed, one added is created, one whose executable differs from what its row records is
    /// replaced (node.delete then node.create at the path, in RDM's order). A mesh named for the
    /// first time is created with its first node-admin; a mesh left out is mesh.delete. Refused by
    /// name when `based_on` is no longer the current Build (nothing accepted; the caller re-reads),
    /// when it lists no node (that is fabric.delete), and when a path it creates names no
    /// executable. Appended last: the Build wire is positional.
    ///
    /// Position 15 on the wire.
    Reconcile {
        /// The build_id of the current Build the caller read.
        based_on: crate::BuildId,
        /// Every node of the fabric, each by its create spec.
        nodes: Vec<crate::meta::NodeSpec>,
        /// The fabric's `topology_storage_provider` (storage.md §2): the registered name of the backend
        /// RDM's five record stores go to. `None` leaves the fabric's current one unchanged (Q166); a
        /// name the fabric-primary's wiring does not register is refused by name at acceptance, nothing
        /// moved. Appended last: the Build wire is positional.
        #[serde(default)]
        topology_storage_provider: Option<String>,
    },
}

impl BuildChange {
    /// Whether this is a reserved position (positions 0, 1, 2, 4, 6 and 14: the removed count forms, the
    /// removed restart and the removed executable-set change): never sent, and a request at one is an
    /// unknown variant.
    pub fn is_reserved(&self) -> bool {
        matches!(self, Self::Reserved0 | Self::Reserved1 | Self::Reserved2 | Self::Reserved4 | Self::Reserved6 | Self::Reserved14)
    }
}

/// One step of a [`BuildChange::Steps`]: one node primitive acting on the node a `path.name` names
/// when the step runs. A path an earlier step of the same steps creates or renames to names the node
/// that step left there.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Step {
    /// node.create at `node`: a new node.id, incarnation and data dir at that exact `path.name`
    /// (for a replace's successor, the `<name>` its predecessor released), claimed before anything is
    /// launched.
    ///
    /// Position 0 on the wire.
    Create {
        /// The `path.name` the node is created at.
        node: PathName,
        /// The executable binding the Build fixes for the node (node-rpc-envelope.md "Build schema";
        /// node-replace.md step 3): whichever admin runs or resumes the step launches it, re-hashed at
        /// the launch, and the node's row records it beside `config`. Absent, the node runs the
        /// executable its seeding row records (the predecessor's, a recovered node's), and only a
        /// node no row records one for takes its kind's binding from the admin that launches it.
        /// A record written before the step carried it decodes with none.
        #[serde(default)]
        executable: Option<crate::binding::Binding>,
    },
    /// node.stop of the live node at `node`: it is stopped, its process and data kept.
    ///
    /// Position 1 on the wire.
    Stop {
        /// The node.
        node: PathName,
    },
    /// node.update of the node at `node`: its `path.name` becomes `name` (lifecycles/node-name-update.md, under the
    /// rename precondition: the node is live, its `node.connections.deleted` of an `All` and then its
    /// `node.mesh.left` held, and it holds no connection; a stopped node is never renamed).
    ///
    /// Position 2 on the wire.
    Update {
        /// The node.
        node: PathName,
        /// The `path.name` it takes.
        name: PathName,
    },
    /// node.start of the stopped node at `node`: the same process rejoins under the name its row holds.
    ///
    /// Position 3 on the wire.
    Start {
        /// The node.
        node: PathName,
    },
    /// node.delete of the node at `node`: its identity and name are released, its storage kept.
    ///
    /// Position 4 on the wire.
    Delete {
        /// The node.
        node: PathName,
    },
    /// node.connection.delete `{ scope: All }` of the live node at `node`
    /// (lifecycles/node-connections-delete.md): every connection it holds is closed; it keeps running
    /// on its mesh.
    ///
    /// Position 5 on the wire.
    DeleteConnections {
        /// The node.
        node: PathName,
    },
    /// node.mesh.leave of the live node at `node`, once its connections are deleted
    /// (lifecycles/node-mesh-leave.md): it leaves its mesh (and the backbone, a node-admin) and its Node
    /// RPC pool, its endpoint kept bound.
    ///
    /// Position 6 on the wire.
    LeaveMesh {
        /// The node.
        node: PathName,
    },
}

impl Step {
    /// The `path.name` the step acts on when it runs.
    pub fn node(&self) -> &PathName {
        match self {
            Self::Create { node, .. } | Self::Stop { node } | Self::Update { node, .. } | Self::Start { node } | Self::Delete { node } | Self::DeleteConnections { node } | Self::LeaveMesh { node } => node,
        }
    }
}

/// A storage change (storage.md §2 "Changing it"): the Build that moves RDM's five stores from the
/// fabric's provider to the one it names, recorded with the accepted Build before its first command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageChange {
    /// The change's durable identity: every command of the change carries it.
    pub change_id: crate::ChangeId,
    /// The provider the fabric writes at the acceptance: the old provider.
    pub from: String,
    /// The provider the Build names: the new provider.
    pub to: String,
    /// The change's own Build: its `commit` record is keyed `(build_id, change_id)` whichever Build reads
    /// it, a fabric.delete interrupting the change included.
    pub build_id: crate::BuildId,
}

/// The `commit` record of a storage change (storage.md §2 step 4, "Switch"): inserted for
/// `(build_id, change_id)` on the new provider with the store's insert-and-fail primitive. The insert
/// is the switch; whoever resumes the change learns the decision only by reading it: found is
/// switched, none on a provider that answers is not switched, and a provider that does not answer is
/// neither.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageCommit {
    /// The storage change's Build.
    pub build_id: crate::BuildId,
    /// The change.
    pub change_id: crate::ChangeId,
}

impl StorageCommit {
    /// The name the record is inserted under: `<build_id>.<change_id>`.
    pub fn name(&self) -> String {
        format!("{}.{}", self.build_id, self.change_id)
    }
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
        /// The executor the claim names (a node-admin's path.name).
        executor: String,
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
        hand_off_to: Option<String>,
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
    pub executor: Option<String>,
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
        operation: String,
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
        operation: String,
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
        operation: String,
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
        to: String,
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
        fabric_primary: Option<String>,
    },
    /// Refusal: the recipient is not the executor the claim names.
    ///
    /// Position 10 on the wire.
    NotExecutor {
        /// The executor the claim names.
        named: String,
        /// The recipient.
        recipient: String,
    },
    /// Refusal: the claim is not the current folded attempt of the Build.
    ///
    /// Position 11 on the wire.
    StaleClaim {
        /// The attempt the recipient's fold holds as current.
        held_attempt: u32,
        /// The executor of that attempt, when one is.
        held_executor: Option<String>,
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
        held_by: Option<String>,
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
        node: String,
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
        /// The admin (path.name) that holds the role now, when the receiver's view names one.
        fabric_primary: Option<String>,
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
        fabric_primary: Option<String>,
        /// The mesh's mesh-primary the receiver sees, when it sees one.
        mesh_primary: Option<String>,
    },
    /// A chunk of one Build's topology `GetBuilds` reads: its nodes' create specs, in `path.name`
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
        executor: Option<String>,
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
    pub fn progress_key(&self) -> Option<(&str, u32, &str, &str)> {
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

    /// A change at a reserved position (a removed change, [`BuildChange::is_reserved`]) is a variant
    /// this build does not know: refused as `UnknownVariant`, never a change.
    fn decode_request(bytes: &[u8]) -> Result<Self::Request, DecodeFailure> {
        match crate::protocol::decode_enum(bytes, Self::REQUEST_VARIANTS)? {
            BuildRequest::Create { submit: BuildSubmit::Change(change) } if change.is_reserved() => Err(DecodeFailure::UnknownVariant),
            request => Ok(request),
        }
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
    fn frame_kind(frame: &BuildReply) -> FrameKind {
        match frame {
            BuildReply::Started { .. } => FrameKind::Started,
            BuildReply::Step { .. } | BuildReply::Blocked { .. } | BuildReply::Steps { .. } | BuildReply::Topology { .. } | BuildReply::Build { .. } => FrameKind::Data,
            BuildReply::Complete { .. } | BuildReply::Failed { .. } | BuildReply::HandedOff { .. } | BuildReply::FabricPrimaryMoved { .. } | BuildReply::Got { .. } | BuildReply::Deleted { .. } | BuildReply::End { .. } => FrameKind::Terminal,
            other => FrameKind::Refusal(Build::classify_reply(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Binding;
    use crate::protocol::NodeProtocol;

    // @feature: node-replace
    /// CONTRACT: docs/architecture/lifecycles/node-replace.md:5
    /// "Replace is Rafka's steps, submitted as one Build. RDM executes its primitives and records their receipts"
    /// CONTRACT: docs/architecture/lifecycles/node-replace.md:30
    /// "1. node[1]: node.connection.delete `{ scope: All }`, then node.mesh.leave.
    ///  2. node.name.update of node[1] to `<name>.old` ... Then node[1].stop ... If no rollback is wanted, node[1].delete ...
    ///  3. node.create of node[2] at `<name>` ... The Build fixes the bin on its Create step"
    #[test]
    fn replace_step_primitives_preserve_wire_positions_and_reserved_changes_are_refused() {
        let name: PathName = "mesh1.rpc.1".parse().unwrap();
        let old_name = name.renamed();
        let binding = Binding {
            launch_id: "rpc_node".into(),
            executable: "/bin/rdm-rpc-node-v2".into(),
            sha256: "b".repeat(64),
            image: None,
        };
        let steps = vec![
            Step::DeleteConnections { node: name.clone() },
            Step::LeaveMesh { node: name.clone() },
            Step::Update {
                node: name.clone(),
                name: old_name.clone(),
            },
            Step::Stop {
                node: old_name.clone(),
            },
            Step::Create {
                node: name.clone(),
                executable: Some(binding),
            },
            Step::Start {
                node: old_name.clone(),
            },
            Step::Delete {
                node: old_name.clone(),
            },
        ];
        assert_eq!(steps[0].node(), &name);
        assert_eq!(steps[1].node(), &name);
        assert_eq!(steps[2].node(), &name);
        assert_eq!(steps[3].node(), &old_name);
        assert_eq!(steps[4].node(), &name);
        assert_eq!(steps[5].node(), &old_name);
        assert_eq!(steps[6].node(), &old_name);

        let req = BuildRequest::Create {
            submit: BuildSubmit::Change(BuildChange::Steps {
                steps: steps.clone(),
            }),
        };
        let bytes = Build::encode_request(&req).unwrap();
        let decoded = Build::decode_request(&bytes).unwrap();
        assert_eq!(decoded, req);

        for reserved in [
            BuildChange::Reserved0,
            BuildChange::Reserved1,
            BuildChange::Reserved2,
            BuildChange::Reserved4,
            BuildChange::Reserved6,
            BuildChange::Reserved14,
        ] {
            assert!(reserved.is_reserved());
            let raw = Build::encode_request(&BuildRequest::Create {
                submit: BuildSubmit::Change(reserved),
            })
            .unwrap();
            assert_eq!(
                Build::decode_request(&raw),
                Err(DecodeFailure::UnknownVariant)
            );
        }
    }
}
