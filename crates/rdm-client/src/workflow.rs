//! A workflow's typed reply stream.
//!
//! A workflow (`node.create`, `node.stop`, `node.start`, `node.delete`) is one call that RDM
//! runs as several steps inside one Build. The caller gets a stream read from the fabric-primary's
//! Build family (op `0x20`, `build.create`): `Started`, then one frame per step event as the
//! step's `Complete` receipt becomes durable on the executor that wrote it (`node.created`,
//! `node.joined`, ...), then `Complete` or a typed `Failed { step, reason }` that names the
//! workflow step that failed. A `Blocked` frame is live progress and no step outcome. The caller
//! reads no Build and no gossip: nothing on the path to a frame polls or sleeps.
//!
//! The commit cut: a submit that fails before its connection is made provably reached no handler
//! and ends `NotSent`; once the request was written, a lost stream is `Indeterminate`, whether or
//! not a frame had arrived, because missing frames prove nothing about the step in flight. The
//! caller then re-submits by the Build id ([`crate::Nodes::resume`]): the Build is never created
//! twice, a complete Build answers `AlreadyApplied` with its frames and terminal, and a running
//! one streams from where it is.
//!
//! [`fold`] is the one place a Build's receipt step keys meet the canonical names; the keys
//! themselves are untouched. It maps the receipts `build.get` reads to the frames they justify,
//! for a caller that reattaches by reading rather than by re-submitting.

use rdm_core::names::{NodeEvent, NodeOp, NodeStep};
use crate::build_stream::{Build, BuildFrame, BuildStream};
use crate::{Accepted, BuildId, BuildView, CallEnd, StepView};
use rdm_core::PathName;
use rdm_core::build::StepResult;

/// A frame of a workflow's reply stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// The call was accepted: this Build runs it.
    Started {
        /// The Build.
        build_id: BuildId,
    },
    /// A step completed.
    Event(NodeEvent),
    /// The step in flight is blocked. Live progress: never a step outcome, and not terminal.
    Blocked {
        /// The Build operation the blocked step belongs to (`noun.verb:<node_id>`).
        operation: String,
        /// The blocked step.
        step: String,
        /// What it waits for.
        reason: String,
    },
    /// Every step completed. The terminal frame of a successful workflow.
    Complete,
    /// A step failed. The terminal frame of a failed workflow; nothing downstream of the step
    /// was emitted.
    Failed {
        /// The workflow step that failed.
        step: NodeStep,
        /// The step's own reason.
        reason: String,
    },
    /// The fabric-primary moved while the workflow ran (`node-rpc-envelope.md` "Frames, durable events and reattachment"). Terminal for this
    /// stream, not a failure: the Build goes on at the new fabric-primary, and the caller resumes it there.
    FabricPrimaryMoved {
        /// The node-admin (`(path, name)`) that is the fabric-primary now, when the old fabric-primary's view named one.
        fabric_primary: Option<rdm_core::NodeId>,
    },
}

impl Frame {
    /// Whether no frame follows this one.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Complete | Self::Failed { .. })
    }
}

/// Which workflow a stream belongs to, and so which Build operations carry its steps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowKind {
    /// `node.create`: the Build's `node.create:*` operation.
    Create,
    /// `node.stop` of this node: `node.stop:<path and name>`.
    Stop(PathName),
    /// `node.start` of this stopped node: `node.start:<path and name>`.
    Start(PathName),
    /// `node.delete` of these nodes, by node.id: each `node.delete:<node.id>`.
    Delete(Vec<rdm_core::NodeId>),
    /// `node.connection.delete { All }` of this node: `node.connection.delete:<path and name>`. The
    /// process keeps running.
    DeleteConnections(PathName),
}

/// One row of the mapping from a Build receipt to the canonical names.
struct Row {
    operation: &'static str,
    step: &'static str,
    node_step: NodeStep,
    /// The step events the step's `Complete` receipt announces, in order.
    events: &'static [NodeEvent],
    terminal: bool,
    /// A step of the operation's stop half, which node.delete of an already stopped or proven-exited
    /// birth does not run (node-delete.md: its stop steps are already done): absent while a later row
    /// of the same operation is receipted, it is passed, never waited for.
    stop_half: bool,
}

const fn row(operation: &'static str, step: &'static str, node_step: NodeStep, events: &'static [NodeEvent], terminal: bool) -> Row {
    Row { operation, step, node_step, events, terminal, stop_half: false }
}

impl Row {
    /// Whether the step's operation `key` is this row's operation in workflow `kind`.
    fn matches(&self, kind: &WorkflowKind, key: &str) -> bool {
        kind.operation_matches(self.operation, key)
    }
}

/// The create leg, in pipeline order, under the operation prefix `op`; `last` is what the
/// `Complete` receipt of that operation announces.
macro_rules! create_rows {
    ($op:literal, $last:expr) => {
        [
            row($op, "AllocateIdentity", NodeStep::Create, &[], false),
            row($op, "PrepareStorage", NodeStep::Create, &[], false),
            row($op, "PrepareNetwork", NodeStep::Create, &[], false),
            row($op, "DeployRuntime", NodeStep::Create, &[], false),
            row($op, "RegisterExactRuntimeHandle", NodeStep::Create, &[], false),
            row($op, "ResolveProviderControlDomain", NodeStep::Create, &[], false),
            row($op, "MakeRuntimeFactAvailableToBirth", NodeStep::Create, &[], false),
            row($op, "WaitForBind", NodeStep::Create, &[], false),
            row($op, "PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata", NodeStep::Start, &[], false),
            row($op, "ApplyMeshPending", NodeStep::Start, &[], false),
            row($op, "WaitForMeshJoin", NodeStep::Join, &[NodeEvent::Joined], false),
            row($op, "WaitForNodeReady", NodeStep::Start, &[NodeEvent::Started], false),
            row($op, "Complete", NodeStep::Create, $last, true),
        ]
    };
}

/// The start leg of a stopped process under the operation prefix `op`: `start-node` answered
/// `Started` means the same process rejoined (`node.joined`), then the wait for it
/// to report its hook passed (`node.started`); `last` is what the operation's `Complete` announces.
macro_rules! start_rows {
    ($op:literal, $last:expr, $terminal_step:expr) => {
        [
            row($op, "StartNode", NodeStep::Start, &[NodeEvent::Joined], false),
            row($op, "WaitForNodeReady", NodeStep::Start, &[NodeEvent::Started], false),
            row($op, "Complete", $terminal_step, $last, true),
        ]
    };
}

/// The shutdown leg of `target` under `operation`: its stop call, whose one reply carries its
/// `node.connection.delete { All }` receipt, to its `stopped` (the stop half of a delete under
/// `node.delete`). A primary's role moves by
/// `node.primary.update` before the operation is dispatched, never as a step of it (`lifecycles/node-primary-update.md` §1), so
/// a node-admin target's rows are any target's.
fn shutdown_rows(operation: &'static str) -> Vec<Row> {
    vec![
        row(operation, "StopNode", NodeStep::Stop, &[NodeEvent::ConnectionsDeleted], false),
        row(operation, "AwaitNodeLeft", NodeStep::Stop, &[NodeEvent::Stopped], false),
    ]
}

/// The stop's rows under `stop-node`: its one call deletes the node's connections, cuts its
/// transport and answers `stopped`: its receipts announce both events, and no runtime exits.
fn stop_rows() -> Vec<Row> {
    vec![
        row("node.stop", "StopNode", NodeStep::Stop, &[NodeEvent::ConnectionsDeleted], false),
        row("node.stop", "AwaitNodeLeft", NodeStep::Stop, &[NodeEvent::Stopped], false),
        row("node.stop", "Complete", NodeStep::Stop, &[], true),
    ]
}

/// `node.delete`'s rows (`lifecycles/node-delete.md` steps 1-4): a live birth deletes its connections and stops first; an
/// already stopped or exited birth runs no stop half and leaves no membership to remove
/// (`stop_half`), the same operation.
fn delete_rows() -> Vec<Row> {
    let mut rows = vec![row("node.delete", "NodeDeleting", NodeStep::Delete, &[], false)];
    rows.extend(shutdown_rows("node.delete").into_iter().map(|r| Row { stop_half: true, ..r }));
    rows.extend([
        row("node.delete", "TerminateRuntime", NodeStep::Stop, &[], false),
        row("node.delete", "NodeDeleted", NodeStep::Delete, &[NodeEvent::Deleted], false),
        Row { stop_half: true, ..row("node.delete", "RemoveTopologyMembership", NodeStep::Delete, &[], false) },
        row("node.delete", "Complete", NodeStep::Delete, &[], true),
    ]);
    rows
}

impl WorkflowKind {
    /// The node call this workflow is.
    pub fn op(&self) -> NodeOp {
        match self {
            Self::Create => NodeOp::Create,
            Self::Stop(_) => NodeOp::Stop,
            Self::Start(_) => NodeOp::Start,
            Self::Delete(_) => NodeOp::Delete,
            Self::DeleteConnections(_) => NodeOp::ConnectionsDelete,
        }
    }

    /// The span of the call: the node op's own.
    pub fn span(&self) -> tracing::Span {
        self.op().span()
    }

    /// The name the call is spanned and refused under.
    pub fn name(&self) -> &'static str {
        self.op().name()
    }

    /// The pipeline rows of the workflow in order.
    fn rows(&self) -> Vec<Row> {
        match self {
            Self::Create => create_rows!("node.create", &[NodeEvent::Created]).into(),
            Self::Stop(_) => stop_rows(),
            Self::Start(_) => start_rows!("node.start", &[], NodeStep::Start).into(),
            // A standalone node.connection.delete { All }: one call to the exact birth through its
            // mesh-primary, and its `Complete`. Nothing stops the process and nothing is deleted.
            Self::DeleteConnections(_) => vec![
                row("node.connection.delete", "DeleteConnections", NodeStep::ConnectionsDelete, &[NodeEvent::ConnectionsDeleted], false),
                row("node.connection.delete", "Complete", NodeStep::ConnectionsDelete, &[], true),
            ],
            Self::Delete(_) => delete_rows(),
        }
    }

    /// The Build operation key a row's operation prefix names for this workflow.
    fn operation_matches(&self, prefix: &str, key: &str) -> bool {
        let (action, subject) = match key.split_once(':') {
            Some((action, subject)) => (action, Some(subject)),
            None => (key, None),
        };
        match self {
            Self::Create => prefix == "node.create" && action == prefix,
            Self::Stop(_) | Self::Start(_) | Self::DeleteConnections(_) => action == prefix && subject.is_some(),
            Self::Delete(ids) => action == prefix && subject.is_some_and(|s| ids.iter().any(|id| id.as_str() == s)),
        }
    }

    /// The workflow step a failure names when no row does.
    fn fallback_step(&self) -> NodeStep {
        match self {
            Self::Create => NodeStep::Create,
            Self::Stop(_) => NodeStep::Stop,
            Self::Start(_) => NodeStep::Start,
            Self::Delete(_) => NodeStep::Delete,
            Self::DeleteConnections(_) => NodeStep::ConnectionsDelete,
        }
    }
}

/// What a Build's step events say of one workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Folded {
    /// The frames the events justify, after `Started`, in order.
    pub frames: Vec<Frame>,
    /// The `(operation, step)` of every step whose event ended `complete`, in pipeline order.
    pub completed: Vec<(String, String)>,
    /// The `(operation, step)` of the first step with no `complete` event, when there is one.
    pub next: Option<(String, String)>,
}

/// Fold the step events of `view` for `kind` into frames. Per step the latest event decides; a
/// step whose latest event is `failed` ends the stream `Failed`; an event no row names is refused by
/// name.
pub fn fold(kind: &WorkflowKind, view: &BuildView) -> Result<Folded, CallEnd> {
    fold_steps(kind, &view.steps)
}

/// [`fold`] over the events `build.get` read.
pub fn fold_receipts(kind: &WorkflowKind, build: &Build) -> Result<Folded, CallEnd> {
    fold_steps(kind, &steps_of(build))
}

/// The `rdm.build.step` events of `build`, in event.id order, as step views.
pub fn steps_of(build: &Build) -> Vec<StepView> {
    build
        .events
        .iter()
        .filter(|e| e.get("type").and_then(|t| t.as_str()) == Some("rdm.build.step"))
        .filter_map(|e| {
            let data = e.get("data")?;
            let field = |k: &str| data.get(k).and_then(|v| v.as_str()).map(str::to_string);
            Some(StepView { mesh: field("mesh")?, operation: field("op")?, step: field("step")?, outcome: data.get("state")?.clone() })
        })
        .collect()
}

/// [`fold`] over bare step views.
pub fn fold_steps(kind: &WorkflowKind, steps: &[StepView]) -> Result<Folded, CallEnd> {
    let rows = kind.rows();
    // The latest event of each (operation, step).
    let mut latest: Vec<(String, String, Option<String>)> = Vec::new();
    for s in steps {
        if !rows.iter().any(|r| r.matches(kind, &s.operation)) {
            continue;
        }
        if !rows.iter().any(|r| r.matches(kind, &s.operation) && r.step == s.step) {
            return Err(CallEnd::UnrecognisedReceipt { operation: s.operation.clone(), step: s.step.clone() });
        }
        let failure = match &s.outcome {
            serde_json::Value::String(o) if o == "complete" => None,
            serde_json::Value::Object(o) => match o.get("failed") {
                Some(f) => Some(f.get("reason").and_then(|r| r.as_str()).unwrap_or_default().to_string()),
                None => return Err(CallEnd::UnrecognisedReceipt { operation: s.operation.clone(), step: format!("{}: outcome {}", s.step, s.outcome) }),
            },
            other => return Err(CallEnd::UnrecognisedReceipt { operation: s.operation.clone(), step: format!("{}: outcome {other}", s.step) }),
        };
        match latest.iter_mut().find(|(op, st, ..)| *op == s.operation && *st == s.step) {
            Some(held) => *held = (s.operation.clone(), s.step.clone(), failure),
            None => latest.push((s.operation.clone(), s.step.clone(), failure)),
        }
    }
    // One workflow row belongs to one admitted operation. Never combine events
    // from distinct node IDs or other operation scopes.
    for row in &rows {
        let mut matching = latest.iter().filter(|(operation, ..)| row.matches(kind, operation));
        if let Some((first, ..)) = matching.next() {
            if let Some((other, step, ..)) = matching.find(|(operation, ..)| operation != first) {
                return Err(CallEnd::UnrecognisedReceipt { operation: other.clone(), step: step.clone() });
            }
        }
    }
    let mut frames = Vec::new();
    let mut completed = Vec::new();
    let mut next = None;
    for (i, r) in rows.iter().enumerate() {
        let held = latest.iter().find(|(op, st, ..)| r.matches(kind, op) && *st == r.step);
        // A stop-half row a stopped birth's node.delete never runs: passed once a later row of the
        // same operation is receipted and no row of its stop half is (that delete ran none of it). A
        // delete whose stop half ran has every row of it receipted: none is inferred from its absence.
        let receipted = |row: &Row| latest.iter().any(|(op, st, ..)| row.matches(kind, op) && r.matches(kind, op) && *st == row.step);
        let same_op = |row: &Row| row.operation == r.operation;
        if held.is_none() && r.stop_half && !rows.iter().any(|o| o.stop_half && same_op(o) && receipted(o)) && rows[i + 1..].iter().any(|later| same_op(later) && receipted(later)) {
            continue;
        }
        match held {
            Some((op, st, None)) => {
                completed.push((op.clone(), st.clone()));
                frames.extend(r.events.iter().map(|e| Frame::Event(*e)));
                if r.terminal {
                    frames.push(Frame::Complete);
                    break;
                }
            }
            Some((op, st, Some(reason))) => {
                next.get_or_insert((op.clone(), st.clone()));
                frames.push(Frame::Failed { step: r.node_step, reason: reason.clone() });
                break;
            }
            None => {
                if next.is_none() {
                    let operation = latest.iter().find(|(operation, ..)| r.matches(kind, operation))
                        .map(|(operation, ..)| operation.clone())
                        .unwrap_or_else(|| r.operation.to_string());
                    next = Some((operation, r.step.to_string()));
                }
                break;
            }
        }
    }
    Ok(Folded { frames, completed, next })
}

/// Where a caller that lost a stream picks the workflow up again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resume {
    /// The steps whose event ended `complete`. A resume replays none of them.
    pub completed: Vec<(String, String)>,
    /// The first step with no `complete` event: where the run goes on. `None` when every step is
    /// complete.
    pub from: Option<(String, String)>,
}

impl Resume {
    /// What `build.get`'s `view` says of `kind`.
    pub fn from_view(kind: &WorkflowKind, view: &BuildView) -> Result<Self, CallEnd> {
        Self::of(fold(kind, view)?)
    }

    /// What the events `build.get` read say of `kind`.
    pub fn from_receipts(kind: &WorkflowKind, build: &Build) -> Result<Self, CallEnd> {
        Self::of(fold_receipts(kind, build)?)
    }

    fn of(f: Folded) -> Result<Self, CallEnd> {
        let from = f.frames.iter().all(|fr| !matches!(fr, Frame::Complete)).then_some(f.next).flatten();
        Ok(Self { completed: f.completed, from })
    }
}

/// A workflow's reply stream: the frames of a Build (`node.create`, `node.delete`, steps), of a node
/// command the primary above the node runs (`node.stop`, `node.start`: NodeCommand 0x22, D40), or the
/// one answer of a `node.connection.delete` (its Status hop), each read as the same [`Frame`]s.
pub struct WorkflowStream {
    source: Source,
    kind: WorkflowKind,
    span: tracing::Span,
    started: bool,
    ended: bool,
    /// The frames of one receipt or command frame after the first, delivered before the next is read.
    queued: std::collections::VecDeque<Frame>,
    /// The `(operation, step)` of every step whose event this stream delivered. A step is one event
    /// to the caller, as in [`fold`]: a new mesh-primary that runs again a step whose event the lost
    /// one never wrote announces nothing new.
    delivered: std::collections::HashSet<(String, String)>,
    /// A node command's terminal frame from the primary above the node, once delivered.
    completion: Option<rdm_core::node_command::NodeCommandReply>,
}

/// Where a workflow's frames come from.
enum Source {
    /// The Build the call opened.
    Build { inner: BuildStream, accepted: Accepted },
    /// The node command's reply stream from the primary above the node (D40): it belongs to no Build.
    /// `first` is its first frame, read when the call was made, not yet delivered.
    Command { inner: rdm_transport::stream::ReplyStream<rdm_core::node_command::NodeCommand>, first: Option<rdm_core::node_command::NodeCommandReply> },
    /// The call's one answer, already read into the queued frames.
    Answered,
}

impl WorkflowStream {
    /// The stream of the accepted call `inner` follows; `span` is the call's span, opened when it
    /// was submitted and closed with the stream.
    pub(crate) fn new(inner: BuildStream, kind: WorkflowKind, span: tracing::Span) -> Self {
        let accepted = Accepted { build_id: inner.build_id().clone() };
        span.record("build_id", accepted.build_id.as_str());
        Self { source: Source::Build { inner, accepted }, kind, span, started: false, ended: false, queued: Default::default(), delivered: Default::default(), completion: None }
    }

    /// The stream of the node command `inner` the primary above the node accepted (NodeCommand 0x22,
    /// D40): no Build, so no `Started` frame; its frames as the same events, ending `Complete` or
    /// `Failed { step, reason }`.
    pub(crate) fn of_command(inner: rdm_transport::stream::ReplyStream<rdm_core::node_command::NodeCommand>, first: rdm_core::node_command::NodeCommandReply, kind: WorkflowKind, span: tracing::Span) -> Self {
        Self { source: Source::Command { inner, first: Some(first) }, kind, span, started: true, ended: false, queued: Default::default(), delivered: Default::default(), completion: None }
    }

    /// The stream of a call already answered whole: `frames`, in order, the last its terminal.
    pub(crate) fn answered(frames: Vec<Frame>, kind: WorkflowKind, span: tracing::Span) -> Self {
        Self { source: Source::Answered, kind, span, started: true, ended: false, queued: frames.into(), delivered: Default::default(), completion: None }
    }

    /// The Build the call opened; `None` for a node command, which belongs to no Build.
    pub fn accepted(&self) -> Option<&Accepted> {
        match &self.source {
            Source::Build { accepted, .. } => Some(accepted),
            Source::Command { .. } | Source::Answered => None,
        }
    }

    /// A node command's terminal frame as the primary above the node sent it (for a connections delete,
    /// its `ConnectionsDeleted`: the ids closed, not open and held, and `All`'s receipt), once the
    /// stream delivered its terminal; `None` before, and for a Build.
    pub fn completion(&self) -> Option<&rdm_core::node_command::NodeCommandReply> {
        self.completion.as_ref()
    }

    /// The workflow.
    pub fn kind(&self) -> &WorkflowKind {
        &self.kind
    }

    /// How the call came to its Build: a new one, an attach to a running one, a failed one's remaining
    /// steps dispatched again, or a complete one (`AlreadyApplied`); `None` for a node command.
    pub fn disposition(&self) -> Option<rdm_core::build::Disposition> {
        match &self.source {
            Source::Build { inner, .. } => Some(inner.disposition()),
            Source::Command { .. } | Source::Answered => None,
        }
    }

    /// The next frame, `None` after the terminal one, or the way the stream broke. A broken stream
    /// is `Indeterminate`: the call was accepted, and a lost stream proves nothing about the step
    /// in flight.
    pub async fn next(&mut self) -> Option<Result<Frame, CallEnd>> {
        if self.ended {
            return None;
        }
        if let Some(f) = self.queued.pop_front() {
            return Some(Ok(self.deliver(f)));
        }
        match &self.source {
            Source::Build { .. } => self.next_of_build().await,
            Source::Command { .. } => self.next_of_command().await,
            Source::Answered => Some(Err(self.broke(CallEnd::Indeterminate { code: rdm_core::outcome::Code::Unknown, reason: "the answer carried no terminal frame".into() }))),
        }
    }

    /// `frame` delivered: its event spanned, its terminal recorded on the call's span.
    fn deliver(&mut self, frame: Frame) -> Frame {
        match &frame {
            Frame::Event(e) => e.span().in_scope(|| tracing::info!(op = self.kind.name(), "a step event was delivered on the reply stream")),
            Frame::Complete => {
                self.ended = true;
                self.span.record("outcome", "complete");
                self.span.in_scope(|| tracing::info!("the workflow completed"));
            }
            Frame::Failed { step, reason } => {
                self.ended = true;
                self.span.record("outcome", "failed");
                self.span.record("failed_step", step.name());
                self.span.in_scope(|| tracing::info!(step = step.name(), reason = %reason, "the workflow failed at a step"));
            }
            _ => {}
        }
        frame
    }

    /// The next frame of the node command's stream: the node's frames, sent on by the primary above
    /// it, as the events they report (node-stop.md, node-start.md).
    async fn next_of_command(&mut self) -> Option<Result<Frame, CallEnd>> {
        use rdm_core::node_command::NodeCommandReply as R;
        use rdm_transport::stream::StreamItem;
        loop {
            let Source::Command { inner, first } = &mut self.source else { unreachable!("a command source") };
            let frame = match first.take() {
                Some(f) => f,
                None => {
                    let f = match inner.next().await {
                        None => return Some(Err(self.broke(CallEnd::Indeterminate { code: rdm_core::outcome::Code::Unknown, reason: "the node command's stream ended without a terminal frame".into() }))),
                        Some(StreamItem::Failed(f)) => return Some(Err(self.broke(CallEnd::Indeterminate { code: f.code(), reason: format!("the node command's stream broke: {f:?}") }))),
                        Some(StreamItem::Frame(_, f)) => f,
                    };
                    inner.accepted().await;
                    f
                }
            };
            if <rdm_core::node_command::NodeCommand as rdm_core::streaming::StreamingProtocol>::frame_kind(&frame) == rdm_core::streaming::FrameKind::Terminal {
                self.completion = Some(frame.clone());
            }
            let frames: Vec<Frame> = match frame {
                R::CommandAccepted { .. } | R::ConnectionsDeleting { .. } | R::Leaving { .. } | R::StopMeshLeft { .. } => continue,
                R::StopConnectionsDeleted { .. } => vec![Frame::Event(NodeEvent::ConnectionsDeleted)],
                // A standalone connections delete's terminal (node-connections-delete.md "Completion").
                R::ConnectionsDeleted { .. } => vec![Frame::Event(NodeEvent::ConnectionsDeleted), Frame::Complete],
                R::NodeStopped => vec![Frame::Event(NodeEvent::Stopped), Frame::Complete],
                R::NodeJoined => vec![Frame::Event(NodeEvent::Joined)],
                R::NodeStarted => vec![Frame::Event(NodeEvent::Started), Frame::Complete],
                R::Failed { step, reason, .. } => vec![Frame::Failed { step: self.kind.fallback_step(), reason: format!("{step}: {reason}") }],
                other => return Some(Err(self.broke(CallEnd::Rejected { reason: other.name().into(), detail: format!("{} after the command was accepted: {other:?}", self.kind.name()) }))),
            };
            self.queued.extend(frames);
            let first = self.queued.pop_front().expect("a frame");
            return Some(Ok(self.deliver(first)));
        }
    }

    /// The next frame of the Build's stream.
    async fn next_of_build(&mut self) -> Option<Result<Frame, CallEnd>> {
        let accepted = match &self.source {
            Source::Build { accepted, .. } => accepted.clone(),
            _ => unreachable!("a Build source"),
        };
        if !self.started {
            self.started = true;
            self.span.in_scope(|| tracing::info!(build_id = %accepted.build_id.log(), "the workflow call was accepted"));
            return Some(Ok(Frame::Started { build_id: accepted.build_id.clone() }));
        }
        let rows = self.kind.rows();
        loop {
            let Source::Build { inner, .. } = &mut self.source else { unreachable!("a Build source") };
            let frame = match inner.next().await {
                None => return Some(Err(self.broke(CallEnd::Indeterminate { code: rdm_core::outcome::Code::Unknown, reason: "the stream ended without a terminal frame".into() }))),
                Some(Err(e)) => return Some(Err(self.broke(e))),
                Some(Ok(f)) => f,
            };
            match frame {
                BuildFrame::Step { operation, step, result, .. } => {
                    // A failed step's event is followed by the Build's own `Failed` terminal.
                    if result != StepResult::Complete {
                        continue;
                    }
                    if !rows.iter().any(|r| r.matches(&self.kind, &operation)) {
                        continue;
                    }
                    let Some(row) = rows.iter().find(|r| r.matches(&self.kind, &operation) && r.step == step) else {
                        return Some(Err(self.broke(CallEnd::UnrecognisedReceipt { operation, step })));
                    };
                    if !self.delivered.insert((operation.clone(), step.clone())) {
                        self.span.in_scope(|| tracing::info!(operation = %operation, step = %step, "a step whose event this stream delivered completed again: one event per step"));
                        continue;
                    }
                    let mut events = row.events.iter().copied();
                    let Some(e) = events.next() else { continue };
                    self.queued.extend(events.map(Frame::Event));
                    return Some(Ok(self.deliver(Frame::Event(e))));
                }
                BuildFrame::Blocked { operation, step, reason, .. } => {
                    self.span.in_scope(|| tracing::info!(operation = %operation, step = %step, reason = %reason, "the step in flight is blocked"));
                    return Some(Ok(Frame::Blocked { operation, step, reason }));
                }
                BuildFrame::Complete => return Some(Ok(self.deliver(Frame::Complete))),
                BuildFrame::Failed { reason, .. } => {
                    return Some(Ok(self.deliver(Frame::Failed { step: self.kind.fallback_step(), reason })));
                }
                // The workflow's Build ran and its section ended short: the workflow failed at that
                // section's step (Q346).
                BuildFrame::Incomplete { sections } => {
                    let Some(u) = sections.into_iter().next() else {
                        return Some(Ok(self.deliver(Frame::Failed { step: self.kind.fallback_step(), reason: "the Build ended incomplete naming no section".into() })));
                    };
                    let node_step = rows.iter().find(|r| r.step == u.step && r.matches(&self.kind, &u.operation)).map(|r| r.node_step).unwrap_or_else(|| self.kind.fallback_step());
                    return Some(Ok(self.deliver(Frame::Failed { step: node_step, reason: u.reason })));
                }
                BuildFrame::FabricPrimaryMoved { fabric_primary } => {
                    self.ended = true;
                    self.span.record("outcome", "fabric-primary-moved");
                    self.span.in_scope(|| tracing::info!(fabric_primary = fabric_primary.as_ref().map(|id| id.as_str()).unwrap_or("none"), "the fabric-primary role moved; the Build goes on at the new fabric-primary"));
                    return Some(Ok(Frame::FabricPrimaryMoved { fabric_primary }));
                }
                other => return Some(Err(self.broke(CallEnd::Rejected { reason: "unexpected-frame".into(), detail: format!("a workflow stream carried {other:?}") }))),
            }
        }
    }

    fn broke(&mut self, end: CallEnd) -> CallEnd {
        self.ended = true;
        self.span.record("outcome", end.outcome());
        self.span.in_scope(|| tracing::info!(reason = %end, "the reply stream broke; the step in flight is not known to have failed"));
        end
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::build::BuildPhase;

    fn view(operation: &str, step: &str, outcome: serde_json::Value) -> StepView {
        StepView { mesh: "mesh1".into(), operation: operation.into(), step: step.into(), outcome }
    }

    /// CONTRACT: build.md §5: a step's `rdm.build.step` event names its mesh, op, step and state
    /// (`complete` or `failed { reason, code }`); a client folds a workflow's frames from those events,
    /// the latest event of a step deciding, and refuses a step no row of the workflow names.
    #[test]
    fn fold_steps_and_fold_receipts_map_step_events_to_workflow_frames_and_refuse_unknown_steps() {
        let node: PathName = rdm_core::PathName::new("mesh1.node", "1").unwrap();
        let kind = WorkflowKind::Start(node.clone());
        let op = rdm_core::OperationId::node("node.start", &rdm_core::fixture_node_id(&node)).to_string();
        let complete = || serde_json::Value::String("complete".into());

        // 1. `fold_steps` folds complete events into workflow `Frame`s.
        let steps = vec![view(&op, "StartNode", complete()), view(&op, "WaitForNodeReady", complete()), view(&op, "Complete", complete())];
        let folded = fold_steps(&kind, &steps).unwrap();
        assert_eq!(folded.frames, vec![Frame::Event(NodeEvent::Joined), Frame::Event(NodeEvent::Started), Frame::Complete]);
        assert_eq!(folded.completed.len(), 3);
        assert_eq!(folded.next, None);

        // 2. `fold_receipts` reads a `build.get` Build's step events (`complete` and `failed`).
        let event = |step: &str, state: serde_json::Value| serde_json::json!({"type": "rdm.build.step", "data": {"build_id": "b", "mesh": "mesh1", "op": op, "step": step, "state": state}});
        let build = Build {
            build_id: BuildId::mint(),
            phase: BuildPhase::Failed,
            current: false,
            nodes: Vec::new(),
            events: vec![
                serde_json::json!({"type": "rdm.build.started", "data": {"build_id": "b"}}),
                event("StartNode", complete()),
                event("WaitForNodeReady", serde_json::json!({"failed": {"reason": "hook refused", "code": "FAILED_PRECONDITION"}})),
            ],
        };
        let folded_failed = fold_receipts(&kind, &build).unwrap();
        assert_eq!(folded_failed.frames, vec![Frame::Event(NodeEvent::Joined), Frame::Failed { step: NodeStep::Start, reason: "hook refused".into() }]);

        // 3. An unrecognised step name for a matching operation is refused with `UnrecognisedReceipt`.
        assert!(matches!(fold_steps(&kind, &[view(&op, "NoSuchStep", complete())]), Err(CallEnd::UnrecognisedReceipt { .. })));
    }

    /// CONTRACT: a workflow folds the events of the node.ids it names only, and a step's latest event
    /// decides it: a step that failed and then completed is complete.
    #[test]
    fn resume_reads_the_named_nodes_events_and_the_latest_event_of_each_step() {
        let deleted = rdm_core::NodeId::mint();
        let kind = WorkflowKind::Delete(vec![deleted.clone()]);
        let operation = rdm_core::OperationId::node("node.delete", &deleted).to_string();
        let first = view(&operation, "NodeDeleting", serde_json::json!("complete"));
        assert_eq!(fold_steps(&kind, &[first.clone()]).unwrap().next, Some((operation.clone(), "StopNode".into())));
        // An event of a node the workflow does not name is not this workflow's: it folds as if absent.
        let other = view(&rdm_core::OperationId::node("node.delete", &rdm_core::NodeId::mint()).to_string(), "NodeDeleting", serde_json::json!("complete"));
        assert_eq!(fold_steps(&kind, &[first.clone(), other.clone()]).unwrap().next, Some((operation.clone(), "StopNode".into())));
        let mut failed = first.clone();
        failed.outcome = serde_json::json!({"failed": {"reason": "try failed"}});
        let latest = fold_steps(&kind, &[failed, first]).unwrap();
        assert_eq!(latest.next, Some((operation, "StopNode".into())));
        assert!(latest.frames.is_empty());
    }
}
