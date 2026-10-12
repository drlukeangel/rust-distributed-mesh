//! A workflow's typed reply stream.
//!
//! A workflow (`node.create`, `node.stop`, `node.restart`, `node.delete`) is one call that RDM
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

use crate::names::{NodeEvent, NodeOp, NodeStep};
use crate::build_stream::{BuildFrame, BuildReceipts, BuildStream};
use crate::{Accepted, BuildId, BuildView, CallEnd, StepView};
use rafka_mesh_entity::{NodeKind, PathName};
use rafka_node_rpc_contract::build::{RecipeStep, StepResult};

/// A frame of a workflow's reply stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// The call was accepted: this Build and attempt run it.
    Started {
        /// The Build.
        build_id: BuildId,
        /// The attempt the call opened.
        attempt: u32,
    },
    /// A step completed.
    Event(NodeEvent),
    /// The step in flight is blocked. Live progress: never a step outcome, and not terminal.
    Blocked {
        /// The Build operation the blocked step belongs to.
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
    /// The fabric-primary seat moved while the workflow ran (a planned handover). Terminal for this
    /// stream, not a failure: the Build goes on at the new seat, and the caller resumes it there.
    SeatMoved {
        /// The admin (path.name) that holds the seat now, when the old seat's view named one.
        fabric_primary: Option<String>,
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
    /// `node.create`: the Build's `create-node:*` operation.
    Create,
    /// `node.stop` of this node: `stop-node:<path.name>`.
    Stop(PathName),
    /// `node.start` of this parked node: `start-node:<path.name>`.
    Start(PathName),
    /// `node.restart` of this node: `restart-node:<path.name>`, one operation.
    Restart(PathName),
    /// `node.delete` of this node: `retire-node:<path.name>`.
    Delete(PathName),
    /// A recipe: one Build of node primitives, each step its own op's operation, in order
    /// (`BuildChange::Recipe`; build.md §1, lifecycles/node-replace.md).
    Recipe(Vec<RecipeStep>),
}

/// One row of the mapping from a Build receipt to the canonical names.
struct Row {
    operation: &'static str,
    step: &'static str,
    node_step: NodeStep,
    /// The step events the step's `Complete` receipt announces, in order.
    events: &'static [NodeEvent],
    terminal: bool,
    /// The exact operation key the row belongs to, for a recipe's step: two steps of one recipe
    /// share an operation prefix (a stop of the predecessor and of the successor) and differ by path.
    key: Option<String>,
    /// A step of the operation's stop half, which node.delete of an already stopped or proven-exited
    /// birth does not run (node-delete.md: its stop steps are already done): absent while a later row
    /// of the same operation is receipted, it is passed, never waited for.
    stop_half: bool,
}

const fn row(operation: &'static str, step: &'static str, node_step: NodeStep, events: &'static [NodeEvent], terminal: bool) -> Row {
    Row { operation, step, node_step, events, terminal, key: None, stop_half: false }
}

impl Row {
    /// Whether the receipt key `key` is this row's operation in workflow `kind`.
    fn matches(&self, kind: &WorkflowKind, key: &str) -> bool {
        match &self.key {
            Some(k) => k == key,
            None => kind.operation_matches(self.operation, key),
        }
    }
}

/// The SeatTransfer row under `operation`, a row of the workflow only when `target` is a node-admin:
/// the pipeline moves a node-admin target's seats to the executor by TakeSeats before it drains or
/// stops (`seat_transfer_step`, decided by the target's kind); any other target has no such step,
/// and so its workflow has no such row.
fn seat_transfer_row(operation: &'static str, node_step: NodeStep, target: &PathName) -> Option<Row> {
    (target.kind == NodeKind::NodeAdmin).then(|| row(operation, "SeatTransfer", node_step, &[], false))
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

/// The start leg of a parked process under the operation prefix `op`: `start-node` answered
/// `Started` means the same process rejoined (`node.joined`), then the wait for it
/// to report hydrated (`node.started`); `last` is what the operation's `Complete` announces.
macro_rules! start_rows {
    ($op:literal, $last:expr, $terminal_step:expr) => {
        [
            row($op, "StartNode", NodeStep::Start, &[NodeEvent::Joined], false),
            row($op, "WaitForNodeReady", NodeStep::Start, &[NodeEvent::Started], false),
            row($op, "Complete", $terminal_step, $last, true),
        ]
    };
}

/// The shutdown leg of `target` under `operation`, from its drain to its `stopped` (the stop half of
/// a delete under `retire-node`, the stop of a restart under `restart-node`); its SeatTransfer row
/// exists for a node-admin target alone.
fn shutdown_rows(operation: &'static str, target: &PathName) -> Vec<Row> {
    let mut rows: Vec<Row> = seat_transfer_row(operation, NodeStep::Drain, target).into_iter().collect();
    rows.extend([
        row(operation, "DrainNode", NodeStep::Drain, &[NodeEvent::Draining], false),
        row(operation, "AwaitNodeDrained", NodeStep::Drain, &[NodeEvent::Drained], false),
        row(operation, "StopNode", NodeStep::Stop, &[NodeEvent::ConnectionsDeleted], false),
        row(operation, "AwaitNodeLeft", NodeStep::Stop, &[NodeEvent::Stopped], false),
    ]);
    rows
}

/// The rows of one recipe step, in pipeline order, each keyed to the step's exact operation. Each
/// step is its op's own workflow: the same rows that op's call streams.
fn recipe_step_rows(step: &RecipeStep) -> Vec<Row> {
    let mut rows: Vec<Row> = match step {
        RecipeStep::Create { .. } => create_rows!("create-node", &[NodeEvent::Created]).into(),
        RecipeStep::Stop { node } => stop_rows(node),
        RecipeStep::Update { .. } => vec![row("update-node", "UpdateNode", NodeStep::Update, &[], false), row("update-node", "Complete", NodeStep::Update, &[NodeEvent::Updated], false)],
        RecipeStep::Start { .. } => start_rows!("start-node", &[], NodeStep::Start).into(),
        // node.delete's own workflow, whatever the node's name: a stopped birth's runs no stop half.
        RecipeStep::Delete { node } => delete_rows(node),
    };
    for r in &mut rows {
        r.terminal = false;
        r.key = Some(format!("{}:{}", r.operation, step.node()));
    }
    rows
}

/// The workflow step a recipe step is, for a failure no row names.
fn recipe_node_step(step: &RecipeStep) -> NodeStep {
    match step {
        RecipeStep::Create { .. } => NodeStep::Create,
        RecipeStep::Stop { .. } => NodeStep::Stop,
        RecipeStep::Update { .. } => NodeStep::Update,
        RecipeStep::Start { .. } => NodeStep::Start,
        RecipeStep::Delete { .. } => NodeStep::Delete,
    }
}

/// The stop's rows under `stop-node`: its one call drains, cuts the mesh connections and answers
/// `stopped`, so its receipts announce all four events, and no runtime exits.
fn stop_rows(n: &PathName) -> Vec<Row> {
    let mut rows: Vec<Row> = seat_transfer_row("stop-node", NodeStep::Stop, n).into_iter().collect();
    rows.extend([
        row("stop-node", "StopNode", NodeStep::Stop, &[NodeEvent::Draining, NodeEvent::Drained, NodeEvent::ConnectionsDeleted], false),
        row("stop-node", "AwaitNodeLeft", NodeStep::Stop, &[NodeEvent::Stopped], false),
        row("stop-node", "Complete", NodeStep::Stop, &[], true),
    ]);
    rows
}

/// node.delete's rows under its one key, `retire-node`: a live birth's drains and stops first; an
/// already stopped or proven-exited birth's runs no stop half and leaves no membership to remove
/// (`stop_half`), the same operation.
fn delete_rows(n: &PathName) -> Vec<Row> {
    let mut rows = vec![row("retire-node", "NodeDeleting", NodeStep::Delete, &[], false)];
    rows.extend(shutdown_rows("retire-node", n).into_iter().map(|r| Row { stop_half: true, ..r }));
    rows.extend([
        row("retire-node", "TerminateRuntime", NodeStep::Stop, &[], false),
        row("retire-node", "NodeDeleted", NodeStep::Delete, &[NodeEvent::Deleted], false),
        Row { stop_half: true, ..row("retire-node", "RemoveTopologyMembership", NodeStep::Delete, &[], false) },
        row("retire-node", "Complete", NodeStep::Delete, &[], true),
    ]);
    rows
}

impl WorkflowKind {
    /// The node call this workflow is; `None` for a recipe, which is a Build of several.
    pub fn op(&self) -> Option<NodeOp> {
        Some(match self {
            Self::Create => NodeOp::Create,
            Self::Stop(_) => NodeOp::Stop,
            Self::Start(_) => NodeOp::Start,
            Self::Restart(_) => NodeOp::Restart,
            Self::Delete(_) => NodeOp::Delete,
            Self::Recipe(_) => return None,
        })
    }

    /// The span of the call: the node op's own, or for a recipe
    /// `rdm.node_admin.build.create.via-recipe` naming its steps.
    pub fn span(&self) -> tracing::Span {
        match (self.op(), self) {
            (Some(op), _) => op.span(),
            (None, Self::Recipe(steps)) => tracing::info_span!(
                "rdm.node_admin.build.create.via-recipe",
                op = "build.create",
                steps = %steps.iter().map(|s| format!("{}:{}", recipe_node_step(s).name(), s.node())).collect::<Vec<_>>().join(" "),
                outcome = tracing::field::Empty,
                failed_step = tracing::field::Empty,
                build_id = tracing::field::Empty,
                attempt = tracing::field::Empty,
            ),
            (None, _) => unreachable!("only a recipe names no node op"),
        }
    }

    /// The name the call is spanned and refused under.
    pub fn name(&self) -> &'static str {
        self.op().map(NodeOp::name).unwrap_or("build.create")
    }

    /// The pipeline rows of the workflow in order.
    fn rows(&self) -> Vec<Row> {
        match self {
            Self::Create => create_rows!("create-node", &[NodeEvent::Created]).into(),
            Self::Stop(n) => stop_rows(n),
            Self::Start(_) => start_rows!("start-node", &[], NodeStep::Start).into(),
            // A restart is one operation: the birth is held, drains and stops (it parks), then the
            // same process starts, and its one `Complete` ends it.
            Self::Restart(n) => {
                let mut rows = vec![row("restart-node", "NodeRestarting", NodeStep::Restart, &[NodeEvent::Restarting], false)];
                rows.extend(shutdown_rows("restart-node", n));
                rows.extend(start_rows!("restart-node", &[NodeEvent::Restarted], NodeStep::Restart));
                rows
            }
            Self::Delete(n) => delete_rows(n),
            // A recipe's rows are its steps' own, each keyed to its exact operation; only the last
            // row of the last step ends the workflow.
            Self::Recipe(steps) => {
                let mut rows: Vec<Row> = steps.iter().flat_map(recipe_step_rows).collect();
                if let Some(last) = rows.last_mut() {
                    last.terminal = true;
                }
                rows
            }
        }
    }

    /// The Build operation key a row's operation prefix names for this workflow.
    fn operation_matches(&self, prefix: &str, key: &str) -> bool {
        match self {
            Self::Create => prefix == "create-node" && key.starts_with("create-node:"),
            Self::Stop(n) | Self::Start(n) | Self::Restart(n) | Self::Delete(n) => key == format!("{prefix}:{n}"),
            Self::Recipe(steps) => steps.iter().any(|s| key == format!("{prefix}:{}", s.node())),
        }
    }

    /// The workflow step a failure of operation `key` names when no row does.
    fn fallback_step(&self, key: &str) -> NodeStep {
        match self {
            Self::Create => NodeStep::Create,
            Self::Stop(_) => NodeStep::Stop,
            Self::Start(_) => NodeStep::Start,
            Self::Restart(_) => NodeStep::Restart,
            Self::Delete(_) => NodeStep::Delete,
            Self::Recipe(steps) => steps.iter().find(|s| key.split_once(':').is_some_and(|(_, p)| p == s.node().to_string())).map(recipe_node_step).unwrap_or(NodeStep::Create),
        }
    }
}

/// What a Build's receipts say of one workflow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Folded {
    /// The frames the receipts justify, after `Started`, in order.
    pub frames: Vec<Frame>,
    /// The `(operation, step)` of every step with a `Complete` receipt, in pipeline order.
    pub completed: Vec<(String, String)>,
    /// The `(operation, step)` of the first step with no `Complete` receipt, when there is one.
    pub next: Option<(String, String)>,
}

/// Fold the receipts of `view` that attempts `from_attempt` and later wrote for `kind` into
/// frames. Per step the receipt of the latest attempt decides. A step whose latest receipt is
/// `Failed` ends the stream `Failed` only when an attempt this call opened wrote it; a receipt
/// no row names is refused by name.
pub fn fold(kind: &WorkflowKind, view: &BuildView, from_attempt: u32) -> Result<Folded, CallEnd> {
    fold_steps(kind, &view.steps, from_attempt)
}

/// [`fold`] over the receipts `build.get` read.
pub fn fold_receipts(kind: &WorkflowKind, receipts: &BuildReceipts, from_attempt: u32) -> Result<Folded, CallEnd> {
    let steps: Vec<StepView> = receipts
        .steps
        .iter()
        .map(|s| StepView {
            attempt: s.attempt,
            operation: s.operation.clone(),
            step: s.step.clone(),
            outcome: match &s.result {
                StepResult::Complete => serde_json::Value::String("complete".into()),
                StepResult::Failed { reason } => serde_json::json!({ "failed": { "reason": reason } }),
            },
        })
        .collect();
    fold_steps(kind, &steps, from_attempt)
}

/// [`fold`] over bare receipts.
pub fn fold_steps(kind: &WorkflowKind, steps: &[StepView], from_attempt: u32) -> Result<Folded, CallEnd> {
    let rows = kind.rows();
    // The latest receipt of each (operation, step) among the attempts this call opened.
    let mut latest: Vec<(String, String, u32, Option<String>)> = Vec::new();
    for s in steps.iter().filter(|s| s.attempt >= from_attempt) {
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
            Some(held) if held.2 > s.attempt => {}
            Some(held) => *held = (s.operation.clone(), s.step.clone(), s.attempt, failure),
            None => latest.push((s.operation.clone(), s.step.clone(), s.attempt, failure)),
        }
    }
    let mut frames = Vec::new();
    let mut completed = Vec::new();
    let mut next = None;
    for (i, r) in rows.iter().enumerate() {
        let held = latest.iter().find(|(op, st, ..)| r.matches(kind, op) && *st == r.step);
        // A stop-half row a stopped birth's node.delete never runs: passed once a later row of the
        // same operation is receipted.
        if held.is_none() && r.stop_half && rows[i + 1..].iter().any(|later| latest.iter().any(|(op, st, ..)| later.matches(kind, op) && r.matches(kind, op) && *st == later.step)) {
            continue;
        }
        match held {
            Some((op, st, _, None)) => {
                completed.push((op.clone(), st.clone()));
                frames.extend(r.events.iter().map(|e| Frame::Event(*e)));
                if r.terminal {
                    frames.push(Frame::Complete);
                    break;
                }
            }
            Some((op, st, _, Some(reason))) => {
                next.get_or_insert((op.clone(), st.clone()));
                frames.push(Frame::Failed { step: r.node_step, reason: reason.clone() });
                break;
            }
            None => {
                if next.is_none() {
                    next = Some((r.key.clone().unwrap_or_else(|| r.operation.to_string()), r.step.to_string()));
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
    /// The steps whose `Complete` receipt exists. A resume replays none of them.
    pub completed: Vec<(String, String)>,
    /// The first step with no `Complete` receipt: where the next attempt starts. `None` when
    /// every step is complete.
    pub from: Option<(String, String)>,
}

impl Resume {
    /// What `build.get`'s `view` says of `kind` for the call that opened `from_attempt`.
    pub fn from_view(kind: &WorkflowKind, view: &BuildView, from_attempt: u32) -> Result<Self, CallEnd> {
        Self::of(fold(kind, view, from_attempt)?)
    }

    /// What `build.get`'s `receipts` say of `kind` for the call that opened `from_attempt`.
    pub fn from_receipts(kind: &WorkflowKind, receipts: &BuildReceipts, from_attempt: u32) -> Result<Self, CallEnd> {
        Self::of(fold_receipts(kind, receipts, from_attempt)?)
    }

    fn of(f: Folded) -> Result<Self, CallEnd> {
        let from = f.frames.iter().all(|fr| !matches!(fr, Frame::Complete)).then_some(f.next).flatten();
        Ok(Self { completed: f.completed, from })
    }
}

/// The reply stream of one accepted workflow call.
pub struct WorkflowStream {
    inner: BuildStream,
    kind: WorkflowKind,
    accepted: Accepted,
    span: tracing::Span,
    started: bool,
    ended: bool,
    /// The events of one receipt after the first, delivered before the next frame is read.
    queued: std::collections::VecDeque<NodeEvent>,
    /// The `(operation, step)` of every step whose event this stream delivered. A step is one event
    /// to the caller whichever attempts completed it, as in [`fold`]: a successor attempt that
    /// reissues a step whose receipt the lost executor alone held completes it again under its own
    /// receipt key, and that frame announces nothing new.
    delivered: std::collections::HashSet<(String, String)>,
}

impl WorkflowStream {
    /// The stream of the accepted call `inner` follows; `span` is the call's span, opened when it
    /// was submitted and closed with the stream.
    pub(crate) fn new(inner: BuildStream, kind: WorkflowKind, span: tracing::Span) -> Self {
        let accepted = Accepted { build_id: inner.build_id().clone(), attempt: inner.attempt() };
        span.record("build_id", accepted.build_id.0.as_str());
        span.record("attempt", accepted.attempt);
        Self { inner, kind, accepted, span, started: false, ended: false, queued: Default::default(), delivered: Default::default() }
    }

    /// The Build and attempt the call opened.
    pub fn accepted(&self) -> &Accepted {
        &self.accepted
    }

    /// The workflow.
    pub fn kind(&self) -> &WorkflowKind {
        &self.kind
    }

    /// How the call came to its Build: a new one, an attach to a running one, the next attempt of a
    /// failed one, or a complete one (`AlreadyApplied`).
    pub fn disposition(&self) -> rafka_node_rpc_contract::build::Disposition {
        self.inner.disposition()
    }

    /// The next frame, `None` after the terminal one, or the way the stream broke. A broken stream
    /// is `Indeterminate`: the call was accepted, and a lost stream proves nothing about the step
    /// in flight.
    pub async fn next(&mut self) -> Option<Result<Frame, CallEnd>> {
        if self.ended {
            return None;
        }
        if !self.started {
            self.started = true;
            self.span.in_scope(|| tracing::info!(build_id = %self.accepted.build_id, attempt = self.accepted.attempt, "the workflow call was accepted"));
            return Some(Ok(Frame::Started { build_id: self.accepted.build_id.clone(), attempt: self.accepted.attempt }));
        }
        if let Some(e) = self.queued.pop_front() {
            e.span().in_scope(|| tracing::info!(build_id = %self.accepted.build_id, "a step event was delivered on the reply stream"));
            return Some(Ok(Frame::Event(e)));
        }
        let rows = self.kind.rows();
        loop {
            let frame = match self.inner.next().await {
                None => return Some(Err(self.broke(CallEnd::Indeterminate { reason: "the stream ended without a terminal frame".into() }))),
                Some(Err(e)) => return Some(Err(self.broke(e))),
                Some(Ok(f)) => f,
            };
            match frame {
                BuildFrame::Step { attempt, operation, step } => {
                    if attempt < self.accepted.attempt {
                        continue;
                    }
                    if !rows.iter().any(|r| r.matches(&self.kind, &operation)) {
                        continue;
                    }
                    let Some(row) = rows.iter().find(|r| r.matches(&self.kind, &operation) && r.step == step) else {
                        return Some(Err(self.broke(CallEnd::UnrecognisedReceipt { operation, step })));
                    };
                    if !self.delivered.insert((operation.clone(), step.clone())) {
                        self.span.in_scope(|| tracing::info!(attempt, operation = %operation, step = %step, "a later attempt completed a step whose event this stream delivered: one event per step"));
                        continue;
                    }
                    let mut events = row.events.iter().copied();
                    let Some(e) = events.next() else { continue };
                    self.queued.extend(events);
                    e.span().in_scope(|| tracing::info!(build_id = %self.accepted.build_id, "a step event was delivered on the reply stream"));
                    return Some(Ok(Frame::Event(e)));
                }
                BuildFrame::Blocked { attempt, operation, step, reason } => {
                    if attempt < self.accepted.attempt {
                        continue;
                    }
                    self.span.in_scope(|| tracing::info!(operation = %operation, step = %step, reason = %reason, "the step in flight is blocked"));
                    return Some(Ok(Frame::Blocked { operation, step, reason }));
                }
                BuildFrame::Complete { .. } => {
                    self.ended = true;
                    self.span.record("outcome", "complete");
                    self.span.in_scope(|| tracing::info!("the workflow completed"));
                    return Some(Ok(Frame::Complete));
                }
                BuildFrame::Failed { operation, step, reason, .. } => {
                    self.ended = true;
                    let node_step = rows.iter().find(|r| r.step == step && r.matches(&self.kind, &operation)).map(|r| r.node_step).unwrap_or_else(|| self.kind.fallback_step(&operation));
                    self.span.record("outcome", "failed");
                    self.span.record("failed_step", node_step.name());
                    self.span.in_scope(|| tracing::info!(step = node_step.name(), reason = %reason, "the workflow failed at a step"));
                    return Some(Ok(Frame::Failed { step: node_step, reason }));
                }
                BuildFrame::SeatMoved { fabric_primary } => {
                    self.ended = true;
                    self.span.record("outcome", "seat-moved");
                    self.span.in_scope(|| tracing::info!(fabric_primary = fabric_primary.as_deref().unwrap_or("none"), "the fabric-primary seat moved; the Build goes on at the new seat"));
                    return Some(Ok(Frame::SeatMoved { fabric_primary }));
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

