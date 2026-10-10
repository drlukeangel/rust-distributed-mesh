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
use rafka_mesh_entity::PathName;
use rafka_node_rpc_contract::build::StepResult;

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
    /// `node.restart` of this node: `retire-node:<path.name>` then `restart-node:<path.name>`.
    Restart(PathName),
    /// `node.delete` of this node: `retire-node:<path.name>`.
    Delete(PathName),
}

/// One row of the mapping from a Build receipt to the canonical names.
struct Row {
    operation: &'static str,
    step: &'static str,
    node_step: NodeStep,
    /// The step events the step's `Complete` receipt announces, in order.
    events: &'static [NodeEvent],
    terminal: bool,
}

const fn row(operation: &'static str, step: &'static str, node_step: NodeStep, events: &'static [NodeEvent], terminal: bool) -> Row {
    Row { operation, step, node_step, events, terminal }
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

/// The retire leg under `retire-node` (a delete, or the first half of a restart): `park` is whether
/// the process stays (a restart) or the provider ends it (a delete).
macro_rules! retire_rows {
    ($first:expr, $first_step:expr) => {
        [
            row("retire-node", "NodeDeleting", NodeStep::Delete, &[], false),
            row("retire-node", "NodeRestarting", $first_step, $first, false),
            row("retire-node", "DrainNode", NodeStep::Drain, &[NodeEvent::Draining], false),
            row("retire-node", "AwaitNodeDrained", NodeStep::Drain, &[NodeEvent::Drained], false),
            row("retire-node", "StopNode", NodeStep::Stop, &[NodeEvent::ConnectionsDeleted], false),
            row("retire-node", "AwaitNodeLeft", NodeStep::Stop, &[NodeEvent::Stopped], false),
        ]
    };
}

impl WorkflowKind {
    /// The node call this workflow is.
    pub fn op(&self) -> NodeOp {
        match self {
            Self::Create => NodeOp::Create,
            Self::Stop(_) => NodeOp::Stop,
            Self::Start(_) => NodeOp::Start,
            Self::Restart(_) => NodeOp::Restart,
            Self::Delete(_) => NodeOp::Delete,
        }
    }

    /// The pipeline rows of the workflow in order.
    fn rows(&self) -> Vec<Row> {
        match self {
            Self::Create => create_rows!("create-node", &[NodeEvent::Created]).into(),
            // The stop's one call drains, cuts the mesh connections and answers `stopped`: its receipts
            // announce all four events, and no runtime exits.
            Self::Stop(_) => vec![
                row("stop-node", "StopNode", NodeStep::Stop, &[NodeEvent::Draining, NodeEvent::Drained, NodeEvent::ConnectionsDeleted], false),
                row("stop-node", "AwaitNodeLeft", NodeStep::Stop, &[NodeEvent::Stopped], false),
                row("stop-node", "Complete", NodeStep::Stop, &[], true),
            ],
            Self::Start(_) => start_rows!("start-node", &[], NodeStep::Start).into(),
            // A restart: the birth is held, drains and stops (it parks), then the same process starts.
            Self::Restart(_) => {
                let mut rows: Vec<Row> = retire_rows!(&[NodeEvent::Restarting], NodeStep::Restart).into();
                rows.retain(|r| r.step != "NodeDeleting");
                rows.push(row("retire-node", "Complete", NodeStep::Restart, &[], false));
                rows.extend(start_rows!("restart-node", &[NodeEvent::Restarted], NodeStep::Restart));
                rows
            }
            Self::Delete(_) => {
                let mut rows: Vec<Row> = retire_rows!(&[], NodeStep::Delete).into();
                rows.retain(|r| r.step != "NodeRestarting");
                rows.extend([
                    row("retire-node", "TerminateRuntime", NodeStep::Stop, &[], false),
                    row("retire-node", "NodeDeleted", NodeStep::Delete, &[NodeEvent::Deleted], false),
                    row("retire-node", "ReleaseStorage", NodeStep::Delete, &[], false),
                    row("retire-node", "RemoveTopologyMembership", NodeStep::Delete, &[], false),
                    row("retire-node", "Complete", NodeStep::Delete, &[], true),
                ]);
                rows
            }
        }
    }

    /// The Build operation key a row's operation prefix names for this workflow.
    fn operation_matches(&self, prefix: &str, key: &str) -> bool {
        match self {
            Self::Create => prefix == "create-node" && key.starts_with("create-node:"),
            Self::Stop(n) | Self::Start(n) | Self::Restart(n) | Self::Delete(n) => key == format!("{prefix}:{n}"),
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
    let prefixes: Vec<&str> = {
        let mut p: Vec<&str> = rows.iter().map(|r| r.operation).collect();
        p.dedup();
        p
    };
    // The latest receipt of each (operation, step) among the attempts this call opened.
    let mut latest: Vec<(String, String, u32, Option<String>)> = Vec::new();
    for s in steps.iter().filter(|s| s.attempt >= from_attempt) {
        let Some(prefix) = prefixes.iter().find(|p| kind.operation_matches(p, &s.operation)) else { continue };
        if !rows.iter().any(|r| r.operation == *prefix && r.step == s.step) {
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
    for r in &rows {
        let held = latest.iter().find(|(op, st, ..)| r.operation == op.split(':').next().unwrap_or_default() && *st == r.step);
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
                    next = Some((r.operation.to_string(), r.step.to_string()));
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
}

impl WorkflowStream {
    /// The stream of the accepted call `inner` follows; `span` is the call's span, opened when it
    /// was submitted and closed with the stream.
    pub(crate) fn new(inner: BuildStream, kind: WorkflowKind, span: tracing::Span) -> Self {
        let accepted = Accepted { build_id: inner.build_id().clone(), attempt: inner.attempt() };
        span.record("build_id", accepted.build_id.0.as_str());
        span.record("attempt", accepted.attempt);
        Self { inner, kind, accepted, span, started: false, ended: false }
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
                    let Some(prefix) = rows.iter().map(|r| r.operation).find(|p| self.kind.operation_matches(p, &operation)) else { continue };
                    let Some(row) = rows.iter().find(|r| r.operation == prefix && r.step == step) else {
                        return Some(Err(self.broke(CallEnd::UnrecognisedReceipt { operation, step })));
                    };
                    let Some(e) = row.event else { continue };
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
                    let node_step = rows
                        .iter()
                        .find(|r| r.step == step && operation.split(':').next() == Some(r.operation))
                        .map(|r| r.node_step)
                        .unwrap_or(match self.kind {
                            WorkflowKind::Create => NodeStep::Create,
                            WorkflowKind::Stop(_) => NodeStep::Stop,
                            WorkflowKind::Restart(_) => NodeStep::Restart,
                            WorkflowKind::Delete(_) => NodeStep::Delete,
                        });
                    self.span.record("outcome", "failed");
                    self.span.record("failed_step", node_step.name());
                    self.span.in_scope(|| tracing::info!(step = node_step.name(), reason = %reason, "the workflow failed at a step"));
                    return Some(Ok(Frame::Failed { step: node_step, reason }));
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

