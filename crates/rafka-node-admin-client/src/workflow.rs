//! A workflow's typed reply stream, derived from the step receipts of the Build that runs it.
//!
//! A workflow (`node.create`, `node.stop`, `node.restart`, `node.delete`) is one call that RDM
//! runs as several steps inside one Build. The caller gets a stream: `Started`, then one frame
//! per step event as the Build records the step `Complete` (`node.created`, `node.joined`, …),
//! ending in `Complete` or a typed `Failed { step, reason }` that names the workflow step that
//! failed. [`fold`] is the one place a Build's receipt step keys meet the canonical names; the
//! keys themselves are untouched.
//!
//! The Build is read over the control API today, so the stream polls it; node-RPC carries no
//! Build op yet. A read that fails after the call was accepted ends the stream
//! `Indeterminate`: missing frames prove nothing about the step in flight. The caller then
//! re-attaches with `build.get` and [`Resume::from_view`] names the first step with no
//! `Complete` receipt.

use crate::names::{NodeEvent, NodeOp, NodeStep};
use crate::{Accepted, BuildId, BuildView, CallEnd, ClientError, NodeAdminClient};
use rafka_mesh_entity::PathName;
use std::collections::VecDeque;
use std::future::Future;
use std::time::Duration;

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
    event: Option<NodeEvent>,
    terminal: bool,
}

const fn row(operation: &'static str, step: &'static str, node_step: NodeStep, event: Option<NodeEvent>, terminal: bool) -> Row {
    Row { operation, step, node_step, event, terminal }
}

/// The create leg, in pipeline order, under the operation prefix `op`; `last` is what the
/// `Complete` receipt of that operation announces.
macro_rules! create_rows {
    ($op:literal, $last:expr) => {
        [
            row($op, "AllocateIdentity", NodeStep::Create, None, false),
            row($op, "PrepareStorage", NodeStep::Create, None, false),
            row($op, "PrepareNetwork", NodeStep::Create, None, false),
            row($op, "DeployRuntime", NodeStep::Create, None, false),
            row($op, "RegisterExactRuntimeHandle", NodeStep::Create, None, false),
            row($op, "ResolveProviderControlDomain", NodeStep::Create, None, false),
            row($op, "MakeRuntimeFactAvailableToBirth", NodeStep::Create, None, false),
            row($op, "WaitForBind", NodeStep::Create, Some(NodeEvent::Created), false),
            row($op, "PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata", NodeStep::Start, Some(NodeEvent::Started), false),
            row($op, "ApplyMeshPending", NodeStep::Start, None, false),
            row($op, "WaitForMeshJoin", NodeStep::Join, Some(NodeEvent::Joined), false),
            row($op, "WaitForNodeReady", NodeStep::Ready, Some(NodeEvent::Ready), false),
            row($op, "Complete", NodeStep::Create, $last, true),
        ]
    };
}

/// The retire leg under `retire-node` (a delete, or the first half of a restart).
macro_rules! retire_rows {
    ($op:literal, $first:expr, $first_step:expr, $last:expr, $last_step:expr, $terminal:expr) => {
        [
            row($op, "NodeDeleting", NodeStep::Delete, None, false),
            row($op, "NodeRestarting", $first_step, $first, false),
            row($op, "DrainNode", NodeStep::Drain, Some(NodeEvent::Draining), false),
            row($op, "AwaitNodeDrained", NodeStep::Drain, Some(NodeEvent::Drained), false),
            row($op, "StopNode", NodeStep::Stop, None, false),
            row($op, "AwaitNodeLeft", NodeStep::Stop, Some(NodeEvent::Left), false),
            row($op, "TerminateRuntime", NodeStep::Stop, Some(NodeEvent::Stopped), false),
            row($op, "NodeDeleted", NodeStep::Delete, Some(NodeEvent::Deleted), false),
            row($op, "ReleaseStorage", NodeStep::Delete, None, false),
            row($op, "RemoveTopologyMembership", $last_step, None, false),
            row($op, "Complete", $last_step, $last, $terminal),
        ]
    };
}

impl WorkflowKind {
    /// The node call this workflow is.
    pub fn op(&self) -> NodeOp {
        match self {
            Self::Create => NodeOp::Create,
            Self::Stop(_) => NodeOp::Stop,
            Self::Restart(_) => NodeOp::Restart,
            Self::Delete(_) => NodeOp::Delete,
        }
    }

    /// The pipeline rows of the workflow in order.
    fn rows(&self) -> Vec<Row> {
        match self {
            Self::Create => create_rows!("create-node", None).into(),
            Self::Stop(_) => vec![
                row("stop-node", "StopNode", NodeStep::Stop, None, false),
                row("stop-node", "AwaitNodeLeft", NodeStep::Stop, Some(NodeEvent::Left), false),
                row("stop-node", "TerminateRuntime", NodeStep::Stop, Some(NodeEvent::Stopped), false),
                row("stop-node", "Complete", NodeStep::Stop, None, true),
            ],
            Self::Restart(_) => {
                let mut rows: Vec<Row> = retire_rows!("retire-node", Some(NodeEvent::Restarting), NodeStep::Restart, None, NodeStep::Restart, false).into();
                rows.retain(|r| !matches!(r.step, "NodeDeleting" | "NodeDeleted" | "ReleaseStorage"));
                rows.extend(create_rows!("restart-node", Some(NodeEvent::Restarted)));
                rows
            }
            Self::Delete(_) => {
                let mut rows: Vec<Row> = retire_rows!("retire-node", None, NodeStep::Delete, None, NodeStep::Delete, true).into();
                rows.retain(|r| r.step != "NodeRestarting");
                rows
            }
        }
    }

    /// The Build operation key a row's operation prefix names for this workflow.
    fn operation_matches(&self, prefix: &str, key: &str) -> bool {
        match self {
            Self::Create => prefix == "create-node" && key.starts_with("create-node:"),
            Self::Stop(n) | Self::Restart(n) | Self::Delete(n) => key == format!("{prefix}:{n}"),
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
    if view.steps.is_empty() || from_attempt > 0 {
        // RED stub: the receipts are not read yet.
        return Ok(Folded { frames: Vec::new(), completed: Vec::new(), next: None });
    }
    let rows = kind.rows();
    let prefixes: Vec<&str> = {
        let mut p: Vec<&str> = rows.iter().map(|r| r.operation).collect();
        p.dedup();
        p
    };
    // The latest receipt of each (operation, step) among the attempts this call opened.
    let mut latest: Vec<(String, String, u32, Option<String>)> = Vec::new();
    for s in view.steps.iter().filter(|s| s.attempt >= from_attempt) {
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
                if let Some(e) = r.event {
                    frames.push(Frame::Event(e));
                }
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
        let f = fold(kind, view, from_attempt)?;
        let from = f.frames.iter().all(|fr| !matches!(fr, Frame::Complete)).then_some(f.next).flatten();
        Ok(Self { completed: f.completed, from })
    }
}

/// Where the Build state of a call is read from.
pub trait BuildSource {
    /// `build.get`.
    fn build_view(&self, id: &BuildId) -> impl Future<Output = Result<BuildView, ClientError>> + Send;
}

impl BuildSource for NodeAdminClient {
    fn build_view(&self, id: &BuildId) -> impl Future<Output = Result<BuildView, ClientError>> + Send {
        NodeAdminClient::build_view(self, id)
    }
}

/// The reply stream of one accepted workflow call.
pub struct WorkflowStream<S: BuildSource = NodeAdminClient> {
    source: S,
    kind: WorkflowKind,
    accepted: Accepted,
    poll: Duration,
    span: tracing::Span,
    started: bool,
    emitted: usize,
    queue: VecDeque<Frame>,
    ended: bool,
}

impl<S: BuildSource> WorkflowStream<S> {
    /// The stream of the accepted call `accepted`, reading the Build from `source` every `poll`.
    pub(crate) fn new(source: S, kind: WorkflowKind, accepted: Accepted, poll: Duration) -> Self {
        let span = kind.op().span();
        span.record("build_id", accepted.build_id.0.as_str());
        span.record("attempt", accepted.attempt);
        Self { source, kind, accepted, poll, span, started: false, emitted: 0, queue: VecDeque::new(), ended: false }
    }

    /// The Build and attempt the call opened.
    pub fn accepted(&self) -> &Accepted {
        &self.accepted
    }

    /// The workflow.
    pub fn kind(&self) -> &WorkflowKind {
        &self.kind
    }

    /// The next frame, `None` after the terminal one, or the way the stream broke. A broken stream
    /// is `Indeterminate`: the call was accepted, and a lost read proves nothing about the step in
    /// flight.
    pub async fn next(&mut self) -> Option<Result<Frame, CallEnd>> {
        if self.ended {
            return None;
        }
        if !self.started {
            self.started = true;
            self.span.in_scope(|| tracing::info!(build_id = %self.accepted.build_id, attempt = self.accepted.attempt, "the workflow call was accepted"));
            return Some(Ok(Frame::Started { build_id: self.accepted.build_id.clone(), attempt: self.accepted.attempt }));
        }
        loop {
            if let Some(f) = self.queue.pop_front() {
                if let Frame::Event(e) = &f {
                    e.span().in_scope(|| tracing::info!(build_id = %self.accepted.build_id, "a step event was delivered on the reply stream"));
                }
                if f.is_terminal() {
                    self.ended = true;
                    match &f {
                        Frame::Failed { step, reason } => {
                            self.span.record("outcome", "failed");
                            self.span.record("failed_step", step.name());
                            self.span.in_scope(|| tracing::info!(step = step.name(), reason = %reason, "the workflow failed at a step"));
                        }
                        _ => {
                            self.span.record("outcome", "complete");
                            self.span.in_scope(|| tracing::info!("the workflow completed"));
                        }
                    }
                }
                return Some(Ok(f));
            }
            let view = match self.source.build_view(&self.accepted.build_id).await {
                Ok(v) => v,
                Err(e) => return Some(Err(self.broke(CallEnd::from_read(e)))),
            };
            match fold(&self.kind, &view, self.accepted.attempt) {
                Ok(folded) => {
                    self.queue.extend(folded.frames.into_iter().skip(self.emitted));
                    self.emitted += self.queue.len();
                }
                Err(e) => return Some(Err(self.broke(e))),
            }
            if self.queue.is_empty() {
                tokio::time::sleep(self.poll).await;
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

impl CallEnd {
    /// A read of an accepted call's Build that failed: the call is committed, so a transport loss
    /// is `Indeterminate` whether or not the read's request was written.
    fn from_read(e: ClientError) -> Self {
        match CallEnd::from(e) {
            CallEnd::NotSent { reason } => CallEnd::Indeterminate { reason },
            other => other,
        }
    }
}
