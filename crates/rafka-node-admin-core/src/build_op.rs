//! The Build family on op `0x20` as a node-admin serves and calls it (node-rpc-envelope.md "Build,
//! op `0x20`, rdm"; R-ON8).
//!
//! The wire door is a thin decoder: `build.create` hands the change to the control plane's own cores
//! (`ControlPlane::submit_in` and `open_attempt_in`, the ones the control routes use) and then reads
//! the Build's [`Drive`](crate::build_drive::Drive); `build.get` and `build.delete` read and forget
//! through the same Build state; `build.attempt.run` is the executor's [`RunDoor`]. Only the
//! fabric-primary accepts the first three; any other admin refuses by name and names the one it sees.
//!
//! This module also holds the two callers of the family inside an admin: the fabric-primary's
//! [`RpcDispatcher`] (`build.attempt.run` to an executor, in process when the executor is itself) and
//! the intent a call carries (the Build's acceptance and opened attempts), which the executor absorbs
//! before it plans.

use crate::accepted::TopologyChange;
use crate::build::{BuildId, FabricDesired, MeshDesired};
use crate::build_drive::{Dispatched, Dispatcher, Drive, DriveEnv, Drives, InnerItem, InnerStream, Read, VerdictSink};
use crate::build_run::{bounded_reason, Attached, RunDoor, RunReader};
use crate::build_state::{AttemptReason, BuildFact, BuildState, BuildStateError, LocalBuildLog};
use crate::http::{ActionKind, ControlPlane, Refusal};
use crate::model::PathName;
use crate::topology::Topology;
use rafka_node_rpc::stream::{NotStarted, ReplySink, StreamItem};
use rafka_node_rpc::{Budget, CallOptions, HandlerFault, NodeRpcClient, NodeTarget, ServerBuilder};
use rafka_node_rpc_contract::build::{Build, BuildChange, BuildPhase, BuildReply, BuildRequest, BuildSubmit, Disposition, MeshCounts, StepReceipt, StepResult};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::context::CallContext;
use rafka_node_rpc_contract::outcome::RpcOutcome;
use std::collections::VecDeque;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::Instrument as _;

/// What an admin answers a Build call from.
pub struct BuildDoor {
    /// This admin's `path.name`.
    pub me: PathName,
    /// The control plane: the Build state, the accepted pointer and the cores that accept a change.
    pub control: Arc<ControlPlane>,
    /// The drives this admin holds.
    pub drives: Arc<Drives>,
    /// What the fabric-primary drives a Build with.
    pub env: Arc<DriveEnv>,
    /// The executor's door for `build.attempt.run`.
    pub run: Arc<RunDoor>,
}

/// The door a running admin fills once it holds a view.
pub type BuildSlot = Arc<OnceLock<Arc<BuildDoor>>>;

fn counts(m: &MeshCounts) -> MeshDesired {
    MeshDesired { name: m.name.clone(), node_admin: m.node_admin, rpc_node: m.rpc_node, broker: m.broker, gateway: m.gateway, compute: m.compute }
}

/// The refusal reply a control-plane refusal is, with every non-leaking detail it carries.
pub fn refusal_reply(r: Refusal) -> BuildReply {
    match r {
        Refusal::Reject(r) => BuildReply::Rejected { reason: r.reason().to_string(), detail: r.to_string() },
        Refusal::BadRequest(d) => BuildReply::Rejected { reason: "invalid-request".into(), detail: d },
        Refusal::NotFound(d) => BuildReply::Rejected { reason: "not-found".into(), detail: d },
        Refusal::Conflict(d) => BuildReply::Rejected { reason: "conflict".into(), detail: d },
        Refusal::BuildInProgress(id) => BuildReply::BuildInProgress { current_build_id: id.0 },
        Refusal::NotAuthority(p) => BuildReply::NotFabricPrimary { fabric_primary: p },
        Refusal::AttemptTaken(d) => BuildReply::AttemptTaken { detail: d },
        Refusal::Unavailable(d) => BuildReply::NotReady { reason: d },
        Refusal::State(BuildStateError::Fenced { node, by }) => BuildReply::Fenced { node, by },
        Refusal::State(e) => BuildReply::NotReady { reason: format!("the Build state: {e}") },
    }
}

impl BuildDoor {
    /// A change a caller submitted: a new Build or the next attempt of the accepted one.
    async fn accept(&self, change: &BuildChange) -> Result<crate::http::Opened, BuildReply> {
        let cp = &self.control;
        let node_span = |node: &PathName| tracing::info_span!("rdm.node_admin.build.update.via-node-rpc", route = "build.create", build_id = tracing::field::Empty, attempt = tracing::field::Empty, node = %node);
        let created = |change: &TopologyChange| {
            tracing::info_span!(
                "rdm.node_admin.build.create.via-node-rpc",
                route = "build.create",
                build_id = tracing::field::Empty,
                change = %serde_json::to_string(change).unwrap_or_default(),
                previous_build_id = tracing::field::Empty,
            )
        };
        let topology = |c: TopologyChange| async move {
            let span = created(&c);
            cp.submit_in(span, "build.create", c).await
        };
        let r = match change {
            BuildChange::ReconcileFabric { fabric, meshes } => topology(TopologyChange::ReconcileFabric { desired: FabricDesired { fabric: fabric.clone(), meshes: meshes.iter().map(counts).collect() } }).await,
            BuildChange::ReconcileMesh { mesh } => topology(TopologyChange::ReconcileMesh { desired: counts(mesh) }).await,
            BuildChange::AddNode { mesh, node_kind } => topology(TopologyChange::AddNode { mesh: mesh.clone(), node_kind: *node_kind }).await,
            BuildChange::RemoveNode { node } => topology(TopologyChange::RemoveNode { node: node.clone() }).await,
            BuildChange::CreateMesh { mesh } => topology(TopologyChange::CreateMesh { desired: counts(mesh) }).await,
            BuildChange::RemoveMesh { mesh } => topology(TopologyChange::RemoveMesh { mesh: mesh.clone() }).await,
            BuildChange::Restart { node } => cp.open_attempt_in(node_span(node), "build.create", AttemptReason::Restart, node.clone(), ActionKind::Restart, None).await,
            BuildChange::Replace { node, incarnation } => {
                cp.open_attempt_in(node_span(node), "build.create", AttemptReason::Replace, node.clone(), ActionKind::Replace, incarnation.clone()).await
            }
            BuildChange::Drain { node } => cp.open_attempt_in(node_span(node), "build.create", AttemptReason::Drain, node.clone(), ActionKind::Drain, None).await,
            BuildChange::Stop { node } => cp.open_attempt_in(node_span(node), "build.create", AttemptReason::Stop, node.clone(), ActionKind::Stop, None).await,
        };
        r.map_err(refusal_reply)
    }

    /// Only the fabric-primary accepts a Build call; any other admin names the one it sees.
    async fn not_fabric_primary(&self) -> Option<BuildReply> {
        let seat = self.control.topology.read().await.fabric_primary().map(|n| n.name.clone());
        match seat {
            Some(n) if n.to_string() == self.me.to_string() => None,
            other => Some(BuildReply::NotFabricPrimary { fabric_primary: other.map(|n| n.to_string()) }),
        }
    }

    /// Answer one Build call.
    pub async fn serve(&self, req: BuildRequest, sink: ReplySink<Build, NotStarted>) -> Result<BuildReply, HandlerFault> {
        let span = tracing::info_span!("rdm.node_admin.build.serve.via-node-rpc", node = %self.me, call = req.op(), outcome = tracing::field::Empty);
        let call = req.op();
        let reply = async {
            match req {
                BuildRequest::AttemptRun { build_id, attempt, executor, context, intent } => self.attempt_run(BuildId(build_id), attempt, executor, context, intent, sink).await,
                other => {
                    if let Some(refusal) = self.not_fabric_primary().await {
                        return Ok(refusal);
                    }
                    match other {
                        BuildRequest::Create { submit } => self.create(submit, sink).await,
                        BuildRequest::Get { build_id } => self.get(BuildId(build_id), sink).await,
                        BuildRequest::Delete { build_id } => self.delete(BuildId(build_id)).await,
                        BuildRequest::AttemptRun { .. } => unreachable!("handled above"),
                    }
                }
            }
        }
        .instrument(span.clone())
        .await;
        span.record("outcome", match &reply {
            Ok(r) => r.name(),
            Err(_) => "fault",
        });
        span.in_scope(|| tracing::info!(call, "a Build call ended"));
        reply
    }

    async fn create(&self, submit: BuildSubmit, sink: ReplySink<Build, NotStarted>) -> Result<BuildReply, HandlerFault> {
        match submit {
            BuildSubmit::Change(change) => {
                let opened = match self.accept(&change).await {
                    Ok(o) => o,
                    Err(refusal) => return Ok(refusal),
                };
                let drive = self.drives.ensure(&self.env, &opened.build_id);
                let sink = started(sink, BuildReply::Started { build_id: opened.build_id.0.clone(), attempt: opened.attempt, disposition: Disposition::Created }).await?;
                stream_frames(sink, drive.reader(opened.attempt)).await
            }
            BuildSubmit::Resubmit { build_id, from_attempt } => self.resubmit(BuildId(build_id), from_attempt, sink).await,
        }
    }

    /// A re-submit by build id never creates a second Build: a complete Build answers
    /// `AlreadyApplied` and its terminal; a running one with a holder streams that holder's run; a
    /// failed one, or one whose holder was lost, takes its next attempt.
    async fn resubmit(&self, id: BuildId, from_attempt: u32, sink: ReplySink<Build, NotStarted>) -> Result<BuildReply, HandlerFault> {
        let p = match self.control.builds.read_build(&id).await {
            Ok(p) => p,
            Err(BuildStateError::UnknownBuild(_)) => return Ok(BuildReply::UnknownBuild { build_id: id.0 }),
            Err(e) => return Ok(BuildReply::NotReady { reason: format!("{}: Build {id} could not be read: {e}", self.me) }),
        };
        if p.state == BuildState::Complete {
            let facts = self.control.builds.facts().await.map_err(|e| HandlerFault::invariant_broken(format!("{}: the facts of Build {id}: {e}", self.me)))?;
            let mut sink = started(sink, BuildReply::Started { build_id: id.0.clone(), attempt: from_attempt, disposition: Disposition::AlreadyApplied }).await?;
            let mut sent = Vec::new();
            for f in facts.iter().filter(|f| f.build_id() == &id) {
                if let BuildFact::Step(s) = f {
                    if s.attempt >= from_attempt && s.outcome == crate::build_state::StepOutcome::Complete {
                        let frame = BuildReply::Step { build_id: id.0.clone(), attempt: s.attempt, operation: s.operation.clone(), step: s.step.clone() };
                        if !sent.contains(&frame) {
                            sink.data(frame.clone()).await.map_err(gone)?;
                            sent.push(frame);
                        }
                    }
                }
            }
            return Ok(BuildReply::Complete { build_id: id.0, attempt: p.attempt });
        }
        if self.control.accepted.build_id().await.as_ref() != Some(&id) {
            return Ok(BuildReply::Rejected { reason: "not-accepted".into(), detail: format!("Build {id} is {:?} and is not the accepted Build (Fabric.build_id): nothing drives it", p.state) });
        }
        let disposition = if p.state == BuildState::Failed { Disposition::NextAttempt } else { Disposition::Attached };
        let drive = self.drives.ensure(&self.env, &id);
        // A drive this admin took up after a seat move holds no frame of the attempts before it: the
        // steps the receipts say completed are read, from the attempt the caller opened on.
        drive.replay_receipts(&p.steps, from_attempt, p.attempt + 1);
        let sink = started(sink, BuildReply::Started { build_id: id.0.clone(), attempt: from_attempt, disposition }).await?;
        stream_frames(sink, drive.reader(from_attempt)).await
    }

    async fn get(&self, id: BuildId, sink: ReplySink<Build, NotStarted>) -> Result<BuildReply, HandlerFault> {
        let p = match self.control.builds.read_build(&id).await {
            Ok(p) => p,
            Err(BuildStateError::UnknownBuild(_)) => return Ok(BuildReply::UnknownBuild { build_id: id.0 }),
            Err(e) => return Ok(BuildReply::NotReady { reason: format!("{}: Build {id} could not be read: {e}", self.me) }),
        };
        let steps: Vec<StepReceipt> = p
            .steps
            .iter()
            .map(|s| StepReceipt {
                attempt: s.attempt,
                operation: s.operation.clone(),
                step: s.step.clone(),
                result: match &s.outcome {
                    crate::build_state::StepOutcome::Complete => StepResult::Complete,
                    crate::build_state::StepOutcome::Failed { reason } => StepResult::Failed { reason: bounded_reason(reason.clone()) },
                },
            })
            .collect();
        // Chunks that each fit the reply frame with room to spare.
        let mut chunks: Vec<Vec<StepReceipt>> = vec![Vec::new()];
        let mut size = 0usize;
        for s in steps.iter().cloned() {
            let weight = 32 + s.operation.len() + s.step.len() + match &s.result {
                StepResult::Complete => 0,
                StepResult::Failed { reason } => reason.len(),
            };
            if size + weight > 32 * 1024 && !chunks.last().is_some_and(Vec::is_empty) {
                chunks.push(Vec::new());
                size = 0;
            }
            size += weight;
            chunks.last_mut().expect("a chunk").push(s);
        }
        let count = chunks.len() as u32;
        let mut sink = started(sink, BuildReply::Started { build_id: id.0.clone(), attempt: p.attempt, disposition: Disposition::Attached }).await?;
        for (i, chunk) in chunks.into_iter().enumerate() {
            sink.data(BuildReply::Steps { build_id: id.0.clone(), chunk_index: i as u32, chunk_count: count, steps: chunk }).await.map_err(gone)?;
        }
        Ok(BuildReply::Got {
            build_id: id.0,
            phase: match p.state {
                BuildState::Pending => BuildPhase::Pending,
                BuildState::Running => BuildPhase::Running,
                BuildState::Complete => BuildPhase::Complete,
                BuildState::Failed => BuildPhase::Failed,
            },
            attempt: p.attempt,
            executor: p.executor.clone(),
            reason: p.reason.as_str().to_string(),
            last_failure: p.last_failure.clone(),
            steps: steps.len() as u32,
            chunks: count,
        })
    }

    async fn delete(&self, id: BuildId) -> Result<BuildReply, HandlerFault> {
        Ok(match self.control.forget_build(&id).await {
            Ok(()) => BuildReply::Deleted { build_id: id.0 },
            Err(Refusal::NotFound(_)) => BuildReply::UnknownBuild { build_id: id.0 },
            Err(Refusal::Conflict(reason)) => BuildReply::CannotDelete { build_id: id.0, reason },
            Err(other) => refusal_reply(other),
        })
    }

    async fn attempt_run(&self, id: BuildId, attempt: u32, executor: String, context: CallContext, intent: Vec<Vec<u8>>, sink: ReplySink<Build, NotStarted>) -> Result<BuildReply, HandlerFault> {
        match self.run.attempt_run(&id, attempt, &executor, &context, &intent).await {
            Attached::Refused(r) => Ok(r),
            Attached::Ended { replay, terminal } => {
                let mut sink = started(sink, BuildReply::Started { build_id: id.0.clone(), attempt, disposition: Disposition::Reattached }).await?;
                for f in replay {
                    sink.data(f).await.map_err(gone)?;
                }
                Ok(terminal)
            }
            Attached::Live { disposition, replay, mut reader } => {
                let mut sink = started(sink, BuildReply::Started { build_id: id.0.clone(), attempt, disposition }).await?;
                for f in replay {
                    sink.data(f).await.map_err(gone)?;
                }
                // A cut call ends this stream and nothing else: the run is its own task.
                while let Some(f) = reader.next().await {
                    match f {
                        BuildReply::Step { .. } | BuildReply::Blocked { .. } => sink.data(f).await.map_err(gone)?,
                        terminal => return Ok(terminal),
                    }
                }
                Err(HandlerFault::invariant_broken(format!("{}: the run of attempt {attempt} of Build {id} ended without a terminal", self.me)))
            }
        }
    }
}

fn gone(e: rafka_node_rpc::stream::SinkError) -> HandlerFault {
    HandlerFault::invariant_broken(format!("the caller left the stream, and the Build's drive goes on without it: {e:?}"))
}

async fn started(sink: ReplySink<Build, NotStarted>, frame: BuildReply) -> Result<ReplySink<Build, rafka_node_rpc::stream::Streaming>, HandlerFault> {
    sink.started(frame).await.map_err(gone)
}

/// Read a drive's frames to the caller until its terminal. A seat that moved ends the stream
/// without a terminal: the caller sees it end `Indeterminate` and re-submits by build id.
async fn stream_frames(mut sink: ReplySink<Build, rafka_node_rpc::stream::Streaming>, mut reader: crate::build_drive::DriveReader) -> Result<BuildReply, HandlerFault> {
    while let Some(t) = reader.next().await {
        match t {
            Read::Frame(f) => sink.data(f).await.map_err(gone)?,
            Read::Terminal(t) => return Ok(t),
            Read::SeatLost(why) => return Err(HandlerFault::invariant_broken(format!("the fabric-primary seat moved off this admin; re-submit the Build by its id at the new seat: {why}"))),
        }
    }
    Err(HandlerFault::invariant_broken("the drive's frames ended without a terminal"))
}

/// Serve the Build family on this admin. Until `slot` is filled the admin holds no view and every
/// call is `NotReady` by name.
pub fn serve(b: ServerBuilder, slot: BuildSlot) -> ServerBuilder {
    b.serve_stream::<Build, _, _>(OpOwner::Product("rdm".into()), move |_peer, req: BuildRequest, sink| {
        let slot = slot.clone();
        async move {
            let Some(door) = slot.get().cloned() else {
                return Ok(BuildReply::NotReady { reason: "this admin holds no view yet".into() });
            };
            door.serve(req, sink).await
        }
    })
}

/// What a run held in this process came to, as a dispatch: the fabric-primary running an attempt
/// itself, or a fixture routing to an executor's door.
pub fn local_dispatched(attached: Attached) -> Dispatched {
    match attached {
        Attached::Refused(r) => Dispatched::Refused(r),
        Attached::Ended { replay, terminal } => Dispatched::Stream(Box::new(LocalStream { replay: replay.into(), reader: None, terminal: Some(terminal) })),
        Attached::Live { replay, reader, .. } => Dispatched::Stream(Box::new(LocalStream { replay: replay.into(), reader: Some(reader), terminal: None })),
    }
}

/// The frames of a run held in this process.
struct LocalStream {
    replay: VecDeque<BuildReply>,
    reader: Option<RunReader>,
    terminal: Option<BuildReply>,
}

#[async_trait::async_trait]
impl InnerStream for LocalStream {
    async fn next(&mut self) -> InnerItem {
        if let Some(f) = self.replay.pop_front() {
            return InnerItem::Frame(f);
        }
        if let Some(t) = self.terminal.take() {
            return InnerItem::Frame(t);
        }
        match &mut self.reader {
            Some(reader) => match reader.next().await {
                Some(f) => InnerItem::Frame(f),
                None => InnerItem::Ended,
            },
            None => InnerItem::Ended,
        }
    }
}

/// The frames of a `build.attempt.run` stream from another admin.
struct RemoteStream {
    stream: rafka_node_rpc::stream::ReplyStream<Build>,
}

#[async_trait::async_trait]
impl InnerStream for RemoteStream {
    async fn next(&mut self) -> InnerItem {
        loop {
            match self.stream.next().await {
                Some(StreamItem::Frame(_, BuildReply::Started { .. })) => continue,
                Some(StreamItem::Frame(_, f)) => return InnerItem::Frame(f),
                Some(StreamItem::Failed(f)) => return InnerItem::Broke(format!("{f:?}")),
                None => return InnerItem::Ended,
            }
        }
    }
}

/// Calls the executor `executor` names: in process when that is this admin, over Node RPC otherwise.
pub struct RpcDispatcher {
    /// This admin's `path.name`.
    pub me: PathName,
    /// This admin's own executor door.
    pub local: Arc<RunDoor>,
    /// The client a call to another admin is made through.
    pub client: Arc<NodeRpcClient>,
    /// The observed topology: `executor`'s node id is taken from it.
    pub topology: Arc<RwLock<Topology>>,
}

/// The bound on connecting to an executor and writing its request. The reply has none: a run lasts
/// as long as its steps, and the transport's own liveness (idle, keepalive) ends a dead one.
pub const DISPATCH_SEND: Duration = Duration::from_secs(10);

#[async_trait::async_trait]
impl Dispatcher for RpcDispatcher {
    async fn dispatch(&self, executor: &PathName, build_id: &BuildId, attempt: u32, context: CallContext, intent: Vec<Vec<u8>>) -> Dispatched {
        if *executor == self.me {
            // The fabric-primary runs the attempt itself: it plans from the log the claim was decided on.
            return local_dispatched(self.local.attempt_run(build_id, attempt, &executor.to_string(), &context, &[]).await);
        }
        let node_id = self.topology.read().await.members().find(|n| n.name == *executor).map(|n| n.node_id.clone());
        let Some(node_id) = node_id else {
            return Dispatched::Unreached(format!("{executor} is not a node of {}'s view", self.me));
        };
        let req = BuildRequest::AttemptRun { build_id: build_id.0.clone(), attempt, executor: executor.to_string(), context: context.clone(), intent };
        let opts = CallOptions { budget: Budget::Stream { send: DISPATCH_SEND }, context: Some(context), ..CallOptions::default() };
        match self.client.call_stream::<Build>(&NodeTarget::ExactNode(node_id), &req, &opts).await {
            Ok((stream, _)) => Dispatched::Stream(Box::new(RemoteStream { stream })),
            Err((RpcOutcome::Reply(r), _)) => Dispatched::Refused(r.value().clone()),
            Err((RpcOutcome::Unserved(u), _)) => Dispatched::Refused(BuildReply::NotReady { reason: format!("{executor} does not serve the Build family: {u:?}") }),
            Err((other, _)) => Dispatched::Unreached(format!("the call to {executor} ended {}: {other:?}", other.name())),
        }
    }
}

/// Records an executor's verdict on this admin's own log without gossiping it again.
pub struct LocalVerdicts {
    /// This admin's own log.
    pub local: Arc<dyn LocalBuildLog>,
}

#[async_trait::async_trait]
impl VerdictSink for LocalVerdicts {
    async fn record(&self, receipt: crate::build_state::BuildAttemptReceipt) {
        self.local.absorb_facts(&[BuildFact::Attempt(receipt)]).await;
    }
}

/// The drive of `build_id`, for the reconcile loop: `ensure` it when it is the accepted Build.
pub async fn ensure_active(drives: &Arc<Drives>, env: &Arc<DriveEnv>) -> Vec<Arc<Drive>> {
    let active = match env.builds.list_active().await {
        Ok(a) => a,
        Err(e) => {
            tracing::info!(error = %e, "Build state unreadable; nothing driven");
            return Vec::new();
        }
    };
    let accepted = env.accepted.build_id().await;
    active.into_iter().filter(|b| accepted.as_ref() == Some(&b.build_id)).map(|b| drives.ensure(env, &b.build_id)).collect()
}
