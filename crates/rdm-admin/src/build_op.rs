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
use crate::build::{BuildId, BuildOperation, FabricDesired, MeshDesired};
use crate::executor::RunPlan;
use crate::build_drive::{Dispatched, Dispatcher, Drive, DriveEnv, Drives, InnerItem, InnerStream, Read, VerdictSink};
use crate::build_run::{bounded_reason, Attached, RunDoor, RunReader};
use crate::build_state::{AttemptReason, BuildFact, BuildState, BuildStateError, LocalBuildLog};
use crate::control_plane::{ActionKind, ControlPlane, Refusal};
use crate::model::PathName;
use crate::topology::Topology;
use rdm_core::IncarnationId;
use rdm_transport::membership::SeatBook;
use rdm_transport::stream::{NotStarted, ReplySink, StreamItem};
use rdm_transport::{Budget, CallOptions, HandlerFault, NodeRpcClient, NodeTarget, ServerBuilder};
use rdm_core::build::{Build, BuildChange, BuildPhase, BuildReply, BuildRequest, BuildSubmit, Disposition, MeshCounts, StepReceipt, StepResult};
use rdm_core::catalog::OpOwner;
use rdm_core::context::CallContext;
use rdm_core::outcome::RpcOutcome;
use std::collections::VecDeque;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::Instrument as _;

/// What an admin answers a Build call from.
pub struct BuildDoor {
    /// This admin's `path.name`.
    pub me: PathName,
    /// The control plane: the Build state, the Fabric identity and the cores that accept a change.
    pub control: Arc<ControlPlane>,
    /// The drives this admin holds.
    pub drives: Arc<Drives>,
    /// What the fabric-primary drives a Build with.
    pub env: Arc<DriveEnv>,
    /// The executor's door for `build.attempt.run`.
    pub run: Arc<RunDoor>,
    /// The seat records this admin holds: a committed seat transfer installs into it before the
    /// executor's next step runs, so it names this birth the fabric-primary before the view does.
    pub seats: SeatBook,
    /// This admin's exact birth (with `env.door.node_id`): what the seat book is read against.
    pub incarnation: IncarnationId,
    /// Where a re-submitted Build this admin's own log holds no fact of is read: the incumbent the
    /// committed transfer of its fabric seat names.
    pub incumbent: Arc<dyn IncumbentFacts>,
}

/// The door a running admin fills once it holds a view.
pub type BuildSlot = Arc<OnceLock<Arc<BuildDoor>>>;

/// The facts of a Build this admin's own log holds no fact of, read from the incumbent the committed
/// update of its fabric seat names (node.fabric-primary.update; the approved proposal
/// node-primary-update-fabric-mesh-and-node-type.md Part C1 step 8: "Only then does the commanding
/// Build continue", at the new fabric-primary, which resumes the same attempt; build.md §5.2:
/// "the fabric-primary's claim log reaches every admin through the Build topic and FetchBuildFacts
/// (`0x1F`)"). The incumbent keeps serving after the seat row, so one read reaches the admin that
/// accepted the Build; no other admin is asked and nothing is read again.
#[async_trait::async_trait]
pub trait IncumbentFacts: Send + Sync {
    /// One `FetchBuildFacts` of `build_id` to the incumbent.
    async fn read(&self, build_id: &BuildId) -> IncumbentRead;
}

/// What [`IncumbentFacts::read`] found.
#[derive(Debug)]
pub enum IncumbentRead {
    /// No committed transfer seated this birth in the fabric seat it holds (it holds no fabric seat,
    /// or took it by election or at Day 0): its own log is the only one its Build door answers from.
    NoTransfer,
    /// The incumbent `incumbent` (its `path.name` and node id) was read: its facts, or why none came.
    Read {
        /// The incumbent, named.
        incumbent: String,
        /// The facts, or the read's own failure.
        read: Result<crate::build_facts_read::Fetched, crate::build_facts_read::FetchFailure>,
    },
}

/// A Build door that took no fabric seat by a transfer (an admin built without a seat transfer door).
pub struct NoIncumbent;

#[async_trait::async_trait]
impl IncumbentFacts for NoIncumbent {
    async fn read(&self, _: &BuildId) -> IncumbentRead {
        IncumbentRead::NoTransfer
    }
}

fn counts(m: &MeshCounts) -> MeshDesired {
    MeshDesired { name: m.name.clone(), node_admin: m.node_admin, rpc_node: m.rpc_node, broker: m.broker, gateway: m.gateway, compute: m.compute }
}

/// The refusal reply a control-plane refusal is, with every non-leaking detail it carries.
pub fn refusal_reply(r: Refusal) -> BuildReply {
    match r {
        Refusal::Reject(r) => rdm_core::reply::build::rejected(r.reason().to_string(), r.to_string()),
        Refusal::BadRequest(d) => rdm_core::reply::build::rejected("invalid-request".into(), d),
        Refusal::NotFound(d) => rdm_core::reply::build::rejected("not-found".into(), d),
        Refusal::Conflict(d) => rdm_core::reply::build::rejected("conflict".into(), d),
        Refusal::BuildInProgress(held) => rdm_core::reply::build::build_in_progress(held.current_build_id.to_string(), held.mesh, held.held_by),
        Refusal::NotClaimant { mesh, fabric_primary, mesh_primary } => rdm_core::reply::build::not_claimant(Some(mesh), fabric_primary, mesh_primary),
        Refusal::NotAuthority(p) => rdm_core::reply::build::not_fabric_primary(p),
        Refusal::AttemptTaken(d) => rdm_core::reply::build::attempt_taken(d),
        Refusal::Unavailable(d) => rdm_core::reply::build::not_ready(d),
        Refusal::State(BuildStateError::SeatYielded { node, by, .. }) => rdm_core::reply::build::seat_yielded(node, by),
        Refusal::State(e) => rdm_core::reply::build::not_ready(format!("the Build state: {e}")),
    }
}

/// What a submitted change was admitted as.
enum Admitted {
    /// A new Build or the next attempt of the accepted one.
    Opened(crate::control_plane::Opened),
    /// The fact the change is keyed on was already applied, by this attempt.
    AlreadyApplied(crate::control_plane::Opened),
}

impl BuildDoor {
    /// A change a caller submitted: a new Build or the next attempt of the accepted one.
    async fn admit_change(&self, change: &BuildChange) -> Result<Admitted, BuildReply> {
        let cp = &self.control;
        let node_span = |node: &PathName| tracing::info_span!("rdm.node_admin.build.update.via-node-rpc", route = "build.create", build_id = tracing::field::Empty, attempt = tracing::field::Empty, node = %node);
        let created = |change: &TopologyChange| {
            tracing::info_span!(
                "rdm.node_admin.build.create.via-node-rpc",
                route = "build.create",
                build_id = tracing::field::Empty,
                change = %serde_json::to_string(change).unwrap_or_default(),
                previous_build_id = tracing::field::Empty,
                minted_meshes = tracing::field::Empty,
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
            // The parked predecessor a replacement left at `<path>.old`, and the last admin of a mesh
            // deleted at mesh.deleted (mesh-delete.md step 7: a separate node.delete after
            // mesh.deleted), are deleted by the next attempt of the accepted Build, fenced to the exact
            // birth: the topology never names that path.
            BuildChange::RemoveNode { node } => {
                if node.old || cp.of_deleted_mesh(node).await {
                    cp.open_attempt_in(node_span(node), "build.create", AttemptReason::Delete, node.clone(), ActionKind::Delete).await
                } else {
                    topology(TopologyChange::RemoveNode { node: node.clone() }).await
                }
            }
            BuildChange::CreateMesh { mesh } => topology(TopologyChange::CreateMesh { desired: counts(mesh) }).await,
            BuildChange::RemoveMesh { mesh } => topology(TopologyChange::RemoveMesh { mesh: mesh.clone() }).await,
            BuildChange::DeleteFabric => topology(TopologyChange::DeleteFabric).await,
            BuildChange::Restart { node } => cp.open_attempt_in(node_span(node), "build.create", AttemptReason::Restart, node.clone(), ActionKind::Restart).await,
            BuildChange::DeleteConnections { node } => cp.open_attempt_in(node_span(node), "build.create", AttemptReason::DeleteConnections, node.clone(), ActionKind::DeleteConnections).await,
            BuildChange::Stop { node } => cp.open_attempt_in(node_span(node), "build.create", AttemptReason::Stop, node.clone(), ActionKind::Stop).await,
            BuildChange::Start { node } => cp.open_attempt_in(node_span(node), "build.create", AttemptReason::Start, node.clone(), ActionKind::Start).await,
            BuildChange::Recipe { steps } => {
                let span = tracing::info_span!(
                    "rdm.node_admin.build.update.via-recipe",
                    route = "build.create",
                    build_id = tracing::field::Empty,
                    attempt = tracing::field::Empty,
                    steps = %steps.iter().map(crate::build::recipe_step_key).collect::<Vec<_>>().join(" "),
                );
                cp.open_recipe_in(span, "build.create", steps.clone()).await
            }
            BuildChange::Recover { node, node_id, incarnation } => {
                return match cp.open_recovery("build.create", node.clone(), node_id.clone(), incarnation.clone()).await {
                    Ok(crate::control_plane::Recovery::Opened(o)) => Ok(Admitted::Opened(o)),
                    Ok(crate::control_plane::Recovery::AlreadyApplied(o)) => Ok(Admitted::AlreadyApplied(o)),
                    Err(r) => Err(refusal_reply(r)),
                }
            }
        };
        r.map(Admitted::Opened).map_err(refusal_reply)
    }

    /// Only the fabric-primary accepts a Build call; any other admin names the one it sees.
    async fn not_fabric_primary(&self) -> Option<BuildReply> {
        // Decided by the node id this admin was born with (its claim door's), never by path.name.
        let seat = self.control.topology.read().await.fabric_primary().map(|n| (n.name.clone(), n.node_id.clone()));
        match seat {
            Some((_, id)) if id == self.env.door.node_id => None,
            other => Some(rdm_core::reply::build::not_fabric_primary(other.map(|(n, _)| n.to_string()))),
        }
    }

    /// Whether the seat book names this exact birth the fabric-primary.
    fn holds_fabric_seat(&self) -> bool {
        self.seats.fabric().is_some_and(|h| h.is_birth(&self.env.door.node_id, &self.incarnation))
    }

    /// This admin's currently held `path.name`: what its executor door (`RunDoor::held_name`) resolves.
    pub async fn held_name(&self) -> PathName {
        self.run.held_name().await
    }

    /// Answer one Build call.
    pub async fn serve(&self, req: BuildRequest, sink: ReplySink<Build, NotStarted>) -> Result<BuildReply, HandlerFault> {
        let me = self.held_name().await;
        let span = tracing::info_span!("rdm.node_admin.build.serve.via-node-rpc", node = %me, call = req.op(), outcome = tracing::field::Empty);
        let call = req.op();
        let reply = async {
            match req {
                BuildRequest::AttemptRun { build_id, attempt, executor, context, intent, operations, hand_off_to } => match build_id_of(&build_id) {
                    Ok(id) => self.attempt_run(id, attempt, executor, context, intent, operations, hand_off_to, sink).await,
                    Err(refused) => Ok(refused),
                },
                other => {
                    if let Some(refusal) = self.not_fabric_primary().await {
                        // The refusal's own span, as every other Build refusal has one (one honest name per
                        // reason, `reject_span`): the call, this admin and the fabric-primary it named.
                        let route = match call {
                            "create" => "build.create",
                            "get" => "build.get",
                            _ => "build.delete",
                        };
                        let named = match &refusal {
                            BuildReply::NotFabricPrimary { fabric_primary } => fabric_primary.clone().unwrap_or_default(),
                            _ => String::new(),
                        };
                        tracing::info_span!("rdm.node_admin.build.reject.via-not-fabric-primary", route, node = %me, fabric_primary = %named)
                            .in_scope(|| tracing::info!("a Build call is answered only by the fabric-primary"));
                        return Ok(refusal);
                    }
                    match other {
                        BuildRequest::Create { submit } => self.create(submit, sink).await,
                        BuildRequest::Get { build_id } => match build_id_of(&build_id) {
                            Ok(id) => self.get(id, sink).await,
                            Err(refused) => Ok(refused),
                        },
                        BuildRequest::Delete { build_id } => match build_id_of(&build_id) {
                            Ok(id) => self.delete(id, sink).await,
                            Err(refused) => Ok(refused),
                        },
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
                let opened = match self.admit_change(&change).await {
                    Ok(Admitted::Opened(o)) => o,
                    // A change keyed on a fact already applied (a recovery of a birth already
                    // recovered): nothing runs and no drive is made or started (a drive of a Failed
                    // Build would claim its next attempt); the terminal names the attempt whose
                    // receipts applied it.
                    Ok(Admitted::AlreadyApplied(o)) => {
                        let _sink = started(sink, BuildReply::Started { build_id: o.build_id.to_string(), attempt: o.attempt, disposition: Disposition::AlreadyApplied }).await?;
                        return Ok(BuildReply::Complete { build_id: o.build_id.to_string(), attempt: o.attempt });
                    }
                    Err(refusal) => return Ok(refusal),
                };
                let drive = self.drives.ensure(&self.env, &opened.build_id, opened.attempt);
                let sink = started(sink, BuildReply::Started { build_id: opened.build_id.to_string(), attempt: opened.attempt, disposition: Disposition::Created }).await?;
                stream_frames(sink, drive.reader(opened.attempt)).await
            }
            BuildSubmit::Resubmit { build_id, from_attempt } => match build_id_of(&build_id) {
                Ok(id) => self.resubmit(id, from_attempt, sink).await,
                Err(refused) => Ok(refused),
            },
        }
    }

    /// A re-submit by build id never creates a second Build: a complete Build answers
    /// `AlreadyApplied` and its terminal; a running one with a holder streams that holder's run; a
    /// failed one, or one whose holder was lost, takes its next attempt.
    async fn resubmit(&self, id: BuildId, from_attempt: u32, sink: ReplySink<Build, NotStarted>) -> Result<BuildReply, HandlerFault> {
        let me = self.held_name().await;
        let p = match self.control.builds.read_build(&id).await {
            Ok(p) => p,
            Err(BuildStateError::UnknownBuild(_)) => match self.from_incumbent(&id).await {
                Ok(p) => p,
                Err(refusal) => return Ok(refusal),
            },
            Err(e) => return Ok(rdm_core::reply::build::not_ready(format!("{me}: Build {id} could not be read: {e}"))),
        };
        // The drive's own record answers a Build that ended: the frames it took off the calls it
        // made. A drive still running answers by streaming. A fabric-primary with no drive of the
        // Build (it took the seat after the Build ended) makes one, which attaches to the last
        // attempt's executor and reads its frames.
        let held = self.drives.get(&id);
        if p.state == BuildState::Complete && held.as_ref().map(|d| d.ended()).unwrap_or(true) {
            let drive = self.drives.replay_of(&self.env, &id, p.attempt);
            let sink = started(sink, BuildReply::Started { build_id: id.to_string(), attempt: from_attempt, disposition: Disposition::AlreadyApplied }).await?;
            return stream_frames(sink, drive.reader(from_attempt)).await;
        }
        match crate::build_state::drivable(&*self.control.builds, &p).await {
            Ok(true) => {}
            Ok(false) => return Ok(rdm_core::reply::build::rejected("not-accepted".into(), format!("{me}: Build {id} is {:?} and is neither the current Build nor a failed Build accepted after it: nothing drives it", p.state))),
            Err(e) => return Ok(rdm_core::reply::build::not_ready(format!("{me}: the current Build could not be read to admit Build {id}: {e}"))),
        }
        let disposition = if p.state == BuildState::Failed { Disposition::NextAttempt } else { Disposition::Attached };
        // The drive is the one reader of the receipts: a drive this admin took up after a seat move
        // reads every earlier attempt's receipts at its first dispatch, in its own task.
        let drive = self.drives.ensure(&self.env, &id, p.attempt_to_drive());
        let sink = started(sink, BuildReply::Started { build_id: id.to_string(), attempt: from_attempt, disposition }).await?;
        stream_frames(sink, drive.reader(from_attempt)).await
    }

    /// A re-submitted Build this admin's own log holds no fact of: the new seat has not heard it yet
    /// (the Build topic carries the facts apart, and the re-submit outran it). Its facts are read once
    /// from the incumbent the committed transfer of this admin's fabric seat names, which accepted it
    /// and keeps serving after its row (proposal Part C1 step 6: the holder calls after the row), and are absorbed into
    /// this admin's own log as the Build topic's are (node-rpc-envelope.md, "Build facts": "Facts are
    /// absorbed through the existing Build log"). No Build gossip is authored and nothing is read again.
    ///
    /// Only a Build no admin holds stays `UnknownBuild`: this admin took no seat by a transfer (its own
    /// log is the one it answers from), or the incumbent holds no fact of it either. A read that failed,
    /// or holdings the incumbent says are not whole, is `NotReady` naming the incumbent and the cause.
    async fn from_incumbent(&self, id: &BuildId) -> Result<crate::build_state::BuildProjection, BuildReply> {
        let me = self.held_name().await;
        let span = tracing::info_span!(
            "rdm.node_admin.build.update.via-incumbent-facts",
            node = %me,
            build_id = %id.log(),
            incumbent = tracing::field::Empty,
            facts = tracing::field::Empty,
            chunks = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        let me = &me;
        async {
            let span = tracing::Span::current();
            let (incumbent, read) = match self.incumbent.read(id).await {
                IncumbentRead::NoTransfer => {
                    span.record("outcome", "no-transfer");
                    span.in_scope(|| tracing::info!("this admin took its fabric seat by no transfer: its own log is the one it answers from"));
                    return Err(rdm_core::reply::build::unknown_build(id.to_string()));
                }
                IncumbentRead::Read { incumbent, read } => (incumbent, read),
            };
            span.record("incumbent", incumbent.as_str());
            let fetched = match read {
                Ok(f) if f.complete => f,
                Ok(f) => {
                    span.record("facts", f.facts.len() as u64);
                    span.record("outcome", "incomplete");
                    span.in_scope(|| tracing::info!("the incumbent says its holdings of the Build are not whole: nothing is absorbed"));
                    return Err(rdm_core::reply::build::not_ready(format!("{me}: Build {id} is not in this admin's log, and {incumbent}, whose fabric seat this admin took, holds {} facts of it that it says are not whole: nothing is absorbed", f.facts.len())));
                }
                Err(crate::build_facts_read::FetchFailure::UnknownBuild(_)) => {
                    span.record("outcome", "unknown-build");
                    span.in_scope(|| tracing::info!("the incumbent holds no fact of the Build either"));
                    return Err(rdm_core::reply::build::unknown_build(id.to_string()));
                }
                Err(e) => {
                    span.record("outcome", e.reason());
                    span.in_scope(|| tracing::info!(detail = %e, "the incumbent's facts of the Build could not be read"));
                    return Err(rdm_core::reply::build::not_ready(format!("{me}: Build {id} is not in this admin's log, and its facts could not be read from {incumbent}, whose fabric seat this admin took: {e}")));
                }
            };
            span.record("facts", fetched.facts.len() as u64);
            span.record("chunks", fetched.chunks);
            self.run.local.absorb_facts(&fetched.facts).await;
            match self.control.builds.read_build(id).await {
                Ok(p) => {
                    span.record("outcome", "absorbed");
                    span.in_scope(|| tracing::info!("the incumbent's facts of the re-submitted Build are absorbed into this admin's own log"));
                    Ok(p)
                }
                Err(e) => {
                    span.record("outcome", "unreadable-after-absorb");
                    Err(rdm_core::reply::build::not_ready(format!("{me}: the {} facts of Build {id} read from {incumbent} were absorbed, and this admin's own log still cannot read it: {e}", fetched.facts.len())))
                }
            }
        }
        .instrument(span)
        .await
    }

    async fn get(&self, id: BuildId, sink: ReplySink<Build, NotStarted>) -> Result<BuildReply, HandlerFault> {
        let me = self.held_name().await;
        let p = match self.control.builds.read_build(&id).await {
            Ok(p) => p,
            Err(BuildStateError::UnknownBuild(_)) => return Ok(rdm_core::reply::build::unknown_build(id.to_string())),
            Err(e) => return Ok(rdm_core::reply::build::not_ready(format!("{me}: Build {id} could not be read: {e}"))),
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
                    crate::build_state::StepOutcome::Failed { reason, code, .. } => StepResult::Failed { reason: bounded_reason(reason.clone()), code: *code },
                },
            })
            .collect();
        // Chunks that each fit the reply frame with room to spare.
        let mut chunks: Vec<Vec<StepReceipt>> = vec![Vec::new()];
        let mut size = 0usize;
        for s in steps.iter().cloned() {
            let weight = 32 + s.operation.len() + s.step.len() + match &s.result {
                StepResult::Complete => 0,
                StepResult::Failed { reason , .. } => reason.len() + 1,
            };
            if size + weight > 32 * 1024 && !chunks.last().is_some_and(Vec::is_empty) {
                chunks.push(Vec::new());
                size = 0;
            }
            size += weight;
            chunks.last_mut().expect("a chunk").push(s);
        }
        let count = chunks.len() as u32;
        let mut sink = started(sink, BuildReply::Started { build_id: id.to_string(), attempt: p.attempt, disposition: Disposition::Attached }).await?;
        for (i, chunk) in chunks.into_iter().enumerate() {
            sink.data(BuildReply::Steps { build_id: id.to_string(), chunk_index: i as u32, chunk_count: count, steps: chunk }).await.map_err(gone)?;
        }
        Ok(BuildReply::Got {
            build_id: id.to_string(),
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

    /// A delete stream is one refusal, or `Started` and its one terminal `Deleted` (node-rpc-envelope.md
    /// "Build schema": "A stream is one refusal, or Started, then ... Deleted for Delete"). `Started` names the
    /// Build's last attempt, read before the Build leaves history.
    async fn delete(&self, id: BuildId, sink: ReplySink<Build, NotStarted>) -> Result<BuildReply, HandlerFault> {
        let me = self.held_name().await;
        let attempt = match self.control.builds.read_build(&id).await {
            Ok(p) => p.attempt,
            Err(BuildStateError::UnknownBuild(_)) => return Ok(rdm_core::reply::build::unknown_build(id.to_string())),
            Err(e) => return Ok(rdm_core::reply::build::not_ready(format!("{me}: Build {id} could not be read: {e}"))),
        };
        match self.control.forget_build(&id).await {
            Ok(()) => {
                self.drives.forget(&id);
                let _sink = started(sink, BuildReply::Started { build_id: id.to_string(), attempt, disposition: Disposition::Attached }).await?;
                Ok(BuildReply::Deleted { build_id: id.to_string() })
            }
            Err(Refusal::NotFound(_)) => Ok(rdm_core::reply::build::unknown_build(id.to_string())),
            Err(Refusal::Conflict(reason)) => Ok(rdm_core::reply::build::cannot_delete(id.to_string(), reason)),
            Err(other) => Ok(refusal_reply(other)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn attempt_run(&self, id: BuildId, attempt: u32, executor: String, context: CallContext, intent: Vec<Vec<u8>>, operations: Vec<Vec<u8>>, hand_off_to: Option<String>, sink: ReplySink<Build, NotStarted>) -> Result<BuildReply, HandlerFault> {
        // The caller of `build.attempt.run` is the fabric-primary that drives the Build. When this
        // executor took that seat during the run (a committed seat transfer of the attempt's own
        // SeatTransfer step), the caller is no longer the driver: its seat row fenced it and its
        // stream ends here with the typed new-seat frame naming this admin, never with a write into
        // a stream the old driver stopped. The run goes on in its own task; this admin's own drive
        // reads its frames.
        let held_at_start = self.holds_fabric_seat();
        let me = self.held_name().await;
        let mut plan = RunPlan { operations: Vec::with_capacity(operations.len()), hand_off_to };
        for (i, bytes) in operations.iter().enumerate() {
            match serde_json::from_slice::<BuildOperation>(bytes) {
                Ok(op) => plan.operations.push(op),
                Err(e) => return Ok(rdm_core::reply::build::rejected("operation-undecodable".into(), format!("{me}: operation {i} of the run of attempt {attempt} of Build {id} does not decode: {e}"))),
            }
        }
        match self.run.attempt_run(&id, attempt, &executor, &context, &intent, &plan).await {
            Attached::Refused(r) => Ok(r),
            Attached::Ended { replay, terminal } => {
                let mut sink = started(sink, BuildReply::Started { build_id: id.to_string(), attempt, disposition: Disposition::Reattached }).await?;
                for f in replay {
                    sink.data(f).await.map_err(gone)?;
                }
                Ok(terminal)
            }
            Attached::Live { disposition, replay, mut reader } => {
                let mut sink = started(sink, BuildReply::Started { build_id: id.to_string(), attempt, disposition }).await?;
                for f in replay {
                    sink.data(f).await.map_err(gone)?;
                }
                // A cut call ends this stream and nothing else: the run is its own task.
                while let Some(f) = reader.next().await {
                    if !held_at_start && self.holds_fabric_seat() {
                        let me = self.held_name().await;
                        tracing::info_span!("rdm.node_admin.build.update.via-seat-moved", build_id = %id.log(), attempt, node = %me, to = %me, detail = "this executor took the fabric-primary seat during the run: the old driver's stream ends with the new seat")
                            .in_scope(|| tracing::info!("the inner stream ends with the new seat"));
                        return Ok(BuildReply::SeatMoved { build_id: id.to_string(), fabric_primary: Some(me.to_string()) });
                    }
                    match f {
                        BuildReply::Step { .. } | BuildReply::Blocked { .. } => sink.data(f).await.map_err(gone)?,
                        terminal => return Ok(terminal),
                    }
                }
                let me = self.held_name().await;
                Err(HandlerFault::invariant_broken(format!("{me}: the run of attempt {attempt} of Build {id} ended without a terminal")))
            }
        }
    }
}

fn gone(e: rdm_transport::stream::SinkError) -> HandlerFault {
    HandlerFault::invariant_broken(format!("the caller left the stream, and the Build's drive goes on without it: {e:?}"))
}

async fn started(sink: ReplySink<Build, NotStarted>, frame: BuildReply) -> Result<ReplySink<Build, rdm_transport::stream::Streaming>, HandlerFault> {
    sink.started(frame).await.map_err(gone)
}

/// Read a drive's frames to the caller until its terminal. A seat that moved ends the stream with
/// the typed `SeatMoved` terminal naming the new seat: the caller re-submits the Build id there.
async fn stream_frames(mut sink: ReplySink<Build, rdm_transport::stream::Streaming>, mut reader: crate::build_drive::DriveReader) -> Result<BuildReply, HandlerFault> {
    while let Some(t) = reader.next().await {
        match t {
            Read::Frame(f) => sink.data(f).await.map_err(gone)?,
            Read::Terminal(t) => return Ok(t),
            Read::SeatLost(loss) => {
                tracing::info_span!("rdm.node_admin.build.update.via-seat-moved", build_id = %reader.build_id().log(), to = loss.to.as_deref().unwrap_or("none"), detail = %loss.why).in_scope(|| tracing::info!("the outer stream ends with the new seat"));
                return Ok(BuildReply::SeatMoved { build_id: reader.build_id().to_string(), fabric_primary: loss.to });
            }
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
                return Ok(rdm_core::reply::build::not_ready("this admin holds no view yet".into()));
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
    stream: rdm_transport::stream::ReplyStream<Build>,
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
    fn hold(&self, me: &PathName, build_id: &BuildId, attempt: u32) -> Option<crate::build_run::HeldRun> {
        let is_me = *me == self.me
            || self.local.builds.held_name().as_ref() == Some(me)
            || self.topology.try_read().ok().is_some_and(|t| t.held_name_of(&self.me, None) == *me);
        is_me.then(|| self.local.hold(build_id, attempt)).flatten()
    }
    async fn dispatch(&self, turn: &crate::stop_gate::Turn, executor: &PathName, build_id: &BuildId, attempt: u32, context: CallContext, intent: Vec<Vec<u8>>, plan: RunPlan) -> Dispatched {
        let me = self.local.held_name().await;
        if *executor == self.me || *executor == me {
            // The fabric-primary runs the attempt itself: it plans from the log the claim was decided
            // on, and the run's turn is begun under the drive pass's.
            return local_dispatched(self.local.attempt_run_under(turn, build_id, attempt, &executor.to_string(), &context, &[], &plan).await);
        }
        let node_id = self.topology.read().await.members().find(|n| n.name == *executor).map(|n| n.node_id.clone());
        let Some(node_id) = node_id else {
            return Dispatched::Unreached(format!("{executor} is not a node of {me}'s view"));
        };
        let mut operations = Vec::with_capacity(plan.operations.len());
        for op in &plan.operations {
            match serde_json::to_vec(op) {
                Ok(b) => operations.push(b),
                Err(e) => return Dispatched::Unreached(format!("the operation {} does not encode: {e}", op.key())),
            }
        }
        let req = BuildRequest::AttemptRun { build_id: build_id.to_string(), attempt, executor: executor.to_string(), context: context.clone(), intent, operations, hand_off_to: plan.hand_off_to };
        let opts = CallOptions { budget: Budget::Stream { send: DISPATCH_SEND }, context: Some(context), ..CallOptions::default() };
        match self.client.call_stream::<Build>(&NodeTarget::ExactNode(node_id), &req, &opts).await {
            // The executor records each frame's receipt before it sends the frame (a step's receipt
            // before its Step, the verdict before the terminal: `build_run.rs` `append_step_receipt`,
            // `HeldRun::finish`): the drive records nothing it needs to recover them, and reading a
            // frame is its recovery (node-rpc.md §43, a read-only consumer).
            Ok((stream, _)) => Dispatched::Stream(Box::new(RemoteStream { stream: stream.read_only() })),
            Err((RpcOutcome::Reply(r), _)) => Dispatched::Refused(r.value().clone()),
            Err((RpcOutcome::Unserved(u), _)) => Dispatched::Refused(rdm_core::reply::build::not_ready(format!("{executor} does not serve the Build family: {u:?}"))),
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

/// The drive of the current Build, for the reconcile loop: `ensure` it while it is in progress
/// (R-BI3: the Build in progress is resumed), and once it is complete, the drive this admin holds of
/// it that has not ended (one that paused or stalled while the Build completed): its next pass reads
/// the Build complete and ends its callers' streams. A Build that completed once and whose later
/// attempt failed is the known state too, and only a caller's resubmit drives it on (R-BI3).
pub async fn ensure_active(drives: &Arc<Drives>, env: &Arc<DriveEnv>) -> Vec<Arc<Drive>> {
    let (id, completed) = match env.builds.current_build().await {
        Ok(crate::build_state::CurrentBuild::InProgress(id)) => (id, false),
        Ok(crate::build_state::CurrentBuild::Completed(id)) if drives.get(&id).is_some_and(|d| !d.ended()) => (id, true),
        Ok(_) => return Vec::new(),
        Err(e) => {
            tracing::info!(error = %e, "Build state unreadable; nothing driven");
            return Vec::new();
        }
    };
    match env.builds.read_build(&id).await {
        Ok(b) if completed && b.state != crate::build_state::BuildState::Complete => Vec::new(),
        Ok(b) => vec![drives.ensure(env, &b.build_id, b.attempt_to_drive())],
        Err(e) => {
            tracing::info!(error = %e, build_id = %id.log(), "the current Build is unreadable; nothing driven");
            Vec::new()
        }
    }
}

/// The Build id a request names, or the refusal that names the id and why it is none: every Build id
/// is a Crockford id (R-BI5), so an id of any other form (a stored `bld-` id, hex) is refused here by name.
fn build_id_of(id: &str) -> Result<BuildId, BuildReply> {
    BuildId::parse(id).map_err(|e| rdm_core::reply::build::rejected("invalid-request".into(), format!("the request names a Build id that is not a Crockford id: {e}")))
}
