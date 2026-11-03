//! The Build family on op `0x20` as a node-admin serves and calls it (node-rpc-envelope.md "Build,
//! op `0x20`, rdm"; R-ON8).
//!
//! The wire door is a thin decoder: `build.create` hands the change to the control plane's own cores
//! (`ControlPlane::submit_in`, the one the control routes use) and then reads
//! the Build's [`Drive`]; `build.get` and `build.delete` read and forget
//! through the same Build state; `build.attempt.run` is the executor's [`RunDoor`]. Only the
//! fabric-primary accepts the first three; any other admin refuses by name and names the one it sees.
//!
//! This module also holds the two callers of the family inside an admin: the fabric-primary's
//! [`RpcDispatcher`] (`build.attempt.run` to a mesh's primary, in process when that is itself),
//! which names the Build and the mesh and never carries a copy of the Build: the mesh-primary reads the
//! Build and its events from storage before it plans its section (Q292).

use crate::build::{BuildId, BuildOperation};
use crate::build_drive::{Dispatched, Dispatcher, Drive, DriveEnv, Drives, InnerItem, InnerStream, Read};
use crate::build_run::{bounded_reason, Attached, RunDoor, RunReader};
use crate::build_state::{BuildState, BuildStateError};
use crate::control_plane::{ControlPlane, Refusal};
use crate::model::PathName;
use crate::topology::Topology;
use rdm_core::IncarnationId;
use rdm_transport::topology::PrimaryView;
use rdm_transport::stream::{NotStarted, ReplySink, StreamItem};
use rdm_transport::{CallOptions, HandlerFault, NodeRpcClient, NodeTarget, ServerBuilder};
use rdm_core::build::{Build, BuildChange, BuildPhase, BuildReply, BuildRequest, BuildSubmit, Disposition};
use rdm_core::catalog::OpOwner;
use rdm_core::context::CallContext;
use rdm_core::outcome::RpcOutcome;
use std::collections::VecDeque;
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;
use tracing::Instrument as _;

/// What an admin answers a Build call from.
pub struct BuildDoor {
    /// This admin's path.
    pub me_path: String,
    /// This node's name, beside its path.
    pub me_name: String,
    /// The control plane: the Build state, the Fabric identity and the cores that accept a change.
    pub control: Arc<ControlPlane>,
    /// The drives this admin holds.
    pub drives: Arc<Drives>,
    /// What the fabric-primary drives a Build with.
    pub env: Arc<DriveEnv>,
    /// The executor's door for `build.attempt.run`.
    pub run: Arc<RunDoor>,
    /// The primary records this admin holds: node.primary.update's successor installs into it before
    /// the executor's next step runs, so it names this birth the fabric-primary before the view does.
    pub primaries: PrimaryView,
    /// This admin's exact birth (with `env.door.node_id`): what the primary view is read against.
    pub incarnation: IncarnationId,
}

impl BuildDoor {
    /// The `(path, name)` lookup index key of me_path and me_name.
    pub fn me(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.me_path.clone(), name: self.me_name.clone() } }
}

/// The door a running admin fills once it holds a view.
pub type BuildSlot = Arc<OnceLock<Arc<BuildDoor>>>;

/// The refusal reply a control-plane refusal is, with every non-leaking detail it carries.
pub fn refusal_reply(r: Refusal) -> BuildReply {
    match r {
        Refusal::Reject(r) => rdm_core::reply::build::rejected(r.reason().to_string(), r.to_string()),
        Refusal::BadRequest(d) => rdm_core::reply::build::rejected("invalid-request".into(), d),
        Refusal::NotFound(d) => rdm_core::reply::build::rejected("not-found".into(), d),
        Refusal::Conflict(d) => rdm_core::reply::build::rejected("conflict".into(), d),
        Refusal::BuildInProgress(held) => rdm_core::reply::build::build_in_progress(held.current_build_id.to_string()),
        // A storage change is no Build: the refusal names no Build.
        Refusal::StorageChangeInProgress { .. } => rdm_core::reply::build::build_in_progress(String::new()),
        Refusal::NotPrimary(p) => rdm_core::reply::build::not_fabric_primary(p),
        Refusal::Unavailable(d) => rdm_core::reply::build::not_ready(d),
        Refusal::State(BuildStateError::RoleYielded { node, by, .. }) => rdm_core::reply::build::role_yielded(node, by),
        Refusal::State(e) => rdm_core::reply::build::not_ready(format!("the Build state: {e}")),
    }
}

/// What a submitted change was admitted as.
enum Admitted {
    /// A new Build.
    Opened(crate::control_plane::Opened),
    /// The fabric.delete in force, which a second fabric.delete answers (build.md §1): nothing accepted,
    /// its drive streamed.
    Resumed(crate::control_plane::Opened, Disposition),
}

impl BuildDoor {
    /// A change a caller submitted: a new Build.
    async fn admit_change(&self, change: &BuildChange) -> Result<Admitted, BuildReply> {
        let cp = &self.control;
        let created = |change: &BuildChange| {
            tracing::info_span!(
                "rdm.node_admin.build.create.via-node-rpc",
                route = "build.create",
                build_id = tracing::field::Empty,
                change = %serde_json::to_string(change).unwrap_or_default(),
                previous_build_id = tracing::field::Empty,
                minted_meshes = tracing::field::Empty,
            )
        };
        let topology = |c: BuildChange| async move {
            let span = created(&c);
            cp.submit_in(span, "build.create", c).await
        };
        let r = match change {
            // The meshes and nodes a Create adds, and the nodes and meshes a Delete removes, as it came in
            // (build.md §1): each its own Build, composed from the records at acceptance, a replaced
            // predecessor's delete and a deleted mesh's last admin's among them.
            BuildChange::Create { .. } | BuildChange::Delete { .. } => topology(change.clone()).await,
            BuildChange::DeleteFabric => match cp.fabric_delete_in_force().await {
                Ok(Some((o, failed))) => return Ok(Admitted::Resumed(o, if failed { Disposition::Created } else { Disposition::Attached })),
                Ok(None) => topology(change.clone()).await,
                Err(r) => Err(r),
            },
        };
        r.map(Admitted::Opened).map_err(refusal_reply)
    }

    /// Only the fabric-primary accepts a Build call; any other admin names the one it sees.
    async fn not_fabric_primary(&self) -> Option<BuildReply> {
        // Decided by the node id this admin was born with, never by path and name.
        let role = self.control.topology.read().await.fabric_primary().map(|n| (n.location(), n.node_id.clone()));
        match role {
            Some((_, id)) if id == self.env.birth.node_id => None,
            other => Some(rdm_core::reply::build::not_fabric_primary(other.map(|(_, id)| id))),
        }
    }

    /// Whether the primary view names this exact birth the fabric-primary.
    fn is_fabric_primary(&self) -> bool {
        self.primaries.fabric().is_some_and(|h| h.is_birth(&self.env.birth.node_id, &self.incarnation))
    }

    /// This admin's currently held `(path, name)`: what its executor door (`RunDoor::held_name`) resolves.
    pub async fn held_name(&self) -> PathName {
        self.run.held_name().await
    }

    /// Answer one Build call from the caller at `caller` (its endpoint key).
    pub async fn serve(&self, caller: Option<iroh::PublicKey>, req: BuildRequest, sink: ReplySink<Build, NotStarted>) -> Result<BuildReply, HandlerFault> {
        let me = self.held_name().await;
        let me_id = &self.env.birth.node_id;
        let span = tracing::info_span!("rdm.node_admin.build.serve.via-node-rpc", node_path = %me.path, node_name = %me.name, call = req.op(), outcome = tracing::field::Empty);
        let call = req.op();
        let reply = async {
            match req {
                BuildRequest::AttemptRun { build_id, mesh, context } => match build_id_of(&build_id) {
                    Ok(id) => self.attempt_run(id, mesh, context, sink).await,
                    Err(refused) => Ok(refused),
                },
                other => {
                    if let Some(refusal) = self.not_fabric_primary().await {
                        // The refusal's own span, as every other Build refusal has one (one honest name per
                        // reason, `reject_span`): the call, this admin and the fabric-primary it named.
                        let route = match call {
                            "create" => "build.create",
                            "get" => "build.get",
                            "build.get" => "build.get",
                            _ => "build.delete",
                        };
                        let named = match &refusal {
                            BuildReply::NotFabricPrimary { fabric_primary } => fabric_primary.as_ref().map(ToString::to_string).unwrap_or_default(),
                            _ => String::new(),
                        };
                        tracing::info_span!("rdm.node_admin.build.reject.via-not-fabric-primary", route, node_path = %me.path, node_name = %me.name, fabric_primary = %named)
                            .in_scope(|| tracing::info!("a Build call is answered only by the fabric-primary"));
                        return Ok(refusal);
                    }
                    match other {
                        BuildRequest::Create { submit } => self.create(submit, caller, sink).await,
                        BuildRequest::Get { build_id } => match build_id_of(&build_id) {
                            Ok(id) => self.get(id, sink).await,
                            Err(refused) => Ok(refused),
                        },
                        BuildRequest::Delete { build_id } => match build_id_of(&build_id) {
                            Ok(id) => self.delete(id, sink).await,
                            Err(refused) => Ok(refused),
                        },
                        BuildRequest::GetBuilds { filter } => serve_builds(&me, &*self.control.builds, filter, sink).await,
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

    async fn create(&self, submit: BuildSubmit, caller: Option<iroh::PublicKey>, sink: ReplySink<Build, NotStarted>) -> Result<BuildReply, HandlerFault> {
        match submit {
            BuildSubmit::Change(change) => {
                let (opened, disposition) = match self.admit_change(&change).await {
                    Ok(Admitted::Opened(o)) => (o, Disposition::Created),
                    Ok(Admitted::Resumed(o, disposition)) => (o, disposition),
                    Err(refusal) => return Ok(refusal),
                };
                let drive = self.drives.ensure(&self.env, &opened.build_id);
                let sink = started(sink, BuildReply::Started { build_id: opened.build_id.to_string(), disposition }).await?;
                stream_frames(sink, drive.reader().of_caller(caller)).await
            }
            BuildSubmit::Resubmit { build_id } => match build_id_of(&build_id) {
                Ok(id) => self.resubmit(id, caller, sink).await,
                Err(refused) => Ok(refused),
            },
        }
    }

    /// A re-submit by build id never creates a second Build: a complete Build answers
    /// `AlreadyApplied` and its terminal; a running one streams its drive; a failed one is run again
    /// (`Created`): its steps whose event ended `complete` stand, and the rest are dispatched again.
    async fn resubmit(&self, id: BuildId, caller: Option<iroh::PublicKey>, sink: ReplySink<Build, NotStarted>) -> Result<BuildReply, HandlerFault> {
        let me_id = &self.env.birth.node_id;
        let p = match self.control.builds.read_build(&id).await {
            Ok(p) => p,
            Err(BuildStateError::UnknownBuild(_)) => return Ok(rdm_core::reply::build::unknown_build(id.to_string())),
            Err(e) => return Ok(rdm_core::reply::build::not_ready(format!("{me_id}: Build {id} could not be read: {e}"))),
        };
        // The drive's own record answers a Build that ended: the frames it took off the calls it
        // made. A drive still running answers by streaming. A fabric-primary with no drive of the
        // Build (it took the role after the Build ended) makes one, which reads the Build's events.
        let held = self.drives.get(&id);
        if p.state == BuildState::Complete && held.as_ref().map(|d| d.ended()).unwrap_or(true) {
            let drive = self.drives.replay_of(&self.env, &id);
            let sink = started(sink, BuildReply::Started { build_id: id.to_string(), disposition: Disposition::AlreadyApplied }).await?;
            return stream_frames(sink, drive.reader().of_caller(caller)).await;
        }
        match crate::build_state::drivable(&*self.control.builds, &p).await {
            Ok(true) => {}
            Ok(false) => return Ok(rdm_core::reply::build::rejected("not-accepted".into(), format!("{me_id}: Build {id} is {:?} and is neither the current Build nor a failed Build accepted after it: nothing drives it", p.state))),
            Err(e) => return Ok(rdm_core::reply::build::not_ready(format!("{me_id}: the current Build could not be read to admit Build {id}: {e}"))),
        }
        let disposition = if p.state == BuildState::Failed { Disposition::Created } else { Disposition::Attached };
        let drive = self.drives.ensure(&self.env, &id);
        let sink = started(sink, BuildReply::Started { build_id: id.to_string(), disposition }).await?;
        stream_frames(sink, drive.reader().of_caller(caller)).await
    }

    async fn get(&self, id: BuildId, sink: ReplySink<Build, NotStarted>) -> Result<BuildReply, HandlerFault> {
        let me_id = &self.env.birth.node_id;
        let p = match self.control.builds.read_build(&id).await {
            Ok(p) => p,
            Err(BuildStateError::UnknownBuild(_)) => return Ok(rdm_core::reply::build::unknown_build(id.to_string())),
            Err(e) => return Ok(rdm_core::reply::build::not_ready(format!("{me_id}: Build {id} could not be read: {e}"))),
        };
        let (events, chunks) = match event_chunks(&*self.control.builds, &id).await {
            Ok(c) => c,
            Err(e) => return Ok(rdm_core::reply::build::not_ready(format!("{me_id}: the events of Build {id} could not be read: {e}"))),
        };
        let count = chunks.len() as u32;
        let mut sink = started(sink, BuildReply::Started { build_id: id.to_string(), disposition: Disposition::Attached }).await?;
        for (i, chunk) in chunks.into_iter().enumerate() {
            sink.data(BuildReply::Events { build_id: id.to_string(), chunk_index: i as u32, chunk_count: count, events: chunk }).await.map_err(gone)?;
        }
        let (reason, last_failure) = reason_of(&p);
        Ok(BuildReply::Got { build_id: id.to_string(), phase: phase_of(p.state), reason, last_failure, events, chunks: count })
    }

    /// A delete stream is one refusal, or `Started` and its one terminal `Deleted` (node-rpc-envelope.md
    /// "Build schema": "A stream is one refusal, or Started, then ... Deleted for Delete").
    async fn delete(&self, id: BuildId, sink: ReplySink<Build, NotStarted>) -> Result<BuildReply, HandlerFault> {
        let me_id = &self.env.birth.node_id;
        match self.control.builds.read_build(&id).await {
            Ok(_) => {}
            Err(BuildStateError::UnknownBuild(_)) => return Ok(rdm_core::reply::build::unknown_build(id.to_string())),
            Err(e) => return Ok(rdm_core::reply::build::not_ready(format!("{me_id}: Build {id} could not be read: {e}"))),
        }
        match self.control.forget_build(&id).await {
            Ok(()) => {
                self.drives.forget(&id);
                let _sink = started(sink, BuildReply::Started { build_id: id.to_string(), disposition: Disposition::Attached }).await?;
                Ok(BuildReply::Deleted { build_id: id.to_string() })
            }
            Err(Refusal::NotFound(_)) => Ok(rdm_core::reply::build::unknown_build(id.to_string())),
            Err(Refusal::Conflict(reason)) => Ok(rdm_core::reply::build::cannot_delete(id.to_string(), reason)),
            Err(other) => Ok(refusal_reply(other)),
        }
    }

    /// `build.attempt.run`: `mesh`'s section of Build `id`, run by this admin as the mesh's primary. When
    /// this admin became fabric-primary during the run (a step's own node.primary.update), the caller is
    /// no longer the driver: its stream ends here with FabricPrimaryMoved naming this admin. The run goes
    /// on in its own task; this admin's own drive reads the Build's events.
    async fn attempt_run(&self, id: BuildId, mesh: String, context: CallContext, sink: ReplySink<Build, NotStarted>) -> Result<BuildReply, HandlerFault> {
        let held_at_start = self.is_fabric_primary();
        match self.run.attempt_run(&id, &mesh, &context).await {
            Attached::Refused(r) => Ok(r),
            Attached::Ended { replay, terminal } => {
                let mut sink = started(sink, BuildReply::Started { build_id: id.to_string(), disposition: Disposition::Reattached }).await?;
                for f in replay {
                    sink.data(f).await.map_err(gone)?;
                }
                Ok(terminal)
            }
            Attached::Live { disposition, replay, mut reader } => {
                let mut sink = started(sink, BuildReply::Started { build_id: id.to_string(), disposition }).await?;
                for f in replay {
                    sink.data(f).await.map_err(gone)?;
                }
                // A cut call ends this stream and nothing else: the run is its own task.
                while let Some(f) = reader.next().await {
                    if !held_at_start && self.is_fabric_primary() {
                        let me = self.held_name().await;
                        let me_id = &self.env.birth.node_id;
                        tracing::info_span!("rdm.node_admin.build.update.via-fabric-primary-changed", build_id = %id.log(), mesh = %mesh, node_path = %me.path, node_name = %me.name, to_node_id = %me_id, detail = "this mesh-primary took the fabric-primary role during the run: the old driver's stream ends with the new role")
                            .in_scope(|| tracing::info!("the inner stream ends with the new role"));
                        return Ok(BuildReply::FabricPrimaryMoved { build_id: id.to_string(), fabric_primary: Some(me_id.clone()) });
                    }
                    match f {
                        BuildReply::Step { .. } | BuildReply::Pending { .. } => sink.data(f).await.map_err(gone)?,
                        terminal => return Ok(terminal),
                    }
                }
                let me_id = &self.env.birth.node_id;
                Err(HandlerFault::invariant_broken(format!("{me_id}: the run of mesh {mesh}'s section of Build {id} ended without a terminal")))
            }
        }
    }
}

/// A Build's phase as the wire names it.
fn phase_of(state: BuildState) -> BuildPhase {
    match state {
        BuildState::Pending => BuildPhase::Pending,
        BuildState::Running => BuildPhase::Running,
        BuildState::Complete => BuildPhase::Complete,
        BuildState::Failed => BuildPhase::Failed,
        BuildState::Incomplete => BuildPhase::Incomplete,
    }
}

/// Why the Build is where it is, and why it failed when it did, as its events say it.
fn reason_of(p: &crate::build_state::BuildProjection) -> (String, Option<String>) {
    let failure = p.failure.as_ref().map(|f| format!("could not start: {}", bounded_reason(f.reason.clone())))
        .or_else(|| (!p.unfinished.is_empty()).then(|| bounded_reason(p.unfinished.iter().map(|u| format!("mesh {}'s section ended at {} step {}: {}", u.mesh, u.operation, u.step, u.reason)).collect::<Vec<_>>().join("; "))));
    let reason = match p.state {
        BuildState::Pending => "accepted: no rdm.build.started written yet".to_string(),
        BuildState::Running => "rdm.build.started written: its steps are running".to_string(),
        BuildState::Complete => "rdm.build.ended complete".to_string(),
        BuildState::Failed => failure.clone().unwrap_or_else(|| "rdm.build.ended failed".into()),
        BuildState::Incomplete => failure.clone().unwrap_or_else(|| "rdm.build.ended incomplete".into()),
    };
    (reason, failure)
}

/// The Build's events as the wire carries them, each one envelope's JSON, in chunks that each fit a
/// reply frame with room to spare; always one chunk at least. Read from the events store.
async fn event_chunks(builds: &dyn crate::build_state::BuildStateAdapter, id: &BuildId) -> Result<(u32, Vec<Vec<String>>), crate::storage_error::StorageError> {
    let events = builds.events().of_build(id).await?;
    let mut chunks: Vec<Vec<String>> = vec![Vec::new()];
    let mut size = 0usize;
    for e in &events {
        let json = serde_json::to_string(e).expect("an event encodes");
        if size + json.len() > CHUNK_BYTES && !chunks.last().is_some_and(Vec::is_empty) {
            chunks.push(Vec::new());
            size = 0;
        }
        size += json.len();
        chunks.last_mut().expect("a chunk").push(json);
    }
    Ok((events.len() as u32, chunks))
}

/// The Build's topology as the wire carries it: every node's create spec in `(path, name)` order, in
/// chunks that each fit a reply frame with room to spare (a spec's JSON is longer than its postcard);
/// always one chunk at least.
fn topology_chunks(p: &crate::build_state::BuildProjection) -> Vec<Vec<rdm_core::meta::NodeSpec>> {
    let mut specs: Vec<&rdm_core::meta::NodeSpec> = p.topology.meshes.values().flat_map(|m| m.spec_by_path.values()).collect();
    specs.sort_by(|a, b| a.location().cmp(&b.location()));
    let mut chunks: Vec<Vec<rdm_core::meta::NodeSpec>> = vec![Vec::new()];
    let mut size = 0usize;
    for spec in specs {
        let weight = serde_json::to_vec(spec).map(|v| v.len()).unwrap_or(CHUNK_BYTES);
        if size + weight > CHUNK_BYTES && !chunks.last().is_some_and(Vec::is_empty) {
            chunks.push(Vec::new());
            size = 0;
        }
        size += weight;
        chunks.last_mut().expect("a chunk").push(spec.clone());
    }
    chunks
}

/// What one chunk of a Build read carries at most: half the 64 KiB reply frame.
const CHUNK_BYTES: usize = 32 * 1024;

/// `build.get` (Q167 R-RD5; node-rpc-envelope.md "Build, op `0x20`"), served by the fabric-primary from
/// its own Build log (its builds journal, which holds the Builds in flight and their history): every
/// Build whose header matches every pair of `filter`, newest first by acceptance, each whole. The
/// current Build is selected once for the read and every Build is read before `Started`, so a log that
/// cannot be read is refused `NotReady` and nothing streams; the stream is `Started` naming the current
/// Build, then per Build its `Topology` chunks, its `Events` chunks and its `Build` header, then `End`.
/// A forgotten Build has left the views and is passed over. A pair the Build header's schema refuses
/// is `RejectedFilter`, naming it. One `rdm.node_admin.build.resolve.via-build-get` span.
pub async fn serve_builds(me: &PathName, builds: &dyn crate::build_state::BuildStateAdapter, filter: rdm_core::filter::Filter, sink: ReplySink<Build, NotStarted>) -> Result<BuildReply, HandlerFault> {
    let span = tracing::info_span!(
        "rdm.node_admin.build.resolve.via-build-get",
        node_path = %me.path, node_name = %me.name,
        properties = %filter.properties(),
        current = tracing::field::Empty,
        held = tracing::field::Empty,
        builds = tracing::field::Empty,
        outcome = tracing::field::Empty,
    );
    let reply = async {
        if let Some(why) = filter.builds_refusal() {
            return Ok(rdm_core::reply::build::rejected_filter(bounded_reason(why)));
        }
        let unreadable = |what: String, e: BuildStateError| rdm_core::reply::build::not_ready(format!("path={:?} name={:?}: {what} could not be read for build.get: {e}", me.path, me.name));
        let current = match builds.current_build().await {
            Ok(c) => c.into_build_id(),
            Err(e) => return Ok(unreadable("the current Build".into(), e)),
        };
        let order = match builds.accepted_newest_first().await {
            Ok(o) => o,
            Err(e) => return Ok(unreadable("the acceptance order".into(), e)),
        };
        let mut held = Vec::new();
        for id in order {
            match builds.read_build(&id).await {
                Ok(p) => match event_chunks(builds, &id).await {
                    Ok(events) => held.push((p, events)),
                    Err(e) => return Ok(rdm_core::reply::build::not_ready(format!("path={:?} name={:?}: the events of Build {id} could not be read for build.get: {e}", me.path, me.name))),
                },
                Err(BuildStateError::UnknownBuild(_)) => {}
                Err(e) => return Ok(unreadable(format!("Build {id}"), e)),
            }
        }
        tracing::Span::current().record("current", current.as_ref().map(|c| c.to_string()).unwrap_or_default().as_str()).record("held", held.len());
        let mut sink = started(sink, BuildReply::Started { build_id: current.as_ref().map(|c| c.to_string()).unwrap_or_default(), disposition: Disposition::Attached }).await?;
        let mut streamed = 0u32;
        for (p, (events, chunks)) in held {
            let (reason, last_failure) = reason_of(&p);
            let header = rdm_core::build::BuildHeader {
                build_id: p.build_id.to_string(),
                phase: phase_of(p.state),
                reason,
                last_failure,
                current: current.as_ref() == Some(&p.build_id),
            };
            if !filter.matches(&header) {
                continue;
            }
            let topology = topology_chunks(&p);
            let count = topology.len() as u32;
            for (i, nodes) in topology.into_iter().enumerate() {
                sink.data(BuildReply::Topology { build_id: header.build_id.clone(), chunk_index: i as u32, chunk_count: count, nodes }).await.map_err(gone)?;
            }
            let chunk_count = chunks.len() as u32;
            for (i, events) in chunks.into_iter().enumerate() {
                sink.data(BuildReply::Events { build_id: header.build_id.clone(), chunk_index: i as u32, chunk_count, events }).await.map_err(gone)?;
            }
            let rdm_core::build::BuildHeader { build_id, phase, current, .. } = header;
            sink.data(BuildReply::Build { build_id, phase, current, events, chunks: chunk_count, change: p.change.clone(), time: p.time }).await.map_err(gone)?;
            streamed += 1;
        }
        tracing::Span::current().record("builds", streamed);
        Ok(BuildReply::End { builds: streamed })
    }
    .instrument(span.clone())
    .await;
    span.record("outcome", match &reply {
        Ok(r) => r.name(),
        Err(_) => "fault",
    });
    span.in_scope(|| tracing::info!("a build.get read ended"));
    reply
}

fn gone(e: rdm_transport::stream::SinkError) -> HandlerFault {
    HandlerFault::invariant_broken(format!("the caller left the stream, and the Build's drive goes on without it: {e:?}"))
}

async fn started(sink: ReplySink<Build, NotStarted>, frame: BuildReply) -> Result<ReplySink<Build, rdm_transport::stream::Streaming>, HandlerFault> {
    sink.started(frame).await.map_err(gone)
}

/// Read a drive's frames to the caller until its terminal. A role that moved ends the stream with
/// the typed `FabricPrimaryMoved` terminal naming the new role: the caller re-submits the Build id there.
async fn stream_frames(mut sink: ReplySink<Build, rdm_transport::stream::Streaming>, mut reader: crate::build_drive::DriveReader) -> Result<BuildReply, HandlerFault> {
    while let Some(t) = reader.next().await {
        match t {
            Read::Frame(f) => sink.data(f).await.map_err(gone)?,
            Read::Terminal(t) => return Ok(t),
            Read::RoleLost(loss) => {
                tracing::info_span!("rdm.node_admin.build.update.via-fabric-primary-changed", build_id = %reader.build_id().log(), to_node_id = ?loss.to, detail = %loss.why).in_scope(|| tracing::info!("the outer stream ends with the new role"));
                return Ok(BuildReply::FabricPrimaryMoved { build_id: reader.build_id().to_string(), fabric_primary: loss.to });
            }
        }
    }
    Err(HandlerFault::invariant_broken("the drive's frames ended without a terminal"))
}

/// Serve the Build family on this admin. Until `slot` is filled the admin holds no view and every
/// call is `NotReady` by name.
pub fn serve(b: ServerBuilder, slot: BuildSlot) -> ServerBuilder {
    b.serve_stream::<Build, _, _>(OpOwner::Product("rdm".into()), move |peer, req: BuildRequest, sink| {
        let slot = slot.clone();
        async move {
            let Some(door) = slot.get().cloned() else {
                return Ok(rdm_core::reply::build::not_ready("this admin holds no view yet".into()));
            };
            // The caller's endpoint key: a stop of this admin that the Build's run commands spares
            // its connection while it reads the Build's frames (`Drive::callers`).
            door.serve(Some(peer.endpoint_id), req, sink).await
        }
    })
}

/// What a section run in this process came to, as a dispatch: the fabric-primary running its own
/// mesh's section, or a fixture routing to a mesh-primary's door.
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
    async fn accepted(&mut self) {
        self.stream.accepted().await;
    }
}

/// Calls a mesh's primary with its section of a Build: in process when that is this admin, over Node
/// RPC otherwise.
pub struct RpcDispatcher {
    /// This admin's path.
    pub me_path: String,
    /// This node's name, beside its path.
    pub me_name: String,
    /// This admin's own door for `build.attempt.run`.
    pub local: Arc<RunDoor>,
    /// The client a call to another admin is made through.
    pub client: Arc<NodeRpcClient>,
    /// The observed topology.
    pub topology: Arc<RwLock<Topology>>,
    /// How the call that sends a mesh its section is sent (Q344, Q315): the fabric-primary's, as every
    /// caller sets its own; a spent call ends `NotSent` or `Indeterminate` and `on_call_failed`.
    pub send: rdm_transport::Send,
}

impl RpcDispatcher {
    /// The `(path, name)` lookup index key of me_path and me_name.
    pub fn me(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.me_path.clone(), name: self.me_name.clone() } }
}

#[async_trait::async_trait]
impl Dispatcher for RpcDispatcher {
    async fn dispatch(&self, build_id: &BuildId, mesh: &str, context: CallContext) -> Dispatched {
        if self.topology.read().await.is_mesh_primary(mesh, &self.local.exec.executor.node_id) {
            return local_dispatched(self.local.attempt_run(build_id, mesh, &context).await);
        }
        let req = BuildRequest::AttemptRun { build_id: build_id.to_string(), mesh: mesh.to_string(), context: context.clone() };
        let opts = CallOptions { send: self.send.clone(), context: Some(context), ..CallOptions::default() };
        // One call, addressed to the mesh's primary, under the call's one retry mechanism (Q344): its
        // attempts follow the birth the view names for the role; a spent call ends the section.
        let to = NodeTarget::Primary { mesh: Some(mesh.to_string()) };
        match self.client.call_stream::<Build>(&to, &req, &opts).await {
            // The mesh-primary writes its steps' events itself; the stream carries them as frames.
            Ok((stream, _)) => Dispatched::Stream(Box::new(RemoteStream { stream })),
            Err((RpcOutcome::Reply(r), _)) => Dispatched::Refused(r.value().clone()),
            Err((RpcOutcome::Unserved(u), _)) => Dispatched::Refused(rdm_core::reply::build::not_ready(format!("mesh {mesh}'s primary does not serve the Build family: {u:?}"))),
            Err((other, _)) => Dispatched::Unreached(format!("the call to mesh {mesh}'s primary ended {}: {other:?}", other.name())),
        }
    }

    async fn run_own(&self, turn: &crate::stop_gate::Turn, build_id: &BuildId, mesh: &str, context: CallContext, operations: Vec<BuildOperation>) -> Dispatched {
        local_dispatched(self.local.run_under(turn, build_id, mesh, &context, operations).await)
    }
}

/// The drive of the current Build, for the reconcile loop: `ensure` it while it is in progress
/// (R-BI3: the Build in progress is resumed), and once it is complete, the drive this admin holds of
/// it that has not ended (one that paused or stalled while the Build completed): its next pass reads
/// the Build complete and ends its callers' streams. A Build that completed once and whose later
/// run failed is the known state too, and only a caller's resubmit drives it on (R-BI3). The
/// Build as read decides, never the current-Build read before it: a verdict that landed between the
/// two reads leaves the Build no longer in progress, and nothing is driven
/// (`rdm.node_admin.build.reject.via-not-in-progress-when-read`).
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
        Ok(b) if !completed && !matches!(crate::build_state::current_of(&b), Some(crate::build_state::CurrentBuild::InProgress(_))) => {
            tracing::info_span!("rdm.node_admin.build.reject.via-not-in-progress-when-read", build_id = %b.build_id.log(), state = ?b.state)
                .in_scope(|| tracing::info!("the Build read after the current-Build read is no longer in progress: a failed Build is never current, and only a caller's resubmit drives it on (R-BI3)"));
            Vec::new()
        }
        Ok(b) => {
            let mut driven = vec![drives.ensure(env, &b.build_id)];
            // Every Build a fabric.delete interrupts that has not ended is driven on, a drive this admin
            // holds of it or a new one (a successor fabric-primary holds none), so each ends `incomplete`
            // naming the delete and its callers receive that terminal (build.md §1).
            if b.deletes_fabric() {
                let open = crate::build_state::interrupted_open(&*env.builds, &b).await.unwrap_or_default();
                let mut interrupted = drives.unended();
                interrupted.extend(open);
                interrupted.sort();
                interrupted.dedup();
                for held in interrupted.into_iter().filter(|h| *h != b.build_id) {
                    driven.push(drives.ensure(env, &held));
                }
            }
            driven
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accepted::{FabricTopology, MeshTopology};
    use crate::build::MeshDesired;
    use crate::build_state::{BuildAccepted, BuildStateAdapter as _, MemoryBuildStateAdapter};
    use crate::control_plane::InProgress;
    use crate::model::{MeshId, NodeId};
    use rdm_core::meta::NodeSpec;
    use std::collections::{BTreeMap, BTreeSet};

    /// CONTRACT: docs/architecture/build.md:453
    /// "One writer per key: the fabric-primary writes the Fabric record and every topology attempt's Build facts"
    #[tokio::test]
    async fn refusal_reply_and_with_entries_translate_refusals_and_populate_plan_entry_specs() {
        // 1. `refusal_reply` maps each control-plane `Refusal` variant to its typed `BuildReply`.
        let actor_id = rdm_core::NodeId::mint();
        let held_id = BuildId::mint();
        assert!(matches!(
            refusal_reply(Refusal::BadRequest("bad".into())),
            BuildReply::Rejected { ref reason, ref detail } if reason == "invalid-request" && detail == "bad"
        ));
        assert!(matches!(
            refusal_reply(Refusal::NotFound("missing".into())),
            BuildReply::Rejected { ref reason, ref detail } if reason == "not-found" && detail == "missing"
        ));
        assert!(matches!(
            refusal_reply(Refusal::Conflict("clash".into())),
            BuildReply::Rejected { ref reason, ref detail } if reason == "conflict" && detail == "clash"
        ));
        assert!(matches!(
            refusal_reply(Refusal::BuildInProgress(InProgress {
                current_build_id: held_id.clone(),
            })),
            BuildReply::BuildInProgress { ref current_build_id, .. } if current_build_id == held_id.as_str()
        ));
        assert!(matches!(
            refusal_reply(Refusal::NotPrimary(Some(actor_id.clone()))),
            BuildReply::NotFabricPrimary { fabric_primary: Some(ref fp) } if fp == &actor_id
        ));
        assert!(matches!(
            refusal_reply(Refusal::Unavailable("wait".into())),
            BuildReply::NotReady { ref reason } if reason == "wait"
        ));
        assert!(matches!(
            refusal_reply(Refusal::State(BuildStateError::RoleYielded {
                node: actor_id.clone(),
                by: "revision 2".into(),
                to: None,
            })),
            BuildReply::RoleYielded { .. }
        ));
    }
}

