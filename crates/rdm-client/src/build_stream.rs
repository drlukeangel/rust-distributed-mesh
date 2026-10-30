//! The typed client of the Build family on op `0x20` (node-rpc-envelope.md "Build, op `0x20`").
//!
//! A [`BuildCarrier`] opens one call to the fabric-primary: `build.create` (a change, or a re-submit
//! by build id), `build.get` and `build.delete`. A call that changes something is a stream
//! ([`BuildStream`]): `Started`, then a frame per step as its `Complete` receipt becomes durable on
//! the executor that wrote it, then `Complete` or `Failed { step, reason }`. The caller waits on the
//! stream and reads no Build and no gossip.
//!
//! Every progress frame carries `(build_id, attempt, operation, step)`; the stream drops a frame
//! whose key it already delivered. A broken stream is `Indeterminate` (the request committed) or
//! `NotSent` (it provably reached no handler), never a failed step. A call refused by a node that
//! is not the fabric-primary follows the redirect it names, once for each admin it names.

use crate::calls::ended;
use crate::{BuildId, CallEnd};
use rdm_core::PathName;
use rdm_transport::stream::{ReplyStream, StreamFailure, StreamItem};
use rdm_transport::{Budget, CallOptions, NodeRpcClient, NodeTarget};
use rdm_core::build::{Build as BuildFamily, BuildChange, BuildPhase, BuildReply, BuildRequest, BuildSubmit, Disposition, StepReceipt};
use rdm_core::filter::Filter;
use rdm_core::streaming::StreamingProtocol as _;
use rdm_core::meta::NodeSpec;
use rdm_core::outcome::RpcOutcome;
use rdm_core::protocol::NodeProtocol;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// What `build.create` submits (`Change`) and how the accepted Build started (`StartDisposition`).
pub use rdm_core::build::{BuildChange as Change, Disposition as StartDisposition};

/// The bound on connecting to the fabric-primary and writing the request. The stream after it has
/// no deadline: the transport's own liveness ends a dead one.
pub const SEND: Duration = Duration::from_secs(10);

/// How the typed `node` and `build` objects reach the fabric-primary's Build family.
#[derive(Clone)]
pub struct BuildCarrier {
    client: Arc<NodeRpcClient>,
    fabric_primary: PathName,
}

impl std::fmt::Debug for BuildCarrier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuildCarrier").field("fabric_primary", &self.fabric_primary).finish()
    }
}

/// A frame of a Build stream after `Started`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildFrame {
    /// A step completed; its receipt is durable on the executor that wrote it.
    Step {
        /// The attempt.
        attempt: u32,
        /// The operation's idempotency key.
        operation: String,
        /// The step.
        step: String,
    },
    /// The step in flight is blocked: live progress, never a step outcome.
    Blocked {
        /// The attempt.
        attempt: u32,
        /// The operation.
        operation: String,
        /// The blocked step.
        step: String,
        /// What it waits for.
        reason: String,
    },
    /// A chunk of the receipts a `build.get` reads.
    Steps(Vec<StepReceipt>),
    /// Terminal: the Build converged.
    Complete {
        /// The attempt that converged it.
        attempt: u32,
    },
    /// Terminal: a step failed.
    Failed {
        /// The attempt that failed.
        attempt: u32,
        /// The operation the failed step belongs to.
        operation: String,
        /// The step that failed.
        step: String,
        /// The attempt's reason.
        reason: String,
        /// The original failed step code.
        code: rdm_core::outcome::Code,
    },
    /// Terminal of `build.get`: the Build's header.
    Got {
        /// Where the Build is in its life.
        phase: BuildPhase,
        /// The current attempt.
        attempt: u32,
        /// The executor of the current attempt.
        executor: Option<String>,
        /// Why the current attempt exists.
        reason: String,
        /// Why the last attempt failed.
        last_failure: Option<String>,
        /// The number of receipts sent.
        steps: u32,
        /// The number of chunks sent.
        chunks: u32,
    },
    /// Terminal of `build.create`: the fabric-primary moved while the Build ran (`node-rpc-envelope.md` "Frames, durable receipts and reattachment").
    /// Deliberate outer-stream end naming the new fabric-primary; the caller resubmits the same `build_id` there.
    FabricPrimaryMoved {
        /// The node-admin (`path.name`) that is the fabric-primary now, when the old fabric-primary's view named one.
        fabric_primary: Option<String>,
    },
    /// Terminal of `build.delete`.
    Deleted,
}

/// One accepted Build call: its `Started` and the frames after it.
pub struct BuildStream {
    stream: ReplyStream<BuildFamily>,
    /// The carrier the call was opened over. A QUIC connection does not keep its endpoint: the
    /// last handle of the endpoint ends its runtime and with it every stream it carries. The stream
    /// holds the carrier, so a caller that keeps only the stream reads it to its terminal.
    _carrier: BuildCarrier,
    build_id: BuildId,
    attempt: u32,
    disposition: Disposition,
    seen: HashSet<(String, u32, String, String, String)>,
    ended: bool,
}

impl BuildStream {
    /// The Build the call follows.
    pub fn build_id(&self) -> &BuildId {
        &self.build_id
    }

    /// The attempt the call opened (or, for a re-submit, the attempt the caller named).
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// How the call came to the Build.
    pub fn disposition(&self) -> Disposition {
        self.disposition
    }

    /// The next frame, `None` after the terminal. A stream that breaks is `Indeterminate`: the call
    /// committed, and a lost read proves nothing about the step in flight.
    pub async fn next(&mut self) -> Option<Result<BuildFrame, CallEnd>> {
        if self.ended {
            return None;
        }
        loop {
            let item = self.stream.next().await?;
            let reply = match item {
                StreamItem::Frame(_, r) => r,
                StreamItem::Failed(f) => {
                    self.ended = true;
                    return Some(Err(CallEnd::Indeterminate { code: f.code(), reason: format!("the Build stream broke before its terminal: {f:?}") }));
                }
            };
            match reply {
                BuildReply::Step { attempt, operation, step, .. } => {
                    if !self.seen.insert(("step".into(), attempt, operation.clone(), step.clone(), String::new())) {
                        continue;
                    }
                    return Some(Ok(BuildFrame::Step { attempt, operation, step }));
                }
                BuildReply::Blocked { attempt, operation, step, reason, .. } => {
                    if !self.seen.insert(("blocked".into(), attempt, operation.clone(), step.clone(), reason.clone())) {
                        continue;
                    }
                    return Some(Ok(BuildFrame::Blocked { attempt, operation, step, reason }));
                }
                BuildReply::Steps { steps, .. } => return Some(Ok(BuildFrame::Steps(steps))),
                BuildReply::Complete { attempt, .. } => {
                    self.ended = true;
                    return Some(Ok(BuildFrame::Complete { attempt }));
                }
                BuildReply::Failed { attempt, operation, step, reason, code, .. } => {
                    self.ended = true;
                    return Some(Ok(BuildFrame::Failed { attempt, operation, step, reason, code }));
                }
                BuildReply::Got { phase, attempt, executor, reason, last_failure, steps, chunks, .. } => {
                    self.ended = true;
                    return Some(Ok(BuildFrame::Got { phase, attempt, executor, reason, last_failure, steps, chunks }));
                }
                BuildReply::Deleted { .. } => {
                    self.ended = true;
                    return Some(Ok(BuildFrame::Deleted));
                }
                BuildReply::FabricPrimaryMoved { fabric_primary, .. } => {
                    self.ended = true;
                    return Some(Ok(BuildFrame::FabricPrimaryMoved { fabric_primary }));
                }
                other => {
                    self.ended = true;
                    return Some(Err(CallEnd::Rejected { reason: other.name().into(), detail: format!("an unexpected frame in a Build stream: {other:?}") }));
                }
            }
        }
    }
}

/// The refusal `reply` is, as the call ends.
fn refusal(reply: BuildReply) -> CallEnd {
    if let BuildReply::BuildInProgress { current_build_id, .. } = &reply {
        return match BuildId::parse(current_build_id) {
            Ok(current) => CallEnd::BuildInProgress { current },
            Err(e) => CallEnd::Rejected { reason: "malformed-build-id".into(), detail: format!("the fabric-primary answered BuildInProgress naming a Build id that is not a Crockford id: {e}") },
        };
    }
    let detail = match &reply {
        BuildReply::NotFabricPrimary { fabric_primary } => format!("the receiver is not the fabric-primary; it sees {}", fabric_primary.as_deref().unwrap_or("none")),
        BuildReply::NotExecutor { named, recipient } => format!("the claim names {named}, the recipient is {recipient}"),
        BuildReply::StaleClaim { held_attempt, held_executor, carried_attempt } => format!("the recipient holds attempt {held_attempt} for {}, the call carried {carried_attempt}", held_executor.as_deref().unwrap_or("no executor")),
        BuildReply::UnknownBuild { build_id } => format!("the fabric-primary holds no fact of Build {build_id}"),
        BuildReply::Rejected { reason, detail } => format!("{reason}: {detail}"),
        BuildReply::BuildInProgress { current_build_id, mesh, held_by } => match mesh {
            Some(mesh) => format!("an attempt of Build {current_build_id} holds mesh {mesh} (claimed by {}); one attempt in flight per mesh", held_by.as_deref().unwrap_or("no recorded claimant")),
            None => format!("Build {current_build_id} is still reconciling; one Build at a time"),
        },
        BuildReply::NotClaimant { mesh, fabric_primary, mesh_primary } => format!(
            "the receiver may not claim on mesh {}: a topology attempt is the fabric-primary's ({}), a drift attempt the mesh-primary's ({})",
            mesh.as_deref().unwrap_or("unknown"),
            fabric_primary.as_deref().unwrap_or("none seen"),
            mesh_primary.as_deref().unwrap_or("none seen")
        ),
        BuildReply::AttemptTaken { detail } => detail.clone(),
        BuildReply::RoleYielded { node, by } => format!("{node} is no longer the fabric-primary: {by}"),
        BuildReply::CannotDelete { build_id, reason } => format!("Build {build_id} stays in history: {reason}"),
        BuildReply::PeerUnresolved { reason } | BuildReply::NotReady { reason } | BuildReply::Busy { reason } | BuildReply::ConnectionsDeleting { reason } | BuildReply::Unauthorized { reason } => reason.clone(),
        BuildReply::Malformed { kind } => format!("{kind:?}"),
        other => format!("{other:?}"),
    };
    // A topology rule's own name is the reason: `unknown-node` and `unheard-mesh` read as themselves.
    let reason = match &reply {
        BuildReply::Rejected { reason, .. } => reason.clone(),
        other => other.name().to_string(),
    };
    CallEnd::Rejected { reason, detail }
}

/// A followed Build: every frame of every attachment, in order, each step once, and how many times
/// the watcher reattached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Followed {
    /// The frames, the terminal last.
    pub frames: Vec<BuildFrame>,
    /// The reattachments made: one per stream that broke or ended `FabricPrimaryMoved`.
    pub reattached: u32,
}

/// What one reattachment read and opened (node-rpc-envelope.md "Frames, durable receipts and
/// reattachment": "`build.get [id: build_id]` for the receipts, then the outer stream again, resuming from the
/// receipts").
pub struct Reattached {
    /// The receipts `build.get` read, every attempt's, in order.
    pub receipts: Vec<StepReceipt>,
    /// The outer stream, opened again from the current attempt the receipts name.
    pub stream: BuildStream,
}

/// Follow `stream` to its terminal (build.md: "a live run is reattached, never re-claimed"; i146 Q22:
/// the reattach after a `Budget::Stream` connection closes is the owner's). A stream that breaks
/// before its terminal (its connection closed: `Indeterminate`), ends without one, or ends
/// `FabricPrimaryMoved` is reattached through `reattach` by its `build_id` from the attempt it carried: the
/// receipts it reads are kept as step frames, each completed step once, then the stream it opens again
/// is read on, a step it replays kept once. The terminal is `Complete`, `Failed`, `Got` or `Deleted`.
/// `Err` is a reattachment refused or not made, named: nothing here waits or tries again on its own.
pub async fn follow_with<F, Fut>(mut stream: BuildStream, mut reattach: F) -> Result<Followed, CallEnd>
where
    F: FnMut(BuildId, u32) -> Fut,
    Fut: std::future::Future<Output = Result<Reattached, CallEnd>>,
{
    let (build_id, attempt) = (stream.build_id().clone(), stream.attempt());
    let mut out = Followed { frames: Vec::new(), reattached: 0 };
    let mut steps = HashSet::new();
    loop {
        while let Some(frame) = stream.next().await {
            let frame = match frame {
                Ok(f) => f,
                Err(CallEnd::Indeterminate { .. }) => break,
                Err(e) => return Err(e),
            };
            if let BuildFrame::Step { attempt, operation, step } = &frame {
                if !steps.insert((*attempt, operation.clone(), step.clone())) {
                    continue;
                }
            }
            let moved = matches!(frame, BuildFrame::FabricPrimaryMoved { .. });
            let terminal = matches!(frame, BuildFrame::Complete { .. } | BuildFrame::Failed { .. } | BuildFrame::Got { .. } | BuildFrame::Deleted);
            out.frames.push(frame);
            if terminal {
                return Ok(out);
            }
            if moved {
                break;
            }
        }
        out.reattached += 1;
        let again = reattach(build_id.clone(), attempt).await?;
        for r in again.receipts.into_iter().filter(|r| r.result == rdm_core::build::StepResult::Complete) {
            if steps.insert((r.attempt, r.operation.clone(), r.step.clone())) {
                out.frames.push(BuildFrame::Step { attempt: r.attempt, operation: r.operation, step: r.step });
            }
        }
        stream = again.stream;
    }
}

impl BuildCarrier {
    /// [`follow_with`] through this carrier, each reattachment [`Self::reattach`].
    pub async fn follow(&self, stream: BuildStream) -> Result<Followed, CallEnd> {
        follow_with(stream, |id, _from| async move { self.reattach(&id).await }).await
    }

    /// One reattachment by `build_id` (node-rpc-envelope.md "Frames, durable receipts and
    /// reattachment"): `build.get [id: build_id]` for the receipts, then the outer stream again, a
    /// re-submit from the current attempt the receipts name. Never a new execution: the re-submit of a
    /// Build already held carries its frames.
    pub async fn reattach(&self, build_id: &BuildId) -> Result<Reattached, CallEnd> {
        let got = self.build(build_id).await?;
        let stream = self.resubmit(build_id, got.attempt).await?;
        Ok(Reattached { receipts: got.steps, stream })
    }

    /// A carrier over `client`, starting at the fabric-primary `fabric_primary`.
    pub fn new(client: Arc<NodeRpcClient>, fabric_primary: PathName) -> Self {
        Self { client, fabric_primary }
    }

    /// The node-RPC client the carrier calls with: a caller that reads a node-admin's own log (the
    /// Build journal over `FetchBuildFacts`, which only the node-admin's core can decode) reaches the
    /// admin the carrier starts at through it.
    pub fn client(&self) -> &NodeRpcClient {
        &self.client
    }

    /// The fabric-primary calls start at.
    pub fn fabric_primary(&self) -> &PathName {
        &self.fabric_primary
    }

    /// Open `req` at the fabric-primary: follow the redirect of a node that is not it, once for each
    /// admin named, and read the `Started` frame.
    async fn open(&self, req: BuildRequest) -> Result<BuildStream, CallEnd> {
        let (stream, build_id, attempt, disposition) = self.open_started(req).await?;
        let build_id = BuildId::parse(&build_id)
            .map_err(|e| CallEnd::Rejected { reason: "malformed-build-id".into(), detail: format!("the fabric-primary started a Build under an id that is not a Crockford id: {e}") })?;
        Ok(BuildStream { stream, _carrier: self.clone(), build_id, attempt, disposition, seen: HashSet::new(), ended: false })
    }

    /// [`Self::open`]'s redirect and `Started` read, the stream and the `Started` frame's fields as sent.
    async fn open_started(&self, req: BuildRequest) -> Result<(ReplyStream<BuildFamily>, String, u32, Disposition), CallEnd> {
        let mut asked: BTreeSet<PathName> = BTreeSet::new();
        let mut target = self.fabric_primary.clone();
        loop {
            asked.insert(target.clone());
            let opts = CallOptions { budget: Budget::Stream { send: SEND }, ..CallOptions::default() };
            // The first reply of the call: an early refusal of the receiving node, or the first frame
            // of the stream (`Started`, or the handler's own refusal).
            // The frames are the executor's receipts, recorded before they are sent; this client records
            // nothing of them: reading a frame is its recovery (node-rpc.md §43).
            let (first, stream) = match self.client.call_stream::<BuildFamily>(&NodeTarget::CurrentPath(target.clone()), &req, &opts).await.map(|(s, e)| (s.read_only(), e)) {
                Err((RpcOutcome::Reply(r), _)) => (r.into_value(), None),
                Err((other, _)) => return Err(ended(other).err().unwrap_or_else(|| CallEnd::Indeterminate { code: rdm_core::outcome::Code::Internal, reason: "an early reply that is no refusal".into() })),
                Ok((mut stream, _)) => match stream.next().await {
                    Some(StreamItem::Frame(_, f)) => (f, Some(stream)),
                    // A node that does not serve the op answers 421: nothing was dispatched.
                    Some(StreamItem::Failed(StreamFailure::Unserved(_))) => return Err(CallEnd::Unserved { op: BuildFamily::OP }),
                    Some(StreamItem::Failed(f)) => return Err(CallEnd::Indeterminate { code: f.code(), reason: format!("the stream broke before its Started frame: {f:?}") }),
                    None => return Err(CallEnd::Indeterminate { code: rdm_core::outcome::Code::Unknown, reason: "the stream ended before its Started frame".into() }),
                },
            };
            match (first, stream) {
                (BuildReply::Started { build_id, attempt, disposition }, Some(stream)) => return Ok((stream, build_id, attempt, disposition)),
                (BuildReply::NotFabricPrimary { fabric_primary: Some(named) }, _) => match named.parse::<PathName>() {
                    Ok(next) if !asked.contains(&next) => target = next,
                    Ok(_) => return Err(CallEnd::Rejected { reason: "not-fabric-primary".into(), detail: format!("{target} is not the fabric-primary and names {named}, which was already asked ({asked:?})") }),
                    Err(e) => return Err(CallEnd::Rejected { reason: "not-fabric-primary".into(), detail: format!("{target} is not the fabric-primary and names {named:?}, which is not a path.name: {e}") }),
                },
                (other, _) => return Err(refusal(other)),
            }
        }
    }

    /// `build.create` (`node-rpc-envelope.md` "Build, op `0x20`"): accept `change` and follow the Build until it ends.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rdm_client::{birth_of_line, BuildCarrier, Change, Followed, Peers};
    ///
    /// async fn delete_fabric(admin_stdout_line: &str) -> Result<Followed, String> {
    ///     let birth = birth_of_line(admin_stdout_line).ok_or("not a birth line")??;
    ///     let peers = Peers::at(&birth).await?;
    ///     let carrier: BuildCarrier = peers.carrier_at(birth.name);
    ///     let stream = carrier.create(Change::DeleteFabric).await.map_err(|e| e.to_string())?;
    ///     carrier.follow(stream).await.map_err(|e| e.to_string())
    /// }
    /// ```
    pub async fn create(&self, change: BuildChange) -> Result<BuildStream, CallEnd> {
        self.open(BuildRequest::Create { submit: BuildSubmit::Change(change) }).await
    }

    /// `build.create` for a Build the caller already holds the id of: never a second Build. The
    /// stream carries the frames of `from_attempt` and later attempts.
    pub async fn resubmit(&self, build_id: &BuildId, from_attempt: u32) -> Result<BuildStream, CallEnd> {
        self.open(BuildRequest::Create { submit: BuildSubmit::Resubmit { build_id: build_id.to_string(), from_attempt } }).await
    }

    /// `build.get` (Q167 R-RD5): every Build the fabric-primary holds whose header matches every pair
    /// of `filter` (`id`, `current`, `phase`, `attempt`, `executor`), newest first, each whole: its
    /// header, its topology and its receipts, the current one marked. Only `End` completes the read: a
    /// stream that ends before it, or whose chunks do not add up to the headers that close them, is an
    /// incomplete read (`Indeterminate`), never a list.
    pub async fn builds(&self, filter: &Filter) -> Result<Vec<Build>, CallEnd> {
        let (mut stream, _current, _attempt, _disposition) = self.open_started(BuildRequest::GetBuilds { filter: filter.clone() }).await?;
        let incomplete = |reason: String| CallEnd::Indeterminate { code: rdm_core::outcome::Code::Unknown, reason: format!("an incomplete build.get read: {reason}") };
        let mut out: Vec<Build> = Vec::new();
        // The Build whose chunks are arriving: its id, its topology chunks and nodes, its receipt chunks and receipts.
        let mut open: Option<(String, u32, Vec<NodeSpec>, u32, Vec<StepReceipt>)> = None;
        loop {
            let reply = match stream.next().await {
                Some(StreamItem::Frame(_, r)) => r,
                Some(StreamItem::Failed(f)) => return Err(CallEnd::Indeterminate { code: f.code(), reason: format!("the build.get stream broke before its End: {f:?}") }),
                None => return Err(incomplete("the stream ended before its End".into())),
            };
            match reply {
                BuildReply::Topology { build_id, chunk_index, chunk_count: _, nodes } => {
                    let o = open.get_or_insert_with(|| (build_id.clone(), 0, Vec::new(), 0, Vec::new()));
                    if o.0 != build_id || o.1 != chunk_index {
                        return Err(incomplete(format!("topology chunk {chunk_index} of Build {build_id} arrived where chunk {} of Build {} was owed", o.1, o.0)));
                    }
                    o.1 += 1;
                    o.2.extend(nodes);
                }
                BuildReply::Steps { build_id, chunk_index, chunk_count: _, steps } => {
                    let o = open.get_or_insert_with(|| (build_id.clone(), 0, Vec::new(), 0, Vec::new()));
                    if o.0 != build_id || o.3 != chunk_index {
                        return Err(incomplete(format!("receipt chunk {chunk_index} of Build {build_id} arrived where chunk {} of Build {} was owed", o.3, o.0)));
                    }
                    o.3 += 1;
                    o.4.extend(steps);
                }
                BuildReply::Build { build_id, phase, attempt, executor, reason, last_failure, current, steps, chunks } => {
                    let (id, _topology_chunks, nodes, receipt_chunks, receipts) = open.take().unwrap_or_else(|| (build_id.clone(), 0, Vec::new(), 0, Vec::new()));
                    if id != build_id || receipt_chunks != chunks || receipts.len() != steps as usize {
                        return Err(incomplete(format!("Build {build_id}'s header counts {steps} receipts in {chunks} chunks, and {} in {receipt_chunks} chunks of Build {id} arrived", receipts.len())));
                    }
                    let build_id = BuildId::parse(&build_id).map_err(|e| CallEnd::Rejected { reason: "malformed-build-id".into(), detail: format!("build.get answered a Build id that is not a Crockford id: {e}") })?;
                    out.push(Build { build_id, phase, attempt, executor, reason, last_failure, current, nodes, steps: receipts });
                }
                BuildReply::End { builds } => {
                    if let Some((id, ..)) = open {
                        return Err(incomplete(format!("End arrived with Build {id}'s chunks unclosed by its header")));
                    }
                    if builds as usize != out.len() {
                        return Err(incomplete(format!("End counts {builds} Builds and {} arrived", out.len())));
                    }
                    return Ok(out);
                }
                other if matches!(BuildFamily::frame_kind(&other), rdm_core::streaming::FrameKind::Refusal(_)) => return Err(refusal(other)),
                other => return Err(CallEnd::Rejected { reason: "unexpected-frame".into(), detail: format!("a build.get stream carried {other:?}") }),
            }
        }
    }

    /// `build.get [id: build_id]`: the one Build named, whole; refused `unknown-build` when the
    /// fabric-primary holds none of that id.
    pub async fn build(&self, build_id: &BuildId) -> Result<Build, CallEnd> {
        self.builds(&Filter::new().eq("id", [build_id.to_string()])).await?.into_iter().next().ok_or_else(|| refusal(rdm_core::reply::build::unknown_build(build_id.to_string())))
    }

    /// `build.delete`: a finished Build leaves history.
    pub async fn delete(&self, build_id: &BuildId) -> Result<(), CallEnd> {
        let mut stream = self.open(BuildRequest::Delete { build_id: build_id.to_string() }).await?;
        match stream.next().await {
            Some(Ok(BuildFrame::Deleted)) => Ok(()),
            Some(Ok(other)) => Err(CallEnd::Rejected { reason: "unexpected-frame".into(), detail: format!("a build.delete stream carried {other:?}") }),
            Some(Err(e)) => Err(e),
            None => Err(CallEnd::Indeterminate { code: rdm_core::outcome::Code::Unknown, reason: "the build.delete stream ended without its terminal".into() }),
        }
    }
}

/// One Build as `build.get` reads it whole (Q167 R-RD5): its header, its topology and its receipts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Build {
    /// The Build.
    pub build_id: BuildId,
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
    /// Its topology: every node's create spec, in `path.name` order.
    pub nodes: Vec<NodeSpec>,
    /// The receipts of every attempt, in order.
    pub steps: Vec<StepReceipt>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdm_core::{IncarnationId, NodeId};
    use rdm_transport::{HandlerFault, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
    use rdm_core::catalog::OpOwner;

    /// CONTRACT (node-rpc-envelope.md "Build, op `0x20`": the caller "waits on the stream" of a
    /// `build.create`, and a frame the server wrote reaches it): a Build stream is the whole of the
    /// accepted call. The caller keeps only the stream and drops the carrier it opened it with (the
    /// `node` objects, their Node RPC client and endpoint); every frame the fabric-primary writes
    /// after that still reaches the caller, in order, up to the terminal.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_build_stream_whose_carrier_was_dropped_still_delivers_every_frame_to_its_terminal() {
        let (node_id, incarnation) = (NodeId::mint(), IncarnationId::mint());
        let go = Arc::new(tokio::sync::Notify::new());
        let g = go.clone();
        let server = ServerBuilder::new()
            .serve_stream::<BuildFamily, _, _>(OpOwner::Product("rdm".into()), move |_peer, _req: BuildRequest, sink| {
                let g = g.clone();
                async move {
                    let gone = |e| HandlerFault::invariant_broken(format!("{e:?}"));
                    let mut sink = sink.started(BuildReply::Started { build_id: "cq2tyb03krac".into(), attempt: 3, disposition: Disposition::Created }).await.map_err(gone)?;
                    // The frames after `Started` are written once the caller holds only the stream.
                    g.notified().await;
                    for step in ["AllocateIdentity", "WaitForMeshJoin", "Complete"] {
                        sink.data(BuildReply::Step { build_id: "cq2tyb03krac".into(), attempt: 3, operation: "node.create:mesh1.admin.1".into(), step: step.into(), receipt: None }).await.map_err(gone)?;
                    }
                    Ok(BuildReply::FabricPrimaryMoved { build_id: "cq2tyb03krac".into(), fabric_primary: Some("mesh1.admin.2".into()) })
                }
            })
            .build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
            .unwrap();
        let key = iroh::SecretKey::generate();
        let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let _router = iroh::protocol::Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
        let fp: PathName = "mesh1.admin.1".parse().unwrap();
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(ResolvedNode { node_id, name: fp.clone(), endpoint_id: key.public(), transport_addr: addr, incarnation });
        let cep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let carrier = BuildCarrier::new(Arc::new(NodeRpcClient::new(cep, resolver).with_caller_system("rdm")), fp);
        let mut stream = carrier.create(BuildChange::Stop { node: "mesh1.admin.1".parse().unwrap() }).await.expect("the call was accepted");
        assert_eq!((stream.build_id().as_str(), stream.attempt()), ("cq2tyb03krac", 3));
        drop(carrier);
        go.notify_one();
        // A loopback frame arrives in milliseconds; the bound only names what arrived if it never does.
        let mut got = Vec::new();
        let read = tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(f) = stream.next().await {
                got.push(f);
            }
        })
        .await;
        let step = |s: &str| Ok(BuildFrame::Step { attempt: 3, operation: "node.create:mesh1.admin.1".into(), step: s.into() });
        assert!(read.is_ok(), "the stream delivered no terminal within 10 s; it delivered {got:?}");
        assert_eq!(got, vec![step("AllocateIdentity"), step("WaitForMeshJoin"), step("Complete"), Ok(BuildFrame::FabricPrimaryMoved { fabric_primary: Some("mesh1.admin.2".into()) })]);
    }
}
