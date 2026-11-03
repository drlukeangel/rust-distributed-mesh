//! The typed client of the Build family on op `0x20` (node-rpc-envelope.md "Build, op `0x20`").
//!
//! A [`BuildCarrier`] opens one call to the fabric-primary: `build.create` (a change, or a re-submit
//! by build id), `build.get` and `build.delete`. A call that changes something is a stream
//! ([`BuildStream`]): `Started`, then a frame per step once its `rdm.build.step` event is written,
//! then `Complete`, `Incomplete { sections }` or `Failed { reason }` (Q346). The caller waits on the
//! stream and reads no Build and no gossip.
//!
//! Every progress frame carries `(build_id, mesh, operation, step)`; the stream drops a frame
//! whose key it already delivered. A broken stream is `Indeterminate` (the request committed) or
//! `NotSent` (it provably reached no handler), never a failed step. A call refused by a node that
//! is not the fabric-primary follows the redirect it names, once for each admin it names.

use crate::calls::ended;
use crate::{BuildId, CallEnd};
use rdm_core::PathName;
use rdm_transport::stream::{ReplyStream, StreamFailure, StreamItem};
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget};
use rdm_core::build::{Build as BuildFamily, BuildChange, BuildPhase, BuildReply, BuildRequest, BuildSubmit, Disposition, StepResult};
use rdm_core::filter::Filter;
use rdm_core::streaming::StreamingProtocol as _;
use rdm_core::meta::NodeSpec;
use rdm_core::outcome::RpcOutcome;
use rdm_core::protocol::NodeProtocol;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

/// What `build.create` submits (`Change`) and how the accepted Build started (`StartDisposition`).
pub use rdm_core::build::{BuildChange as Change, Disposition as StartDisposition};

/// How the typed `node` and `build` objects reach the fabric-primary's Build family.
#[derive(Clone)]
pub struct BuildCarrier {
    client: Arc<NodeRpcClient>,
    target: NodeTarget,
}

impl std::fmt::Debug for BuildCarrier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuildCarrier").field("target", &self.target).finish()
    }
}

/// A frame of a Build stream after `Started`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildFrame {
    /// A step ended; its `rdm.build.step` event is written.
    Step {
        /// The mesh whose section the step belongs to.
        mesh: String,
        /// The step's operation, `noun.verb:<node_id>`.
        operation: String,
        /// The step.
        step: String,
        /// How the step ended.
        result: StepResult,
    },
    /// The step in flight is pending: its node's `pre_node_ready` answered `Pending`. Live progress,
    /// never a step outcome.
    Pending {
        /// The mesh whose section the step belongs to.
        mesh: String,
        /// The operation.
        operation: String,
        /// The pending step.
        step: String,
        /// What it waits for.
        reason: String,
    },
    /// A chunk of the events a `build.get` reads, each the events store's envelope.
    Events(Vec<serde_json::Value>),
    /// Terminal: the Build converged.
    Complete,
    /// Terminal: the Build could not start (Q346).
    Failed {
        /// Why it could not start.
        reason: String,
        /// The standard code captured where it could not start.
        code: rdm_core::outcome::Code,
    },
    /// Terminal: the Build ran and not every section finished; what ran stands (Q346).
    Incomplete {
        /// Each section that ended short of its end: its mesh, operation, step, reason and code.
        sections: Vec<rdm_core::build::UnfinishedSection>,
    },
    /// Terminal of `build.get`: the Build's header.
    Got {
        /// Where the Build is in its life.
        phase: BuildPhase,
        /// Why the Build is where it is.
        reason: String,
        /// Why the Build failed, when it did.
        last_failure: Option<String>,
        /// The number of events sent.
        events: u32,
        /// The number of chunks sent.
        chunks: u32,
    },
    /// Terminal of `build.create`: the fabric-primary moved while the Build ran (`node-rpc-envelope.md` "Frames, durable events and reattachment").
    /// Deliberate outer-stream end naming the new fabric-primary; the caller resubmits the same `build_id` there.
    FabricPrimaryMoved {
        /// The node-admin (`(path, name)`) that is the fabric-primary now, when the old fabric-primary's view named one.
        fabric_primary: Option<rdm_core::NodeId>,
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
    disposition: Disposition,
    seen: HashSet<(String, String, String, String, String)>,
    ended: bool,
}

impl BuildStream {
    /// The Build the call follows.
    pub fn build_id(&self) -> &BuildId {
        &self.build_id
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
                BuildReply::Step { mesh, operation, step, result, .. } => {
                    if !self.seen.insert(("step".into(), mesh.clone(), operation.clone(), step.clone(), String::new())) {
                        continue;
                    }
                    return Some(Ok(BuildFrame::Step { mesh, operation, step, result }));
                }
                BuildReply::Pending { mesh, operation, step, reason, .. } => {
                    if !self.seen.insert(("pending".into(), mesh.clone(), operation.clone(), step.clone(), reason.clone())) {
                        continue;
                    }
                    return Some(Ok(BuildFrame::Pending { mesh, operation, step, reason }));
                }
                BuildReply::Events { events, .. } => match parse_events(events) {
                    Ok(events) => return Some(Ok(BuildFrame::Events(events))),
                    Err(e) => {
                        self.ended = true;
                        return Some(Err(e));
                    }
                },
                BuildReply::Complete { .. } => {
                    self.ended = true;
                    return Some(Ok(BuildFrame::Complete));
                }
                BuildReply::Failed { reason, code, .. } => {
                    self.ended = true;
                    return Some(Ok(BuildFrame::Failed { reason, code }));
                }
                BuildReply::Incomplete { sections, .. } => {
                    self.ended = true;
                    return Some(Ok(BuildFrame::Incomplete { sections }));
                }
                BuildReply::Got { phase, reason, last_failure, events, chunks, .. } => {
                    self.ended = true;
                    return Some(Ok(BuildFrame::Got { phase, reason, last_failure, events, chunks }));
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
    if let BuildReply::BuildInProgress { current_build_id } = &reply {
        // A storage change is in progress (storage.md §2 "Changing it"): no Build holds it, so the
        // refusal names none.
        if current_build_id.is_empty() {
            return CallEnd::Rejected { reason: "build-in-progress".into(), detail: "a storage change is in progress at the fabric-primary: no Build is admitted beside it".into() };
        }
        return match BuildId::parse(current_build_id) {
            Ok(current) => CallEnd::BuildInProgress { current },
            Err(e) => CallEnd::Rejected { reason: "malformed-build-id".into(), detail: format!("the fabric-primary answered BuildInProgress naming a Build id that is not a Crockford id: {e}") },
        };
    }
    let detail = match &reply {
        BuildReply::NotFabricPrimary { fabric_primary } => format!("the receiver is not the fabric-primary; it sees {}", fabric_primary.as_ref().map(|id| id.as_str()).unwrap_or("none")),
        BuildReply::NotExecutor { named, recipient } => format!("the mesh's primary is {named}, the recipient is {recipient}"),
        BuildReply::UnknownBuild { build_id } => format!("the fabric-primary holds no fact of Build {build_id}"),
        BuildReply::Rejected { reason, detail } => format!("{reason}: {detail}"),
        BuildReply::BuildInProgress { current_build_id } => format!("Build {current_build_id} is still reconciling; one Build at a time"),
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

/// What one reattachment read and opened (node-rpc-envelope.md "Frames, durable events and
/// reattachment": "`build.get [id: build_id]` for the events, then the outer stream again, resuming from
/// the events").
pub struct Reattached {
    /// The step events `build.get` read, every attempt's, in order.
    pub events: Vec<serde_json::Value>,
    /// The outer stream, opened again from the current attempt the step events name.
    pub stream: BuildStream,
}

/// Follow `stream` to its terminal (build.md: "a live run is reattached, never re-claimed"; i146 Q22:
/// the reattach after a stream op's connection closes is the owner's). A stream that breaks
/// before its terminal (its connection closed: `Indeterminate`), ends without one, or ends
/// `FabricPrimaryMoved` is reattached through `reattach` by its `build_id` from the attempt it carried: the
/// step events it reads are kept as step frames, each completed step once, then the stream it opens again
/// is read on, a step it replays kept once. The terminal is `Complete`, `Incomplete`, `Failed`, `Got` or `Deleted`.
/// `Err` is a reattachment refused or not made, named: nothing here waits or tries again on its own.
pub async fn follow_with<F, Fut>(mut stream: BuildStream, mut reattach: F) -> Result<Followed, CallEnd>
where
    F: FnMut(BuildId) -> Fut,
    Fut: std::future::Future<Output = Result<Reattached, CallEnd>>,
{
    let build_id = stream.build_id().clone();
    let mut out = Followed { frames: Vec::new(), reattached: 0 };
    let mut steps = HashSet::new();
    loop {
        while let Some(frame) = stream.next().await {
            let frame = match frame {
                Ok(f) => f,
                Err(CallEnd::Indeterminate { .. }) => break,
                Err(e) => return Err(e),
            };
            if let BuildFrame::Step { mesh, operation, step, .. } = &frame {
                if !steps.insert((mesh.clone(), operation.clone(), step.clone())) {
                    continue;
                }
            }
            let moved = matches!(frame, BuildFrame::FabricPrimaryMoved { .. });
            let terminal = matches!(frame, BuildFrame::Complete { .. } | BuildFrame::Failed { .. } | BuildFrame::Incomplete { .. } | BuildFrame::Got { .. } | BuildFrame::Deleted);
            out.frames.push(frame);
            if terminal {
                return Ok(out);
            }
            if moved {
                break;
            }
        }
        out.reattached += 1;
        let again = reattach(build_id.clone()).await?;
        for (mesh, operation, step) in again.events.iter().filter_map(completed_step) {
            if steps.insert((mesh.clone(), operation.clone(), step.clone())) {
                out.frames.push(BuildFrame::Step { mesh, operation, step, result: StepResult::Complete });
            }
        }
        stream = again.stream;
    }
}

impl BuildCarrier {
    /// [`follow_with`] through this carrier, each reattachment [`Self::reattach`].
    pub async fn follow(&self, stream: BuildStream) -> Result<Followed, CallEnd> {
        follow_with(stream, |id| async move { self.reattach(&id).await }).await
    }

    /// One reattachment by `build_id` (node-rpc-envelope.md "Frames, durable events and
    /// reattachment"): `build.get [id: build_id]` for the events, then the outer stream again, a
    /// re-submit of the same build_id. Never a new execution: the re-submit of a
    /// Build already held carries its frames.
    pub async fn reattach(&self, build_id: &BuildId) -> Result<Reattached, CallEnd> {
        let got = self.build(build_id).await?;
        let stream = self.resubmit(build_id).await?;
        Ok(Reattached { events: got.events, stream })
    }

    /// A carrier over `client`, starting at the fabric-primary `fabric_primary`.
    pub fn new(client: Arc<NodeRpcClient>, fabric_primary: PathName) -> Self {
        Self { client, target: NodeTarget::CurrentPath(fabric_primary) }
    }

    /// The node-RPC client the carrier calls with: a caller that makes its own call reaches the admin
    /// the carrier starts at through it.
    pub fn client(&self) -> &NodeRpcClient {
        &self.client
    }

    /// A carrier selecting its entry admin by globally unique node ID.
    pub fn at_node(client: Arc<NodeRpcClient>, node_id: rdm_core::NodeId) -> Self {
        Self { client, target: NodeTarget::ExactNode(node_id) }
    }

    /// The explicit target calls start at.
    pub fn target(&self) -> &NodeTarget {
        &self.target
    }

    /// Open `req` at the fabric-primary: follow the redirect of a node that is not it, once for each
    /// admin named, and read the `Started` frame.
    async fn open(&self, req: BuildRequest) -> Result<BuildStream, CallEnd> {
        let (stream, build_id, disposition) = self.open_started(req).await?;
        let build_id = BuildId::parse(&build_id)
            .map_err(|e| CallEnd::Rejected { reason: "malformed-build-id".into(), detail: format!("the fabric-primary started a Build under an id that is not a Crockford id: {e}") })?;
        Ok(BuildStream { stream, _carrier: self.clone(), build_id, disposition, seen: HashSet::new(), ended: false })
    }

    /// [`Self::open`]'s redirect and `Started` read, the stream and the `Started` frame's fields as sent.
    async fn open_started(&self, req: BuildRequest) -> Result<(ReplyStream<BuildFamily>, String, Disposition), CallEnd> {
        let mut asked: BTreeSet<rdm_core::NodeId> = BTreeSet::new();
        let mut target = self.target.clone();
        loop {
            let opts = CallOptions::default();
            // The first reply of the call: an early refusal of the receiving node, or the first frame
            // of the stream (`Started`, or the handler's own refusal).
            // The frames are the executor's step events, recorded before they are sent; this client records
            // nothing of them: reading a frame is its recovery (node-rpc.md §43).
            let (first, stream) = match self.client.call_stream::<BuildFamily>(&target, &req, &opts).await.map(|(s, e)| (s.read_only(), e)) {
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
                (BuildReply::Started { build_id, disposition }, Some(stream)) => return Ok((stream, build_id, disposition)),
                (BuildReply::NotFabricPrimary { fabric_primary: Some(node_id) }, _) => {
                    if !asked.insert(node_id.clone()) {
                        return Err(CallEnd::Rejected { reason: "not-fabric-primary".into(), detail: format!("redirect to node {node_id} was already followed") });
                    }
                    target = NodeTarget::ExactNode(node_id);
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

    /// `build.create` for a Build the caller already holds the id of: never a second Build.
    pub async fn resubmit(&self, build_id: &BuildId) -> Result<BuildStream, CallEnd> {
        self.open(BuildRequest::Create { submit: BuildSubmit::Resubmit { build_id: build_id.to_string() } }).await
    }

    /// `build.get` (Q167 R-RD5): every Build the fabric-primary holds whose header matches every pair
    /// of `filter` (`id`, `current`, `phase`), newest first, each whole: its header, its topology and
    /// its events, the current one marked. Only `End` completes the read: a
    /// stream that ends before it, or whose chunks do not add up to the headers that close them, is an
    /// incomplete read (`Indeterminate`), never a list.
    pub async fn builds(&self, filter: &Filter) -> Result<Vec<Build>, CallEnd> {
        let (mut stream, _current, _disposition) = self.open_started(BuildRequest::GetBuilds { filter: filter.clone() }).await?;
        let incomplete = |reason: String| CallEnd::Indeterminate { code: rdm_core::outcome::Code::Unknown, reason: format!("an incomplete build.get read: {reason}") };
        let mut out: Vec<Build> = Vec::new();
        // The Build whose chunks are arriving: its id, its topology chunks and nodes, its event chunks and events.
        let mut open: Option<(String, u32, Vec<NodeSpec>, u32, Vec<serde_json::Value>)> = None;
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
                BuildReply::Events { build_id, chunk_index, chunk_count: _, events } => {
                    let events = parse_events(events)?;
                    let o = open.get_or_insert_with(|| (build_id.clone(), 0, Vec::new(), 0, Vec::new()));
                    if o.0 != build_id || o.3 != chunk_index {
                        return Err(incomplete(format!("event chunk {chunk_index} of Build {build_id} arrived where chunk {} of Build {} was owed", o.3, o.0)));
                    }
                    o.3 += 1;
                    o.4.extend(events);
                }
                BuildReply::Build { build_id, phase, current, events, chunks, change, time } => {
                    let (id, _topology_chunks, nodes, event_chunks, held) = open.take().unwrap_or_else(|| (build_id.clone(), 0, Vec::new(), 0, Vec::new()));
                    if id != build_id || event_chunks != chunks || held.len() != events as usize {
                        return Err(incomplete(format!("Build {build_id}'s header counts {events} events in {chunks} chunks, and {} in {event_chunks} chunks of Build {id} arrived", held.len())));
                    }
                    let build_id = BuildId::parse(&build_id).map_err(|e| CallEnd::Rejected { reason: "malformed-build-id".into(), detail: format!("build.get answered a Build id that is not a Crockford id: {e}") })?;
                    out.push(Build { build_id, phase, current, change, time, nodes, events: held });
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

/// One Build as `build.get` reads it whole (Q167 R-RD5): its header, its topology and its events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Build {
    /// The Build.
    pub build_id: BuildId,
    /// Where the Build is in its life.
    pub phase: BuildPhase,
    /// Whether this is the current Build, selected once for the read.
    pub current: bool,
    /// The change as it came in, the Build record's (build.md §1): history only.
    pub change: BuildChange,
    /// When the Build was accepted, the Build record's: milliseconds since the Unix epoch.
    pub time: u64,
    /// Its topology: every node's create spec, in `(path, name)` order.
    pub nodes: Vec<NodeSpec>,
    /// Its events, in event.id order: each the events store's CloudEvents envelope (build.md §5).
    pub events: Vec<serde_json::Value>,
}

impl Build {
    /// The steps whose `rdm.build.step` event ended `complete`: `(mesh, op, step)` each.
    pub fn completed_steps(&self) -> Vec<(String, String, String)> {
        self.events.iter().filter_map(completed_step).collect()
    }
}

/// The `(mesh, op, step)` of an `rdm.build.step` event that ended `complete`.
pub fn completed_step(event: &serde_json::Value) -> Option<(String, String, String)> {
    if event.get("type").and_then(|t| t.as_str()) != Some("rdm.build.step") {
        return None;
    }
    let data = event.get("data")?;
    if data.get("state").and_then(|s| s.as_str()) != Some("complete") {
        return None;
    }
    let field = |k: &str| data.get(k).and_then(|v| v.as_str()).map(str::to_string);
    Some((field("mesh")?, field("op")?, field("step")?))
}

/// Each event's JSON, as the `Events` chunk carries it; one that is not JSON is an incomplete read.
fn parse_events(events: Vec<String>) -> Result<Vec<serde_json::Value>, CallEnd> {
    events
        .iter()
        .map(|e| serde_json::from_str(e).map_err(|err| CallEnd::Indeterminate { code: rdm_core::outcome::Code::DataLoss, reason: format!("an Events chunk carried an event that is not JSON: {err}") }))
        .collect()
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
                    let mut sink = sink.started(BuildReply::Started { build_id: "cq2tyb03krac".into(), disposition: Disposition::Created }).await.map_err(gone)?;
                    // The frames after `Started` are written once the caller holds only the stream.
                    g.notified().await;
                    for step in ["AllocateIdentity", "WaitForMeshJoin", "Complete"] {
                        sink.data(BuildReply::Step { build_id: "cq2tyb03krac".into(), mesh: "mesh1".into(), operation: rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node-admin", "1").unwrap())).to_string(), step: step.into(), result: StepResult::Complete }).await.map_err(gone)?;
                    }
                    Ok(BuildReply::FabricPrimaryMoved { build_id: "cq2tyb03krac".into(), fabric_primary: Some(rdm_core::NodeId::parse("0123456789ab").unwrap()) })
                }
            })
            .build(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.to_string() })
            .unwrap();
        let key = iroh::SecretKey::generate();
        let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let _router = iroh::protocol::Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
        let fp: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(ResolvedNode { mesh: "mesh1".into(), r#type: "node-admin".into(), node_id, path: fp.path.clone(), name: fp.name.clone(), endpoint_id: key.public(), transport_addr: addr, incarnation });
        let cep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let carrier = BuildCarrier::new(Arc::new(NodeRpcClient::new(cep, resolver).with_caller_system("rdm")), fp);
        let mut stream = carrier.create(BuildChange::DeleteFabric).await.expect("the call was accepted");
        assert_eq!(stream.build_id().as_str(), "cq2tyb03krac");
        drop(carrier);
        go.notify_one();
        // A loopback frame arrives in milliseconds; the bound only names what arrived if it never does.
        let mut got = Vec::new();
        let read = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while let Some(f) = stream.next().await {
                got.push(f);
            }
        })
        .await;
        let step = |s: &str| Ok(BuildFrame::Step { mesh: "mesh1".into(), operation: rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node-admin", "1").unwrap())).to_string(), step: s.into(), result: StepResult::Complete });
        assert!(read.is_ok(), "the stream delivered no terminal within 10 s; it delivered {got:?}");
        assert_eq!(got, vec![step("AllocateIdentity"), step("WaitForMeshJoin"), step("Complete"), Ok(BuildFrame::FabricPrimaryMoved { fabric_primary: Some(rdm_core::NodeId::parse("0123456789ab").unwrap()) })]);
    }
}
