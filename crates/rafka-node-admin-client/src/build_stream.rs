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
use rafka_mesh_entity::PathName;
use rafka_node_rpc::stream::{ReplyStream, StreamFailure, StreamItem};
use rafka_node_rpc::{Budget, CallOptions, NodeRpcClient, NodeTarget};
use rafka_node_rpc_contract::build::{Build, BuildChange, BuildPhase, BuildReply, BuildRequest, BuildSubmit, Disposition, StepReceipt};
use rafka_node_rpc_contract::codes::ResetCode;
use rafka_node_rpc_contract::outcome::{IndeterminateReason, RpcOutcome};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;

pub use rafka_node_rpc_contract::build::{BuildChange as Change, Disposition as StartDisposition, MeshCounts};

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
    /// Terminal of `build.delete`.
    Deleted,
}

/// One accepted Build call: its `Started` and the frames after it.
pub struct BuildStream {
    stream: ReplyStream<Build>,
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
                    return Some(Err(CallEnd::Indeterminate { reason: format!("the Build stream broke before its terminal: {f:?}") }));
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
                BuildReply::Failed { attempt, operation, step, reason, .. } => {
                    self.ended = true;
                    return Some(Ok(BuildFrame::Failed { attempt, operation, step, reason }));
                }
                BuildReply::Got { phase, attempt, executor, reason, last_failure, steps, chunks, .. } => {
                    self.ended = true;
                    return Some(Ok(BuildFrame::Got { phase, attempt, executor, reason, last_failure, steps, chunks }));
                }
                BuildReply::Deleted { .. } => {
                    self.ended = true;
                    return Some(Ok(BuildFrame::Deleted));
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
        return CallEnd::BuildInProgress { current: BuildId(current_build_id.clone()) };
    }
    let detail = match &reply {
        BuildReply::NotFabricPrimary { fabric_primary } => format!("the receiver is not the fabric-primary; it sees {}", fabric_primary.as_deref().unwrap_or("none")),
        BuildReply::NotExecutor { named, recipient } => format!("the claim names {named}, the recipient is {recipient}"),
        BuildReply::StaleClaim { held_attempt, held_executor, carried_attempt } => format!("the recipient holds attempt {held_attempt} for {}, the call carried {carried_attempt}", held_executor.as_deref().unwrap_or("no executor")),
        BuildReply::UnknownBuild { build_id } => format!("the fabric-primary holds no fact of Build {build_id}"),
        BuildReply::Rejected { reason, detail } => format!("{reason}: {detail}"),
        BuildReply::BuildInProgress { current_build_id } => format!("Build {current_build_id} is still reconciling; one Build at a time"),
        BuildReply::AttemptTaken { detail } => detail.clone(),
        BuildReply::Fenced { node, by } => format!("{node} yielded the fabric-primary seat ({by})"),
        BuildReply::CannotDelete { build_id, reason } => format!("Build {build_id} stays in history: {reason}"),
        BuildReply::PeerUnresolved { reason } | BuildReply::NotReady { reason } | BuildReply::Busy { reason } | BuildReply::Draining { reason } | BuildReply::Unauthorized { reason } => reason.clone(),
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

impl BuildCarrier {
    /// A carrier over `client`, starting at the fabric-primary `fabric_primary`.
    pub fn new(client: Arc<NodeRpcClient>, fabric_primary: PathName) -> Self {
        Self { client, fabric_primary }
    }

    /// The fabric-primary calls start at.
    pub fn fabric_primary(&self) -> &PathName {
        &self.fabric_primary
    }

    /// Open `req` at the fabric-primary: follow the redirect of a node that is not it, once for each
    /// admin named, and read the `Started` frame.
    async fn open(&self, req: BuildRequest) -> Result<BuildStream, CallEnd> {
        let mut asked: BTreeSet<PathName> = BTreeSet::new();
        let mut target = self.fabric_primary.clone();
        loop {
            asked.insert(target.clone());
            let opts = CallOptions { budget: Budget::Stream { send: SEND }, ..CallOptions::default() };
            // The first reply of the call: an early refusal of the receiving node, or the first frame
            // of the stream (`Started`, or the handler's own refusal).
            let (first, stream) = match self.client.call_stream::<Build>(&NodeTarget::CurrentPath(target.clone()), &req, &opts).await {
                Err((RpcOutcome::Reply(r), _)) => (r.into_value(), None),
                Err((other, _)) => return Err(ended(other).err().unwrap_or_else(|| CallEnd::Indeterminate { reason: "an early reply that is no refusal".into() })),
                Ok((mut stream, _)) => match stream.next().await {
                    Some(StreamItem::Frame(_, f)) => (f, Some(stream)),
                    // The reserved reset 421 is how a node that does not serve the op answers: nothing was dispatched.
                    Some(StreamItem::Failed(StreamFailure::Indeterminate(IndeterminateReason::Reset(code)))) if code == u64::from(ResetCode::UnservedOp.code()) => return Err(CallEnd::Unserved { op: Build::OP }),
                    Some(StreamItem::Failed(f)) => return Err(CallEnd::Indeterminate { reason: format!("the stream broke before its Started frame: {f:?}") }),
                    None => return Err(CallEnd::Indeterminate { reason: "the stream ended before its Started frame".into() }),
                },
            };
            match (first, stream) {
                (BuildReply::Started { build_id, attempt, disposition }, Some(stream)) => {
                    return Ok(BuildStream { stream, build_id: BuildId(build_id), attempt, disposition, seen: HashSet::new(), ended: false });
                }
                (BuildReply::NotFabricPrimary { fabric_primary: Some(named) }, _) => match named.parse::<PathName>() {
                    Ok(next) if !asked.contains(&next) => target = next,
                    Ok(_) => return Err(CallEnd::Rejected { reason: "not-fabric-primary".into(), detail: format!("{target} is not the fabric-primary and names {named}, which was already asked ({asked:?})") }),
                    Err(e) => return Err(CallEnd::Rejected { reason: "not-fabric-primary".into(), detail: format!("{target} is not the fabric-primary and names {named:?}, which is not a path.name: {e}") }),
                },
                (other, _) => return Err(refusal(other)),
            }
        }
    }

    /// `build.create`: accept `change` and follow the Build until it ends.
    pub async fn create(&self, change: BuildChange) -> Result<BuildStream, CallEnd> {
        self.open(BuildRequest::Create { submit: BuildSubmit::Change(change) }).await
    }

    /// `build.create` for a Build the caller already holds the id of: never a second Build. The
    /// stream carries the frames of `from_attempt` and later attempts.
    pub async fn resubmit(&self, build_id: &BuildId, from_attempt: u32) -> Result<BuildStream, CallEnd> {
        self.open(BuildRequest::Create { submit: BuildSubmit::Resubmit { build_id: build_id.0.clone(), from_attempt } }).await
    }

    /// `build.get`: the Build and its receipts, for reattachment after a cut.
    pub async fn get(&self, build_id: &BuildId) -> Result<BuildReceipts, CallEnd> {
        let mut stream = self.open(BuildRequest::Get { build_id: build_id.0.clone() }).await?;
        let mut steps = Vec::new();
        let mut header = None;
        while let Some(frame) = stream.next().await {
            match frame? {
                BuildFrame::Steps(chunk) => steps.extend(chunk),
                BuildFrame::Got { phase, attempt, executor, reason, last_failure, steps: n, chunks } => header = Some((phase, attempt, executor, reason, last_failure, n, chunks)),
                other => return Err(CallEnd::Rejected { reason: "unexpected-frame".into(), detail: format!("a build.get stream carried {other:?}") }),
            }
        }
        let Some((phase, attempt, executor, reason, last_failure, n, _chunks)) = header else {
            return Err(CallEnd::Indeterminate { reason: "the build.get stream ended without its header".into() });
        };
        if n as usize != steps.len() {
            return Err(CallEnd::Indeterminate { reason: format!("the build.get header counts {n} receipts and {} arrived", steps.len()) });
        }
        Ok(BuildReceipts { build_id: build_id.clone(), phase, attempt, executor, reason, last_failure, steps })
    }

    /// `build.delete`: a finished Build leaves history.
    pub async fn delete(&self, build_id: &BuildId) -> Result<(), CallEnd> {
        let mut stream = self.open(BuildRequest::Delete { build_id: build_id.0.clone() }).await?;
        match stream.next().await {
            Some(Ok(BuildFrame::Deleted)) => Ok(()),
            Some(Ok(other)) => Err(CallEnd::Rejected { reason: "unexpected-frame".into(), detail: format!("a build.delete stream carried {other:?}") }),
            Some(Err(e)) => Err(e),
            None => Err(CallEnd::Indeterminate { reason: "the build.delete stream ended without its terminal".into() }),
        }
    }
}

/// What `build.get` reads: the Build's header and its receipts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildReceipts {
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
    /// The receipts of every attempt, in order.
    pub steps: Vec<StepReceipt>,
}
