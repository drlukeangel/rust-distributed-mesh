//! Who decides a Build attempt's claim, and the context the attempt carries (R-A1, R-T4, R-X1).
//!
//! An executor never claims from its own log. The claim of attempt `n` of a Build is an
//! insert-and-fail on the FABRIC-PRIMARY's Build log, asked over Node RPC (`BuildClaim`, op
//! `0x1C`); the executor runs the attempt only on `Won`. The fabric-primary's own executor asks
//! the same door in process. A claim that cannot be put to the fabric-primary (no fabric-primary
//! in the view, a transport failure, a refusal) is a claim not made: the attempt does not run.
//!
//! Context is attempt-scoped. The fabric-primary keeps one [`CallContext`] per `(build_id,
//! attempt)` in a record of its own data dir (`attempt-context/`), written when the attempt comes
//! into being (the REST request span, or the `via-proven-drift` span) and returned in `Won`. It is
//! never a Build fact: Build facts are re-broadcast to every new neighbour. A next attempt that
//! has no context of its own inherits the previous attempt's at claim time.

use crate::build::BuildId;
use crate::build_state::{BuildAttemptClaim, BuildStateAdapter, ClaimOutcome};
use crate::model::{EndpointId, IncarnationId, NodeId, NodeKind, PathName};
use crate::record_store::{FileRecords, StorageError};
use crate::topology::Topology;
use rafka_node_rpc::{NodeRpcClient, NodeTarget, ServerBuilder};
use rafka_node_rpc_contract::build_claim::{BuildClaim, BuildClaimReply, BuildClaimRequest};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::context::CallContext;
use rafka_node_rpc_contract::outcome::RpcOutcome;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use tracing::Instrument as _;

const CONTEXT_DIR: &str = "attempt-context";
const CONTEXT_FORMAT: &str = "attempt-context/1";

/// The caller system every RDM-originated attempt context names.
pub const CALLER_SYSTEM: &str = "rdm";

/// The context of the current span: where an attempt created now comes from.
pub fn current_context() -> CallContext {
    CallContext {
        caller_system: Some(CALLER_SYSTEM.into()),
        traceparent: rafka_mesh_telemetry::current_traceparent(),
        tracestate: rafka_mesh_telemetry::current_tracestate(),
        baggage: None,
    }
}

enum Backing {
    Files(FileRecords),
    Memory(Mutex<BTreeMap<String, CallContext>>),
}

/// The fabric-primary's local record of each attempt's context, keyed `(build_id, attempt)`.
/// Local to the admin that wrote it: nothing here is gossiped, journaled as a fact or read by
/// another admin.
pub struct AttemptContexts {
    backing: Backing,
}

impl AttemptContexts {
    /// The record under `<own_data_dir>/attempt-context/`.
    pub fn open(own_data_dir: &Path) -> Result<Self, StorageError> {
        Ok(Self { backing: Backing::Files(FileRecords::open(own_data_dir, CONTEXT_DIR)?) })
    }

    /// A record held in memory only: a fixture whose admin has no data dir.
    pub fn in_memory() -> Self {
        Self { backing: Backing::Memory(Mutex::new(BTreeMap::new())) }
    }

    fn key(build_id: &BuildId, attempt: u32) -> String {
        format!("{build_id}-{attempt}")
    }

    /// Put the context of `(build_id, attempt)`: a blind put of its own key.
    pub async fn put(&self, build_id: &BuildId, attempt: u32, context: &CallContext) -> Result<(), StorageError> {
        let key = Self::key(build_id, attempt);
        match &self.backing {
            Backing::Files(f) => f.write(&key, CONTEXT_FORMAT, context).await,
            Backing::Memory(m) => {
                m.lock().unwrap().insert(key, context.clone());
                Ok(())
            }
        }
    }

    /// The context of `(build_id, attempt)`, when one was put.
    pub fn get(&self, build_id: &BuildId, attempt: u32) -> Result<Option<CallContext>, StorageError> {
        let key = Self::key(build_id, attempt);
        match &self.backing {
            Backing::Files(f) => f.read(&key, CONTEXT_FORMAT),
            Backing::Memory(m) => Ok(m.lock().unwrap().get(&key).cloned()),
        }
    }

    /// The context `attempt` is claimed with: its own, else the previous attempt's (a hand-off
    /// continues the trace it came from), stored under `attempt` so a repeat claim answers the
    /// same. An attempt nothing here knows (the fabric-primary changed) starts a new trace.
    pub async fn for_claim(&self, build_id: &BuildId, attempt: u32) -> Result<CallContext, StorageError> {
        if let Some(own) = self.get(build_id, attempt)? {
            return Ok(own);
        }
        let inherited = match attempt.checked_sub(1).filter(|p| *p > 0) {
            Some(previous) => self.get(build_id, previous)?,
            None => None,
        };
        let context = inherited.unwrap_or_else(|| CallContext { caller_system: Some(CALLER_SYSTEM.into()), ..CallContext::default() });
        self.put(build_id, attempt, &context).await?;
        Ok(context)
    }
}

/// The fabric-primary's claim door: the one place a Build attempt's claim is decided.
pub struct ClaimDoor {
    /// This admin's path.name.
    pub me: PathName,
    pub topology: Arc<tokio::sync::RwLock<Topology>>,
    /// This admin's Build log: the fabric-primary's own, when it holds the seat.
    pub builds: Arc<dyn BuildStateAdapter>,
    pub contexts: Arc<AttemptContexts>,
}

impl ClaimDoor {
    /// The reply of a receiver that does not hold the seat, naming the one its view shows.
    async fn not_fabric_primary(&self) -> Option<BuildClaimReply> {
        let t = self.topology.read().await;
        match t.fabric_primary() {
            Some(n) if n.name == self.me => None,
            other => Some(BuildClaimReply::NotFabricPrimary { fabric_primary: other.map(|n| n.name.to_string()) }),
        }
    }

    /// Decide the claim of `attempt` of `build_id` for `executor` (a node-admin's path.name):
    /// only the fabric-primary decides. `Won` carries the attempt's context.
    pub async fn claim(&self, executor: &str, build_id: &BuildId, attempt: u32) -> BuildClaimReply {
        let span = tracing::info_span!(
            "rdm.node_admin.build.update.via-claim-decision",
            node = %self.me,
            build_id = %build_id,
            attempt,
            executor,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
        );
        let reply = self.decide(executor, build_id, attempt).instrument(span.clone()).await;
        span.record("outcome", reply.name());
        match &reply {
            BuildClaimReply::Lost { holder } => span.record("detail", holder.as_str()),
            BuildClaimReply::NotOpen { next } => span.record("detail", format!("next {next:?}").as_str()),
            BuildClaimReply::NotFabricPrimary { fabric_primary } => span.record("detail", format!("sees {fabric_primary:?}").as_str()),
            BuildClaimReply::NotReady { reason } | BuildClaimReply::Unauthorized { reason } => span.record("detail", reason.as_str()),
            _ => &span,
        };
        span.in_scope(|| tracing::info!("attempt claim decided by the fabric-primary's log"));
        reply
    }

    async fn decide(&self, executor: &str, build_id: &BuildId, attempt: u32) -> BuildClaimReply {
        if let Some(refusal) = self.not_fabric_primary().await {
            return refusal;
        }
        let claim = BuildAttemptClaim { build_id: build_id.clone(), attempt, executor: executor.to_string() };
        match self.builds.claim_attempt(&claim).await {
            Ok(ClaimOutcome::Won) => match self.contexts.for_claim(build_id, attempt).await {
                Ok(context) => BuildClaimReply::Won { context },
                Err(e) => BuildClaimReply::NotReady { reason: format!("{}: attempt {attempt} of {build_id} is claimed, its context record is unreadable: {e}", self.me) },
            },
            Ok(ClaimOutcome::Lost { holder }) => BuildClaimReply::Lost { holder },
            Ok(ClaimOutcome::NotOpen { next }) => BuildClaimReply::NotOpen { next },
            Err(e) => BuildClaimReply::NotReady { reason: format!("{}: claiming attempt {attempt} of {build_id}: {e}", self.me) },
        }
    }

    /// A request off the wire: the seat first (a receiver that is not the fabric-primary only
    /// redirects), then the sender, taken from the authenticated peer and held to the request.
    pub async fn serve(&self, peer: EndpointId, req: BuildClaimRequest) -> BuildClaimReply {
        let BuildClaimRequest::ClaimAttempt { build_id, attempt, executor_node_id, executor_incarnation } = req;
        if let Some(refusal) = self.not_fabric_primary().await {
            return refusal;
        }
        let sender = self.topology.read().await.nodes.iter().find(|n| n.endpoint_id.as_ref() == Some(&peer)).cloned();
        let Some(sender) = sender else {
            return BuildClaimReply::Unauthorized { reason: format!("{}: the calling endpoint {} is not a node of this admin's view", self.me, peer.0) };
        };
        if sender.kind != NodeKind::NodeAdmin {
            return BuildClaimReply::Unauthorized { reason: format!("{}: {} is a {:?}, and only a node-admin executes a Build", self.me, sender.name, sender.kind) };
        }
        if sender.node_id != executor_node_id {
            return BuildClaimReply::Unauthorized { reason: format!("{}: the request names node {executor_node_id} and the caller is {} ({})", self.me, sender.name, sender.node_id) };
        }
        if sender.incarnation_id.as_ref() != Some(&executor_incarnation) {
            return BuildClaimReply::Unauthorized {
                reason: format!("{}: the request names birth {} of {} and this view holds {:?}", self.me, executor_incarnation.0, sender.name, sender.incarnation_id.as_ref().map(|i| &i.0)),
            };
        }
        self.claim(&sender.name.to_string(), &BuildId(build_id), attempt).await
    }
}

pub type ClaimSlot = Arc<OnceLock<Arc<ClaimDoor>>>;

/// Serve `BuildClaim` on this admin. `slot` is filled once the admin holds a view; until then a
/// claim is `NotReady` by name.
pub fn serve(b: ServerBuilder, slot: ClaimSlot) -> ServerBuilder {
    b.serve::<BuildClaim, _, _>(OpOwner::Product("rdm".into()), move |peer: rafka_node_rpc::PeerContext, req: BuildClaimRequest| {
        let slot = slot.clone();
        async move {
            let Some(door) = slot.get().cloned() else {
                return Ok(BuildClaimReply::NotReady { reason: "this admin holds no view yet".into() });
            };
            Ok(door.serve(EndpointId(peer.endpoint_id.to_string()), req).await)
        }
    })
}

/// What an executor learns of its claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Claimed {
    /// The attempt is the executor's; `context` is the attempt's.
    Won { context: CallContext },
    Lost { holder: String },
    NotOpen { next: Option<u32> },
    /// The claim could not be put to the fabric-primary, or it answered something that is not a
    /// decision. The attempt does not run.
    Undecided { reason: String },
}

/// Puts an executor's claim to the fabric-primary.
#[async_trait::async_trait]
pub trait AttemptClaimer: Send + Sync {
    async fn claim(&self, executor: &str, build_id: &BuildId, attempt: u32) -> Claimed;
}

fn decided(reply: BuildClaimReply) -> Result<Claimed, BuildClaimReply> {
    match reply {
        BuildClaimReply::Won { context } => Ok(Claimed::Won { context: context.sanitized().0 }),
        BuildClaimReply::Lost { holder } => Ok(Claimed::Lost { holder }),
        BuildClaimReply::NotOpen { next } => Ok(Claimed::NotOpen { next }),
        other => Err(other),
    }
}

/// The claimer of an admin that holds the door itself and is the fabric-primary: the same door,
/// in process. Fixtures with one log use it; a refused seat is `Undecided`.
pub struct DoorClaimer(pub Arc<ClaimDoor>);

#[async_trait::async_trait]
impl AttemptClaimer for DoorClaimer {
    async fn claim(&self, executor: &str, build_id: &BuildId, attempt: u32) -> Claimed {
        match decided(self.0.claim(executor, build_id, attempt).await) {
            Ok(c) => c,
            Err(other) => Claimed::Undecided { reason: format!("{} did not decide the claim: {other:?}", self.0.me) },
        }
    }
}

/// The claimer of a running node-admin: the fabric-primary of its view decides, in process when
/// that is this admin and over Node RPC otherwise.
pub struct FabricPrimaryClaimer {
    pub me: PathName,
    pub node_id: NodeId,
    pub incarnation: IncarnationId,
    pub topology: Arc<tokio::sync::RwLock<Topology>>,
    pub door: Arc<ClaimDoor>,
    pub client: Arc<NodeRpcClient>,
}

impl FabricPrimaryClaimer {
    async fn ask(&self, target: &PathName, build_id: &BuildId, attempt: u32) -> Result<BuildClaimReply, String> {
        if *target == self.me {
            return Ok(self.door.claim(&self.me.to_string(), build_id, attempt).await);
        }
        let node_id = self
            .topology
            .read()
            .await
            .nodes
            .iter()
            .find(|n| n.name == *target)
            .map(|n| n.node_id.clone())
            .ok_or_else(|| format!("{target} is not a node of this admin's view"))?;
        let req = BuildClaimRequest::ClaimAttempt {
            build_id: build_id.0.clone(),
            attempt,
            executor_node_id: self.node_id.clone(),
            executor_incarnation: self.incarnation.clone(),
        };
        let (out, _) = self.client.call::<BuildClaim>(&NodeTarget::ExactNode(node_id), &req, &rafka_node_rpc::CallOptions::default()).await;
        match out {
            RpcOutcome::Reply(r) => Ok(r.value().clone()),
            other => Err(format!("the claim to {target} ended {}: {other:?}", other.name())),
        }
    }
}

#[async_trait::async_trait]
impl AttemptClaimer for FabricPrimaryClaimer {
    /// Follow the fabric-primary: the one this view shows, then each one a `NotFabricPrimary`
    /// names, once each. A redirect is not a retry; a failure to put the claim is `Undecided`.
    async fn claim(&self, executor: &str, build_id: &BuildId, attempt: u32) -> Claimed {
        let span = tracing::info_span!(
            "rdm.node_admin.build.update.via-claim-request",
            node = %self.me,
            build_id = %build_id,
            attempt,
            executor,
            decided_by = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        let result = async {
            let mut asked: BTreeSet<PathName> = BTreeSet::new();
            let mut target = self.topology.read().await.fabric_primary().map(|n| n.name.clone());
            loop {
                let Some(t) = target.take() else {
                    return (None, Claimed::Undecided { reason: format!("{}: no fabric-primary in this admin's view to decide attempt {attempt} of {build_id}", self.me) });
                };
                if !asked.insert(t.clone()) {
                    return (Some(t.clone()), Claimed::Undecided { reason: format!("{}: the redirect to {t} came back to a fabric-primary already asked ({asked:?})", self.me) });
                }
                match self.ask(&t, build_id, attempt).await {
                    Err(reason) => return (Some(t), Claimed::Undecided { reason }),
                    Ok(BuildClaimReply::NotFabricPrimary { fabric_primary }) => match fabric_primary.map(|p| p.parse::<PathName>()) {
                        Some(Ok(named)) => target = Some(named),
                        Some(Err(e)) => return (Some(t.clone()), Claimed::Undecided { reason: format!("{t} is not the fabric-primary and names an unparseable one: {e}") }),
                        None => return (Some(t.clone()), Claimed::Undecided { reason: format!("{t} is not the fabric-primary and sees none") }),
                    },
                    Ok(reply) => {
                        return match decided(reply) {
                            Ok(c) => (Some(t), c),
                            Err(other) => (Some(t.clone()), Claimed::Undecided { reason: format!("{t} did not decide the claim: {other:?}") }),
                        }
                    }
                }
            }
        }
        .instrument(span.clone())
        .await;
        let (by, claimed) = result;
        span.record("decided_by", by.map(|p| p.to_string()).unwrap_or_default().as_str());
        span.record(
            "outcome",
            match &claimed {
                Claimed::Won { .. } => "won",
                Claimed::Lost { .. } => "lost",
                Claimed::NotOpen { .. } => "not-open",
                Claimed::Undecided { .. } => "undecided",
            },
        );
        span.in_scope(|| tracing::info!(?claimed, "attempt claim put to the fabric-primary"));
        claimed
    }
}
