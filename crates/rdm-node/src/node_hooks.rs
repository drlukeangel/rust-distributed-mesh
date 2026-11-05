//! `pre_node_ready`: the one hook an embedding app supplies to be born full.
//!
//! A node reaches the hook after its `JoinNode` was accepted and its mesh channel joined
//! (node-start.md step 3). RDM runs it under the hook's own [`PreNodeReady::send`] (Q350), through
//! [`rdm_transport::Send::run_paced`], the one retry mechanism (Q344): each call
//! must answer within the `TimeLimit`, and between calls RDM waits the one Send schedule
//! ([`rdm_transport::Pacing`]). The hook answers [`HookOutcome`]:
//!
//! - `Ok`: RDM's own checks go on to Ready, and the app's own ops are served
//!   ([`PreNodeReadyGate`] opens).
//! - `Pending { reason }`: that attempt ends; the node stays Pending, its reason is the blocker
//!   evidence and the probe's Status `Pending` answer, and RDM calls the hook again after the backoff.
//!   No answer within the `TimeLimit` is `Pending { reason: "time limit" }`.
//! - `Failed { reason }`, or the attempts spent: the start ends Failed with the last reason, back to
//!   whoever started the node. The node stays Pending, its process keeps running, and nothing calls
//!   the hook again; a probe of it is answered `StartFailed { step: node.pre-node-ready, reason }`.
//!
//! The Day-0 root and a fabric-primary recovered from its durable map have no node-admin to pull
//! from: their context says so and the hook completes from local state
//! (`fabric-node-hooks.md` "What the context carries", `gossip.md` §9).
//!
//! One span per attempt (`rdm.mesh.node.update.via-pre-node-ready-attempt`) under the boot span carries
//! the outcome and the reason; `via-ready` follows the successful attempt.
//! [`OnNodeReady`] is for optional work only: nothing required may live there.
//!
//! [`OnCallFailed`] is a notice on every node: once per call it opened whose attempts are spent, on a
//! task of its own under `rdm.mesh.node.update.via-on-call-failed`, after the call has ended to its
//! owner (`fabric-node-hooks.md` "on_call_failed"). It gates nothing.

use crate::model::{FabricId, IncarnationId, MeshId, NodeId, PathName};
use rdm_gossip::topology::TopologyView;
use rdm_transport::{CancelToken, PreNodeReadyGate, NodeRpcClient};
use std::sync::Arc;
use tracing::Instrument;

/// The exact birth the hook runs for.
#[derive(Debug, Clone)]
pub struct Birth {
    /// The birth's path.
    pub path: String,
    /// The name, beside its path.
    pub name: String,
    /// Its node id.
    pub node_id: NodeId,
    /// Its incarnation.
    pub incarnation: IncarnationId,
    /// Its fabric.
    pub fabric_id: FabricId,
    /// Its mesh.
    pub mesh_id: MeshId,
    /// Its type.
    pub r#type: String,
}

impl Birth {
    /// The `(path, name)` lookup index key of path and name.
    pub fn location(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.path.clone(), name: self.name.clone() } }
}

/// The node-admin that answered this node's `JoinNode` (node-mesh-join.md's accepting primary), which
/// the hook reads its kind channels from (`fabric-node-hooks.md` "What the context carries"). A start
/// after a stop reached it back over the call that commanded the start, holding no record of it
/// (`lifecycles/node-start.md` step 2): it is known by the node ID its join answer gave and the
/// endpoint it answered on, and the hook calls it through `target`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptingPrimary {
    /// The globally unique admin ID supplied by the join answer.
    pub node_id: NodeId,
    /// Its endpoint key.
    pub endpoint: iroh::PublicKey,
    /// Where a call to it goes: the launcher's birth at a first start; after a stop, the endpoint
    /// and path of the call the join went back over.
    pub target: rdm_transport::NodeTarget,
}

/// Why a node has no node-admin that answered its `JoinNode` (`fabric-node-hooks.md` "What the
/// context carries").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoAcceptingPrimary {
    /// The Day-0 root: no primary exists before it.
    Day0Root,
    /// An admin started on an inherited root (fabric recovery): it has no maker, and its hook reads the
    /// inherited storage.
    InheritedRoot,
    /// A recovery start (mesh-primary or fabric-primary flags): its hook reads from the nodes it reaches.
    Recovery,
}

impl NoAcceptingPrimary {
    /// The reason in words.
    pub fn reason(self) -> &'static str {
        match self {
            Self::Day0Root => "the Day-0 root: no primary exists before this node",
            Self::InheritedRoot => "an admin started on an inherited root: its hook reads the inherited storage",
            Self::Recovery => "a recovery start: no admin accepted this birth",
        }
    }
}

/// The node-admin the hook pulls from, or why there is none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcceptingPrimarySource {
    /// The admin that answered this node's join.
    Accepted(AcceptingPrimary),
    /// There is none, and why: the hook resolves from local state.
    None(NoAcceptingPrimary),
}

impl AcceptingPrimarySource {
    /// The admin that answered the join, when there is one.
    pub fn accepted(&self) -> Option<&AcceptingPrimary> {
        match self {
            Self::Accepted(a) => Some(a),
            Self::None(_) => None,
        }
    }
}

/// Why the hook runs: the birth's first start, or a start of the same birth after a stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartKind {
    /// The birth joins for the first time (create).
    FirstStart,
    /// The stopped birth rejoins as itself (`node.start` after `node.stop`): its data dir is its own
    /// and what it held before the stop is still there.
    AfterStop,
}

/// What the hook is handed.
#[derive(Clone)]
pub struct NodeReadyCtx {
    /// Whether this is the birth's first start or a start after a stop.
    pub start_kind: StartKind,
    /// The exact birth.
    pub birth: Birth,
    /// The node-admin that answered the join, to pull from, or why there is none
    /// (`fabric-node-hooks.md` "What the context carries").
    pub accepting_primary: AcceptingPrimarySource,
    /// The process's one Node RPC client.
    pub client: Arc<NodeRpcClient>,
    /// The node's topology view: the cache of the fabric's topology its channels fill.
    pub topology_view: TopologyView,
    /// The node's channels: the app joins its application channels through them
    /// (`Channels::join`, `ChannelId::app`) before it takes its snapshot, so what changes during the
    /// pull arrives by gossip.
    pub channels: rdm_gossip::channel::Channels,
    /// Cancelled when the node is stopped or deleted: the hook and any pull it has in flight end.
    pub cancel: CancelToken,
    /// Rafka-time: the process's one instance, adopted by the join that was accepted.
    pub rafka_time: rdm_transport::clock::RafkaTime,
    /// The member cert the answering node-admin's signer issued for this exact birth, as opaque
    /// bytes (`fabric-certs.md`): empty when that node-admin is configured with no certs; `None`
    /// for a birth that made no `JoinNode` (the Day-0 root, a recovery start).
    pub member_cert: Option<Vec<u8>>,
    /// Which attempt of the hook's `Send` this is, from 1.
    pub attempt: u32,
    /// Whether this birth's data dir was inherited from a previous birth (R-FM2).
    pub inherited: bool,
}

/// What the hook answers (`fabric-node-hooks.md` "pre_node_ready").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookOutcome {
    /// Proceed: the node becomes Ready.
    Ok,
    /// Not yet: this attempt ends, and RDM calls again on the hook's `Send`.
    Pending {
        /// What the node is waiting for, in words.
        reason: String,
    },
    /// Stop now: the node's start ends Failed.
    Failed {
        /// Why.
        reason: String,
    },
}

/// The one hook: run before Ready, for every birth (`fabric-node-hooks.md` "pre_node_ready").
#[async_trait::async_trait]
pub trait PreNodeReady: Send + Sync {
    /// The Send RDM runs this hook under: how many tries, and how long each may take.
    /// The application gives its own; with none, `Send::default()` (3 attempts, 10 s).
    fn send(&self) -> rdm_transport::Send {
        rdm_transport::Send::default()
    }

    /// Called before the node reaches Ready. ctx.cancel fires if the node is stopped or deleted meanwhile.
    async fn pre_node_ready(&self, ctx: &NodeReadyCtx) -> HookOutcome;
}

/// Optional work after Ready. Nothing required may live here, and it is not a second projection
/// path: the node is already serving.
#[async_trait::async_trait]
pub trait OnNodeReady: Send + Sync {
    /// Run once, after the node published Ready.
    async fn on_node_ready(&self, ctx: &NodeReadyCtx);
}

/// One birth as `on_call_failed` names it: `(node_id, incarnation, path, name)` (Q314).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallFailedBirth {
    /// Its node id.
    pub node_id: NodeId,
    /// Its incarnation.
    pub incarnation: IncarnationId,
    /// Its path, the name the birth holds at the call's end.
    pub path: String,
    /// The name, beside its path.
    pub name: String,
}

impl CallFailedBirth {
    /// The `(path, name)` lookup index key of path and name.
    pub fn location(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.path.clone(), name: self.name.clone() } }
}

/// Why the call ended: `NotSent` with its reason or `Indeterminate` with its reason, the end the
/// call's span stores.
pub use rdm_transport::CallFailedReason;

/// What `on_call_failed` is told (Q314, Q322): the calling birth, the birth the call was addressed
/// to, and the call's stored end, and nothing else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallFailedCtx {
    /// This node's birth: the call's sender.
    pub sender: CallFailedBirth,
    /// The birth the call was addressed to; `None` for a call that names no birth (an endpoint no
    /// record names) or whose receiver never resolved in this node's view.
    pub receiver: Option<CallFailedBirth>,
    /// The call's stored end.
    pub reason: CallFailedReason,
}

/// A notice on every node: one of its calls ended with its attempts spent (`fabric-node-hooks.md`
/// "on_call_failed"). It gates nothing: the call's owner already has its outcome by name.
#[async_trait::async_trait]
pub trait OnCallFailed: Send + Sync {
    /// Run once per spent call, on a task of its own, after the call has ended to its owner.
    async fn on_call_failed(&self, ctx: &CallFailedCtx);
}

/// What an embedding app registers, before RDM starts.
#[derive(Clone, Default)]
pub struct NodeHooks {
    before: Option<Arc<dyn PreNodeReady>>,
    after: Option<Arc<dyn OnNodeReady>>,
    call_failed: Option<Arc<dyn OnCallFailed>>,
    gate: PreNodeReadyGate,
}

impl NodeHooks {
    /// No hook: the node passes at once and runs no attempt.
    pub fn none() -> Self {
        Self::default()
    }

    /// This hook before Ready (`fabric-node-hooks.md` "pre_node_ready").
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rdm_node::node_hooks::{HookOutcome, NodeHooks, NodeReadyCtx, PreNodeReady};
    /// use std::sync::Arc;
    ///
    /// struct Warm;
    ///
    /// #[async_trait::async_trait]
    /// impl PreNodeReady for Warm {
    ///     async fn pre_node_ready(&self, _ctx: &NodeReadyCtx) -> HookOutcome {
    ///         HookOutcome::Ok
    ///     }
    /// }
    ///
    /// let hooks = NodeHooks::new(Arc::new(Warm));
    /// assert!(hooks.is_registered());
    /// ```
    pub fn new(hook: Arc<dyn PreNodeReady>) -> Self {
        Self { before: Some(hook), ..Self::default() }
    }

    /// Also run `after` once the node is Ready.
    pub fn with_on_node_ready(mut self, after: Arc<dyn OnNodeReady>) -> Self {
        self.after = Some(after);
        self
    }

    /// Also tell `on_call_failed` of each of this node's calls whose attempts are spent.
    pub fn with_on_call_failed(mut self, hook: Arc<dyn OnCallFailed>) -> Self {
        self.call_failed = Some(hook);
        self
    }

    /// What this node's Node RPC clients report a spent call to (`fabric-node-hooks.md`
    /// "on_call_failed"), the birth `node_id` at `incarnation` the sender, its name read per call
    /// from `name`; `None` when no `on_call_failed` is registered.
    pub fn call_failed_observer(&self, node_id: NodeId, incarnation: IncarnationId, name: crate::held_name::HeldName) -> Option<Arc<dyn rdm_transport::CallEndObserver>> {
        let hook = self.call_failed.clone()?;
        Some(Arc::new(CallFailedNotice { hook, node_id, incarnation, name }))
    }

    /// The gate RDM opens when the hook passes. The app clones it into its own server-side ops
    /// (`ServerBuilder::serve_gated`) before RDM starts.
    pub fn gate(&self) -> PreNodeReadyGate {
        self.gate.clone()
    }

    /// Whether a hook is registered.
    pub fn is_registered(&self) -> bool {
        self.before.is_some()
    }
}

/// The client's report of a spent call, turned into the `on_call_failed` notice: one task per call,
/// under `rdm.mesh.node.update.via-on-call-failed`, a child of the call's span.
struct CallFailedNotice {
    hook: Arc<dyn OnCallFailed>,
    node_id: NodeId,
    incarnation: IncarnationId,
    name: crate::held_name::HeldName,
}

impl rdm_transport::CallEndObserver for CallFailedNotice {
    /// The client tells `on_call_failed` a spent call alone; an answered one is never its.
    fn call_ended(&self, receiver: Option<&rdm_transport::ResolvedNode>, end: rdm_transport::CallEnd) {
        let rdm_transport::CallEnd::Spent(reason) = end else { return };
        let held = self.name.now();
        let ctx = CallFailedCtx {
            sender: CallFailedBirth { node_id: self.node_id.clone(), incarnation: self.incarnation.clone(), path: held.path, name: held.name },
            receiver: receiver.map(|r| CallFailedBirth { node_id: r.node_id.clone(), incarnation: r.incarnation.clone(), path: r.path.clone(), name: r.name.clone() }),
            reason,
        };
        let span = tracing::info_span!(
            "rdm.mesh.node.update.via-on-call-failed",
            node_path = %ctx.sender.path,
            node_name = %ctx.sender.name,
            node_id = %ctx.sender.node_id,
            incarnation_id = %ctx.sender.incarnation.as_str(),
            receiver_path = %ctx.receiver.as_ref().map(|r| r.path.as_str()).unwrap_or_default(),
            receiver_name = %ctx.receiver.as_ref().map(|r| r.name.as_str()).unwrap_or_default(),
            receiver_node_id = %ctx.receiver.as_ref().map(|r| r.node_id.to_string()).unwrap_or_default(),
            receiver_incarnation_id = %ctx.receiver.as_ref().map(|r| r.incarnation.as_str().to_string()).unwrap_or_default(),
            outcome = ctx.reason.name(),
            reason = ?ctx.reason,
            "otel.kind" = "internal",
        );
        let hook = self.hook.clone();
        tokio::spawn(
            async move {
                hook.on_call_failed(&ctx).await;
                tracing::info!("the application was told the call's attempts are spent");
            }
            .instrument(span),
        );
    }
}

/// Where one birth's hook stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreNodeReadyState {
    /// An attempt is running.
    Running {
        /// Which attempt.
        attempt: u32,
    },
    /// The last attempt answered `Pending` (or its time limit passed); the next follows the backoff.
    Pending {
        /// Which attempt.
        attempt: u32,
        /// What it waits for, in words.
        reason: String,
    },
    /// The hook returned `Ok`.
    Passed,
    /// The start ended Failed at the hook: it answered `Failed`, or its attempts are spent. The node
    /// stays Pending and nothing calls the hook again.
    Failed(String),
    /// The hook was cancelled before it passed.
    Cancelled,
}

impl PreNodeReadyState {
    /// The blocker in words, `None` once passed. This is what the Ready check names.
    pub fn blocker(&self, me: &PathName) -> Option<String> {
        match self {
            Self::Passed => None,
            Self::Running { attempt } => Some(format!("{me:?}: pre_node_ready is running (attempt {attempt})")),
            Self::Pending { attempt, reason } => Some(format!("{me:?}: pre_node_ready answered Pending (attempt {attempt}): {reason}")),
            Self::Failed(why) => Some(format!("{me:?}: its start ended Failed at pre_node_ready: {why}")),
            Self::Cancelled => Some(format!("{me:?}: cancelled before pre_node_ready passed")),
        }
    }

    /// What a `ProbeNodeState` is answered while the hook stands here (node-rpc-envelope.md, Status):
    /// `Pending` with the hook's own reason, `StartFailed` with the last reason once the start ended
    /// Failed, `None` (Current) in every other state. No attempt counter: a rerun pending for the same
    /// reason answers the same blocker, so the executor frames a new `Pending` only on a new reason.
    pub fn probe_answer(&self) -> Option<ProbeAnswer> {
        match self {
            Self::Pending { reason, .. } => Some(ProbeAnswer::Pending(reason.clone())),
            Self::Failed(why) => Some(ProbeAnswer::StartFailed(why.clone())),
            Self::Running { .. } | Self::Passed | Self::Cancelled => None,
        }
    }
}

/// A hook state a probe answers by name (node-rpc-envelope.md, Status: ProbeNodeState).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeAnswer {
    /// Status `Pending { node_id, incarnation, reason }`.
    Pending(String),
    /// Status `StartFailed { step: node.pre-node-ready, reason }`.
    StartFailed(String),
}

/// The step a start that ended Failed at the hook names (`StartFailed { step, reason }`).
pub const PRE_NODE_READY_STEP: &str = "node.pre-node-ready";

/// The Status reply a probe of the exact node (`node_id`, `incarnation`) is answered with for
/// `answer`, each within its reply bound, named on `rdm.node_admin.status.update.via-probe` with the
/// probe's `sender`: `Pending { node_id, incarnation, reason }`, or
/// `StartFailed { step: node.pre-node-ready, reason }` (node-rpc-envelope.md, Status).
pub fn probe_reply(answer: &ProbeAnswer, node_id: &NodeId, incarnation: &IncarnationId, sender: &str) -> rdm_core::status::StatusReply {
    let (state, reason, reply) = match answer {
        ProbeAnswer::Pending(reason) => ("pending", reason, rdm_core::reply::status::bounded_pending(node_id.clone(), incarnation.clone(), reason)),
        ProbeAnswer::StartFailed(reason) => ("start-failed", reason, rdm_core::reply::status::bounded_start_failed(PRE_NODE_READY_STEP, reason)),
    };
    tracing::info_span!("rdm.node_admin.status.update.via-probe", node = %node_id, sender = %sender, state, reason = %reason, "otel.kind" = "internal")
        .in_scope(|| tracing::info!("probed: the hook's answer is the probe's, nothing re-published"));
    reply
}

/// A read-only view of a birth's hook state.
#[derive(Clone)]
pub struct PreNodeReadyHandle {
    rx: tokio::sync::watch::Receiver<PreNodeReadyState>,
}

impl PreNodeReadyHandle {
    /// The state now.
    pub fn state(&self) -> PreNodeReadyState {
        self.rx.borrow().clone()
    }

    /// Resolves once the hook passed or the birth ended without passing.
    pub async fn settled(&self) -> PreNodeReadyState {
        match self.rx.clone().wait_for(|s| !matches!(s, PreNodeReadyState::Running { .. } | PreNodeReadyState::Pending { .. })).await {
            Ok(s) => s.clone(),
            Err(_) => self.state(),
        }
    }
}

/// How a birth's hook ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum End {
    /// Passed.
    Passed,
    /// The start ended Failed at the hook, and why: the hook's `Failed`, or its last `Pending` reason
    /// once its attempts were spent.
    Failed(String),
    /// Cancelled first.
    Cancelled,
}

/// A hook's answer that is not `Ok`: `Pending` is sent again while the hook's `Send` has attempts
/// left; `Failed` is an answer and ends the start at once.
enum NotOk {
    Pending(String),
    Failed(String),
}

/// The rest of a hook's run once its first attempt answered `Pending` with attempts left: its later
/// attempts on the same `Send`, to its end ([`HookDriver::first`]).
pub type Rest = std::pin::Pin<Box<dyn std::future::Future<Output = End> + Send>>;

/// Runs one birth's hook under its `Send` ([`rdm_transport::Send::run_paced`]) and holds the state the
/// Ready check and the probe read.
pub struct HookDriver {
    hook: Arc<dyn PreNodeReady>,
    after: Option<Arc<dyn OnNodeReady>>,
    ctx: NodeReadyCtx,
    gate: PreNodeReadyGate,
    state: tokio::sync::watch::Sender<PreNodeReadyState>,
    boot: tracing::Span,
    send: rdm_transport::Send,
    pacing: rdm_transport::Pacing,
}

impl NodeHooks {
    /// The driver for this birth, `None` when no hook is registered. The gate is opened for a node
    /// with no hook, so an app op registered without a hook is never refused.
    pub fn driver(&self, ctx: NodeReadyCtx, boot: tracing::Span) -> Option<Arc<HookDriver>> {
        self.driver_paced(ctx, boot, rdm_transport::Pacing::from_cadences())
    }

    /// [`NodeHooks::driver`] on the Send schedule `pacing` (a cell turns its jitter off).
    pub fn driver_paced(&self, ctx: NodeReadyCtx, boot: tracing::Span, pacing: rdm_transport::Pacing) -> Option<Arc<HookDriver>> {
        let Some(hook) = self.before.clone() else {
            self.gate.open();
            return None;
        };
        let send = hook.send();
        let (state, _) = tokio::sync::watch::channel(PreNodeReadyState::Running { attempt: 0 });
        Some(Arc::new(HookDriver { hook, after: self.after.clone(), ctx, gate: self.gate.clone(), state, boot, send, pacing }))
    }
}

impl HookDriver {
    /// A view of this hook's state.
    pub fn handle(&self) -> PreNodeReadyHandle {
        PreNodeReadyHandle { rx: self.state.subscribe() }
    }

    /// The hook under its `Send` ([`rdm_transport::Send::run_paced`]; Q344, Q350), the one retry
    /// mechanism: each attempt bounded by the `TimeLimit`, the Send's backoff before each after the
    /// first. `Pending`, and no answer within the time limit, are sent again while attempts are left;
    /// `Ok` passes; `Failed` is an answer and ends the start at once; the attempts spent end it Failed
    /// with the last reason. A cancel ends the attempt running or the wait between attempts. Nothing
    /// calls the hook again after it ends.
    pub async fn run(&self) -> End {
        let attempts = self.send.attempts.max(1);
        let made = std::sync::atomic::AtomicU32::new(0);
        let cancel = self.ctx.cancel.clone();
        let sent = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                self.state.send_replace(PreNodeReadyState::Cancelled);
                return End::Cancelled;
            }
            sent = self.send.run_paced(self.pacing, |e: &NotOk| matches!(e, NotOk::Failed(_)), || self.attempt(made.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1)) => sent,
        };
        match sent {
            Ok(()) => End::Passed,
            Err(rdm_transport::SendEnd::Answered(NotOk::Failed(why)) | rdm_transport::SendEnd::Spent { last: Some(NotOk::Failed(why)), .. }) => self.failed(why),
            Err(rdm_transport::SendEnd::Answered(NotOk::Pending(reason)) | rdm_transport::SendEnd::Spent { last: Some(NotOk::Pending(reason)), .. }) => {
                self.failed(format!("{reason} (pre_node_ready answered Pending on each of its {attempts} attempts)"))
            }
            Err(rdm_transport::SendEnd::Spent { last: None, .. }) => self.failed(format!("time limit (pre_node_ready answered Pending on each of its {attempts} attempts)")),
        }
    }

    /// The hook's run, its first attempt awaited here: the hook's end when it ended there, or the
    /// [`Rest`] of the same run once the first attempt answered `Pending` with attempts left, for its
    /// caller to await or hand to a task. One `Send` covers every attempt.
    pub async fn first(self: &Arc<Self>) -> Result<End, Rest> {
        let me = self.clone();
        let mut run: Rest = Box::pin(async move { me.run().await });
        let mut state = self.state.subscribe();
        tokio::select! {
            biased;
            end = &mut run => return Ok(end),
            _ = state.wait_for(|s| matches!(s, PreNodeReadyState::Pending { .. })) => {}
        }
        Err(run)
    }

    fn failed(&self, why: String) -> End {
        self.state.send_replace(PreNodeReadyState::Failed(why.clone()));
        End::Failed(why)
    }

    /// Attempt `n`: one span under the boot span carrying the outcome and the reason. Its future is
    /// dropped when its time limit passes (the Send's) or the birth is cancelled, which ends any pull
    /// the hook has in flight with it; [`Unanswered`] records which.
    async fn attempt(&self, n: u32) -> Result<(), NotOk> {
        self.state.send_replace(PreNodeReadyState::Running { attempt: n });
        let span = tracing::info_span!(
            parent: &self.boot,
            "rdm.mesh.node.update.via-pre-node-ready-attempt",
            node_path = %self.ctx.birth.path, node_name = %self.ctx.birth.name,
            node_id = %self.ctx.birth.node_id,
            incarnation_id = %self.ctx.birth.incarnation.as_str(),
            attempt = n,
            attempts = self.send.attempts,
            time_limit_ms = self.send.time_limit.0.as_millis() as u64,
            inherited = self.ctx.inherited,
            accepting_primary = %match &self.ctx.accepting_primary { AcceptingPrimarySource::Accepted(a) => a.node_id.to_string(), AcceptingPrimarySource::None(why) => format!("none: {}", why.reason()) },
            outcome = tracing::field::Empty,
            reason = tracing::field::Empty,
        );
        let ctx = NodeReadyCtx { attempt: n, ..self.ctx.clone() };
        let mut unanswered = Unanswered { driver: self, span: span.clone(), attempt: n, answered: false };
        let out = self.hook.pre_node_ready(&ctx).instrument(span.clone()).await;
        unanswered.answered = true;
        match out {
            HookOutcome::Ok => {
                self.gate.open();
                span.record("outcome", "ok");
                span.in_scope(|| tracing::info!("pre_node_ready returned Ok"));
                self.state.send_replace(PreNodeReadyState::Passed);
                Ok(())
            }
            HookOutcome::Pending { reason } => {
                span.record("outcome", "pending");
                span.record("reason", reason.as_str());
                span.in_scope(|| tracing::info!("pre_node_ready answered Pending: this attempt ends"));
                self.state.send_replace(PreNodeReadyState::Pending { attempt: n, reason: reason.clone() });
                Err(NotOk::Pending(reason))
            }
            HookOutcome::Failed { reason } => {
                span.record("outcome", "failed");
                span.record("reason", reason.as_str());
                span.in_scope(|| tracing::info!("pre_node_ready answered Failed"));
                Err(NotOk::Failed(reason))
            }
        }
    }

    /// Run the optional after-Ready hook, once, as a task that ends with the birth.
    pub fn spawn_on_node_ready(self: &Arc<Self>) {
        let Some(after) = self.after.clone() else { return };
        let me = self.clone();
        tokio::spawn(async move {
            let span = tracing::info_span!(parent: &me.boot, "rdm.mesh.node.update.via-on-node-ready", node_path = %me.ctx.birth.path, node_name = %me.ctx.birth.name);
            let ctx = me.ctx.clone();
            let cancel = me.ctx.cancel.clone();
            tokio::select! {
                () = after.on_node_ready(&ctx).instrument(span) => {}
                () = cancel.cancelled() => {}
            }
        });
    }
}

/// An attempt the hook has not answered yet. Dropped unanswered, it records why the attempt ended:
/// the birth cancelled (`cancelled`), or else its time limit passed, which is `Pending { reason: "time
/// limit" }` (fabric-node-hooks.md "Outcomes and the gate").
struct Unanswered<'a> {
    driver: &'a HookDriver,
    span: tracing::Span,
    attempt: u32,
    answered: bool,
}

impl Drop for Unanswered<'_> {
    fn drop(&mut self) {
        if self.answered {
            return;
        }
        if self.driver.ctx.cancel.is_cancelled() {
            self.span.record("outcome", "cancelled");
            self.span.in_scope(|| tracing::info!("pre_node_ready was cancelled: the hook and its pull ended"));
            self.driver.state.send_replace(PreNodeReadyState::Cancelled);
        } else {
            self.span.record("outcome", "pending");
            self.span.record("reason", "time limit");
            self.span.in_scope(|| tracing::info!("pre_node_ready gave no answer within its time limit: this attempt ends as Pending"));
            self.driver.state.send_replace(PreNodeReadyState::Pending { attempt: self.attempt, reason: "time limit".into() });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;

    /// CONTRACT (node-rpc-envelope.md, Status ProbeNodeState): the probe's answer is `Pending` with
    /// the hook's own reason while it answered Pending, `StartFailed` with the last reason once the
    /// start ended Failed, and Current otherwise; two attempts pending for the same reason answer the
    /// same blocker, and a changed reason a different one. What must NOT happen: an attempt counter
    /// in the answer, which would frame a new `Pending` on every rerun.
    #[test]
    fn a_rerun_pending_for_the_same_reason_answers_the_same_probe_answer() {
        let pending = |attempt, reason: &str| PreNodeReadyState::Pending { attempt, reason: reason.into() };
        let first = pending(1, "rows not yet published").probe_answer().expect("pending answers its reason");
        assert_eq!(first, ProbeAnswer::Pending("rows not yet published".into()), "the hook's own reason");
        assert_eq!(Some(first.clone()), pending(7, "rows not yet published").probe_answer(), "the attempt is not part of the answer");
        assert_ne!(Some(first), pending(8, "the node-admin is connections-deleting").probe_answer(), "a changed reason is a new answer");
        assert_eq!(PreNodeReadyState::Failed("x".into()).probe_answer(), Some(ProbeAnswer::StartFailed("x".into())), "a start that ended Failed answers StartFailed");
        for other in [PreNodeReadyState::Running { attempt: 2 }, PreNodeReadyState::Passed, PreNodeReadyState::Cancelled] {
            assert_eq!(other.probe_answer(), None, "Running, Passed and Cancelled answer Current: {other:?}");
        }
    }

    /// A hook that answers from a script, one outcome per attempt, holding the attempt the cell names
    /// until it is cancelled or its time limit passes.
    struct ScriptedHook {
        outcomes: Mutex<Vec<HookOutcome>>,
        hang_on_attempt: Option<u32>,
        started: Arc<tokio::sync::Notify>,
        send: rdm_transport::Send,
        attempts_seen: Mutex<Vec<(u32, tokio::time::Instant)>>,
    }

    impl ScriptedHook {
        fn new(outcomes: Vec<HookOutcome>, send: rdm_transport::Send) -> Arc<Self> {
            Arc::new(Self { outcomes: Mutex::new(outcomes), hang_on_attempt: None, started: Arc::new(tokio::sync::Notify::new()), send, attempts_seen: Mutex::default() })
        }

        fn hanging(on: u32, send: rdm_transport::Send) -> Arc<Self> {
            Arc::new(Self { outcomes: Mutex::new(Vec::new()), hang_on_attempt: Some(on), started: Arc::new(tokio::sync::Notify::new()), send, attempts_seen: Mutex::default() })
        }
    }

    #[async_trait::async_trait]
    impl PreNodeReady for ScriptedHook {
        fn send(&self) -> rdm_transport::Send {
            self.send
        }

        async fn pre_node_ready(&self, ctx: &NodeReadyCtx) -> HookOutcome {
            self.attempts_seen.lock().unwrap().push((ctx.attempt, tokio::time::Instant::now()));
            if self.hang_on_attempt == Some(ctx.attempt) {
                self.started.notify_one();
                std::future::pending::<()>().await;
            }
            self.outcomes.lock().unwrap().remove(0)
        }
    }

    /// What every cell's context is built from.
    struct Rig {
        ep: iroh::Endpoint,
        client: Arc<NodeRpcClient>,
        topology_view: TopologyView,
        channels: rdm_gossip::channel::Channels,
        rafka_time: rdm_transport::clock::RafkaTime,
        birth: Birth,
    }

    impl Rig {
        async fn new() -> Self {
            let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().expect("addr")).await.expect("bind ep");
            let client = Arc::new(NodeRpcClient::new(ep.clone(), Arc::new(rdm_transport::StaticResolver::new())));
            let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
            let rafka_time = rdm_transport::clock::RafkaTime::unadopted();
            let _ = rafka_time.adopt(1_000);
            let (fabric_id, mesh_id, name) = (FabricId::mint(), MeshId::mint(), PathName::numbered("mesh1", "node", 1));
            let topology_view = rdm_gossip::topology::TopologyView::new(&fabric_id, "mesh1", &mesh_id, &name.name, Arc::new(rafka_time.clone()));
            let channels = rdm_gossip::channel::Channels::new(&gossip, &ep, &topology_view).expect("join topology view");
            channels.join(rdm_gossip::channel::ChannelId::mesh(topology_view.fabric_id(), topology_view.mesh_id()), Vec::new()).await.expect("join topology view");
            let birth = Birth { path: name.path.clone(), name: name.name.clone(), node_id: NodeId::mint(), incarnation: IncarnationId::mint(), fabric_id, mesh_id, r#type: "node".into() };
            Self { ep, client, topology_view, channels, rafka_time, birth }
        }

        fn ctx(&self, cancel: CancelToken) -> NodeReadyCtx {
            NodeReadyCtx {
                start_kind: StartKind::FirstStart,
                birth: self.birth.clone(),
                accepting_primary: AcceptingPrimarySource::None(NoAcceptingPrimary::Day0Root),
                client: self.client.clone(),
                topology_view: self.topology_view.clone(), channels: self.channels.clone(),
                cancel,
                rafka_time: self.rafka_time.clone(),
                member_cert: None,
                attempt: 0,
                inherited: false,
            }
        }
    }

    /// The cells' backoff: 40 ms doubling, capped at 200 ms, no jitter, so the gaps are exact.
    const PACING: rdm_transport::Pacing = rdm_transport::Pacing { cadence: Duration::from_millis(40), floor: Duration::from_millis(200), jitter: false };

    fn send(attempts: u8, time_limit: Duration) -> rdm_transport::Send {
        rdm_transport::Send { attempts, time_limit: rdm_transport::TimeLimit(time_limit) }
    }

    // @feature: pre-node-ready
    /// CONTRACT (fabric-node-hooks.md "Outcomes and the gate", Q350): RDM runs the hook under the
    /// `Send` its own `send()` gives. `Ok` opens the gate and the node goes on to Ready; `Failed` ends
    /// the start Failed with its reason at once, never calling again; `Pending` ends that attempt, the
    /// node stays Pending with the reason as its blocker and probe answer, and RDM calls again after
    /// the backoff (cadence × 2^(n-1)), so a hook that answers Pending twice then Ok passes on its third
    /// attempt. What must NOT happen: a call before the backoff, a call after Ok or Failed, or the gate
    /// open on anything but Ok.
    #[test]
    fn the_hook_answers_ok_pending_or_failed_under_its_send() {
        let dir = std::env::temp_dir().join(format!("rdm-pre-node-ready-answers-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-pre-node-ready-answers").expect("evidence capture starts");
        capture.run(2, async {
            let rig = Rig::new().await;
            let limit = Duration::from_secs(10);

            // Ok: the first attempt passes and the gate opens.
            let ok = ScriptedHook::new(vec![HookOutcome::Ok], send(3, limit));
            let hooks = NodeHooks::new(ok.clone());
            let driver = hooks.driver_paced(rig.ctx(CancelToken::new()), tracing::Span::none(), PACING).expect("driver");
            assert_eq!(driver.run().await, End::Passed);
            assert!(hooks.gate().is_open(), "Ok opens the gate");
            assert_eq!(ok.attempts_seen.lock().unwrap().len(), 1, "Ok is called once");

            // Failed: the start ends Failed at once with the hook's reason; no second call.
            let failed = ScriptedHook::new(vec![HookOutcome::Failed { reason: "the rows are corrupt".into() }], send(3, limit));
            let hooks = NodeHooks::new(failed.clone());
            let driver = hooks.driver_paced(rig.ctx(CancelToken::new()), tracing::Span::none(), PACING).expect("driver");
            assert_eq!(driver.run().await, End::Failed("the rows are corrupt".into()));
            assert!(!hooks.gate().is_open(), "Failed never opens the gate");
            assert_eq!(failed.attempts_seen.lock().unwrap().len(), 1, "Failed is never called again");
            assert_eq!(driver.handle().state().probe_answer(), Some(ProbeAnswer::StartFailed("the rows are corrupt".into())), "the probe answers StartFailed with the reason");

            // Pending, Pending, Ok: called again after each backoff, passing on the third attempt.
            let pending = ScriptedHook::new(vec![HookOutcome::Pending { reason: "rows not yet published".into() }, HookOutcome::Pending { reason: "rows not yet published".into() }, HookOutcome::Ok], send(3, limit));
            let hooks = NodeHooks::new(pending.clone());
            let driver = hooks.driver_paced(rig.ctx(CancelToken::new()), tracing::Span::none(), PACING).expect("driver");
            let handle = driver.handle();
            let watch = tokio::spawn({
                let handle = handle.clone();
                async move {
                    let mut rx = handle.rx.clone();
                    let _ = rx.wait_for(|s| matches!(s, PreNodeReadyState::Pending { .. })).await;
                    handle.state()
                }
            });
            assert_eq!(driver.run().await, End::Passed);
            let seen = watch.await.unwrap();
            assert_eq!(seen.probe_answer(), Some(ProbeAnswer::Pending("rows not yet published".into())), "while Pending the probe answers its reason");
            assert!(seen.blocker(&rig.birth.location()).is_some_and(|b| b.contains("rows not yet published")), "the reason is the blocker evidence: {seen:?}");
            let at: Vec<_> = pending.attempts_seen.lock().unwrap().clone();
            assert_eq!(at.iter().map(|(n, _)| *n).collect::<Vec<_>>(), vec![1, 2, 3], "one call per attempt, numbered from 1");
            assert!(at[1].1 - at[0].1 >= Duration::from_millis(40), "the second call follows the first backoff, 40 ms: {at:?}");
            assert!(at[2].1 - at[1].1 >= Duration::from_millis(80), "the third call follows the doubled backoff, 80 ms: {at:?}");
            assert!(hooks.gate().is_open());
            rig.ep.close().await;
        });
        let spans = capture.spans().expect("captured spans");
        let _ = std::fs::remove_dir_all(&dir);
        let attempts: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.node.update.via-pre-node-ready-attempt").collect();
        for outcome in ["ok", "failed", "pending"] {
            assert!(attempts.iter().any(|s| s["attributes"]["outcome"] == outcome), "an attempt span names outcome {outcome}: {attempts:?}");
        }
        assert!(attempts.iter().any(|s| s["attributes"]["outcome"] == "pending" && s["attributes"]["reason"] == "rows not yet published"), "the pending attempt names its reason");
    }

    // @feature: pre-node-ready
    /// CONTRACT (fabric-node-hooks.md "Outcomes and the gate", Q350): a hook that answers `Pending` on
    /// every attempt spends them: after its third `Pending` (the default `Send`, 3 attempts) the start
    /// ends Failed with the last reason, the node stays Pending and nothing calls the hook again; no
    /// answer within the `TimeLimit` is `Pending { reason: "time limit" }`. What must NOT happen: a
    /// fourth call, the gate open, or a hook that never answers holding the start past its attempts.
    #[test]
    fn pending_on_every_attempt_spends_them_and_the_start_ends_failed_with_the_last_reason() {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        rt.block_on(async {
            let rig = Rig::new().await;
            let reasons = ["the node-admin answered NotReady", "rows not yet published", "rows still not published"];
            let hook = ScriptedHook::new(reasons.iter().map(|r| HookOutcome::Pending { reason: (*r).into() }).collect(), send(rdm_transport::Send::default().attempts, Duration::from_secs(10)));
            let hooks = NodeHooks::new(hook.clone());
            let driver = hooks.driver_paced(rig.ctx(CancelToken::new()), tracing::Span::none(), PACING).expect("driver");
            let End::Failed(why) = driver.run().await else { panic!("spent attempts end the start Failed") };
            assert!(why.starts_with("rows still not published"), "the start ends with the last reason: {why}");
            assert_eq!(hook.attempts_seen.lock().unwrap().len(), 3, "three attempts, the default Send's, and no fourth");
            assert_eq!(driver.handle().state(), PreNodeReadyState::Failed(why.clone()));
            assert!(!hooks.gate().is_open());
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(hook.attempts_seen.lock().unwrap().len(), 3, "nothing calls the hook again after the start ended Failed");

            // A hook that never answers: each attempt ends at its time limit as Pending "time limit".
            let silent = ScriptedHook::hanging(1, send(1, Duration::from_millis(100)));
            let driver = NodeHooks::new(silent.clone()).driver_paced(rig.ctx(CancelToken::new()), tracing::Span::none(), PACING).expect("driver");
            let started = tokio::time::Instant::now();
            let End::Failed(why) = driver.run().await else { panic!("a hook that never answers ends the start Failed") };
            assert!(why.starts_with("time limit"), "no answer within the TimeLimit is Pending \"time limit\": {why}");
            assert!(started.elapsed() < Duration::from_secs(5), "the time limit bounds the attempt: {:?}", started.elapsed());
            rig.ep.close().await;
        });
    }

    // @feature: pre-node-ready
    /// CONTRACT (Q344: one `Send` on every call, the only retry mechanism; Q350; D33): a first start's
    /// first attempt is awaited inline and, answered `Pending`, the rest of the same run goes on under
    /// the same `Send`: its attempts count from the first, so a hook answering `Pending` on each spends
    /// three in all and ends Failed with the last reason, and a hook answering `Pending` then `Ok`
    /// passes on its second. Must NOT happen: a fresh count for the rest (a fourth call), a call after
    /// the end, or the first attempt's `Pending` taken for the end.
    #[test]
    fn the_first_attempts_pending_hands_on_the_rest_of_the_same_send() {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        rt.block_on(async {
            let rig = Rig::new().await;
            let limit = Duration::from_secs(10);
            let spent = ScriptedHook::new((0..4).map(|n| HookOutcome::Pending { reason: format!("not yet {n}") }).collect(), send(3, limit));
            let driver = NodeHooks::new(spent.clone()).driver_paced(rig.ctx(CancelToken::new()), tracing::Span::none(), PACING).expect("driver");
            let Err(rest) = driver.first().await else { panic!("the first attempt answered Pending: the rest is handed on") };
            assert_eq!(spent.attempts_seen.lock().unwrap().len(), 1, "the first attempt is the only one awaited inline");
            let End::Failed(why) = rest.await else { panic!("the Send's attempts spent end the start Failed") };
            assert!(why.starts_with("not yet 2") && why.contains("each of its 3 attempts"), "the last reason, after three attempts in all: {why}");
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(spent.attempts_seen.lock().unwrap().len(), 3, "three attempts in all, the first included, and no fourth");

            let passes = ScriptedHook::new(vec![HookOutcome::Pending { reason: "not yet".into() }, HookOutcome::Ok], send(3, limit));
            let hooks = NodeHooks::new(passes.clone());
            let driver = hooks.driver_paced(rig.ctx(CancelToken::new()), tracing::Span::none(), PACING).expect("driver");
            let Err(rest) = driver.first().await else { panic!("the first attempt answered Pending") };
            assert_eq!(rest.await, End::Passed);
            assert_eq!(passes.attempts_seen.lock().unwrap().iter().map(|(n, _)| *n).collect::<Vec<_>>(), vec![1, 2], "the second attempt is the Send's second");
            assert!(hooks.gate().is_open());

            let ok = ScriptedHook::new(vec![HookOutcome::Ok], send(3, limit));
            let driver = NodeHooks::new(ok.clone()).driver_paced(rig.ctx(CancelToken::new()), tracing::Span::none(), PACING).expect("driver");
            assert!(matches!(driver.first().await, Ok(End::Passed)), "an end on the first attempt is the end");
            rig.ep.close().await;
        });
    }

    // @feature: pre-node-ready
    /// CONTRACT (fabric-node-hooks.md "Cancellation and lifecycle commands"): a stop or delete cancels
    /// the token, which ends the running hook (its future is dropped with any pull in flight) and ends
    /// the wait between attempts; the hook ends Cancelled, never Failed. What must NOT happen: a call
    /// after the cancel.
    #[test]
    fn a_cancel_ends_the_running_hook_and_the_wait_between_attempts() {
        let dir = std::env::temp_dir().join(format!("rdm-pre-node-ready-cancel-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-pre-node-ready-cancel").expect("evidence capture starts");
        capture.run(2, async {
            let rig = Rig::new().await;
            // During an attempt.
            let cancel = CancelToken::new();
            let hook = ScriptedHook::hanging(1, send(3, Duration::from_secs(10)));
            let driver = NodeHooks::new(hook.clone()).driver_paced(rig.ctx(cancel.clone()), tracing::Span::none(), PACING).expect("driver");
            let started = hook.started.clone();
            tokio::spawn(async move {
                started.notified().await;
                cancel.cancel();
            });
            assert_eq!(driver.run().await, End::Cancelled);
            assert_eq!(driver.handle().state(), PreNodeReadyState::Cancelled);

            // During the backoff after a Pending.
            let cancel = CancelToken::new();
            let slow = rdm_transport::Pacing { cadence: Duration::from_secs(30), floor: Duration::from_secs(30), jitter: false };
            let hook = ScriptedHook::new(vec![HookOutcome::Pending { reason: "not yet".into() }, HookOutcome::Ok], send(3, Duration::from_secs(10)));
            let driver = NodeHooks::new(hook.clone()).driver_paced(rig.ctx(cancel.clone()), tracing::Span::none(), slow).expect("driver");
            let handle = driver.handle();
            tokio::spawn(async move {
                let _ = handle.rx.clone().wait_for(|s| matches!(s, PreNodeReadyState::Pending { .. })).await;
                cancel.cancel();
            });
            assert_eq!(tokio::time::timeout(Duration::from_secs(5), driver.run()).await.expect("the cancel ends the wait"), End::Cancelled);
            assert_eq!(hook.attempts_seen.lock().unwrap().len(), 1, "no call after the cancel");
            rig.ep.close().await;
        });
        let spans = capture.spans().expect("captured spans");
        let _ = std::fs::remove_dir_all(&dir);
        assert!(spans.iter().any(|s| s["name"] == "rdm.mesh.node.update.via-pre-node-ready-attempt" && s["attributes"]["outcome"] == "cancelled"), "the cancelled attempt names it");
    }

    // @feature: on-node-ready
    #[test]
    fn spawn_on_node_ready_runs_once_on_own_task_under_boot_span_and_aborts_when_birth_is_cancelled() {
        // docs/architecture/fabric-node-hooks.md:119 — "After the node reports node.started, RDM
        // calls `on_node_ready` once, with the node's `NodeReadyCtx`, on a task of its own. It is
        // for optional work that does not gate readiness."
        struct PassBefore;
        #[async_trait::async_trait]
        impl PreNodeReady for PassBefore {
            async fn pre_node_ready(&self, _ctx: &NodeReadyCtx) -> HookOutcome {
                HookOutcome::Ok
            }
        }

        struct RecordAfter {
            seen_nodes: Mutex<Vec<PathName>>,
            done_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        }
        #[async_trait::async_trait]
        impl OnNodeReady for RecordAfter {
            async fn on_node_ready(&self, ctx: &NodeReadyCtx) {
                self.seen_nodes.lock().unwrap().push(ctx.birth.location());
                if let Some(tx) = self.done_tx.lock().unwrap().take() {
                    let _ = tx.send(());
                }
            }
        }

        struct HangUntilCancelledAfter {
            entered: Arc<tokio::sync::Notify>,
            dropped_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        }
        struct DropGuard(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for DropGuard {
            fn drop(&mut self) {
                if let Some(tx) = self.0.take() {
                    let _ = tx.send(());
                }
            }
        }
        #[async_trait::async_trait]
        impl OnNodeReady for HangUntilCancelledAfter {
            async fn on_node_ready(&self, _ctx: &NodeReadyCtx) {
                let _guard = DropGuard(self.dropped_tx.lock().unwrap().take());
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
        }

        let dir = std::env::temp_dir().join(format!("rdm-on-node-ready-unit-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-on-node-ready-unit")
            .expect("evidence capture starts");

        let expected_name = PathName::numbered("mesh1", "node", 1);
        let expected_name_for_assert = expected_name.clone();
        capture.run(2, async move {
            let ep = rdm_transport::endpoint::bind(
                iroh::SecretKey::generate(),
                "127.0.0.1:0".parse().expect("addr"),
            )
            .await
            .expect("bind ep");
            let resolver = Arc::new(rdm_transport::StaticResolver::new());
            let client = Arc::new(NodeRpcClient::new(ep.clone(), resolver));
            let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
            let rafka_time = rdm_transport::clock::RafkaTime::unadopted();
            let _ = rafka_time.adopt(1_000);
            let fabric_id = FabricId::mint();
            let mesh_id = MeshId::mint();
            let topology_view = rdm_gossip::topology::TopologyView::new(&fabric_id, "mesh1", &mesh_id, &expected_name.name, Arc::new(rafka_time.clone()));
            let channels = rdm_gossip::channel::Channels::new(&gossip, &ep, &topology_view).expect("join topology view");
            channels.join(rdm_gossip::channel::ChannelId::mesh(topology_view.fabric_id(), topology_view.mesh_id()), Vec::new()).await
            .expect("join topology view");

            let birth = Birth {
                path: expected_name.path.clone(), name: expected_name.name.clone(),
                node_id: NodeId::mint(),
                incarnation: IncarnationId::mint(),
                fabric_id,
                mesh_id,
                r#type: "node".into(),
            };

            // 1. Without `with_on_node_ready`, `spawn_on_node_ready` is a no-op.
            let no_after_hooks = NodeHooks::new(Arc::new(PassBefore));
            let no_after_driver = no_after_hooks
                .driver(
                    NodeReadyCtx {
                        start_kind: StartKind::FirstStart,
                        birth: birth.clone(),
                        accepting_primary: AcceptingPrimarySource::None(NoAcceptingPrimary::Day0Root),
                        client: client.clone(),
                        topology_view: topology_view.clone(), channels: channels.clone(),
                        cancel: CancelToken::new(),
                        rafka_time: rafka_time.clone(),
                        member_cert: None,
                        attempt: 0,
                        inherited: false,
                    },
                    tracing::Span::none(),
                )
                .expect("driver");
            assert_eq!(no_after_driver.run().await, End::Passed);
            no_after_driver.spawn_on_node_ready();

            // 2. With `with_on_node_ready`, `spawn_on_node_ready` runs `on_node_ready` once on its
            //    own task with `NodeReadyCtx` under `rdm.mesh.node.update.via-on-node-ready`.
            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
            let after = Arc::new(RecordAfter {
                seen_nodes: Mutex::new(Vec::new()),
                done_tx: Mutex::new(Some(done_tx)),
            });
            let hooks = NodeHooks::new(Arc::new(PassBefore)).with_on_node_ready(after.clone());
            let boot_span = tracing::info_span!("rdm.mesh.node.create.via-boot", node_path = %birth.path, node_name = %birth.name);
            let driver = hooks
                .driver(
                    NodeReadyCtx {
                        start_kind: StartKind::FirstStart,
                        birth: birth.clone(),
                        accepting_primary: AcceptingPrimarySource::None(NoAcceptingPrimary::Day0Root),
                        client: client.clone(),
                        topology_view: topology_view.clone(), channels: channels.clone(),
                        cancel: CancelToken::new(),
                        rafka_time: rafka_time.clone(),
                        member_cert: None,
                        attempt: 0,
                        inherited: false,
                    },
                    boot_span,
                )
                .expect("driver");
            assert_eq!(driver.run().await, End::Passed);
            driver.spawn_on_node_ready();
            done_rx.await.expect("on_node_ready completed on its own task");
            assert_eq!(*after.seen_nodes.lock().unwrap(), vec![expected_name]);

            // 3. Cancelling the birth aborts an in-flight `on_node_ready` task and drops its future.
            let cancel = CancelToken::new();
            let entered = Arc::new(tokio::sync::Notify::new());
            let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
            let hang_after = Arc::new(HangUntilCancelledAfter {
                entered: entered.clone(),
                dropped_tx: Mutex::new(Some(dropped_tx)),
            });
            let cancel_hooks =
                NodeHooks::new(Arc::new(PassBefore)).with_on_node_ready(hang_after);
            let cancel_driver = cancel_hooks
                .driver(
                    NodeReadyCtx {
                        start_kind: StartKind::FirstStart,
                        birth,
                        accepting_primary: AcceptingPrimarySource::None(NoAcceptingPrimary::Day0Root),
                        client,
                        topology_view,
                        channels,
                        cancel: cancel.clone(),
                        rafka_time,
                        member_cert: None,
                        attempt: 0,
                        inherited: false,
                    },
                    tracing::Span::none(),
                )
                .expect("driver");
            assert_eq!(cancel_driver.run().await, End::Passed);
            cancel_driver.spawn_on_node_ready();
            entered.notified().await;
            cancel.cancel();
            dropped_rx
                .await
                .expect("cancelling the birth drops the in-flight on_node_ready future");

            ep.close().await;
        });

        let spans = capture.spans().expect("captured spans");
        let _ = std::fs::remove_dir_all(&dir);
        let on_ready_spans: Vec<&serde_json::Value> = spans
            .iter()
            .filter(|s| s["name"] == "rdm.mesh.node.update.via-on-node-ready")
            .collect();
        assert!(
            on_ready_spans
                .iter()
                .any(|s| s["attributes"]["node_path"] == expected_name_for_assert.path && s["attributes"]["node_name"] == expected_name_for_assert.name),
            "expected rdm.mesh.node.update.via-on-node-ready span for node: {on_ready_spans:?}"
        );
    }
}
