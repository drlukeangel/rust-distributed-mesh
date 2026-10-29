//! `pre_node_ready`: the one hook an embedding app supplies to be born full.
//!
//! A node reaches the hook after its `JoinNode` was accepted, its `GetTopology` read installed
//! and its mesh channel joined (node-start.md step 3). The hook returns
//! [`HookOutcome`]:
//!
//! - `Ok`: RDM's own checks go on to Ready, and the app's own ops are served
//!   ([`PreNodeReadyGate`] opens).
//! - `Blocked { reason, retry_on }`: the node stays Pending, the blocker is named where Ready
//!   blockers are named, and the hook runs again only when the named [`RetryOn`] event happens.
//!   The wait arms, then re-checks the condition against a count taken before the attempt began, so
//!   an event that happened while the attempt ran cannot strand the node. No timer and no sleep.
//! - `Failed { reason }`: the node ends by name.
//!
//! The Day-0 root and a fabric-primary re-born from its durable map have no authority to pull from:
//! their context says so ([`Authority::None`]) and the hook completes from local state
//! (`gossip.md` section 9's targeted exception).
//!
//! One span per attempt (`rdm.mesh.node.update.via-pre-node-ready-attempt`) under the boot span carries
//! the outcome and the blocker or reason; `via-ready` follows the successful attempt.
//! [`OnNodeReady`] is for optional work only: nothing required may live there.

use crate::model::{FabricId, IncarnationId, MeshId, NodeId, NodeKind, PathName};
use rdm_core::{MemberStatus, MeshDigest};
use std::time::Duration;
use rdm_transport::topology::{DigestView, TopologyView};
use rdm_transport::{CancelToken, PreNodeReadyGate, NodeRpcClient};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use tracing::Instrument;

/// The exact birth the hook runs for.
#[derive(Debug, Clone)]
pub struct Birth {
    /// The birth's `path.name`.
    pub name: PathName,
    /// Its node id.
    pub node_id: NodeId,
    /// Its incarnation.
    pub incarnation: IncarnationId,
    /// Its fabric.
    pub fabric_id: FabricId,
    /// Its mesh.
    pub mesh_id: MeshId,
    /// Its kind.
    pub kind: NodeKind,
}

/// The admin that accepted this birth's `JoinNode`: the authority the hook pulls from. A start after
/// a stop reached it back over the call that commanded the start, holding no record of it
/// (node-start.md step 2): it is known by the name its join answer gave and the endpoint it answered
/// on, and the hook calls it through `target`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptingAuthority {
    /// The admin's `path.name`, as the join answer's `served_by` (or, at a first start, the
    /// launcher's record) names it.
    pub name: PathName,
    /// Its endpoint key.
    pub endpoint: iroh::PublicKey,
    /// Where a call to it goes: the launcher's birth at a first start; after a stop, the endpoint
    /// and path of the call the join went back over.
    pub target: rdm_transport::NodeTarget,
}

/// Why a birth has no authority to pull from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoAuthority {
    /// The Day-0 root: no fabric authority exists before it.
    Day0Root,
    /// An admin started on an inherited root (fabric recovery): it has no maker, and its hook reads the
    /// inherited storage.
    InheritedRoot,
    /// A recovery start (mesh-primary or fabric-primary flags): its hook reads from the nodes it reaches.
    Recovery,
}

impl NoAuthority {
    /// The reason in words.
    pub fn reason(self) -> &'static str {
        match self {
            Self::Day0Root => "the Day-0 root: no authority exists before this node",
            Self::InheritedRoot => "an admin started on an inherited root: its hook reads the inherited storage",
            Self::Recovery => "a recovery start: no admin accepted this birth",
        }
    }
}

/// Where the hook pulls from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authority {
    /// The admin that accepted this birth's join.
    Accepted(AcceptingAuthority),
    /// There is none, and why: the hook resolves from local state.
    None(NoAuthority),
}

impl Authority {
    /// The accepting admin, when there is one.
    pub fn accepted(&self) -> Option<&AcceptingAuthority> {
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
    /// The authority to pull from, or why there is none.
    pub authority: Authority,
    /// The process's one Node RPC client.
    pub client: Arc<NodeRpcClient>,
    /// The topology/gossip handle: the app subscribes its kind topics before it takes its
    /// snapshot, so what changes during the pull arrives by gossip.
    pub topology_view: TopologyView,
    /// Cancelled when the birth is deleted (stop-node, its process stopping): the hook
    /// and any pull it has in flight end.
    pub cancel: CancelToken,
    /// Rafka-time: the process's one instance, adopted by the join that was accepted.
    pub rafka_time: rdm_transport::clock::RafkaTime,
    /// The member cert the accepting authority's signer issued for exactly this birth, as opaque
    /// bytes (`fabric-certs.md`): empty when that authority is configured with no certs; `None`
    /// for a birth that made no JoinNode (the Day-0 root, a recovery start).
    pub member_cert: Option<Vec<u8>>,
    /// Which attempt this is, from 1.
    pub attempt: u32,
    /// Whether this birth's data dir was inherited from a previous birth (R-FM2).
    pub inherited: bool,
}

impl NodeReadyCtx {
    /// The outcome for a pull the authority answered with its typed `NotReady`: wait for that
    /// authority to be Ready.
    pub fn authority_not_ready(&self, reason: impl Into<String>) -> HookOutcome {
        HookOutcome::Blocked { reason: reason.into(), retry_on: RetryOn::PrimaryReady }
    }
}

/// The event a blocked hook waits for: the envelope's typed `RetryOn` (node-rpc-envelope.md, Status
/// op 0x1B), the same value a probe of a blocked birth answers in `Blocked`.
pub use rdm_core::status::RetryOn;

/// What the hook returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookOutcome {
    /// The node holds the state its role needs: go on to Ready.
    Ok,
    /// Not yet: stay Pending and run again on `retry_on`.
    Blocked {
        /// What the node is waiting for, in words.
        reason: String,
        /// The event that runs the hook again.
        retry_on: RetryOn,
    },
    /// Never: the node ends by name.
    Failed {
        /// Why.
        reason: String,
    },
}

/// The one hook: run before Ready, for every birth.
#[async_trait::async_trait]
pub trait PreNodeReady: Send + Sync {
    /// The step this node's role takes before it reports Ready, with `ctx.authority`.
    async fn pre_node_ready(&self, ctx: &NodeReadyCtx) -> HookOutcome;
}

/// Optional work after Ready. Nothing required may live here, and it is not a second projection
/// path: the node is already serving.
#[async_trait::async_trait]
pub trait OnNodeReady: Send + Sync {
    /// Run once, after the node published Ready.
    async fn on_node_ready(&self, ctx: &NodeReadyCtx);
}

/// What an embedding app registers, before RDM starts.
#[derive(Clone, Default)]
pub struct NodeHooks {
    before: Option<Arc<dyn PreNodeReady>>,
    after: Option<Arc<dyn OnNodeReady>>,
    gate: PreNodeReadyGate,
}

impl NodeHooks {
    /// No hook: the node passes at once and runs no attempt.
    pub fn none() -> Self {
        Self::default()
    }

    /// This hook before Ready.
    pub fn new(hook: Arc<dyn PreNodeReady>) -> Self {
        Self { before: Some(hook), ..Self::default() }
    }

    /// Also run `after` once the node is Ready.
    pub fn with_on_node_ready(mut self, after: Arc<dyn OnNodeReady>) -> Self {
        self.after = Some(after);
        self
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

/// Which endpoints addressed this node with a down op, counted: a count survives an event that
/// happened before anything waited on it. Keyed by the endpoint the transport proved for the call,
/// so an authority no record names (a commander born after this node's stop, node-start.md step 2)
/// is counted too.
#[derive(Debug, Default)]
pub struct Addressed {
    counts: Mutex<std::collections::HashMap<iroh::PublicKey, u64>>,
    tick: tokio::sync::watch::Sender<u64>,
}

impl Addressed {
    /// An empty record.
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    /// The endpoint `from` sent this node a down op.
    pub fn note(&self, from: &iroh::PublicKey) {
        *self.counts.lock().unwrap().entry(*from).or_default() += 1;
        self.tick.send_modify(|v| *v += 1);
    }

    fn count(&self, from: &iroh::PublicKey) -> u64 {
        self.counts.lock().unwrap().get(from).copied().unwrap_or(0)
    }
}

/// The counts a retry waits against, taken before an attempt begins.
#[derive(Debug, Clone)]
pub struct Mark {
    heard: u64,
    addressed: u64,
    authority_incarnation: Option<IncarnationId>,
}

/// How a wait for a retry event ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Waited {
    /// The event happened.
    Fired(&'static str),
    /// The birth was deleted.
    Cancelled,
}

/// The retry events of one birth, read from its topology view and the down ops it was sent.
#[derive(Clone)]
pub struct Events {
    digest_view: DigestView,
    addressed: Arc<Addressed>,
}

impl Events {
    /// The events of the birth holding `digest_view`, with the down ops recorded in `addressed`.
    pub fn new(digest_view: DigestView, addressed: Arc<Addressed>) -> Self {
        Self { digest_view, addressed }
    }

    /// The counts now, for `authority`.
    pub fn mark(&self, authority: &Authority) -> Mark {
        let heard = *self.digest_view.heard_changes().borrow();
        match authority.accepted() {
            Some(a) => Mark { heard, addressed: self.addressed.count(&a.endpoint), authority_incarnation: self.birth_of(a).map(|(d, _)| d.node.incarnation) },
            None => Mark { heard, addressed: 0, authority_incarnation: None },
        }
    }

    /// The birth this node's view holds at the authority's endpoint, and how long it has been silent:
    /// the authority is known by its endpoint, which names one process.
    fn birth_of(&self, authority: &AcceptingAuthority) -> Option<(MeshDigest, Duration)> {
        let key = authority.endpoint.to_string();
        let held = self.digest_view.all().into_iter().find(|d| d.node.endpoint_id.0 == key)?;
        self.digest_view.get(held.node.node_id.as_str())
    }

    fn holds(&self, on: &RetryOn, authority: Option<&AcceptingAuthority>, mark: &Mark) -> bool {
        let moved = *self.digest_view.heard_changes().borrow() > mark.heard;
        if let RetryOn::TargetReachable { node_id: id } = on {
            return moved && self.digest_view.current(self.digest_view.staleness_floor()).iter().any(|d| &d.node.node_id == id);
        }
        // An authority event of a birth with no authority never happens; the attempt refuses such a
        // request by name before any wait begins.
        let Some(authority) = authority else { return false };
        let birth = self.birth_of(authority);
        match on {
            RetryOn::PrimaryChanged => match &birth {
                Some((d, _)) => self.digest_view.is_deleted(d.node.node_id.as_str()) || Some(&d.node.incarnation) != mark.authority_incarnation.as_ref(),
                None => mark.authority_incarnation.is_some(),
            },
            RetryOn::PrimaryReady => moved && birth.is_some_and(|(d, silent)| d.status == MemberStatus::Ready && silent <= self.digest_view.staleness_floor()),
            RetryOn::TargetReachable { .. } => unreachable!("answered above"),
            RetryOn::PrimaryAddressed => self.addressed.count(&authority.endpoint) > mark.addressed,
        }
    }

    /// Wait for `on`: arm on the view's and the down ops' change counts, then check the condition
    /// against `mark`, and only then sleep until a count moves or the birth is cancelled.
    pub async fn wait(&self, on: &RetryOn, authority: Option<&AcceptingAuthority>, mark: &Mark, cancel: &CancelToken) -> Waited {
        loop {
            let mut heard = self.digest_view.heard_changes();
            let mut addressed = self.addressed.tick.subscribe();
            heard.borrow_and_update();
            addressed.borrow_and_update();
            if self.holds(on, authority, mark) {
                return Waited::Fired(on.name());
            }
            tokio::select! {
                _ = heard.changed() => {}
                _ = addressed.changed() => {}
                () = cancel.cancelled() => return Waited::Cancelled,
            }
        }
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
    /// The last attempt was blocked and the hook waits on its event.
    Blocked {
        /// Which attempt.
        attempt: u32,
        /// What it waits for, in words.
        reason: String,
        /// The event that runs the hook again.
        retry_on: RetryOn,
    },
    /// The hook returned `Ok`.
    Passed,
    /// The hook failed: the node ends by name.
    Failed(String),
    /// The hook was cancelled before it passed.
    Cancelled,
}

impl PreNodeReadyState {
    /// The blocker in words, `None` once passed. This is what the Ready check names.
    pub fn blocker(&self, me: &PathName) -> Option<String> {
        match self {
            Self::Passed => None,
            Self::Running { attempt } => Some(format!("{me}: pre_node_ready is running (attempt {attempt})")),
            Self::Blocked { attempt, reason, retry_on } => Some(format!("{me}: pre_node_ready is blocked (attempt {attempt}): {reason}; it runs again on {}", retry_on.name())),
            Self::Failed(why) => Some(format!("{me}: pre_node_ready failed: {why}")),
            Self::Cancelled => Some(format!("{me}: cancelled before pre_node_ready passed")),
        }
    }

    /// What a `ProbeNodeState` is answered `Blocked` with while the hook is `Blocked`
    /// (node-start.md step 3): the hook's own reason and the event it runs again on, `None` in every
    /// other state. No attempt counter: a rerun blocked for the same reason answers the same blocker,
    /// so the executor frames a new `Blocked` only on a new reason.
    pub fn probe_blocker(&self) -> Option<(String, RetryOn)> {
        match self {
            Self::Blocked { reason, retry_on, .. } => Some((reason.clone(), retry_on.clone())),
            Self::Running { .. } | Self::Passed | Self::Failed(_) | Self::Cancelled => None,
        }
    }
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

    /// Resolves with the reason once the hook failed; never resolves otherwise. A process selects
    /// on it beside its stop signal and ends by name.
    pub async fn failed(&self) -> String {
        match self.rx.clone().wait_for(|s| matches!(s, PreNodeReadyState::Failed(_))).await {
            Ok(s) => match &*s {
                PreNodeReadyState::Failed(why) => why.clone(),
                _ => unreachable!("waited for Failed"),
            },
            Err(_) => std::future::pending().await,
        }
    }

    /// Resolves once the hook passed or the birth ended without passing.
    pub async fn settled(&self) -> PreNodeReadyState {
        match self.rx.clone().wait_for(|s| !matches!(s, PreNodeReadyState::Running { .. } | PreNodeReadyState::Blocked { .. })).await {
            Ok(s) => s.clone(),
            Err(_) => self.state(),
        }
    }
}

/// A blocked attempt waiting on its event.
pub struct Pending {
    retry_on: RetryOn,
    mark: Mark,
    span: tracing::Span,
}

/// How one attempt ended.
pub enum Step {
    /// The hook returned `Ok`; the gate is open.
    Passed,
    /// The hook failed.
    Failed(String),
    /// The hook was cancelled during the attempt; its future was dropped.
    Cancelled,
    /// The hook is blocked.
    Blocked(Pending),
}

/// How a birth's hook ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum End {
    /// Passed.
    Passed,
    /// Failed, and why.
    Failed(String),
    /// Cancelled first.
    Cancelled,
}

/// Runs one birth's hook: the attempts, the waits between them, and the state the Ready check reads.
pub struct HookDriver {
    hook: Arc<dyn PreNodeReady>,
    after: Option<Arc<dyn OnNodeReady>>,
    ctx: NodeReadyCtx,
    events: Events,
    gate: PreNodeReadyGate,
    state: tokio::sync::watch::Sender<PreNodeReadyState>,
    boot: tracing::Span,
    attempts: AtomicU32,
}

impl NodeHooks {
    /// The driver for this birth, `None` when no hook is registered. The gate is opened for a node
    /// with no hook, so an app op registered without a hook is never refused.
    pub fn driver(&self, ctx: NodeReadyCtx, events: Events, boot: tracing::Span) -> Option<Arc<HookDriver>> {
        let Some(hook) = self.before.clone() else {
            self.gate.open();
            return None;
        };
        let (state, _) = tokio::sync::watch::channel(PreNodeReadyState::Running { attempt: 0 });
        Some(Arc::new(HookDriver { hook, after: self.after.clone(), ctx, events, gate: self.gate.clone(), state, boot, attempts: AtomicU32::new(0) }))
    }
}

impl HookDriver {
    /// A view of this hook's state.
    pub fn handle(&self) -> PreNodeReadyHandle {
        PreNodeReadyHandle { rx: self.state.subscribe() }
    }

    /// One attempt: one span under the boot span carrying the outcome and the blocker or reason.
    /// The hook future is dropped when the birth is cancelled, which ends any pull it has in
    /// flight with it.
    pub async fn attempt(&self) -> Step {
        let n = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        let mark = self.events.mark(&self.ctx.authority);
        self.state.send_replace(PreNodeReadyState::Running { attempt: n });
        let span = tracing::info_span!(
            parent: &self.boot,
            "rdm.mesh.node.update.via-pre-node-ready-attempt",
            node = %self.ctx.birth.name,
            node_id = %self.ctx.birth.node_id,
            incarnation_id = %self.ctx.birth.incarnation.as_str(),
            attempt = n,
            inherited = self.ctx.inherited,
            authority = %match &self.ctx.authority { Authority::Accepted(a) => a.name.to_string(), Authority::None(why) => format!("none: {}", why.reason()) },
            outcome = tracing::field::Empty,
            retry_on = tracing::field::Empty,
            reason = tracing::field::Empty,
        );
        let ctx = NodeReadyCtx { attempt: n, ..self.ctx.clone() };
        let cancel = self.ctx.cancel.clone();
        let out = async {
            tokio::select! {
                biased;
                () = cancel.cancelled() => None,
                o = self.hook.pre_node_ready(&ctx) => Some(o),
            }
        }
        .instrument(span.clone())
        .await;
        match out {
            Some(HookOutcome::Ok) => {
                self.gate.open();
                span.record("outcome", "ok");
                span.in_scope(|| tracing::info!("pre_node_ready returned Ok"));
                self.state.send_replace(PreNodeReadyState::Passed);
                Step::Passed
            }
            Some(HookOutcome::Blocked { reason, retry_on }) => {
                // A Blocked outcome names its blocker (fabric-node-hooks.md, "Outcomes and the gate"):
                // its reason is the blocker evidence and the executor's ProbeNodeState answer. A reason
                // that names nothing is no Blocked outcome; the hook ends by name.
                if reason.trim().is_empty() {
                    let why = format!("the hook returned Blocked with no reason, asking to run again on {}: a blocker it does not name cannot be answered to the executor", retry_on.name());
                    span.record("outcome", "failed");
                    span.record("reason", why.as_str());
                    span.in_scope(|| tracing::info!("pre_node_ready returned Blocked with no reason"));
                    self.state.send_replace(PreNodeReadyState::Failed(why.clone()));
                    return Step::Failed(why);
                }
                if self.ctx.authority.accepted().is_none() && !matches!(retry_on, RetryOn::TargetReachable { .. }) {
                    let why = format!("the hook asked to wait for {} but this birth has no authority ({}): {reason}", retry_on.name(), match &self.ctx.authority { Authority::None(w) => w.reason(), Authority::Accepted(_) => "" });
                    span.record("outcome", "failed");
                    span.record("reason", why.as_str());
                    span.in_scope(|| tracing::info!("pre_node_ready asked to wait on an authority this birth does not have"));
                    self.state.send_replace(PreNodeReadyState::Failed(why.clone()));
                    return Step::Failed(why);
                }
                span.record("outcome", "blocked");
                span.record("retry_on", retry_on.name());
                span.record("reason", reason.as_str());
                span.in_scope(|| tracing::info!("pre_node_ready is blocked"));
                let blocked = tracing::info_span!(
                    parent: &self.boot,
                    "rdm.mesh.node.reject.via-pre-node-ready-blocked",
                    node = %self.ctx.birth.name,
                    attempt = n,
                    blocker = %reason,
                    retry_on = retry_on.name(),
                    cleared_by = tracing::field::Empty,
                );
                blocked.in_scope(|| tracing::info!("not ready: pre_node_ready is blocked"));
                self.state.send_replace(PreNodeReadyState::Blocked { attempt: n, reason, retry_on: retry_on.clone() });
                Step::Blocked(Pending { retry_on, mark, span: blocked })
            }
            Some(HookOutcome::Failed { reason }) => {
                span.record("outcome", "failed");
                span.record("reason", reason.as_str());
                span.in_scope(|| tracing::info!("pre_node_ready failed"));
                self.state.send_replace(PreNodeReadyState::Failed(reason.clone()));
                Step::Failed(reason)
            }
            None => {
                span.record("outcome", "cancelled");
                span.in_scope(|| tracing::info!("pre_node_ready was cancelled: the hook and its pull ended"));
                self.state.send_replace(PreNodeReadyState::Cancelled);
                Step::Cancelled
            }
        }
    }

    /// From `step` on: wait for each blocked attempt's event and run again, until the hook passes,
    /// fails or the birth is deleted.
    pub async fn run(&self, mut step: Step) -> End {
        loop {
            match step {
                Step::Passed => return End::Passed,
                Step::Failed(why) => return End::Failed(why),
                Step::Cancelled => return End::Cancelled,
                Step::Blocked(pending) => {
                    match self.events.wait(&pending.retry_on, self.ctx.authority.accepted(), &pending.mark, &self.ctx.cancel).await {
                        Waited::Fired(event) => {
                            pending.span.record("cleared_by", event);
                            pending.span.in_scope(|| tracing::info!(event, "the blocker's event happened: pre_node_ready runs again"));
                            drop(pending);
                            step = self.attempt().await;
                        }
                        Waited::Cancelled => {
                            pending.span.record("cleared_by", "cancelled");
                            self.state.send_replace(PreNodeReadyState::Cancelled);
                            return End::Cancelled;
                        }
                    }
                }
            }
        }
    }

    /// Run the optional after-Ready hook, once, as a task that ends with the birth.
    pub fn spawn_on_node_ready(self: &Arc<Self>) {
        let Some(after) = self.after.clone() else { return };
        let me = self.clone();
        tokio::spawn(async move {
            let span = tracing::info_span!(parent: &me.boot, "rdm.mesh.node.update.via-on-node-ready", node = %me.ctx.birth.name);
            let ctx = me.ctx.clone();
            let cancel = me.ctx.cancel.clone();
            tokio::select! {
                () = after.on_node_ready(&ctx).instrument(span) => {}
                () = cancel.cancelled() => {}
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT (node-start.md step 3): the probe's `Blocked` answer is the hook's own reason and
    /// retry event while Blocked and nothing otherwise; two attempts blocked for the same reason
    /// answer the same blocker, and a changed reason a different one. What must NOT happen: an
    /// attempt counter in the blocker, which would frame a new `Blocked` on every rerun.
    #[test]
    fn a_rerun_blocked_for_the_same_reason_answers_the_same_probe_blocker() {
        let blocked = |attempt, reason: &str| PreNodeReadyState::Blocked { attempt, reason: reason.into(), retry_on: RetryOn::PrimaryAddressed };
        let first = blocked(1, "rows not yet published").probe_blocker().expect("blocked answers its blocker");
        assert_eq!(first, ("rows not yet published".to_string(), RetryOn::PrimaryAddressed), "the hook's own reason and event");
        assert_eq!(Some(first.clone()), blocked(7, "rows not yet published").probe_blocker(), "the attempt is not part of the blocker");
        assert_ne!(Some(first), blocked(8, "the authority is connections-deleting").probe_blocker(), "a changed reason is a new blocker");
        for other in [PreNodeReadyState::Running { attempt: 2 }, PreNodeReadyState::Passed, PreNodeReadyState::Failed("x".into()), PreNodeReadyState::Cancelled] {
            assert_eq!(other.probe_blocker(), None, "only Blocked answers Blocked: {other:?}");
        }
    }

    // @feature: pre-node-ready
    #[test]
    fn hook_driver_refuses_blank_reason_and_missing_accepting_primary_reruns_on_level_triggered_retry_event_and_cancels_in_flight_attempt() {
        // docs/architecture/fabric-node-hooks.md:89 — "`Blocked { reason, retry_on }` | readiness
        // remains blocked; the reason is named in the blocker evidence and answered to the
        // executor's ProbeNodeState as Status `Blocked { node_id, incarnation, reason, retry_on }`;
        // the hook runs again only when retry_on occurs"
        // docs/architecture/fabric-node-hooks.md:93 — "A retry is level-triggered: RDM arms the
        // wait, then checks the condition again, so an event that fired before arming cannot leave
        // the node waiting. RDM never retries on a timer."
        struct ScriptedHook {
            outcomes: Mutex<Vec<HookOutcome>>,
            addressed_during_attempt: Option<(Arc<Addressed>, iroh::PublicKey)>,
            wait_forever_on_attempt: Option<u32>,
            started_notify: Arc<tokio::sync::Notify>,
        }

        #[async_trait::async_trait]
        impl PreNodeReady for ScriptedHook {
            async fn pre_node_ready(&self, ctx: &NodeReadyCtx) -> HookOutcome {
                if self.wait_forever_on_attempt == Some(ctx.attempt) {
                    self.started_notify.notify_one();
                    std::future::pending::<()>().await;
                }
                if ctx.attempt == 1 {
                    if let Some((addr, key)) = &self.addressed_during_attempt {
                        addr.note(key);
                    }
                }
                self.outcomes.lock().unwrap().remove(0)
            }
        }

        let dir = std::env::temp_dir().join(format!("rdm-pre-node-ready-unit-{}", NodeId::mint()));
        std::fs::create_dir_all(&dir).unwrap();
        let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-pre-node-ready-unit")
            .expect("evidence capture starts");

        capture.run(2, async {
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
            let name = PathName::new("mesh1", NodeKind::RpcNode, 1);
            let topology_view = TopologyView::join(
                &gossip,
                &ep,
                &fabric_id,
                "mesh1",
                &mesh_id,
                &name.to_string(),
                Arc::new(rafka_time.clone()),
                Vec::new(),
                &[],
            )
            .await
            .expect("join topology view");

            let birth = Birth {
                name: name.clone(),
                node_id: NodeId::mint(),
                incarnation: IncarnationId::mint(),
                fabric_id,
                mesh_id,
                kind: NodeKind::RpcNode,
            };
            let auth_key = iroh::SecretKey::generate().public();
            let accepting = AcceptingAuthority {
                name: PathName::new("mesh1", NodeKind::NodeAdmin, 1),
                endpoint: auth_key,
                target: rdm_transport::NodeTarget::ExactNode(NodeId::mint()),
            };

            // 1. Blank reason in `Blocked` is refused by name (`Step::Failed`).
            let blank_hook = Arc::new(ScriptedHook {
                outcomes: Mutex::new(vec![HookOutcome::Blocked {
                    reason: "   ".into(),
                    retry_on: RetryOn::PrimaryAddressed,
                }]),
                addressed_during_attempt: None,
                wait_forever_on_attempt: None,
                started_notify: Arc::new(tokio::sync::Notify::new()),
            });
            let blank_hooks = NodeHooks::new(blank_hook);
            let blank_driver = blank_hooks
                .driver(
                    NodeReadyCtx {
                        start_kind: StartKind::FirstStart,
                        birth: birth.clone(),
                        authority: Authority::Accepted(accepting.clone()),
                        client: client.clone(),
                        topology_view: topology_view.clone(),
                        cancel: CancelToken::new(),
                        rafka_time: rafka_time.clone(),
                        member_cert: None,
                        attempt: 0,
                        inherited: false,
                    },
                    Events::new(topology_view.digest_view.clone(), Addressed::new()),
                    tracing::Span::none(),
                )
                .expect("driver");
            match blank_driver.attempt().await {
                Step::Failed(why) => assert!(why.contains("Blocked with no reason"), "{why}"),
                _ => panic!("expected Step::Failed for blank Blocked reason"),
            }
            assert!(!blank_hooks.gate().is_open());

            // 2. Waiting on an authority event when the birth has `Authority::None` is refused by name.
            let no_auth_hook = Arc::new(ScriptedHook {
                outcomes: Mutex::new(vec![HookOutcome::Blocked {
                    reason: "waiting on primary".into(),
                    retry_on: RetryOn::PrimaryReady,
                }]),
                addressed_during_attempt: None,
                wait_forever_on_attempt: None,
                started_notify: Arc::new(tokio::sync::Notify::new()),
            });
            let no_auth_hooks = NodeHooks::new(no_auth_hook);
            let no_auth_driver = no_auth_hooks
                .driver(
                    NodeReadyCtx {
                        start_kind: StartKind::FirstStart,
                        birth: birth.clone(),
                        authority: Authority::None(NoAuthority::Day0Root),
                        client: client.clone(),
                        topology_view: topology_view.clone(),
                        cancel: CancelToken::new(),
                        rafka_time: rafka_time.clone(),
                        member_cert: None,
                        attempt: 0,
                        inherited: false,
                    },
                    Events::new(topology_view.digest_view.clone(), Addressed::new()),
                    tracing::Span::none(),
                )
                .expect("driver");
            match no_auth_driver.attempt().await {
                Step::Failed(why) => assert!(why.contains("no authority"), "{why}"),
                _ => panic!("expected Step::Failed when waiting on authority without one"),
            }

            // 3. Level-triggered retry: `addressed.note` fires while attempt 1 is running (before
            //    `run` enters `events.wait`); `run` sees the count advanced past `mark`, clears the
            //    blocker without stranding, runs attempt 2 -> `Ok`, and opens the gate.
            let addressed = Addressed::new();
            let level_hook = Arc::new(ScriptedHook {
                outcomes: Mutex::new(vec![
                    HookOutcome::Blocked {
                        reason: "awaiting primary address".into(),
                        retry_on: RetryOn::PrimaryAddressed,
                    },
                    HookOutcome::Ok,
                ]),
                addressed_during_attempt: Some((addressed.clone(), auth_key)),
                wait_forever_on_attempt: None,
                started_notify: Arc::new(tokio::sync::Notify::new()),
            });
            let level_hooks = NodeHooks::new(level_hook);
            let level_driver = level_hooks
                .driver(
                    NodeReadyCtx {
                        start_kind: StartKind::FirstStart,
                        birth: birth.clone(),
                        authority: Authority::Accepted(accepting.clone()),
                        client: client.clone(),
                        topology_view: topology_view.clone(),
                        cancel: CancelToken::new(),
                        rafka_time: rafka_time.clone(),
                        member_cert: None,
                        attempt: 0,
                        inherited: false,
                    },
                    Events::new(topology_view.digest_view.clone(), addressed),
                    tracing::Span::none(),
                )
                .expect("driver");
            let step1 = level_driver.attempt().await;
            assert!(matches!(step1, Step::Blocked(_)));
            assert!(!level_hooks.gate().is_open());
            let end = level_driver.run(step1).await;
            assert_eq!(end, End::Passed);
            assert!(level_hooks.gate().is_open());
            assert_eq!(level_driver.handle().state(), PreNodeReadyState::Passed);

            // 4. Cancellation drops an in-flight attempt and records `Step::Cancelled`.
            let cancel = CancelToken::new();
            let started_notify = Arc::new(tokio::sync::Notify::new());
            let cancel_hook = Arc::new(ScriptedHook {
                outcomes: Mutex::new(Vec::new()),
                addressed_during_attempt: None,
                wait_forever_on_attempt: Some(1),
                started_notify: started_notify.clone(),
            });
            let cancel_hooks = NodeHooks::new(cancel_hook);
            let cancel_driver = cancel_hooks
                .driver(
                    NodeReadyCtx {
                        start_kind: StartKind::FirstStart,
                        birth,
                        authority: Authority::Accepted(accepting),
                        client,
                        topology_view,
                        cancel: cancel.clone(),
                        rafka_time,
                        member_cert: None,
                        attempt: 0,
                        inherited: false,
                    },
                    Events::new(DigestView::default(), Addressed::new()),
                    tracing::Span::none(),
                )
                .expect("driver");
            let cancel_clone = cancel.clone();
            tokio::spawn(async move {
                started_notify.notified().await;
                cancel_clone.cancel();
            });
            let cancelled_step = cancel_driver.attempt().await;
            assert!(matches!(cancelled_step, Step::Cancelled));
            assert_eq!(cancel_driver.handle().state(), PreNodeReadyState::Cancelled);

            ep.close().await;
        });

        let spans = capture.spans().expect("captured spans");
        let _ = std::fs::remove_dir_all(&dir);
        let attempts: Vec<&serde_json::Value> = spans
            .iter()
            .filter(|s| s["name"] == "rdm.mesh.node.update.via-pre-node-ready-attempt")
            .collect();
        assert!(
            attempts.iter().any(|s| {
                s["attributes"]["outcome"] == "blocked"
                    && s["attributes"]["retry_on"] == "primary-addressed"
            }),
            "expected blocked attempt span: {attempts:?}"
        );
        assert!(
            attempts
                .iter()
                .any(|s| s["attributes"]["outcome"] == "ok"),
            "expected ok attempt span: {attempts:?}"
        );
        assert!(
            attempts
                .iter()
                .any(|s| s["attributes"]["outcome"] == "cancelled"),
            "expected cancelled attempt span: {attempts:?}"
        );
        assert!(
            spans.iter().any(|s| {
                s["name"] == "rdm.mesh.node.reject.via-pre-node-ready-blocked"
                    && s["attributes"]["cleared_by"] == "primary-addressed"
            }),
            "expected via-pre-node-ready-blocked span with cleared_by=primary-addressed"
        );
    }
}
