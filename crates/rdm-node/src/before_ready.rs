//! `before_ready`: the one hook an embedding app supplies to be born full.
//!
//! A node reaches the hook after its `JoinNode` was accepted, its `GetTopology` read installed
//! and its mesh channel joined (node-start.md step 3). The hook returns
//! [`HookOutcome`]:
//!
//! - `Ok`: RDM's own checks go on to ReadyForTraffic, and the app's own ops are served
//!   ([`BeforeReadyGate`] opens).
//! - `Blocked { reason, retry_on }`: the node stays Pending, the blocker is named where Ready
//!   blockers are named, and the hook runs again only when the named [`RetryOn`] event happens.
//!   The wait arms, then re-checks the condition against a count taken before the attempt began, so
//!   an event that happened while the attempt ran cannot strand the node. No timer and no sleep.
//! - `Failed { reason }`: the node ends by name.
//!
//! The Day-0 root and a fabric-primary reborn from its durable map have no authority to pull from:
//! their context says so ([`Authority::None`]) and the hook completes from local state
//! (`gossip.md` section 9's fenced exception).
//!
//! One span per attempt (`rdm.mesh.node.update.via-before-ready-attempt`) under the boot span carries
//! the outcome and the blocker or reason; `via-ready` follows the successful attempt.
//! [`AfterReady`] is for optional work only: nothing required may live there.

use crate::model::{FabricId, IncarnationId, MeshId, NodeId, NodeKind, PathName};
use rdm_core::{MemberStatus, MeshDigest};
use std::time::Duration;
use rdm_transport::membership::{DigestBook, Membership};
use rdm_transport::{CancelToken, BeforeReadyGate, NodeRpcClient};
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
    /// The parked birth rejoins as itself (`node.start` after `node.stop`): its data dir is its own
    /// and what it held before the stop is still there.
    AfterStop,
}

/// What the hook is handed.
#[derive(Clone)]
pub struct BeforeReadyCtx {
    /// Whether this is the birth's first start or a start after a stop.
    pub start_kind: StartKind,
    /// The exact birth.
    pub birth: Birth,
    /// The authority to pull from, or why there is none.
    pub authority: Authority,
    /// The process's one Node RPC client.
    pub client: Arc<NodeRpcClient>,
    /// The membership/gossip handle: the app subscribes its kind topics before it takes its
    /// snapshot, so what changes during the pull arrives by gossip.
    pub membership: Membership,
    /// Cancelled when the birth is retired (stop-node, its process stopping): the hook
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

impl BeforeReadyCtx {
    /// The outcome for a pull the authority answered with its typed `NotReady`: wait for that
    /// authority to be Ready.
    pub fn authority_not_ready(&self, reason: impl Into<String>) -> HookOutcome {
        HookOutcome::Blocked { reason: reason.into(), retry_on: RetryOn::AuthorityReady }
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
pub trait BeforeReady: Send + Sync {
    /// The step this node's role takes before it reports Ready, with `ctx.authority`.
    async fn before_ready(&self, ctx: &BeforeReadyCtx) -> HookOutcome;
}

/// Optional work after Ready. Nothing required may live here, and it is not a second projection
/// path: the node is already serving.
#[async_trait::async_trait]
pub trait AfterReady: Send + Sync {
    /// Run once, after the node published ReadyForTraffic.
    async fn after_ready(&self, ctx: &BeforeReadyCtx);
}

/// What an embedding app registers, before RDM starts.
#[derive(Clone, Default)]
pub struct BeforeReadyHook {
    before: Option<Arc<dyn BeforeReady>>,
    after: Option<Arc<dyn AfterReady>>,
    gate: BeforeReadyGate,
}

impl BeforeReadyHook {
    /// No hook: the node passes at once and runs no attempt.
    pub fn none() -> Self {
        Self::default()
    }

    /// This hook before Ready.
    pub fn new(hook: Arc<dyn BeforeReady>) -> Self {
        Self { before: Some(hook), ..Self::default() }
    }

    /// Also run `after` once the node is Ready.
    pub fn with_after_ready(mut self, after: Arc<dyn AfterReady>) -> Self {
        self.after = Some(after);
        self
    }

    /// The gate RDM opens when the hook passes. The app clones it into its own server-side ops
    /// (`ServerBuilder::serve_gated`) before RDM starts.
    pub fn gate(&self) -> BeforeReadyGate {
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
    /// The birth was retired.
    Cancelled,
}

/// The retry events of one birth, read from its membership book and the down ops it was sent.
#[derive(Clone)]
pub struct Events {
    book: DigestBook,
    addressed: Arc<Addressed>,
}

impl Events {
    /// The events of the birth holding `book`, with the down ops recorded in `addressed`.
    pub fn new(book: DigestBook, addressed: Arc<Addressed>) -> Self {
        Self { book, addressed }
    }

    /// The counts now, for `authority`.
    pub fn mark(&self, authority: &Authority) -> Mark {
        let heard = *self.book.heard_changes().borrow();
        match authority.accepted() {
            Some(a) => Mark { heard, addressed: self.addressed.count(&a.endpoint), authority_incarnation: self.birth_of(a).map(|(d, _)| d.node.incarnation) },
            None => Mark { heard, addressed: 0, authority_incarnation: None },
        }
    }

    /// The birth this node's book holds at the authority's endpoint, and how long it has been silent:
    /// the authority is known by its endpoint, which names one process.
    fn birth_of(&self, authority: &AcceptingAuthority) -> Option<(MeshDigest, Duration)> {
        let key = authority.endpoint.to_string();
        let held = self.book.all().into_iter().find(|d| d.node.endpoint_id.0 == key)?;
        self.book.get(held.node.node_id.as_str())
    }

    fn holds(&self, on: &RetryOn, authority: Option<&AcceptingAuthority>, mark: &Mark) -> bool {
        let moved = *self.book.heard_changes().borrow() > mark.heard;
        if let RetryOn::TargetReachable { node_id: id } = on {
            return moved && self.book.current(self.book.staleness_floor()).iter().any(|d| &d.node.node_id == id);
        }
        // An authority event of a birth with no authority never happens; the attempt refuses such a
        // request by name before any wait begins.
        let Some(authority) = authority else { return false };
        let birth = self.birth_of(authority);
        match on {
            RetryOn::AuthorityChanged => match &birth {
                Some((d, _)) => self.book.is_departed(d.node.node_id.as_str()) || Some(&d.node.incarnation) != mark.authority_incarnation.as_ref(),
                None => mark.authority_incarnation.is_some(),
            },
            RetryOn::AuthorityReady => moved && birth.is_some_and(|(d, silent)| d.status == MemberStatus::ReadyForTraffic && silent <= self.book.staleness_floor()),
            RetryOn::TargetReachable { .. } => unreachable!("answered above"),
            RetryOn::AuthorityAddressed => self.addressed.count(&authority.endpoint) > mark.addressed,
        }
    }

    /// Wait for `on`: arm on the book's and the down ops' change counts, then check the condition
    /// against `mark`, and only then sleep until a count moves or the birth is cancelled.
    pub async fn wait(&self, on: &RetryOn, authority: Option<&AcceptingAuthority>, mark: &Mark, cancel: &CancelToken) -> Waited {
        loop {
            let mut heard = self.book.heard_changes();
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
pub enum BeforeReadyState {
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

impl BeforeReadyState {
    /// The blocker in words, `None` once passed. This is what the Ready check names.
    pub fn blocker(&self, me: &PathName) -> Option<String> {
        match self {
            Self::Passed => None,
            Self::Running { attempt } => Some(format!("{me}: before_ready is running (attempt {attempt})")),
            Self::Blocked { attempt, reason, retry_on } => Some(format!("{me}: before_ready is blocked (attempt {attempt}): {reason}; it runs again on {}", retry_on.name())),
            Self::Failed(why) => Some(format!("{me}: before_ready failed: {why}")),
            Self::Cancelled => Some(format!("{me}: cancelled before before_ready passed")),
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
pub struct BeforeReadyHandle {
    rx: tokio::sync::watch::Receiver<BeforeReadyState>,
}

impl BeforeReadyHandle {
    /// The state now.
    pub fn state(&self) -> BeforeReadyState {
        self.rx.borrow().clone()
    }

    /// Resolves with the reason once the hook failed; never resolves otherwise. A process selects
    /// on it beside its stop signal and ends by name.
    pub async fn failed(&self) -> String {
        match self.rx.clone().wait_for(|s| matches!(s, BeforeReadyState::Failed(_))).await {
            Ok(s) => match &*s {
                BeforeReadyState::Failed(why) => why.clone(),
                _ => unreachable!("waited for Failed"),
            },
            Err(_) => std::future::pending().await,
        }
    }

    /// Resolves once the hook passed or the birth ended without passing.
    pub async fn settled(&self) -> BeforeReadyState {
        match self.rx.clone().wait_for(|s| !matches!(s, BeforeReadyState::Running { .. } | BeforeReadyState::Blocked { .. })).await {
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
    hook: Arc<dyn BeforeReady>,
    after: Option<Arc<dyn AfterReady>>,
    ctx: BeforeReadyCtx,
    events: Events,
    gate: BeforeReadyGate,
    state: tokio::sync::watch::Sender<BeforeReadyState>,
    boot: tracing::Span,
    attempts: AtomicU32,
}

impl BeforeReadyHook {
    /// The driver for this birth, `None` when no hook is registered. The gate is opened for a node
    /// with no hook, so an app op registered without a hook is never refused.
    pub fn driver(&self, ctx: BeforeReadyCtx, events: Events, boot: tracing::Span) -> Option<Arc<HookDriver>> {
        let Some(hook) = self.before.clone() else {
            self.gate.open();
            return None;
        };
        let (state, _) = tokio::sync::watch::channel(BeforeReadyState::Running { attempt: 0 });
        Some(Arc::new(HookDriver { hook, after: self.after.clone(), ctx, events, gate: self.gate.clone(), state, boot, attempts: AtomicU32::new(0) }))
    }
}

impl HookDriver {
    /// A view of this hook's state.
    pub fn handle(&self) -> BeforeReadyHandle {
        BeforeReadyHandle { rx: self.state.subscribe() }
    }

    /// One attempt: one span under the boot span carrying the outcome and the blocker or reason.
    /// The hook future is dropped when the birth is cancelled, which ends any pull it has in
    /// flight with it.
    pub async fn attempt(&self) -> Step {
        let n = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        let mark = self.events.mark(&self.ctx.authority);
        self.state.send_replace(BeforeReadyState::Running { attempt: n });
        let span = tracing::info_span!(
            parent: &self.boot,
            "rdm.mesh.node.update.via-before-ready-attempt",
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
        let ctx = BeforeReadyCtx { attempt: n, ..self.ctx.clone() };
        let cancel = self.ctx.cancel.clone();
        let out = async {
            tokio::select! {
                biased;
                () = cancel.cancelled() => None,
                o = self.hook.before_ready(&ctx) => Some(o),
            }
        }
        .instrument(span.clone())
        .await;
        match out {
            Some(HookOutcome::Ok) => {
                self.gate.open();
                span.record("outcome", "ok");
                span.in_scope(|| tracing::info!("before_ready returned Ok"));
                self.state.send_replace(BeforeReadyState::Passed);
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
                    span.in_scope(|| tracing::info!("before_ready returned Blocked with no reason"));
                    self.state.send_replace(BeforeReadyState::Failed(why.clone()));
                    return Step::Failed(why);
                }
                if self.ctx.authority.accepted().is_none() && !matches!(retry_on, RetryOn::TargetReachable { .. }) {
                    let why = format!("the hook asked to wait for {} but this birth has no authority ({}): {reason}", retry_on.name(), match &self.ctx.authority { Authority::None(w) => w.reason(), Authority::Accepted(_) => "" });
                    span.record("outcome", "failed");
                    span.record("reason", why.as_str());
                    span.in_scope(|| tracing::info!("before_ready asked to wait on an authority this birth does not have"));
                    self.state.send_replace(BeforeReadyState::Failed(why.clone()));
                    return Step::Failed(why);
                }
                span.record("outcome", "blocked");
                span.record("retry_on", retry_on.name());
                span.record("reason", reason.as_str());
                span.in_scope(|| tracing::info!("before_ready is blocked"));
                let blocked = tracing::info_span!(
                    parent: &self.boot,
                    "rdm.mesh.node.reject.via-before-ready-blocked",
                    node = %self.ctx.birth.name,
                    attempt = n,
                    blocker = %reason,
                    retry_on = retry_on.name(),
                    cleared_by = tracing::field::Empty,
                );
                blocked.in_scope(|| tracing::info!("not ready: before_ready is blocked"));
                self.state.send_replace(BeforeReadyState::Blocked { attempt: n, reason, retry_on: retry_on.clone() });
                Step::Blocked(Pending { retry_on, mark, span: blocked })
            }
            Some(HookOutcome::Failed { reason }) => {
                span.record("outcome", "failed");
                span.record("reason", reason.as_str());
                span.in_scope(|| tracing::info!("before_ready failed"));
                self.state.send_replace(BeforeReadyState::Failed(reason.clone()));
                Step::Failed(reason)
            }
            None => {
                span.record("outcome", "cancelled");
                span.in_scope(|| tracing::info!("before_ready was cancelled: the hook and its pull ended"));
                self.state.send_replace(BeforeReadyState::Cancelled);
                Step::Cancelled
            }
        }
    }

    /// From `step` on: wait for each blocked attempt's event and run again, until the hook passes,
    /// fails or the birth is retired.
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
                            pending.span.in_scope(|| tracing::info!(event, "the blocker's event happened: before_ready runs again"));
                            drop(pending);
                            step = self.attempt().await;
                        }
                        Waited::Cancelled => {
                            pending.span.record("cleared_by", "cancelled");
                            self.state.send_replace(BeforeReadyState::Cancelled);
                            return End::Cancelled;
                        }
                    }
                }
            }
        }
    }

    /// Run the optional after-Ready hook, once, as a task that ends with the birth.
    pub fn spawn_after_ready(self: &Arc<Self>) {
        let Some(after) = self.after.clone() else { return };
        let me = self.clone();
        tokio::spawn(async move {
            let span = tracing::info_span!(parent: &me.boot, "rdm.mesh.node.update.via-after-ready", node = %me.ctx.birth.name);
            let ctx = me.ctx.clone();
            let cancel = me.ctx.cancel.clone();
            tokio::select! {
                () = after.after_ready(&ctx).instrument(span) => {}
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
        let blocked = |attempt, reason: &str| BeforeReadyState::Blocked { attempt, reason: reason.into(), retry_on: RetryOn::AuthorityAddressed };
        let first = blocked(1, "rows not yet published").probe_blocker().expect("blocked answers its blocker");
        assert_eq!(first, ("rows not yet published".to_string(), RetryOn::AuthorityAddressed), "the hook's own reason and event");
        assert_eq!(Some(first.clone()), blocked(7, "rows not yet published").probe_blocker(), "the attempt is not part of the blocker");
        assert_ne!(Some(first), blocked(8, "the authority is draining").probe_blocker(), "a changed reason is a new blocker");
        for other in [BeforeReadyState::Running { attempt: 2 }, BeforeReadyState::Passed, BeforeReadyState::Failed("x".into()), BeforeReadyState::Cancelled] {
            assert_eq!(other.probe_blocker(), None, "only Blocked answers Blocked: {other:?}");
        }
    }
}
