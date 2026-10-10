//! `hydrate_before_ready`: the one hook an embedding app supplies to be born full.
//!
//! A node reaches the hook after its `JoinNode` was accepted, its `GetTopology` read installed
//! and its mesh channel joined (`node-birth.md` step 6, "then hydrate"). The hook returns
//! [`HookOutcome`]:
//!
//! - `Ok`: RDM's own checks go on to ReadyForTraffic, and the app's own ops are served
//!   ([`HydrationGate`] opens).
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
//! One span per attempt (`rdm.mesh.node.update.via-hydrate-attempt`) under the boot span carries
//! the outcome and the blocker or reason; `via-ready` follows the successful attempt.
//! [`AfterReady`] is for optional work only: nothing required may live there.

use crate::model::{FabricId, IncarnationId, MeshId, NodeId, NodeKind, PathName};
use rafka_mesh_entity::MemberStatus;
use rafka_mesh_transport::membership::{DigestBook, Membership};
use rafka_node_rpc::{CancelToken, HydrationGate, NodeRpcClient};
use std::collections::BTreeMap;
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

/// The admin that accepted this birth's `JoinNode`: the authority the hook pulls from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptingAuthority {
    /// The admin's `path.name`.
    pub name: PathName,
    /// Its node id.
    pub node_id: NodeId,
    /// The incarnation that accepted the join.
    pub incarnation: IncarnationId,
}

/// Why a birth has no authority to pull from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoAuthority {
    /// The Day-0 root: no fabric authority exists before it.
    Day0Root,
    /// A fabric-primary reborn from its durable map: it has no maker, and its own storage is the
    /// authority.
    FabricPrimaryReborn,
    /// A recovery start (mesh-primary or fabric-primary flags): it hydrates from the nodes it reaches.
    Recovery,
}

impl NoAuthority {
    /// The reason in words.
    pub fn reason(self) -> &'static str {
        match self {
            Self::Day0Root => "the Day-0 root: no authority exists before this node",
            Self::FabricPrimaryReborn => "a fabric-primary reborn from its durable map: its own storage is the authority",
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

/// What the hook is handed.
#[derive(Clone)]
pub struct HydrateCtx {
    /// The exact birth.
    pub birth: Birth,
    /// The authority to pull from, or why there is none.
    pub authority: Authority,
    /// The process's one Node RPC client.
    pub client: Arc<NodeRpcClient>,
    /// The membership/gossip handle: the app subscribes its kind topics before it takes its
    /// snapshot, so what changes during the pull arrives by gossip.
    pub membership: Membership,
    /// Cancelled when the birth is retired (drain-node, stop-node, its process stopping): the hook
    /// and any pull it has in flight end.
    pub cancel: CancelToken,
    /// The Rafka-time source this process composes.
    pub rafka_time: Option<rafka_mesh_transport::clock::SharedClock>,
    /// Which attempt this is, from 1.
    pub attempt: u32,
}

impl HydrateCtx {
    /// The outcome for a pull the authority answered with its typed `NotReady`: wait for that
    /// authority to be Ready.
    pub fn authority_not_ready(&self, reason: impl Into<String>) -> HookOutcome {
        HookOutcome::Blocked { reason: reason.into(), retry_on: RetryOn::AuthorityReady }
    }
}

/// The event a blocked hook waits for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryOn {
    /// The authority's birth changed or departed.
    AuthorityChanged,
    /// This node became reachable again (heard after silence, or first heard).
    TargetReachable(NodeId),
    /// The authority published ReadyForTraffic.
    AuthorityReady,
    /// The authority addressed this node (it sent a down op).
    AuthorityAddressed,
}

impl RetryOn {
    /// The event's name in spans.
    pub fn name(&self) -> &'static str {
        match self {
            Self::AuthorityChanged => "authority-changed",
            Self::TargetReachable(_) => "target-reachable",
            Self::AuthorityReady => "authority-ready",
            Self::AuthorityAddressed => "authority-addressed",
        }
    }
}

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
pub trait HydrateBeforeReady: Send + Sync {
    /// Hydrate the state this node's role needs, from `ctx.authority`.
    async fn hydrate_before_ready(&self, ctx: &HydrateCtx) -> HookOutcome;
}

/// Optional work after Ready. Nothing required may live here, and it is not a second projection
/// path: the node is already serving.
#[async_trait::async_trait]
pub trait AfterReady: Send + Sync {
    /// Run once, after the node published ReadyForTraffic.
    async fn after_ready(&self, ctx: &HydrateCtx);
}

/// What an embedding app registers, before RDM starts.
#[derive(Clone, Default)]
pub struct Hydration {
    before: Option<Arc<dyn HydrateBeforeReady>>,
    after: Option<Arc<dyn AfterReady>>,
    gate: HydrationGate,
}

impl Hydration {
    /// No hook: a node with nothing to hydrate passes at once and runs no attempt.
    pub fn none() -> Self {
        Self::default()
    }

    /// This hook before Ready.
    pub fn new(hook: Arc<dyn HydrateBeforeReady>) -> Self {
        Self { before: Some(hook), ..Self::default() }
    }

    /// Also run `after` once the node is Ready.
    pub fn with_after_ready(mut self, after: Arc<dyn AfterReady>) -> Self {
        self.after = Some(after);
        self
    }

    /// The gate RDM opens when the hook passes. The app clones it into its own server-side ops
    /// (`ServerBuilder::serve_gated`) before RDM starts.
    pub fn gate(&self) -> HydrationGate {
        self.gate.clone()
    }

    /// Whether a hook is registered.
    pub fn is_registered(&self) -> bool {
        self.before.is_some()
    }
}

/// Which authorities addressed this node, counted: a count survives an event that happened before
/// anything waited on it.
#[derive(Default)]
pub struct Addressed {
    counts: Mutex<BTreeMap<NodeId, u64>>,
    tick: tokio::sync::watch::Sender<u64>,
}

impl Addressed {
    /// An empty record.
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    /// `from` sent this node a down op.
    pub fn note(&self, from: &NodeId) {
        *self.counts.lock().unwrap().entry(from.clone()).or_default() += 1;
        self.tick.send_modify(|v| *v += 1);
    }

    fn count(&self, from: &NodeId) -> u64 {
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
            Some(a) => Mark { heard, addressed: self.addressed.count(&a.node_id), authority_incarnation: self.book.get(a.node_id.as_str()).map(|(d, _)| d.node.incarnation) },
            None => Mark { heard, addressed: 0, authority_incarnation: None },
        }
    }

    fn holds(&self, on: &RetryOn, authority: Option<&AcceptingAuthority>, mark: &Mark) -> bool {
        let moved = *self.book.heard_changes().borrow() > mark.heard;
        if let RetryOn::TargetReachable(id) = on {
            return moved && self.book.current(self.book.staleness_floor()).iter().any(|d| &d.node.node_id == id);
        }
        // An authority event of a birth with no authority never happens; the attempt refuses such a
        // request by name before any wait begins.
        let Some(authority) = authority else { return false };
        match on {
            RetryOn::AuthorityChanged => {
                self.book.is_departed(authority.node_id.as_str()) || self.book.get(authority.node_id.as_str()).map(|(d, _)| d.node.incarnation) != mark.authority_incarnation
            }
            RetryOn::AuthorityReady => moved && self.book.get(authority.node_id.as_str()).is_some_and(|(d, silent)| d.status == MemberStatus::ReadyForTraffic && silent <= self.book.staleness_floor()),
            RetryOn::TargetReachable(_) => unreachable!("answered above"),
            RetryOn::AuthorityAddressed => self.addressed.count(&authority.node_id) > mark.addressed,
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
pub enum HydrationState {
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
    /// The birth was retired before the hook passed.
    Retired,
}

impl HydrationState {
    /// The blocker in words, `None` once passed. This is what the Ready check names.
    pub fn blocker(&self, me: &PathName) -> Option<String> {
        match self {
            Self::Passed => None,
            Self::Running { attempt } => Some(format!("{me}: hydrate_before_ready is running (attempt {attempt})")),
            Self::Blocked { attempt, reason, retry_on } => Some(format!("{me}: hydrate_before_ready is blocked (attempt {attempt}): {reason}; it runs again on {}", retry_on.name())),
            Self::Failed(why) => Some(format!("{me}: hydrate_before_ready failed: {why}")),
            Self::Retired => Some(format!("{me}: retired before hydrate_before_ready passed")),
        }
    }
}

/// A read-only view of a birth's hook state.
#[derive(Clone)]
pub struct HydrationHandle {
    rx: tokio::sync::watch::Receiver<HydrationState>,
}

impl HydrationHandle {
    /// The state now.
    pub fn state(&self) -> HydrationState {
        self.rx.borrow().clone()
    }

    /// Resolves with the reason once the hook failed; never resolves otherwise. A process selects
    /// on it beside its stop signal and ends by name.
    pub async fn failed(&self) -> String {
        match self.rx.clone().wait_for(|s| matches!(s, HydrationState::Failed(_))).await {
            Ok(s) => match &*s {
                HydrationState::Failed(why) => why.clone(),
                _ => unreachable!("waited for Failed"),
            },
            Err(_) => std::future::pending().await,
        }
    }

    /// Resolves once the hook passed or the birth ended without passing.
    pub async fn settled(&self) -> HydrationState {
        match self.rx.clone().wait_for(|s| !matches!(s, HydrationState::Running { .. } | HydrationState::Blocked { .. })).await {
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
    /// The birth was retired during the attempt; the hook future was dropped.
    Retired,
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
    /// Retired first.
    Retired,
}

/// Runs one birth's hook: the attempts, the waits between them, and the state the Ready check reads.
pub struct HookDriver {
    hook: Arc<dyn HydrateBeforeReady>,
    after: Option<Arc<dyn AfterReady>>,
    ctx: HydrateCtx,
    events: Events,
    gate: HydrationGate,
    state: tokio::sync::watch::Sender<HydrationState>,
    boot: tracing::Span,
    attempts: AtomicU32,
}

impl Hydration {
    /// The driver for this birth, `None` when no hook is registered. The gate is opened for a node
    /// with no hook, so an app op registered without a hook is never refused.
    pub fn driver(&self, ctx: HydrateCtx, events: Events, boot: tracing::Span) -> Option<Arc<HookDriver>> {
        let Some(hook) = self.before.clone() else {
            self.gate.open();
            return None;
        };
        let (state, _) = tokio::sync::watch::channel(HydrationState::Running { attempt: 0 });
        Some(Arc::new(HookDriver { hook, after: self.after.clone(), ctx, events, gate: self.gate.clone(), state, boot, attempts: AtomicU32::new(0) }))
    }
}

impl HookDriver {
    /// A view of this hook's state.
    pub fn handle(&self) -> HydrationHandle {
        HydrationHandle { rx: self.state.subscribe() }
    }

    /// One attempt: one span under the boot span carrying the outcome and the blocker or reason.
    /// The hook future is dropped when the birth is cancelled, which ends any pull it has in
    /// flight with it.
    pub async fn attempt(&self) -> Step {
        let n = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        let mark = self.events.mark(&self.ctx.authority);
        self.state.send_replace(HydrationState::Running { attempt: n });
        let span = tracing::info_span!(
            parent: &self.boot,
            "rdm.mesh.node.update.via-hydrate-attempt",
            node = %self.ctx.birth.name,
            attempt = n,
            authority = %match &self.ctx.authority { Authority::Accepted(a) => a.name.to_string(), Authority::None(why) => format!("none: {}", why.reason()) },
            outcome = tracing::field::Empty,
            retry_on = tracing::field::Empty,
            reason = tracing::field::Empty,
        );
        let ctx = HydrateCtx { attempt: n, ..self.ctx.clone() };
        let cancel = self.ctx.cancel.clone();
        let out = async {
            tokio::select! {
                biased;
                () = cancel.cancelled() => None,
                o = self.hook.hydrate_before_ready(&ctx) => Some(o),
            }
        }
        .instrument(span.clone())
        .await;
        match out {
            Some(HookOutcome::Ok) => {
                self.gate.open();
                span.record("outcome", "ok");
                span.in_scope(|| tracing::info!("hydrate_before_ready returned Ok"));
                self.state.send_replace(HydrationState::Passed);
                Step::Passed
            }
            Some(HookOutcome::Blocked { reason, retry_on }) => {
                if self.ctx.authority.accepted().is_none() && !matches!(retry_on, RetryOn::TargetReachable(_)) {
                    let why = format!("the hook asked to wait for {} but this birth has no authority ({}): {reason}", retry_on.name(), match &self.ctx.authority { Authority::None(w) => w.reason(), Authority::Accepted(_) => "" });
                    span.record("outcome", "failed");
                    span.record("reason", why.as_str());
                    span.in_scope(|| tracing::info!("hydrate_before_ready asked to wait on an authority this birth does not have"));
                    self.state.send_replace(HydrationState::Failed(why.clone()));
                    return Step::Failed(why);
                }
                span.record("outcome", "blocked");
                span.record("retry_on", retry_on.name());
                span.record("reason", reason.as_str());
                span.in_scope(|| tracing::info!("hydrate_before_ready is blocked"));
                let blocked = tracing::info_span!(
                    parent: &self.boot,
                    "rdm.mesh.node.reject.via-hydration-blocked",
                    node = %self.ctx.birth.name,
                    attempt = n,
                    blocker = %reason,
                    retry_on = retry_on.name(),
                    cleared_by = tracing::field::Empty,
                );
                blocked.in_scope(|| tracing::info!("not ready: hydrate_before_ready is blocked"));
                self.state.send_replace(HydrationState::Blocked { attempt: n, reason, retry_on: retry_on.clone() });
                Step::Blocked(Pending { retry_on, mark, span: blocked })
            }
            Some(HookOutcome::Failed { reason }) => {
                span.record("outcome", "failed");
                span.record("reason", reason.as_str());
                span.in_scope(|| tracing::info!("hydrate_before_ready failed"));
                self.state.send_replace(HydrationState::Failed(reason.clone()));
                Step::Failed(reason)
            }
            None => {
                span.record("outcome", "cancelled");
                span.in_scope(|| tracing::info!("the birth was retired during hydrate_before_ready: the hook and its pull ended"));
                self.state.send_replace(HydrationState::Retired);
                Step::Retired
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
                Step::Retired => return End::Retired,
                Step::Blocked(pending) => {
                    match self.events.wait(&pending.retry_on, self.ctx.authority.accepted(), &pending.mark, &self.ctx.cancel).await {
                        Waited::Fired(event) => {
                            pending.span.record("cleared_by", event);
                            pending.span.in_scope(|| tracing::info!(event, "the blocker's event happened: hydrate_before_ready runs again"));
                            drop(pending);
                            step = self.attempt().await;
                        }
                        Waited::Cancelled => {
                            pending.span.record("cleared_by", "retired");
                            self.state.send_replace(HydrationState::Retired);
                            return End::Retired;
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
