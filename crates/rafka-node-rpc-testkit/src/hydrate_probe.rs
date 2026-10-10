//! The hydration probe: a testkit-only app on op `0x74` that stands in for a product's
//! ProjectionPull. [`PullServer`] is an authority's side (rows behind a [`HydrationGate`]),
//! [`TestHydrator`] is a node's `hydrate_before_ready` that pulls those rows and applies them once
//! per row version, whether a row arrives by the pull, by gossip, or both.
//!
//! Testkit range only ([`TESTKIT_OPS`]); never a product binary.
//!
//! [`TESTKIT_OPS`]: rafka_node_rpc_contract::catalog::TESTKIT_OPS

use rafka_node_admin_core::app_hydration::{HookOutcome, HydrateBeforeReady, HydrateCtx, RetryOn};
use rafka_node_rpc::{CallOptions, HydrationGate, NodeTarget, PeerContext, ServerBuilder};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::outcome::{MalformedKind, ReplyKind, RpcOutcome};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

/// The hydration probe protocol on op `0x74`.
pub struct HydratePull;

/// One row of the authority's state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Row {
    /// The row's version: applying a version already held changes nothing.
    pub version: u64,
    /// The row's value.
    pub value: String,
}

/// A pull.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PullRequest {
    /// Every row this authority holds.
    Pull,
}

/// A pull's answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PullReply {
    /// The rows, complete.
    Rows {
        /// The rows in version order.
        rows: Vec<Row>,
    },
    /// The peer the call needed could not be resolved.
    PeerUnresolved {
        /// Why.
        reason: String,
    },
    /// The authority's own hook has not passed.
    NotReady {
        /// Why.
        reason: String,
    },
    /// At the admission bound.
    Busy {
        /// Which bound.
        reason: String,
    },
    /// Draining.
    Draining {
        /// Why.
        reason: String,
    },
    /// The request frame was malformed.
    Malformed {
        /// How.
        kind: MalformedKind,
    },
    /// Not allowed.
    Unauthorized {
        /// Why.
        reason: String,
    },
}

impl NodeProtocol for HydratePull {
    const OP: u8 = 0x74;
    const NAME: &'static str = "hydrate-pull";
    const MAX_REQUEST_FRAME_BYTES: usize = 256;
    const MAX_REPLY_FRAME_BYTES: usize = 64 * 1024;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 7;
    type Request = PullRequest;
    type Reply = PullReply;
    fn classify_reply(reply: &PullReply) -> ReplyKind {
        match reply {
            PullReply::Rows { .. } => ReplyKind::Success,
            PullReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            PullReply::NotReady { .. } => ReplyKind::NotReady,
            PullReply::Busy { .. } => ReplyKind::Busy,
            PullReply::Draining { .. } => ReplyKind::Draining,
            PullReply::Malformed { kind } => ReplyKind::Malformed(*kind),
            PullReply::Unauthorized { .. } => ReplyKind::Unauthorized,
        }
    }
    fn peer_unresolved(reason: String) -> PullReply {
        PullReply::PeerUnresolved { reason }
    }
    fn not_ready(reason: String) -> PullReply {
        PullReply::NotReady { reason }
    }
    fn busy(reason: String) -> PullReply {
        PullReply::Busy { reason }
    }
    fn draining(reason: String) -> PullReply {
        PullReply::Draining { reason }
    }
    fn malformed(kind: MalformedKind) -> PullReply {
        PullReply::Malformed { kind }
    }
    fn unauthorized(reason: String) -> PullReply {
        PullReply::Unauthorized { reason }
    }
}

/// An authority's side of the probe: its rows, behind the gate of its own hook, and a hold a test
/// closes to keep a pull in flight.
pub struct PullServer {
    /// The gate this authority's hook opens.
    pub gate: HydrationGate,
    rows: Mutex<Vec<Row>>,
    hold: tokio::sync::watch::Sender<bool>,
    entered: tokio::sync::watch::Sender<u32>,
    answered: AtomicU32,
}

impl PullServer {
    /// An authority holding `rows`, gate closed, not holding pulls.
    pub fn new(rows: Vec<Row>) -> Arc<Self> {
        Arc::new(Self { gate: HydrationGate::new(), rows: Mutex::new(rows), hold: tokio::sync::watch::Sender::new(false), entered: tokio::sync::watch::Sender::new(0), answered: AtomicU32::new(0) })
    }

    /// Hold every pull that is served from now until [`PullServer::release`].
    pub fn hold(&self) {
        self.hold.send_replace(true);
    }

    /// Let held pulls answer.
    pub fn release(&self) {
        self.hold.send_replace(false);
    }

    /// Resolves once `n` pulls have been served past the gate (they may be held).
    pub async fn entered(&self, n: u32) {
        let _ = self.entered.subscribe().wait_for(|e| *e >= n).await;
    }

    /// How many pulls were answered with rows.
    pub fn answered(&self) -> u32 {
        self.answered.load(Ordering::SeqCst)
    }

    /// Serve the probe on `b`, behind this authority's gate.
    pub fn serve(self: &Arc<Self>, b: ServerBuilder) -> ServerBuilder {
        let (me, gate) = (self.clone(), self.gate.clone());
        b.serve_gated::<HydratePull, _, _>(OpOwner::Testkit, gate, move |_peer: PeerContext, _req: PullRequest| {
            let me = me.clone();
            async move {
                me.entered.send_modify(|e| *e += 1);
                let _ = me.hold.subscribe().wait_for(|h| !*h).await;
                me.answered.fetch_add(1, Ordering::SeqCst);
                Ok(PullReply::Rows { rows: me.rows.lock().unwrap().clone() })
            }
        })
    }
}

/// What a [`TestHydrator`] does on an attempt.
#[derive(Debug, Clone)]
pub enum Mode {
    /// Pull from the accepting authority; its `NotReady` is `Blocked { AuthorityReady }`.
    Pull,
    /// Complete from local state: no pull (the Day-0 root).
    Local,
    /// Return this outcome.
    Fixed(HookOutcome),
    /// Say the attempt began (`started`), wait until the test says go (`resume`), then return
    /// `outcome`: how a test makes an event happen while an attempt is still running.
    Pause {
        /// Notified when the attempt begins.
        started: Arc<tokio::sync::Notify>,
        /// Waited on before the outcome is returned.
        resume: Arc<tokio::sync::Notify>,
        /// What the attempt returns.
        outcome: HookOutcome,
    },
}

/// What a hydrator holds, and how often anything about it changed.
#[derive(Debug, Default)]
pub struct Held {
    /// The rows held, by version.
    pub rows: BTreeMap<u64, String>,
    /// How many times each version changed the held state (once, however often it arrived).
    pub changes: BTreeMap<u64, u32>,
    /// How many times a row arrived that the held state already had.
    pub ignored: u32,
}

/// A test app's `hydrate_before_ready`.
pub struct TestHydrator {
    mode: Mutex<Mode>,
    held: Mutex<Held>,
    /// What each attempt saw of its context: `(attempt, authority named, authority reason)`.
    pub contexts: Mutex<Vec<(u32, Option<String>, Option<String>)>>,
    /// Attempts begun.
    pub attempts: AtomicU32,
    /// Pulls begun.
    pub pulls_started: AtomicU32,
    /// Pulls that ended with an answer.
    pub pulls_answered: AtomicU32,
    /// Pulls dropped in flight (the hook was cancelled).
    pub pulls_dropped: AtomicU32,
    /// Side effects emitted: one per row version applied, never per arrival.
    pub effects: AtomicU32,
    book: std::sync::OnceLock<rafka_mesh_transport::membership::DigestBook>,
}

/// Counts a pull dropped before it finished.
struct InFlight<'a>(&'a TestHydrator, bool);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        if !self.1 {
            self.0.pulls_dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl TestHydrator {
    /// A hydrator in `mode`.
    pub fn new(mode: Mode) -> Arc<Self> {
        Arc::new(Self {
            mode: Mutex::new(mode),
            held: Mutex::default(),
            contexts: Mutex::default(),
            attempts: AtomicU32::new(0),
            pulls_started: AtomicU32::new(0),
            pulls_answered: AtomicU32::new(0),
            pulls_dropped: AtomicU32::new(0),
            effects: AtomicU32::new(0),
            book: std::sync::OnceLock::new(),
        })
    }

    /// Change what the next attempt does.
    pub fn set_mode(&self, mode: Mode) {
        *self.mode.lock().unwrap() = mode;
    }

    /// A row delivered by gossip. Applying a version already held changes nothing.
    pub fn gossiped(&self, row: Row) {
        self.apply(row);
    }

    fn apply(&self, row: Row) {
        let mut held = self.held.lock().unwrap();
        if held.rows.contains_key(&row.version) {
            held.ignored += 1;
            return;
        }
        *held.changes.entry(row.version).or_default() += 1;
        held.rows.insert(row.version, row.value);
        self.effects.fetch_add(1, Ordering::SeqCst);
    }

    /// The membership book the first attempt was handed: what the app subscribes to before it
    /// takes its snapshot.
    pub fn book(&self) -> Option<rafka_mesh_transport::membership::DigestBook> {
        self.book.get().cloned()
    }

    /// A copy of what is held now.
    pub fn held(&self) -> (BTreeMap<u64, String>, BTreeMap<u64, u32>, u32) {
        let h = self.held.lock().unwrap();
        (h.rows.clone(), h.changes.clone(), h.ignored)
    }
}

#[async_trait::async_trait]
impl HydrateBeforeReady for TestHydrator {
    async fn hydrate_before_ready(&self, ctx: &HydrateCtx) -> HookOutcome {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        let _ = self.book.set(ctx.membership.book.clone());
        self.contexts.lock().unwrap().push((
            ctx.attempt,
            ctx.authority.accepted().map(|a| a.name.to_string()),
            match &ctx.authority {
                rafka_node_admin_core::app_hydration::Authority::None(why) => Some(why.reason().to_string()),
                rafka_node_admin_core::app_hydration::Authority::Accepted(_) => None,
            },
        ));
        let mode = self.mode.lock().unwrap().clone();
        match mode {
            Mode::Fixed(out) => out,
            Mode::Pause { started, resume, outcome } => {
                started.notify_one();
                resume.notified().await;
                outcome
            }
            Mode::Local => HookOutcome::Ok,
            Mode::Pull => {
                let Some(authority) = ctx.authority.accepted() else {
                    return HookOutcome::Failed { reason: "a pull needs an accepting authority and this birth has none".into() };
                };
                self.pulls_started.fetch_add(1, Ordering::SeqCst);
                let mut guard = InFlight(self, false);
                let (out, _) = ctx.client.call::<HydratePull>(&NodeTarget::ExactNode(authority.node_id.clone()), &PullRequest::Pull, &CallOptions::default()).await;
                guard.1 = true;
                self.pulls_answered.fetch_add(1, Ordering::SeqCst);
                match out {
                    RpcOutcome::Reply(r) => match r.into_value() {
                        PullReply::Rows { rows } => {
                            for row in rows {
                                self.apply(row);
                            }
                            HookOutcome::Ok
                        }
                        PullReply::NotReady { reason } => ctx.authority_not_ready(format!("{} answered NotReady: {reason}", authority.name)),
                        other => HookOutcome::Failed { reason: format!("{} answered the pull with {other:?}", authority.name) },
                    },
                    other => HookOutcome::Blocked { reason: format!("the pull to {} was not answered: {}", authority.name, other.name()), retry_on: RetryOn::AuthorityChanged },
                }
            }
        }
    }
}
