//! The pre_node_ready probe: a testkit-only app on op `0x74` that stands in for a product's
//! ProjectionPull. [`PullServer`] is an authority's side (rows behind a [`PreNodeReadyGate`]),
//! [`TestPreNodeReady`] is a node's `pre_node_ready` that pulls those rows and applies them once
//! per row version, whether a row arrives by the pull, by gossip, or both.
//!
//! Testkit range only ([`TESTKIT_OPS`]); never a product binary.
//!
//! [`TESTKIT_OPS`]: rdm_core::catalog::TESTKIT_OPS

use rdm_node::node_hooks::{HookOutcome, PreNodeReady, NodeReadyCtx, RetryOn};
use rdm_transport::{CallOptions, PreNodeReadyGate, PeerContext, ServerBuilder};
use rdm_core::catalog::OpOwner;
use rdm_core::outcome::{MalformedKind, ReplyKind, RpcOutcome};
use rdm_core::protocol::NodeProtocol;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

/// The pre_node_ready probe protocol on op `0x74`.
pub struct PreNodeReadyPull;

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
    ///
    /// Position 0 on the wire.
    Pull,
}

/// A pull's answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PullReply {
    /// The rows, complete.
    ///
    /// Position 0 on the wire.
    Rows {
        /// The rows in version order.
        rows: Vec<Row>,
    },
    /// The peer the call needed could not be resolved.
    ///
    /// Position 1 on the wire.
    PeerUnresolved {
        /// Why.
        reason: String,
    },
    /// The authority's own hook has not passed.
    ///
    /// Position 2 on the wire.
    NotReady {
        /// Why.
        reason: String,
    },
    /// At the admission bound.
    ///
    /// Position 3 on the wire.
    Busy {
        /// Which bound.
        reason: String,
    },
    /// Connections-deleting.
    ///
    /// Position 4 on the wire.
    ConnectionsDeleting {
        /// Why.
        reason: String,
    },
    /// The request frame was malformed.
    ///
    /// Position 5 on the wire.
    Malformed {
        /// How.
        kind: MalformedKind,
    },
    /// Not allowed.
    ///
    /// Position 6 on the wire.
    Unauthorized {
        /// Why.
        reason: String,
    },
}

impl NodeProtocol for PreNodeReadyPull {
    const OP: u8 = 0x74;
    const NAME: &'static str = "pre-node-ready-pull";
    const MAX_REQUEST_FRAME_BYTES: usize = 256;
    const MAX_REPLY_FRAME_BYTES: usize = 64 * 1024;
    const REQUEST_VARIANTS: u32 = 1;
    const REPLY_VARIANTS: u32 = 7;
    type Request = PullRequest;
    type Reply = PullReply;

    fn reply_detail(reply: &Self::Reply) -> &'static str {
        match reply {
            PullReply::Rows { .. } => "rows",
            PullReply::PeerUnresolved { .. } => "peer-unresolved",
            PullReply::NotReady { .. } => "not-ready",
            PullReply::Busy { .. } => "busy",
            PullReply::ConnectionsDeleting { .. } => "connections-deleting",
            PullReply::Malformed { .. } => "malformed",
            PullReply::Unauthorized { .. } => "unauthorized",
        }
    }
    fn classify_reply(reply: &PullReply) -> ReplyKind {
        match reply {
            PullReply::Rows { .. } => ReplyKind::Success,
            PullReply::PeerUnresolved { .. } => ReplyKind::PeerUnresolved,
            PullReply::NotReady { .. } => ReplyKind::NotReady,
            PullReply::Busy { .. } => ReplyKind::Busy,
            PullReply::ConnectionsDeleting { .. } => ReplyKind::ConnectionsDeleting,
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
    fn connections_deleting(reason: String) -> PullReply {
        PullReply::ConnectionsDeleting { reason }
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
    pub gate: PreNodeReadyGate,
    rows: Mutex<Vec<Row>>,
    hold: tokio::sync::watch::Sender<bool>,
    entered: tokio::sync::watch::Sender<u32>,
    answered: AtomicU32,
}

impl PullServer {
    /// An authority holding `rows`, gate closed, not holding pulls.
    pub fn new(rows: Vec<Row>) -> Arc<Self> {
        Self::behind(rows, PreNodeReadyGate::new())
    }

    /// An authority holding `rows` behind `gate`: the gate of the hook of the node that serves it.
    pub fn behind(rows: Vec<Row>, gate: PreNodeReadyGate) -> Arc<Self> {
        Arc::new(Self { gate, rows: Mutex::new(rows), hold: tokio::sync::watch::Sender::new(false), entered: tokio::sync::watch::Sender::new(0), answered: AtomicU32::new(0) })
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
        b.serve_gated::<PreNodeReadyPull, _, _>(OpOwner::Testkit, gate, move |_peer: PeerContext, _req: PullRequest| {
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

/// What a [`TestPreNodeReady`] does on an attempt.
#[derive(Debug, Clone)]
pub enum Mode {
    /// Pull from the accepting authority; its `NotReady` is `Blocked { PrimaryReady }`.
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

/// What a test hook holds, and how often anything about it changed.
#[derive(Debug, Default)]
pub struct Held {
    /// The rows held, by version.
    pub rows: BTreeMap<u64, String>,
    /// How many times each version changed the held state (once, however often it arrived).
    pub changes: BTreeMap<u64, u32>,
    /// How many times a row arrived that the held state already had.
    pub ignored: u32,
}

/// A test app's `pre_node_ready`.
pub struct TestPreNodeReady {
    mode: Mutex<Mode>,
    held: Mutex<Held>,
    /// What each attempt saw of its context: `(attempt, authority named, authority reason)`.
    pub contexts: Mutex<Vec<(u32, Option<String>, Option<String>)>>,
    /// The member cert each attempt's context carried.
    pub member_certs: Mutex<Vec<Option<Vec<u8>>>>,
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
    digest_view: std::sync::OnceLock<rdm_transport::topology::DigestView>,
}

/// Counts a pull dropped before it finished.
struct InFlight<'a>(&'a TestPreNodeReady, bool);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        if !self.1 {
            self.0.pulls_dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl TestPreNodeReady {
    /// A test hook in `mode`.
    pub fn new(mode: Mode) -> Arc<Self> {
        Arc::new(Self {
            mode: Mutex::new(mode),
            held: Mutex::default(),
            contexts: Mutex::default(),
            member_certs: Mutex::default(),
            attempts: AtomicU32::new(0),
            pulls_started: AtomicU32::new(0),
            pulls_answered: AtomicU32::new(0),
            pulls_dropped: AtomicU32::new(0),
            effects: AtomicU32::new(0),
            digest_view: std::sync::OnceLock::new(),
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

    /// The topology view the first attempt was handed: what the app subscribes to before it
    /// takes its snapshot.
    pub fn digest_view(&self) -> Option<rdm_transport::topology::DigestView> {
        self.digest_view.get().cloned()
    }

    /// A copy of what is held now.
    pub fn held(&self) -> (BTreeMap<u64, String>, BTreeMap<u64, u32>, u32) {
        let h = self.held.lock().unwrap();
        (h.rows.clone(), h.changes.clone(), h.ignored)
    }
}

#[async_trait::async_trait]
impl PreNodeReady for TestPreNodeReady {
    async fn pre_node_ready(&self, ctx: &NodeReadyCtx) -> HookOutcome {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        let _ = self.digest_view.set(ctx.topology_view.digest_view.clone());
        self.member_certs.lock().unwrap().push(ctx.member_cert.clone());
        self.contexts.lock().unwrap().push((
            ctx.attempt,
            ctx.accepting_primary.accepted().map(|a| a.name.to_string()),
            match &ctx.accepting_primary {
                rdm_node::node_hooks::AcceptingPrimarySource::None(why) => Some(why.reason().to_string()),
                rdm_node::node_hooks::AcceptingPrimarySource::Accepted(_) => None,
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
                let Some(accepting_primary) = ctx.accepting_primary.accepted() else {
                    return HookOutcome::Failed { reason: "a pull needs an accepting authority and this birth has none".into() };
                };
                self.pulls_started.fetch_add(1, Ordering::SeqCst);
                let mut guard = InFlight(self, false);
                let (out, _) = ctx.client.call::<PreNodeReadyPull>(&accepting_primary.target, &PullRequest::Pull, &CallOptions::default()).await;
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
                        PullReply::NotReady { reason } => ctx.primary_not_ready(format!("{} answered NotReady: {reason}", accepting_primary.name)),
                        other => HookOutcome::Failed { reason: format!("{} answered the pull with {other:?}", accepting_primary.name) },
                    },
                    other => HookOutcome::Blocked { reason: format!("the pull to {} was not answered: {}", accepting_primary.name, other.name()), retry_on: RetryOn::PrimaryChanged },
                }
            }
        }
    }
}

/// A node's `pre_node_ready` read from a scenario's knob file
/// ([`crate::pre_node_ready_knob_dir`]`/<path.name>`): each attempt reads the file; `pass` (or no file)
/// returns `Ok`, any other text is `Blocked` with that text as its reason, run again when the
/// node's authority addresses it. A scenario changes the reason or lets the hook pass by rewriting
/// the file. Testkit only; never a product binary.
pub struct KnobPreNodeReady {
    file: std::path::PathBuf,
}

impl KnobPreNodeReady {
    /// The hook for the node `name` of the estate under `root`, when the scenario recorded a knob
    /// for it; `None` otherwise (the node registers no hook).
    pub fn for_node(root: &std::path::Path, name: &str) -> Option<Arc<Self>> {
        let file = crate::pre_node_ready_knob_dir(root).join(name);
        file.exists().then(|| Arc::new(Self { file }))
    }

    /// The hook for the node `name` of the estate under `root`, passing immediately when no knob
    /// file exists and consulting the knob file when one was written.
    pub fn always(root: &std::path::Path, name: &str) -> Arc<Self> {
        let file = crate::pre_node_ready_knob_dir(root).join(name);
        Arc::new(Self { file })
    }
}

#[async_trait::async_trait]
impl PreNodeReady for KnobPreNodeReady {
    async fn pre_node_ready(&self, _ctx: &NodeReadyCtx) -> HookOutcome {
        if !self.file.exists() {
            return HookOutcome::Ok;
        }
        let outcome = match std::fs::read_to_string(&self.file) {
            Ok(text) if text.trim() != "pass" => HookOutcome::Blocked { reason: text.trim().to_string(), retry_on: RetryOn::PrimaryAddressed },
            _ => HookOutcome::Ok,
        };
        // One line per attempt beside the knob (`<path.name>.attempts`): what the scenario waits on.
        let line = match &outcome {
            HookOutcome::Blocked { reason, .. } => format!("blocked {reason}\n"),
            _ => "ok\n".to_string(),
        };
        let attempts = std::path::PathBuf::from(format!("{}.attempts", self.file.display()));
        let _ = std::fs::OpenOptions::new().create(true).append(true).open(&attempts).and_then(|mut f| std::io::Write::write_all(&mut f, line.as_bytes()));
        outcome
    }
}
