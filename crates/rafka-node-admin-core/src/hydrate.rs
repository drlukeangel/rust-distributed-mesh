//! Hydration of the control facts a node-admin owes before it is Ready (i143 R-G6).
//!
//! The invariant: every Ready blocker caused by missing control facts is recoverable through a
//! request the blocked node can repeat. The blocker is a TYPED reason ([`HydrationBlocker`]); the
//! existing Ready check (`admin.rs`, every 100 ms) hands it to a [`Hydrator`], which issues the
//! request the reason names:
//!
//! - [`HydrationBlocker::NoPointer`] with a `wanted` BuildId (the entry reply supplied the Fabric
//!   record and the Build is not held): `FetchBuildFacts` (op `0x1F`) for that Build.
//! - [`HydrationBlocker::UnreadableBuild`] (the pointed Build cannot be read locally) and
//!   [`HydrationBlocker::BehindFloor`] (the local Build is behind the entry's attempt floor):
//!   `FetchBuildFacts` for the pointed Build.
//! - [`HydrationBlocker::NoPointer`] with nothing wanted: the control entry is retrieved again. A Build
//!   that no pointer and no entry reply identified is never fetched.
//!
//! One request is outstanding at a time. Responders are asked in order: the current fabric-primary
//! (resolved through the canonical leadership projection, `Topology::fabric_primary`), then every
//! other known live node-admin. A target that failed, or answered with holdings that did not clear
//! the blocker, is not asked again until its backoff has passed.

use crate::build::BuildId;
use crate::build_facts_read::fetch_build_facts;
use crate::build_state::{BuildStateAdapter, LocalBuildLog};
use crate::model::{NodeId, PathName};
use rafka_node_rpc::{NodeRpcClient, NodeTarget};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Why this admin holds back its Ready for want of control facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HydrationBlocker {
    /// No installed `Fabric.build_id`. `wanted` is the Build the entry reply (or the Build topic)
    /// named, parked until its facts arrive.
    NoPointer {
        /// This admin.
        me: PathName,
        /// The Build named by the entry reply or the Build topic, when one was.
        wanted: Option<BuildId>,
    },
    /// `Fabric.build_id` names `build_id` and this admin's Build log cannot read it.
    UnreadableBuild {
        /// This admin.
        me: PathName,
        /// The Build the pointer names.
        build_id: BuildId,
        /// Why the Build log cannot read it.
        error: String,
    },
    /// The local Build holds attempt `held`; the admin that served the entry held `floor`.
    BehindFloor {
        /// This admin.
        me: PathName,
        /// The Build.
        build_id: BuildId,
        /// The attempt the local Build holds.
        held: u32,
        /// The attempt the admin that served the entry held.
        floor: u32,
    },
}

impl std::fmt::Display for HydrationBlocker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoPointer { me, .. } => write!(f, "{me}: holds no Fabric.build_id (not hydrated from an existing authority)"),
            Self::UnreadableBuild { me, build_id, error } => write!(f, "{me}: Fabric.build_id names {build_id} and this admin's Build log cannot read it: {error}"),
            Self::BehindFloor { me, build_id, held, floor } => write!(
                f,
                "{me}: holds attempt {held} of Build {build_id}; the admin that served its entry held attempt {floor} (its opens, claims and receipts are still arriving)"
            ),
        }
    }
}

/// What the request that recovers a blocker asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Need {
    /// The facts of this Build.
    BuildFacts(BuildId),
    /// The control entry again: nothing identifies the Build yet.
    Entry,
}

impl HydrationBlocker {
    /// What would clear the blocker.
    pub fn need(&self) -> Need {
        match self {
            Self::NoPointer { wanted: Some(id), .. } => Need::BuildFacts(id.clone()),
            Self::NoPointer { wanted: None, .. } => Need::Entry,
            Self::UnreadableBuild { build_id, .. } | Self::BehindFloor { build_id, .. } => Need::BuildFacts(build_id.clone()),
        }
    }

    /// The blocker's name as it appears in spans and evidence.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::NoPointer { wanted: Some(_), .. } => "no-pointer-wanted",
            Self::NoPointer { wanted: None, .. } => "no-pointer",
            Self::UnreadableBuild { .. } => "unreadable-build",
            Self::BehindFloor { .. } => "behind-floor",
        }
    }
}

/// A node-admin that may hold the facts, in the order it is asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Responder {
    /// The responder's `path.name`.
    pub name: PathName,
    /// The responder's node id.
    pub node_id: NodeId,
}

/// The responders in the order they are asked: the fabric-primary first, then every other live
/// node-admin. Read at each request, so the order follows the leadership projection.
pub type Responders = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Vec<Responder>> + Send>> + Send + Sync>;

/// The control entry retrieved again: the same retrieval that served the first one.
pub type EntryRepeat = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync>;

/// How long a target that failed is left alone: doubling per consecutive failure from `BACKOFF_BASE`
/// to `BACKOFF_CAP`. Luke explicitly authorized this backoff (R-G6) as READ-ONLY recovery: it spaces
/// repeated reads of a peer and gates nothing else; it retries no write and no decision.
pub const BACKOFF_BASE: Duration = Duration::from_millis(250);
/// The longest wait after repeated failures.
pub const BACKOFF_CAP: Duration = Duration::from_secs(4);

fn backoff_for(failures: u32) -> Duration {
    BACKOFF_BASE.saturating_mul(1u32 << failures.saturating_sub(1).min(8)).min(BACKOFF_CAP)
}

#[derive(Default)]
struct Backoff {
    failures: u32,
    until: Option<Instant>,
}

impl Backoff {
    fn failed(&mut self, now: Instant) {
        self.failures += 1;
        self.until = Some(now + backoff_for(self.failures));
    }
    fn open(&self, now: Instant) -> bool {
        self.until.is_none_or(|u| u <= now)
    }
}

/// How one outstanding request ended.
enum End {
    /// A complete stream was absorbed from this responder; whether it cleared the blocker is
    /// judged by the next Ready check.
    Absorbed(NodeId),
    Failed(NodeId),
    Entry(Result<(), String>),
}

/// What the Ready check drives: at most one outstanding request, per-target backoff.
pub struct Hydrator {
    me: PathName,
    client: Arc<NodeRpcClient>,
    local: Arc<dyn LocalBuildLog>,
    builds: Arc<dyn BuildStateAdapter>,
    accepted: Arc<crate::accepted::AcceptedStore>,
    responders: Responders,
    entry: Option<EntryRepeat>,
    in_flight: Option<tokio::task::JoinHandle<End>>,
    backoff: BTreeMap<NodeId, Backoff>,
    entry_backoff: Backoff,
    /// The responder whose complete stream was absorbed last: still owed a verdict.
    judging: Option<NodeId>,
}

impl Hydrator {
    /// A hydrator for the admin `me` over its client, its local Build log, its Build state, its
    /// accepted-Build store, the `responders` it asks and the entry repeat.
    pub fn new(
        me: PathName,
        client: Arc<NodeRpcClient>,
        local: Arc<dyn LocalBuildLog>,
        builds: Arc<dyn BuildStateAdapter>,
        accepted: Arc<crate::accepted::AcceptedStore>,
        responders: Responders,
        entry: Option<EntryRepeat>,
    ) -> Self {
        Self { me, client, local, builds, accepted, responders, entry, in_flight: None, backoff: BTreeMap::new(), entry_backoff: Backoff::default(), judging: None }
    }

    /// One Ready check's turn. `blocker` is what the check just found, `None` when no control fact
    /// is missing. Never waits for the request it starts.
    pub async fn tick(&mut self, blocker: Option<&HydrationBlocker>) {
        let now = Instant::now();
        if self.in_flight.as_ref().is_some_and(|h| h.is_finished()) {
            let ended = self.in_flight.take().expect("checked above").await;
            match ended {
                Ok(End::Absorbed(from)) => self.judging = Some(from),
                Ok(End::Failed(from)) => self.backoff.entry(from).or_default().failed(now),
                Ok(End::Entry(Err(_))) => self.entry_backoff.failed(now),
                Ok(End::Entry(Ok(()))) => {}
                Err(_) => {}
            }
        }
        let Some(blocker) = blocker else {
            if let Some(from) = self.judging.take() {
                self.backoff.remove(&from);
            }
            self.entry_backoff = Backoff::default();
            return;
        };
        // Absorbed a complete stream and still blocked: that responder's holdings were not enough.
        if let Some(from) = self.judging.take() {
            self.backoff.entry(from).or_default().failed(now);
        }
        if self.in_flight.is_some() {
            return;
        }
        match blocker.need() {
            Need::Entry => self.repeat_entry(blocker, now),
            Need::BuildFacts(id) => self.fetch(blocker, id, now).await,
        }
    }

    fn repeat_entry(&mut self, blocker: &HydrationBlocker, now: Instant) {
        let Some(entry) = self.entry.clone() else { return };
        if !self.entry_backoff.open(now) {
            return;
        }
        let (me, kind) = (self.me.clone(), blocker.kind());
        self.in_flight = Some(tokio::spawn(async move {
            let span = tracing::info_span!("rdm.node_admin.fabric.update.via-entry-repeat", node = %me, blocker = kind, outcome = tracing::field::Empty);
            let result = tracing::Instrument::instrument(entry(), span.clone()).await;
            span.record("outcome", match &result { Ok(()) => "retrieved".to_string(), Err(e) => format!("failed: {e}") }.as_str());
            span.in_scope(|| tracing::info!("no pointer and no Build identified: the control entry is retrieved again"));
            End::Entry(result)
        }));
    }

    async fn fetch(&mut self, blocker: &HydrationBlocker, id: BuildId, now: Instant) {
        let candidates = (self.responders)().await;
        let Some(target) = candidates.into_iter().filter(|r| r.name != self.me).find(|r| self.backoff.get(&r.node_id).is_none_or(|b| b.open(now))) else {
            return;
        };
        let (client, local, builds, accepted, me, kind) = (self.client.clone(), self.local.clone(), self.builds.clone(), self.accepted.clone(), self.me.clone(), blocker.kind());
        self.in_flight = Some(tokio::spawn(async move {
            let span = tracing::info_span!(
                "rdm.node_admin.build.update.via-fetch-facts",
                node = %me,
                build_id = %id,
                target = %target.name,
                blocker = kind,
                facts = tracing::field::Empty,
                chunks = tracing::field::Empty,
                outcome = tracing::field::Empty,
            );
            let fetched = {
                use tracing::Instrument as _;
                fetch_build_facts(&client, &NodeTarget::ExactNode(target.node_id.clone()), &id).instrument(span.clone()).await
            };
            match fetched {
                Ok(f) if f.complete => {
                    span.record("facts", f.facts.len() as u64);
                    span.record("chunks", f.chunks);
                    local.absorb_facts(&f.facts).await;
                    accepted.resolve_wanted(&*builds).await;
                    span.record("outcome", "absorbed");
                    span.in_scope(|| tracing::info!("the responder's complete holdings of the Build are absorbed into the local Build log"));
                    End::Absorbed(target.node_id)
                }
                Ok(f) => {
                    span.record("facts", f.facts.len() as u64);
                    span.record("outcome", "responder-incomplete");
                    tracing::info_span!("rdm.node_admin.build.reject.via-fetch-incomplete", node = %me, build_id = %id, target = %target.name)
                        .in_scope(|| tracing::info!("the responder says its holdings of the Build are not whole: nothing is absorbed"));
                    End::Failed(target.node_id)
                }
                Err(e) => {
                    span.record("outcome", e.reason());
                    tracing::info_span!("rdm.node_admin.build.reject.via-fetch-failed", node = %me, build_id = %id, target = %target.name, reason = e.reason(), detail = %e)
                        .in_scope(|| tracing::info!("the Build-facts read produced nothing to absorb"));
                    End::Failed(target.node_id)
                }
            }
        }));
    }
}
