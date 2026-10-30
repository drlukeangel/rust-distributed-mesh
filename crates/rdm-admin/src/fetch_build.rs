//! A node-admin not ready: the Build facts it owes before it is Ready, and the fetch of them (i143 R-G6).
//!
//! The invariant: every Ready blocker caused by missing control facts is recoverable through a
//! request the blocked node can repeat. The blocker is a TYPED reason ([`NodeAdminNotReady`]); the
//! Build-floor step of a node-admin's start (`admin.rs` `AdminFloor`, node-start.md step 4, every
//! 100 ms while below the floor) hands it to a [`FetchBuild`], which issues the request the reason
//! names, a read of the Build's committed records from the Build storage (Q292), never a fetch from
//! a peer:
//!
//! - [`NodeAdminNotReady::NoCurrentBuild`] with a `wanted` BuildId (the entry reply named the
//!   answering admin's current Build and it is not held): that Build's committed records.
//! - [`NodeAdminNotReady::UnreadableBuild`] (the entry's Build cannot be read locally) and
//!   [`NodeAdminNotReady::BehindFloor`] (the local Build is behind the entry's attempt floor): the
//!   entry's Build's committed records.
//! - [`NodeAdminNotReady::NoCurrentBuild`] with nothing wanted (no-pointer: nothing names the Build):
//!   the storage's current Build, by R-BI3's acceptance order, and its committed records; none in
//!   progress or completed leaves the blocker.
//!
//! One read is outstanding at a time. A read that failed, or whose records did not clear the
//! blocker, is not made again until its backoff has passed.

use crate::build::BuildId;
use crate::build_state::{BuildFact, BuildStateAdapter, LocalBuildLog};
use crate::model::{NodeId, PathName};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::instrument::WithSubscriber as _;
use tracing::Instrument as _;

/// Why this admin holds back its Ready for want of control facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeAdminNotReady {
    /// No current Build: no Build in this admin's builds store is in progress or completed, or the
    /// Build the entry reply named is not held. `wanted` is that Build, until its facts arrive.
    NoCurrentBuild {
        /// This admin.
        me: PathName,
        /// The Build the entry reply named (the answering admin's current Build), when one was.
        wanted: Option<BuildId>,
    },
    /// The entry reply named `build_id` and this admin's Build log cannot read it.
    UnreadableBuild {
        /// This admin.
        me: PathName,
        /// The Build the entry reply named.
        build_id: BuildId,
        /// Why the Build log cannot read it.
        error: String,
    },
    /// This admin's own builds store could not be read for its current Build: named, never taken
    /// for a store that holds none. No request recovers it.
    BuildStoreUnreadable {
        /// This admin.
        me: PathName,
        /// Why the store could not be read.
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

impl std::fmt::Display for NodeAdminNotReady {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoCurrentBuild { me, wanted: None } => write!(f, "{me}: holds no current Build (no Build in its builds store is in progress or completed; not fetched from an existing authority)"),
            Self::NoCurrentBuild { me, wanted: Some(id) } => write!(f, "{me}: holds no facts of Build {id}, the current Build of the admin that served its entry"),
            Self::UnreadableBuild { me, build_id, error } => write!(f, "{me}: its entry named Build {build_id} and this admin's Build log cannot read it: {error}"),
            Self::BuildStoreUnreadable { me, error } => write!(f, "{me}: its builds store cannot be read for the current Build: {error}"),
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
    /// The facts of the responder's current accepted Build: nothing names the Build yet.
    CurrentBuild,
    /// No request: the blocker is this admin's own store, named.
    Nothing,
}

impl NodeAdminNotReady {
    /// What would clear the blocker.
    pub fn need(&self) -> Need {
        match self {
            Self::NoCurrentBuild { wanted: Some(id), .. } => Need::BuildFacts(id.clone()),
            Self::NoCurrentBuild { wanted: None, .. } => Need::CurrentBuild,
            Self::UnreadableBuild { build_id, .. } | Self::BehindFloor { build_id, .. } => Need::BuildFacts(build_id.clone()),
            Self::BuildStoreUnreadable { .. } => Need::Nothing,
        }
    }

    /// The blocker's name as it appears in spans and evidence.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::NoCurrentBuild { wanted: Some(_), .. } => "no-current-build-wanted",
            Self::NoCurrentBuild { wanted: None, .. } => "no-current-build",
            Self::UnreadableBuild { .. } => "unreadable-build",
            Self::BuildStoreUnreadable { .. } => "build-store-unreadable",
            Self::BehindFloor { .. } => "behind-floor",
        }
    }
}

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
    /// judged by the next floor check.
    Absorbed(NodeId),
    Failed(NodeId),
}

/// What the Build-floor check drives: at most one outstanding request, per-target backoff.
pub struct FetchBuild {
    me: PathName,
    /// The Build storage the read is made of (Q292).
    store: Arc<dyn BuildStateAdapter>,
    local: Arc<dyn LocalBuildLog>,
    /// The key the storage's backoff is held at.
    store_key: NodeId,
    in_flight: Option<tokio::task::JoinHandle<End>>,
    backoff: BTreeMap<NodeId, Backoff>,
    /// The responder whose complete stream was absorbed last: still owed a verdict.
    judging: Option<NodeId>,
    /// Told when a request ends: the floor check waiting on it checks again at once.
    ended: Arc<tokio::sync::Notify>,
}

impl FetchBuild {
    /// A fetch_build for the admin `me` over the Build storage `store` it reads and its local Build log.
    pub fn new(me: PathName, store: Arc<dyn BuildStateAdapter>, local: Arc<dyn LocalBuildLog>) -> Self {
        Self { me, store, local, store_key: NodeId::mint(), in_flight: None, backoff: BTreeMap::new(), judging: None, ended: Arc::default() }
    }

    /// One floor check's turn. `blocker` is what the check just found, `None` when no control fact
    /// is missing. Never waits for the request it starts.
    pub async fn tick(&mut self, blocker: Option<&NodeAdminNotReady>) {
        let now = Instant::now();
        if self.in_flight.as_ref().is_some_and(|h| h.is_finished()) {
            let ended = self.in_flight.take().expect("checked above").await;
            match ended {
                Ok(End::Absorbed(from)) => self.judging = Some(from),
                Ok(End::Failed(from)) => self.backoff.entry(from).or_default().failed(now),
                Err(_) => {}
            }
        }
        let Some(blocker) = blocker else {
            if let Some(from) = self.judging.take() {
                self.backoff.remove(&from);
            }
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
            Need::CurrentBuild => self.fetch(blocker, None, now).await,
            Need::BuildFacts(id) => self.fetch(blocker, Some(id), now).await,
            Need::Nothing => {}
        }
    }

    /// Resolves once no request is outstanding: one still in flight is let end (its call has its own
    /// budget) and its outcome taken as a turn would take it. A node-admin's start calls it when the
    /// Build floor is held, so nothing of the step is pending at node.started.
    pub async fn settle(&mut self) {
        if let Some(h) = self.in_flight.take() {
            match h.await {
                Ok(End::Absorbed(from)) => self.judging = Some(from),
                Ok(End::Failed(from)) => self.backoff.entry(from).or_default().failed(Instant::now()),
                Err(_) => {}
            }
        }
    }

    /// Whether the request in flight has ended with a complete stream absorbed: taken once, its
    /// responder's backoff cleared; a request that failed backs its responder off and answers `false`,
    /// as does one still in flight or none. A primary taking its role reads its Build this way
    /// (states.md "The check-in checklist", Q276).
    pub async fn absorbed(&mut self) -> bool {
        if !self.in_flight.as_ref().is_some_and(|h| h.is_finished()) {
            return false;
        }
        match self.in_flight.take().expect("checked above").await {
            Ok(End::Absorbed(from)) => {
                self.backoff.remove(&from);
                true
            }
            Ok(End::Failed(from)) => {
                self.backoff.entry(from).or_default().failed(Instant::now());
                false
            }
            Err(_) => false,
        }
    }

    /// Resolves when the request in flight ends; never while none is.
    pub async fn request_ended(&self) {
        if self.in_flight.is_some() {
            self.ended.notified().await
        } else {
            std::future::pending().await
        }
    }

    /// One read of the blocker's Build from the Build storage (Q292: a starting admin and an admin
    /// taking a primary role read the Build's committed records, never a copy a peer serves): the
    /// committed facts of `id`, or with no Build named the storage's current Build, absorbed whole into
    /// the local Build log. A read that fails is spaced by the backoff and made again.
    async fn fetch(&mut self, blocker: &NodeAdminNotReady, id: Option<BuildId>, now: Instant) {
        if !self.backoff.get(&self.store_key).is_none_or(|b| b.open(now)) {
            return;
        }
        let (store, local, me, kind, ended, key) = (self.store.clone(), self.local.clone(), self.me.clone(), blocker.kind(), self.ended.clone(), self.store_key.clone());
        // The read is a step of the turn that started it: it runs under that turn's span and dispatcher.
        let parent = tracing::Span::current();
        self.in_flight = Some(tokio::spawn(
            async move {
                let span = tracing::info_span!("rdm.node_admin.build.update.via-read-committed", node = %me, build_id = tracing::field::Empty, blocker = kind, facts = tracing::field::Empty, outcome = tracing::field::Empty);
                let read = async {
                    match id {
                        Some(id) => store.committed_facts(Some(&id)).await.map(|facts| (id, facts)),
                        None => current_committed(&*store).await,
                    }
                }
                .instrument(span.clone())
                .await;
                let end = match read {
                    Ok((id, facts)) => {
                        span.record("build_id", tracing::field::display(id.log()));
                        span.record("facts", facts.len() as u64);
                        // The Build's committed records, merged into the local log (insert-and-fail on their keys).
                        local.absorb_build(&facts).await;
                        span.record("outcome", "absorbed");
                        span.in_scope(|| tracing::info!("the Build's committed records are read from its storage and absorbed into the local Build log"));
                        End::Absorbed(key)
                    }
                    Err(e) => {
                        span.record("outcome", e.to_string().as_str());
                        tracing::info_span!("rdm.node_admin.build.reject.via-read-committed-failed", node = %me, detail = %e)
                            .in_scope(|| tracing::info!("the Build's committed records could not be read: nothing is absorbed, and the read is made again"));
                        End::Failed(key)
                    }
                };
                ended.notify_one();
                end
            }
            .instrument(parent)
            .with_current_subscriber(),
        ));
    }
}

/// The storage's current Build and its committed facts (R-BI3's acceptance order, read fresh): an
/// error naming that no Build is in progress or completed when none is.
async fn current_committed(store: &dyn BuildStateAdapter) -> Result<(BuildId, Vec<BuildFact>), crate::build_state::BuildStateError> {
    let all = store.committed_facts(None).await?;
    let folded = crate::build_state::MemoryBuildStateAdapter::new();
    folded.absorb(&all).await;
    let id = match folded.current_build().await? {
        crate::build_state::CurrentBuild::InProgress(id) | crate::build_state::CurrentBuild::Completed(id) => id,
        crate::build_state::CurrentBuild::None => return Err(crate::build_state::BuildStateError::Io("the Build storage holds no Build in progress or completed".into())),
    };
    let facts = all.into_iter().filter(|f| *f.build_id() == id).collect();
    Ok((id, facts))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT: docs/architecture/node-rpc-envelope.md:1303 "The Ready check drives reads through typed blockers: no-pointer-wanted, no-pointer, unreadable-build, behind-floor. Every blocker reads `FetchBuildSection` of the admin's own mesh. no-pointer, which names no Build, reads it naming none: the responder selects its current accepted Build once for the request and streams that Build's facts, every chunk and End naming its id, or answers `NoCurrentBuild` and the blocker stays; every other blocker reads it of the Build it names. No blocker is read by a second JoinNode. It asks the fabric-primary first, then other node-admins, then the launcher. Read-only recovery is authorized and uses bounded backoff, 250 ms doubling to 4 s."
    #[test]
    fn ready_blockers_select_typed_fetch_need_and_double_backoff_from_250ms_to_4s_cap() {
        let me: PathName = "mesh1.admin.2".parse().unwrap();
        let bid = BuildId::parse("cq2tyb03krac").unwrap();

        let no_current = NodeAdminNotReady::NoCurrentBuild {
            me: me.clone(),
            wanted: None,
        };
        assert_eq!(no_current.kind(), "no-current-build");
        assert_eq!(no_current.need(), Need::CurrentBuild);

        let wanted = NodeAdminNotReady::NoCurrentBuild {
            me: me.clone(),
            wanted: Some(bid.clone()),
        };
        assert_eq!(wanted.kind(), "no-current-build-wanted");
        assert_eq!(wanted.need(), Need::BuildFacts(bid.clone()));

        let unreadable = NodeAdminNotReady::UnreadableBuild {
            me: me.clone(),
            build_id: bid.clone(),
            error: "corrupt".into(),
        };
        assert_eq!(unreadable.kind(), "unreadable-build");
        assert_eq!(unreadable.need(), Need::BuildFacts(bid.clone()));

        let behind = NodeAdminNotReady::BehindFloor {
            me: me.clone(),
            build_id: bid.clone(),
            held: 1,
            floor: 3,
        };
        assert_eq!(behind.kind(), "behind-floor");
        assert_eq!(behind.need(), Need::BuildFacts(bid));

        let store_unreadable = NodeAdminNotReady::BuildStoreUnreadable {
            me,
            error: "io".into(),
        };
        assert_eq!(store_unreadable.kind(), "build-store-unreadable");
        assert_eq!(store_unreadable.need(), Need::Nothing);

        assert_eq!(BACKOFF_BASE, Duration::from_millis(250));
        assert_eq!(BACKOFF_CAP, Duration::from_secs(4));
        assert_eq!(backoff_for(1), Duration::from_millis(250));
        assert_eq!(backoff_for(2), Duration::from_millis(500));
        assert_eq!(backoff_for(3), Duration::from_millis(1000));
        assert_eq!(backoff_for(4), Duration::from_millis(2000));
        assert_eq!(backoff_for(5), Duration::from_secs(4));
        assert_eq!(backoff_for(6), Duration::from_secs(4));
        assert_eq!(backoff_for(20), Duration::from_secs(4));
    }
}
