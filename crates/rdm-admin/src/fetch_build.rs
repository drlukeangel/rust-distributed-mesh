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
//! - [`NodeAdminNotReady::UnreadableBuild`] (the entry's Build cannot be read locally): the entry's
//!   Build's committed records.
//! - [`NodeAdminNotReady::NoCurrentBuild`] with nothing wanted (no-pointer: nothing names the Build):
//!   the storage's current Build, by R-BI3's acceptance order, and its committed records; none in
//!   progress or completed leaves the blocker.
//!
//! One read is outstanding at a time. A read that failed, or whose records did not clear the
//! blocker, is not made again until its backoff has passed.

use crate::build::BuildId;
use crate::build_state::{BuildFact, BuildStateAdapter as _, LocalBuildLog};
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
        me_path: String,
        /// This node's name, beside its path.
        me_name: String,
        /// The Build the entry reply named (the answering admin's current Build), when one was.
        wanted: Option<BuildId>,
    },
    /// The entry reply named `build_id` and this admin's Build log cannot read it.
    UnreadableBuild {
        /// This admin.
        me_path: String,
        /// This node's name, beside its path.
        me_name: String,
        /// The Build the entry reply named.
        build_id: BuildId,
        /// Why the Build log cannot read it.
        error: String,
    },
    /// This admin's own builds store could not be read for its current Build: named, never taken
    /// for a store that holds none. No request recovers it.
    BuildStoreUnreadable {
        /// This admin.
        me_path: String,
        /// This node's name, beside its path.
        me_name: String,
        /// Why the store could not be read.
        error: String,
    },
}

impl std::fmt::Display for NodeAdminNotReady {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoCurrentBuild { me_path, me_name, wanted: None } => write!(f, "path {me_path:?}, name {me_name:?}: holds no current Build (no Build in its builds store is in progress or completed; not fetched from an existing authority)"),
            Self::NoCurrentBuild { me_path, me_name, wanted: Some(id) } => write!(f, "path {me_path:?}, name {me_name:?}: holds no facts of Build {id}, the current Build of the admin that served its entry"),
            Self::UnreadableBuild { me_path, me_name, build_id, error } => write!(f, "path {me_path:?}, name {me_name:?}: its entry named Build {build_id} and this admin's Build log cannot read it: {error}"),
            Self::BuildStoreUnreadable { me_path, me_name, error } => write!(f, "path {me_path:?}, name {me_name:?}: its builds store cannot be read for the current Build: {error}"),
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
            Self::UnreadableBuild { build_id, .. } => Need::BuildFacts(build_id.clone()),
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
        }
    }
}

/// How long a target that failed is left alone: the one schedule's delay for its consecutive
/// failures (node-rpc.md §43, `rdm_transport::Pacing`; Q344: no per-site constants). It spaces
/// repeated reads and gates nothing else; it retries no write and no decision (R-G6).
fn backoff_for(failures: u32) -> Duration {
    rdm_transport::Pacing::from_cadences().after(failures.max(1))
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
    me_path: String,
    /// This node's name, beside its path.
    me_name: String,
    /// The Build storage the read is made of (Q292).
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
    /// The `(path, name)` lookup index key of me_path and me_name.
    fn me(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.me_path.clone(), name: self.me_name.clone() } }
}

impl FetchBuild {
    /// A fetch_build for the admin `me` over its local Build log, which reads the Build storage.
    pub fn new(me: PathName, local: Arc<dyn LocalBuildLog>) -> Self {
        Self { me_path: me.path.clone(), me_name: me.name.clone(), local, store_key: NodeId::mint(), in_flight: None, backoff: BTreeMap::new(), judging: None, ended: Arc::default() }
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
        let (local, me, kind, ended, key) = (self.local.clone(), self.me(), blocker.kind(), self.ended.clone(), self.store_key.clone());
        // The read is a step of the turn that started it: it runs under that turn's span and dispatcher.
        let parent = tracing::Span::current();
        self.in_flight = Some(tokio::spawn(
            async move {
                let span = tracing::info_span!("rdm.node_admin.build.update.via-read-committed", node_path = %me.path, node_name = %me.name, build_id = tracing::field::Empty, blocker = kind, facts = tracing::field::Empty, outcome = tracing::field::Empty);
                let read = async {
                    match id {
                        Some(id) => local.committed_facts(Some(&id)).await.map(|facts| (id, facts)),
                        None => current_committed(&*local).await,
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
                        tracing::info_span!("rdm.node_admin.build.reject.via-read-committed-failed", node_path = %me.path, node_name = %me.name, detail = %e)
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
async fn current_committed(local: &dyn LocalBuildLog) -> Result<(BuildId, Vec<BuildFact>), crate::build_state::BuildStateError> {
    let all = local.committed_facts(None).await?;
    // The Builds' state is their events: the records are folded over the same events store.
    let folded = crate::build_state::MemoryBuildStateAdapter::with_events(local.events());
    folded.absorb_facts(&all).await;
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

    /// CONTRACT: docs/architecture/node-rpc-envelope.md "Build floor read (op `0x1F` reserved)": "The Ready check drives reads through typed blockers: no-pointer-wanted, no-pointer, unreadable-build, behind-floor. Every blocker reads the Build storage: no-pointer, which names no Build, reads the storage's current Build, selected once for the read; every other blocker reads the Build it names. [...] A read that fails is made again after the [§43 schedule](node-rpc.md#43-send-delivery-is-the-runtimes)'s delay."
    #[test]
    fn ready_blockers_select_typed_fetch_need_and_space_reads_on_the_one_schedule() {
        let me: PathName = rdm_core::PathName::new("mesh1.node-admin", "2").unwrap();
        let bid = BuildId::parse("cq2tyb03krac").unwrap();

        let no_current = NodeAdminNotReady::NoCurrentBuild {
            me_path: me.path.clone(), me_name: me.name.clone(),
            wanted: None,
        };
        assert_eq!(no_current.kind(), "no-current-build");
        assert_eq!(no_current.need(), Need::CurrentBuild);

        let wanted = NodeAdminNotReady::NoCurrentBuild {
            me_path: me.path.clone(), me_name: me.name.clone(),
            wanted: Some(bid.clone()),
        };
        assert_eq!(wanted.kind(), "no-current-build-wanted");
        assert_eq!(wanted.need(), Need::BuildFacts(bid.clone()));

        let unreadable = NodeAdminNotReady::UnreadableBuild {
            me_path: me.path.clone(), me_name: me.name.clone(),
            build_id: bid.clone(),
            error: "corrupt".into(),
        };
        assert_eq!(unreadable.kind(), "unreadable-build");
        assert_eq!(unreadable.need(), Need::BuildFacts(bid.clone()));


        let store_unreadable = NodeAdminNotReady::BuildStoreUnreadable {
            me_path: me.path.clone(), me_name: me.name.clone(),
            error: "io".into(),
        };
        assert_eq!(store_unreadable.kind(), "build-store-unreadable");
        assert_eq!(store_unreadable.need(), Need::Nothing);

        // The one schedule (node-rpc.md §43): the first delay the gossip interval, capped at the floor,
        // each with its ±25 % jitter.
        let (cadence, floor) = (rdm_transport::topology::gossip_interval(), rdm_transport::topology::staleness_floor());
        assert!(backoff_for(1) >= cadence.mul_f64(0.75) && backoff_for(1) <= cadence.mul_f64(1.25), "the first delay is the gossip interval");
        assert!(backoff_for(20) <= floor.mul_f64(1.25), "every delay is capped at the staleness floor");
    }
}
