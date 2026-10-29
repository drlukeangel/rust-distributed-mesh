//! A node-admin not ready: the Build facts it owes before it is Ready, and the fetch of them (i143 R-G6).
//!
//! The invariant: every Ready blocker caused by missing control facts is recoverable through a
//! request the blocked node can repeat. The blocker is a TYPED reason ([`NodeAdminNotReady`]); the
//! Build-floor step of a node-admin's start (`admin.rs` `AdminFloor`, node-start.md step 4, every
//! 100 ms while below the floor) hands it to a [`FetchBuild`], which issues the request the reason
//! names:
//!
//! - [`NodeAdminNotReady::NoCurrentBuild`] with a `wanted` BuildId (the entry reply named the
//!   answering admin's current Build and it is not held): `FetchBuildFacts` (op `0x1F`) for that
//!   Build.
//! - [`NodeAdminNotReady::UnreadableBuild`] (the entry's Build cannot be read locally) and
//!   [`NodeAdminNotReady::BehindFloor`] (the local Build is behind the entry's attempt floor):
//!   `FetchBuildFacts` for the entry's Build.
//! - [`NodeAdminNotReady::NoCurrentBuild`] with nothing wanted (no-pointer: nothing names the Build):
//!   `FetchCurrentBuild` (op `0x1F`). The responder selects its current accepted Build once for the
//!   request and streams that Build's facts, every chunk and `End` naming its id, or answers
//!   `NoCurrentBuild` and the blocker stays. No blocker is read by a second `JoinNode`.
//!
//! One request is outstanding at a time. Responders are asked in order: the current fabric-primary
//! (resolved through the canonical leadership projection, `Topology::fabric_primary`), then every
//! other known live node-admin. A target that failed, or answered with holdings that did not clear
//! the blocker, is not asked again until its backoff has passed.

use crate::build::BuildId;
use crate::build_facts_read::{fetch_build_facts, fetch_current_build};
use crate::build_state::LocalBuildLog;
use crate::model::{NodeId, PathName};
use rdm_transport::{NodeRpcClient, NodeTarget};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
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
    client: Arc<NodeRpcClient>,
    local: Arc<dyn LocalBuildLog>,
    responders: Responders,
    in_flight: Option<tokio::task::JoinHandle<End>>,
    backoff: BTreeMap<NodeId, Backoff>,
    /// The responder whose complete stream was absorbed last: still owed a verdict.
    judging: Option<NodeId>,
    /// Told when a request ends: the floor check waiting on it checks again at once.
    ended: Arc<tokio::sync::Notify>,
}

impl FetchBuild {
    /// A fetch_build for the admin `me` over its client, its local Build log and the `responders` it
    /// asks.
    pub fn new(me: PathName, client: Arc<NodeRpcClient>, local: Arc<dyn LocalBuildLog>, responders: Responders) -> Self {
        Self { me, client, local, responders, in_flight: None, backoff: BTreeMap::new(), judging: None, ended: Arc::default() }
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

    /// Resolves when the request in flight ends; never while none is.
    pub async fn request_ended(&self) {
        if self.in_flight.is_some() {
            self.ended.notified().await
        } else {
            std::future::pending().await
        }
    }

    /// One read of the blocker's Build from the first responder whose backoff has passed: the facts
    /// of `id` (`FetchBuildFacts`), or with no Build named the responder's current accepted Build
    /// (`FetchCurrentBuild`), whose id the read's `End` names.
    async fn fetch(&mut self, blocker: &NodeAdminNotReady, id: Option<BuildId>, now: Instant) {
        let candidates = (self.responders)().await;
        let Some(target) = candidates.into_iter().filter(|r| r.name != self.me).find(|r| self.backoff.get(&r.node_id).is_none_or(|b| b.open(now))) else {
            return;
        };
        let (client, local, me, kind, ended) = (self.client.clone(), self.local.clone(), self.me.clone(), blocker.kind(), self.ended.clone());
        // The request is a step of the turn that started it: it runs under that turn's span and dispatcher.
        let parent = tracing::Span::current();
        self.in_flight = Some(tokio::spawn(
            async move {
                let span = tracing::info_span!(
                    "rdm.node_admin.build.update.via-fetch-facts",
                    node = %me,
                    build_id = tracing::field::Empty,
                    target = %target.name,
                    blocker = kind,
                    facts = tracing::field::Empty,
                    chunks = tracing::field::Empty,
                    outcome = tracing::field::Empty,
                );
                let to = NodeTarget::ExactNode(target.node_id.clone());
                let fetched = match &id {
                    Some(id) => {
                        span.record("build_id", tracing::field::display(id.log()));
                        fetch_build_facts(&client, &to, id).instrument(span.clone()).await
                    }
                    None => fetch_current_build(&client, &to).instrument(span.clone()).await,
                };
                // The Build read: the one named, or the one the current Build read's End named.
                let read = match (&fetched, id) {
                    (Ok(f), None) => {
                        span.record("build_id", tracing::field::display(f.build_id.log()));
                        Some(f.build_id.log().to_string())
                    }
                    (_, id) => id.map(|b| b.log().to_string()),
                };
                let end = match fetched {
                    Ok(f) if f.complete => {
                        span.record("facts", f.facts.len() as u64);
                        span.record("chunks", f.chunks);
                        // A whole Build's facts, merged reading no store (Q227).
                        local.absorb_build(&f.facts).await;
                        span.record("outcome", "absorbed");
                        span.in_scope(|| tracing::info!("the responder's complete holdings of the Build are absorbed into the local Build log"));
                        End::Absorbed(target.node_id)
                    }
                    Ok(f) => {
                        span.record("facts", f.facts.len() as u64);
                        span.record("outcome", "responder-incomplete");
                        tracing::info_span!("rdm.node_admin.build.reject.via-fetch-incomplete", node = %me, build_id = read.as_deref(), target = %target.name)
                            .in_scope(|| tracing::info!("the responder says its holdings of the Build are not whole: nothing is absorbed"));
                        End::Failed(target.node_id)
                    }
                    Err(e) => {
                        span.record("outcome", e.reason());
                        tracing::info_span!("rdm.node_admin.build.reject.via-fetch-failed", node = %me, build_id = read.as_deref(), target = %target.name, reason = e.reason(), detail = %e)
                            .in_scope(|| tracing::info!("the Build-facts read produced nothing to absorb"));
                        End::Failed(target.node_id)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT: docs/architecture/node-rpc-envelope.md:1290 "The Ready check drives reads through typed blockers: no-pointer-wanted, no-pointer, unreadable-build, behind-floor. no-pointer, which names no Build, reads `FetchCurrentBuild`: the responder selects its current accepted Build once for the request and streams that Build's facts, every chunk and End naming its id, or answers `NoCurrentBuild` and the blocker stays; every other blocker reads `FetchBuildFacts` of the Build it names. No blocker is read by a second JoinNode. It asks the fabric-primary first, then other node-admins, then the launcher. Read-only recovery is authorized and uses bounded backoff, 250 ms doubling to 4 s."
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
