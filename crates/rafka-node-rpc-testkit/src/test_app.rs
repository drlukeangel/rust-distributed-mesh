//! The testkit's application: it implements both fabric hooks the way an embedding does, so a
//! scenario can watch the fabric states move on an application's answer.
//!
//! `RDM_TEST_APP` picks the behaviour of `sync_state` (unset is `answer:0`):
//!
//! - `answer[:<ms>]`: simulated work for `<ms>` milliseconds, then `StateSynced` for the round.
//! - `never`: the application never answers; the fabric stays in state-sync with a named blocker.
//! - `foreign-build`: answers `StateSynced` naming another Build.
//! - `old-attempt`: answers `StateSynced` naming attempt 0, an attempt older than any claimed.
//!
//! `traffic_opened` is the one notice after the open-traffic round; it only records a span.

use rafka_node_admin_core::wiring::{FabricHooks, StateSyncRound, SyncState};
use std::time::Duration;
use tracing::Instrument;

/// How the application answers `sync_state`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mode {
    /// Work for the duration, then answer the round.
    Answer(Duration),
    /// Never answer.
    Never,
    /// Answer with another Build's id.
    ForeignBuild,
    /// Answer with an attempt older than any claimed.
    OldAttempt,
}

impl Mode {
    /// The mode `RDM_TEST_APP` names (unset: answer at once), refused by name when malformed.
    pub fn from_env(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let Some(v) = get("RDM_TEST_APP").filter(|v| !v.trim().is_empty()) else { return Ok(Self::Answer(Duration::ZERO)) };
        match v.split_once(':').unwrap_or((v.as_str(), "")) {
            ("answer", "") => Ok(Self::Answer(Duration::ZERO)),
            ("answer", ms) => ms.parse().map(|ms| Self::Answer(Duration::from_millis(ms))).map_err(|e| format!("RDM_TEST_APP `{v}`: {e}")),
            ("never", "") => Ok(Self::Never),
            ("foreign-build", "") => Ok(Self::ForeignBuild),
            ("old-attempt", "") => Ok(Self::OldAttempt),
            _ => Err(format!("RDM_TEST_APP `{v}` is not answer[:<ms>], never, foreign-build or old-attempt")),
        }
    }

    fn label(&self) -> String {
        match self {
            Self::Answer(d) => format!("answer:{}", d.as_millis()),
            Self::Never => "never".into(),
            Self::ForeignBuild => "foreign-build".into(),
            Self::OldAttempt => "old-attempt".into(),
        }
    }
}

/// The hooks of this application in `mode`.
pub fn hooks(mode: Mode) -> FabricHooks {
    let sync_mode = mode.clone();
    let sync_state = SyncState::hook(move |round: StateSyncRound| {
        let mode = sync_mode.clone();
        let span = tracing::info_span!("rdm.testkit.app.update.via-sync-state", mode = %mode.label(), fabric_id = %round.fabric_id, build_id = %round.build_id, attempt = round.attempt, operation = %round.operation, "otel.kind" = "internal");
        tracing::info_span!(parent: &span, "rdm.testkit.app.update.via-sync-state-taken", mode = %mode.label(), build_id = %round.build_id, attempt = round.attempt, "otel.kind" = "internal").in_scope(|| tracing::info!("the application took the round"));
        async move {
            match mode {
                Mode::Answer(work) => {
                    tokio::time::sleep(work).await;
                    tracing::info!(work_ms = work.as_millis() as u64, "the application finished its state-sync work: state-synced");
                    round
                }
                Mode::Never => {
                    tracing::info!("the application takes the round and never answers");
                    std::future::pending().await
                }
                Mode::ForeignBuild => {
                    tracing::info!("the application answers state-synced for another Build");
                    StateSyncRound { build_id: "bld_foreign".into(), ..round }
                }
                Mode::OldAttempt => {
                    tracing::info!("the application answers state-synced for attempt 0");
                    StateSyncRound { attempt: 0, ..round }
                }
            }
        }
        .instrument(span)
    });
    let traffic_opened: rafka_node_admin_core::wiring::TrafficOpenedNotice = std::sync::Arc::new(|round: StateSyncRound| {
        let span = tracing::info_span!("rdm.testkit.app.update.via-traffic-opened", fabric_id = %round.fabric_id, build_id = %round.build_id, attempt = round.attempt, operation = %round.operation, "otel.kind" = "internal");
        Box::pin(async move { tracing::info!("the application opens client traffic") }.instrument(span))
    });
    FabricHooks { sync_state: Some(sync_state), traffic_opened: Some(traffic_opened) }
}
