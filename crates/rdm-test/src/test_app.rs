//! The testkit's application: it implements both fabric hooks the way an embedding does, so a
//! scenario can watch the fabric states move on an application's answer.
//!
//! `RDM_TEST_APP` picks the behaviour of `sync_state` (unset is `answer:0`):
//!
//! - `answer[:<ms>]`: simulated work for `<ms>` milliseconds, then `StateSynced` for the round.
//! - `never`: the application never answers; the fabric stays in state-sync with a named blocker.
//! - `foreign-build`: answers `StateSynced` naming another Build.
//! - `old-attempt`: answers `StateSynced` naming attempt 0, an attempt older than any claimed.
//! - `hold`: takes the round and holds its answer at the admin's fault cut `state-sync` (armed at
//!   boot) until the fault door releases it, then answers `StateSynced` for the round.
//!
//! `traffic_opened` is the one notice after the open-traffic round; it only records a span.
//!
//! It declares one secret (Q150), [`DECLARED_SECRET`], set on the broker and the gateway only: a
//! scenario sets its value in the bootstrap admin's environment and reads which processes hold it.
//!
//! It declares three listeners (Q151), [`LISTENERS`], on the gateway: a gateway binds them from its
//! launch, reports them at its join and its row publishes them.

use crate::admin_faults::{AdminFaults, CutSpec, Probe};
use rdm_admin::wiring::{FabricHooks, StateSyncRound, SyncState};
use std::sync::Arc;
use std::time::Duration;
use tracing::Instrument;

/// The testkit application's declared secret: set in the process environment of [`DECLARED_KINDS`]
/// only.
pub const DECLARED_SECRET: &str = "TESTKIT_DECLARED_SECRET";

/// The kinds [`DECLARED_SECRET`] is set on.
pub const DECLARED_KINDS: [rdm_core::NodeKind; 2] = [rdm_core::NodeKind::Broker, rdm_core::NodeKind::Gateway];

/// The kind the testkit application declares listeners on (Q151).
pub const LISTENER_KIND: rdm_core::NodeKind = rdm_core::NodeKind::Gateway;

/// The listeners the testkit application declares on [`LISTENER_KIND`], as Rafka's gateway names its
/// own.
pub const LISTENERS: [&str; 3] = ["kafka", "rest", "proxy"];

/// The testkit application's listener declarations, as its wiring registers them.
pub fn listeners() -> Vec<rdm_core::launch::DeclaredListeners> {
    vec![rdm_core::launch::DeclaredListeners { kind: LISTENER_KIND, names: LISTENERS.iter().map(|n| n.to_string()).collect() }]
}

/// The testkit application's declarations, as its wiring registers them.
pub fn declared() -> Vec<rdm_core::launch::DeclaredSecret> {
    vec![rdm_core::launch::DeclaredSecret { name: DECLARED_SECRET.into(), kinds: DECLARED_KINDS.to_vec() }]
}

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
    /// Hold the answer at the fault cut `state-sync` until the door releases it, then answer.
    Hold,
}

/// The fault cut a `hold` application parks its answers at.
pub const STATE_SYNC_CUT: &str = "state-sync";

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
            ("hold", "") => Ok(Self::Hold),
            _ => Err(format!("RDM_TEST_APP `{v}` is not answer[:<ms>], never, foreign-build, old-attempt or hold")),
        }
    }

    fn label(&self) -> String {
        match self {
            Self::Answer(d) => format!("answer:{}", d.as_millis()),
            Self::Never => "never".into(),
            Self::ForeignBuild => "foreign-build".into(),
            Self::OldAttempt => "old-attempt".into(),
            Self::Hold => "hold".into(),
        }
    }
}

/// The hooks of this application in `mode`, held at the cuts of `faults` (the admin's own). A `hold`
/// application arms its cut here, at boot, before any round can reach it.
pub fn hooks(mode: Mode, faults: Arc<AdminFaults>) -> Result<FabricHooks, String> {
    if mode == Mode::Hold {
        faults.arm(STATE_SYNC_CUT, CutSpec::StateSync)?;
    }
    let sync_mode = mode.clone();
    let sync_state = SyncState::hook(move |round: StateSyncRound| {
        let mode = sync_mode.clone();
        let faults = faults.clone();
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
                    StateSyncRound { build_id: "b5zscevkjkja".into(), ..round }
                }
                Mode::OldAttempt => {
                    tracing::info!("the application answers state-synced for attempt 0");
                    StateSyncRound { attempt: 0, ..round }
                }
                Mode::Hold => {
                    faults.hold(Probe::StateSync { build_id: round.build_id.clone(), attempt: round.attempt }).await;
                    tracing::info!("the fault door released the held round: state-synced");
                    round
                }
            }
        }
        .instrument(span)
    });
    let traffic_opened: rdm_admin::wiring::TrafficOpenedNotice = std::sync::Arc::new(|round: StateSyncRound| {
        let span = tracing::info_span!("rdm.testkit.app.update.via-traffic-opened", fabric_id = %round.fabric_id, build_id = %round.build_id, attempt = round.attempt, operation = %round.operation, "otel.kind" = "internal");
        Box::pin(async move { tracing::info!("the application opens client traffic") }.instrument(span))
    });
    Ok(FabricHooks { sync_state: Some(sync_state), traffic_opened: Some(traffic_opened) })
}
