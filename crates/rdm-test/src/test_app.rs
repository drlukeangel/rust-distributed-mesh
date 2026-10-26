//! The testkit's application: it implements both fabric hooks the way an embedding does, so a
//! scenario can watch the fabric states move on an application's answer.
//!
//! `RDM_TEST_APP` picks the behaviour of `sync_state` (unset is `answer:0`):
//!
//! - `answer[:<ms>]`: simulated work for `<ms>` milliseconds, then `StateSynced` for the checklist.
//! - `never`: the application never answers; the fabric stays in state-sync with a named blocker.
//! - `foreign-build`: answers `StateSynced` naming another Build.
//! - `old-attempt`: answers `StateSynced` naming attempt 0, an attempt older than any claimed.
//! - `hold`: takes the checklist and holds its answer at the admin's fault cut `state-sync` (armed at
//!   boot) until the fault door releases it, then answers `StateSynced` for the checklist.
//! - `fail:<reason>`: holds like `hold`, then reports its work failed naming `<reason>` (Q158).
//! - `after-day0:<mode>`: answers Day 0's `sync_state` at once, never consulting the fault cut, and
//!   takes every later Build's in `<mode>` (`hold`, `fail:<reason>`, ...): the bootstrap admin's first
//!   checklist is Day 0's, the Build it accepts at its start before any other (fabric-create.md step 3);
//!   a launched admin never takes Day 0's. A scenario about a resize's state-sync arms it so, since the
//!   Day-0 Build is Complete only from its own ready-for-traffic (states.md "Resize and executable
//!   replacement": "as fabric.create's is") and no other Build is accepted before it.
//!
//! `traffic_opened` is the one notice after the open-traffic checklist; it only records a span.
//!
//! It declares one secret (Q150), [`DECLARED_SECRET`], set on the broker and the gateway only: a
//! scenario sets its value in the bootstrap admin's environment and reads which processes hold it.
//!
//! It declares three listeners (Q151), [`LISTENERS`], on the gateway: a gateway binds them from its
//! launch, reports them at its join and its row publishes them.

use crate::admin_faults::{AdminFaults, CutSpec, Probe};
use rdm_admin::wiring::{FabricHooks, StateSyncAnswer, StateSyncFailed, StateSyncHandoff, SyncState};
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
    /// Work for the duration, then answer the checklist.
    Answer(Duration),
    /// Never answer.
    Never,
    /// Answer with another Build's id.
    ForeignBuild,
    /// Answer with an attempt older than any claimed.
    OldAttempt,
    /// Hold the answer at the fault cut `state-sync` until the door releases it, then answer.
    Hold,
    /// Hold like [`Mode::Hold`], then report the work failed, naming the reason (Q158).
    Fail(String),
    /// Answer Day 0's checklist at once; every later Build's in the inner mode.
    AfterDayZero(Box<Mode>),
}

/// The fault cut a `hold` application holds its answers at.
pub const STATE_SYNC_CUT: &str = "state-sync";

impl Mode {
    /// The mode `RDM_TEST_APP` names (unset: answer at once), refused by name when malformed.
    pub fn from_env(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let Some(v) = get("RDM_TEST_APP").filter(|v| !v.trim().is_empty()) else { return Ok(Self::Answer(Duration::ZERO)) };
        Self::parse(&v)
    }

    fn parse(v: &str) -> Result<Self, String> {
        if let Some(inner) = v.strip_prefix("after-day0:") {
            return match Self::parse(inner)? {
                Self::AfterDayZero(_) => Err(format!("RDM_TEST_APP `{v}`: after-day0 names a mode, not another after-day0")),
                inner => Ok(Self::AfterDayZero(Box::new(inner))),
            };
        }
        match v.split_once(':').unwrap_or((v, "")) {
            ("answer", "") => Ok(Self::Answer(Duration::ZERO)),
            ("answer", ms) => ms.parse().map(|ms| Self::Answer(Duration::from_millis(ms))).map_err(|e| format!("RDM_TEST_APP `{v}`: {e}")),
            ("never", "") => Ok(Self::Never),
            ("foreign-build", "") => Ok(Self::ForeignBuild),
            ("old-attempt", "") => Ok(Self::OldAttempt),
            ("hold", "") => Ok(Self::Hold),
            ("fail", reason) if !reason.trim().is_empty() => Ok(Self::Fail(reason.into())),
            _ => Err(format!("RDM_TEST_APP `{v}` is not answer[:<ms>], never, foreign-build, old-attempt, hold, fail:<reason> or after-day0:<mode>")),
        }
    }

    fn label(&self) -> String {
        match self {
            Self::Answer(d) => format!("answer:{}", d.as_millis()),
            Self::Never => "never".into(),
            Self::ForeignBuild => "foreign-build".into(),
            Self::OldAttempt => "old-attempt".into(),
            Self::Hold => "hold".into(),
            Self::Fail(reason) => format!("fail:{reason}"),
            Self::AfterDayZero(inner) => format!("after-day0:{}", inner.label()),
        }
    }
}

/// The hooks of this application in `mode`, held at the cuts of `faults` (the admin's own). A `hold`
/// application arms its cut here, at boot, before any checklist can reach it.
pub fn hooks(mode: Mode, faults: Arc<AdminFaults>) -> Result<FabricHooks, String> {
    let applied = match &mode {
        Mode::AfterDayZero(inner) => inner.as_ref(),
        other => other,
    };
    if matches!(applied, Mode::Hold | Mode::Fail(_)) {
        faults.arm(STATE_SYNC_CUT, CutSpec::StateSync)?;
    }
    // An `after-day0` application in the bootstrap admin (no `RDM_NODE_NAME`: CLAUDE.md, a
    // `rdm-node-admin` without one is its fabric's bootstrap admin) keeps the Build of the first
    // checklist it takes, Day 0's, and answers that Build's checklists at once.
    let day_zero: Arc<std::sync::Mutex<Option<Option<String>>>> =
        Arc::new(std::sync::Mutex::new(if std::env::var_os("RDM_NODE_NAME").is_some() { Some(None) } else { None }));
    let sync_mode = mode.clone();
    let sync_state = SyncState::hook(move |handoff: StateSyncHandoff| {
        let mode = match &sync_mode {
            Mode::AfterDayZero(inner) => {
                let mut first = day_zero.lock().unwrap();
                let day_zero_build = first.get_or_insert_with(|| Some(handoff.build_id.clone()));
                if day_zero_build.as_deref() == Some(handoff.build_id.as_str()) { Mode::Answer(Duration::ZERO) } else { inner.as_ref().clone() }
            }
            other => other.clone(),
        };
        let faults = faults.clone();
        let span = tracing::info_span!("rdm.testkit.app.update.via-sync-state", mode = %mode.label(), fabric_id = %handoff.fabric_id, build_id = %handoff.build_id, attempt = handoff.attempt, operation = %handoff.operation, "otel.kind" = "internal");
        tracing::info_span!(parent: &span, "rdm.testkit.app.update.via-sync-state-taken", mode = %mode.label(), build_id = %handoff.build_id, attempt = handoff.attempt, "otel.kind" = "internal").in_scope(|| tracing::info!("the application took the checklist"));
        async move {
            let answer: StateSyncAnswer = match mode {
                Mode::Answer(work) => {
                    tokio::time::sleep(work).await;
                    tracing::info!(work_ms = work.as_millis() as u64, "the application finished its state-sync work: state-synced");
                    Ok(handoff)
                }
                Mode::Never => {
                    tracing::info!("the application takes the checklist and never answers");
                    std::future::pending().await
                }
                Mode::ForeignBuild => {
                    tracing::info!("the application answers state-synced for another Build");
                    Ok(StateSyncHandoff { build_id: "b5zscevkjkja".into(), ..handoff })
                }
                Mode::OldAttempt => {
                    tracing::info!("the application answers state-synced for attempt 0");
                    Ok(StateSyncHandoff { attempt: 0, ..handoff })
                }
                Mode::Hold => {
                    faults.hold(Probe::StateSync { build_id: handoff.build_id.clone(), attempt: handoff.attempt }).await;
                    tracing::info!("the fault door released the held checklist: state-synced");
                    Ok(handoff)
                }
                Mode::Fail(reason) => {
                    faults.hold(Probe::StateSync { build_id: handoff.build_id.clone(), attempt: handoff.attempt }).await;
                    tracing::info!(reason = %reason, "the fault door released the held call: the application reports its work failed");
                    Err(StateSyncFailed { reason })
                }
                Mode::AfterDayZero(_) => unreachable!("an after-day0 application takes each checklist in Day 0's answer or its inner mode"),
            };
            answer
        }
        .instrument(span)
    });
    let traffic_opened: rdm_admin::wiring::TrafficOpenedNotice = std::sync::Arc::new(|handoff: StateSyncHandoff| {
        let span = tracing::info_span!("rdm.testkit.app.update.via-traffic-opened", fabric_id = %handoff.fabric_id, build_id = %handoff.build_id, attempt = handoff.attempt, operation = %handoff.operation, "otel.kind" = "internal");
        Box::pin(async move { tracing::info!("the application opens client traffic") }.instrument(span))
    });
    Ok(FabricHooks { sync_state: Some(sync_state), traffic_opened: Some(traffic_opened) })
}
