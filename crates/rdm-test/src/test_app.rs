//! The testkit's application: it takes the fabric's one notice the way an embedding does, and a
//! scenario can hold the fabric short of ready-for-traffic.
//!
//! `RDM_TEST_APP` (unset is `answer`):
//!
//! - `answer`: nothing is held; the fabric opens traffic once every mesh is ready-for-traffic.
//! - `hold`: arms the admin's fault cut `open-traffic` at boot, so the fabric-primary's open-traffic
//!   action holds until the fault door releases it and the fabric stays pending: Day 0's Build is
//!   Blocked on its fabric change. A scenario that holds a later Build's change arms the same cut
//!   through the fault door instead.
//!
//! `traffic_opened` is the one notice after the open-traffic checklist; it only records a span.
//!
//! It declares one secret (Q150), [`DECLARED_SECRET`], set on the broker and the gateway only: a
//! scenario sets its value in the bootstrap admin's environment and reads which processes hold it.
//!
//! It declares three listeners (Q151), [`LISTENERS`], on the gateway: a gateway binds them from its
//! launch, reports them at its join and its row publishes them.

use crate::admin_faults::{AdminFaults, CutSpec};
use rdm_admin::wiring::{FabricHooks, TrafficOpened};
use std::sync::Arc;
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

/// What the application holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Nothing is held.
    Answer,
    /// The fabric-primary's open-traffic action is held at the fault cut [`OPEN_TRAFFIC_CUT`] until the
    /// door releases it.
    Hold,
}

/// The fault cut a `hold` application holds the open-traffic action at.
pub const OPEN_TRAFFIC_CUT: &str = "open-traffic";

impl Mode {
    /// The mode `RDM_TEST_APP` names (unset: `answer`), refused by name when malformed.
    pub fn from_env(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        match get("RDM_TEST_APP").as_deref().map(str::trim) {
            None | Some("") | Some("answer") => Ok(Self::Answer),
            Some("hold") => Ok(Self::Hold),
            Some(v) => Err(format!("RDM_TEST_APP `{v}` is not answer or hold")),
        }
    }
}

/// The hooks of this application in `mode`, held at the cuts of `faults` (the admin's own). A `hold`
/// application arms its cut here, at boot, before any checklist can reach it.
pub fn hooks(mode: Mode, faults: Arc<AdminFaults>) -> Result<FabricHooks, String> {
    if mode == Mode::Hold {
        faults.arm(OPEN_TRAFFIC_CUT, CutSpec::StateAction { node: None, command: Some("open-traffic".into()) })?;
    }
    let traffic_opened: rdm_admin::wiring::TrafficOpenedNotice = std::sync::Arc::new(|opened: TrafficOpened| {
        let span = tracing::info_span!("rdm.testkit.app.update.via-traffic-opened", fabric_id = %opened.fabric_id, build_id = %opened.build_id, attempt = opened.attempt, operation = %opened.operation, "otel.kind" = "internal");
        Box::pin(async move { tracing::info!("the application opens client traffic") }.instrument(span))
    });
    Ok(FabricHooks { traffic_opened: Some(traffic_opened) })
}
