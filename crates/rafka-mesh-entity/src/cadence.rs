//! The fabric's three configured cadences, read once from the environment, as a set, on the first
//! read of any of them. They live here, the lowest crate that membership and Node RPC both depend
//! on, so the transport configuration (`rafka_node_rpc::endpoint`) and the membership channel read
//! the same values.
//!
//! A variable that is unset takes its default. A variable that is set and is not a whole number of
//! milliseconds (empty, signed, fractional, with a unit, not text) is refused: the first read panics
//! naming every such variable with its value, whichever cadence that read asked for, and every
//! later read refuses with the same message. No reader answers the default for a value it was given.

use std::time::Duration;

/// The three cadences of one process.
struct Cadences {
    staleness_floor: Duration,
    gossip_interval: Duration,
    backbone_gossip_interval: Duration,
}

impl Cadences {
    /// The cadences `env` names, or every variable it names that is not a whole number of milliseconds.
    fn from_env(env: impl Fn(&str) -> Option<std::ffi::OsString>) -> Result<Self, String> {
        let mut refused = Vec::new();
        let mut ms = |key: &str, default_ms: u64| match env(key) {
            None => Duration::from_millis(default_ms),
            Some(raw) => match raw.to_str().ok_or_else(|| "it is not text".to_string()).and_then(|v| v.parse::<u64>().map_err(|e| e.to_string())) {
                Ok(ms) => Duration::from_millis(ms),
                Err(why) => {
                    refused.push(format!("{key}={raw:?} is not a whole number of milliseconds ({why})"));
                    Duration::from_millis(default_ms)
                }
            },
        };
        let cadences = Self {
            staleness_floor: ms("RDM_STALENESS_MS", 30_000),
            gossip_interval: ms("RDM_GOSSIP_INTERVAL_MS", 2_000),
            backbone_gossip_interval: ms("RDM_BACKBONE_INTERVAL_MS", 2_000),
        };
        if refused.is_empty() {
            Ok(cadences)
        } else {
            Err(format!("the cadence environment is refused: {}; a cadence variable is a whole number of milliseconds, or unset for its default", refused.join("; ")))
        }
    }
}

/// The process's cadences: read on the first call, a refusal panics with its message.
fn cadences() -> &'static Cadences {
    static HELD: std::sync::OnceLock<Cadences> = std::sync::OnceLock::new();
    HELD.get_or_init(|| Cadences::from_env(|key| std::env::var_os(key)).unwrap_or_else(|refusal| panic!("{refusal}")))
}

/// The staleness floor, `RDM_STALENESS_MS` (default 30 s): how long a member stays heard without
/// a fresh word, local receipt age (fabric-node-lifecycle.md §7.3, i77 PRD row 18). Past it a
/// member is `PendingReconnect`: soft and reversible, never death.
pub fn staleness_floor() -> Duration {
    cadences().staleness_floor
}

/// The mesh channel's gossip interval, `RDM_GOSSIP_INTERVAL_MS` (default 2 s; never slower,
/// gossip.md §3.2): how often a node publishes its digest. Each gossip topic has its own interval.
pub fn gossip_interval() -> Duration {
    cadences().gossip_interval
}

/// The backbone topic's gossip interval, `RDM_BACKBONE_INTERVAL_MS` (default 2 s,
/// fabric-node-lifecycle.md:355): how often a mesh's primary publishes its mesh on the backbone.
pub fn backbone_gossip_interval() -> Duration {
    cadences().backbone_gossip_interval
}
