//! rafka-chaos — chaos harness for the v2 mesh substrate.
//!
//! Implements the chaos primitives catalog from `docs/plans/mesh-v1/04-chaos-harness-prd.md`.
//! Each primitive drives the topology-ui's subprocess control endpoints to disturb the
//! mesh in a reproducible way, then watches Jaeger for the detection criterion.
//!
//! The primitives form a tree:
//! - Process primitives (kill_node, restart_node, wedge_node) — operate via topology-ui's
//!   `/api/nodes/spawn` and `/api/nodes/{name}` endpoints
//! - Network primitives (partition_pair, partition_subset, flap_link, slow_link, lossy_link,
//!   firewall_inbound, nat_shift) — Windows firewall rules + iroh endpoint manipulation
//! - System primitives (clock_skew, disk_full) — OS-level injection
//!
//! Sprint 11 ships the process primitives + a smoke soak runner. Network + system primitives
//! follow in subsequent sprints as their Windows-specific implementations stabilize.

use async_trait::async_trait;
use rand_chacha::ChaCha20Rng;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

pub mod primitives;
pub mod soak;

/// Detection result for a chaos primitive's `detect()` call.
#[derive(Debug, Serialize, Deserialize)]
pub enum DetectionResult {
    /// Primitive's success criterion was met within the deadline.
    Passed { waited_ms: u64 },
    /// Deadline elapsed without seeing the criterion.
    FailedTimeout { waited_ms: u64 },
    /// Saw the criterion partially or saw something contradicting it.
    FailedAssertion { msg: String, waited_ms: u64 },
}

/// Outcome of a primitive's `execute()` call — opaque payload the primitive uses to
/// later run `detect()` and `revert()`.
#[derive(Debug, Serialize, Deserialize)]
pub struct ChaosOutcome {
    pub primitive_name: String,
    pub targets: Vec<String>,
    /// Serialized state the primitive needs for detect/revert. JSON for inspection.
    pub state: serde_json::Value,
}

/// Errors a chaos primitive can produce.
#[derive(Error, Debug)]
pub enum ChaosError {
    #[error("topology-ui unreachable: {0}")]
    TopologyUiUnreachable(String),
    #[error("invalid target: {0}")]
    InvalidTarget(String),
    #[error("jaeger query failed: {0}")]
    JaegerQuery(String),
    #[error("primitive execution failed: {0}")]
    Execution(String),
}

/// Shared context every chaos primitive receives.
#[derive(Clone)]
pub struct ChaosContext {
    /// HTTP client for talking to topology-ui's spawn/kill endpoints + Jaeger query API.
    pub http: reqwest::Client,
    /// Base URL of the topology-ui process (default http://localhost:19090).
    pub topology_ui_url: String,
    /// Base URL of Jaeger Query API (default http://localhost:16686).
    pub jaeger_url: String,
    /// Seeded RNG for reproducible primitive execution.
    pub rng: Arc<tokio::sync::Mutex<ChaCha20Rng>>,
}

/// Every chaos primitive implements this trait.
#[async_trait]
pub trait ChaosPrimitive: Send + Sync {
    /// Stable name, used in spans and CLI commands. Matches §2 of the chaos PRD.
    fn name(&self) -> &str;

    /// Pick targets + perform the disturbance. Returns an opaque outcome for detect/revert.
    async fn execute(&self, ctx: &ChaosContext) -> Result<ChaosOutcome, ChaosError>;

    /// Watch for the primitive's success criterion. Returns within `deadline_ms` or sooner.
    async fn detect(
        &self,
        ctx: &ChaosContext,
        outcome: &ChaosOutcome,
        deadline_ms: u64,
    ) -> Result<DetectionResult, ChaosError>;

    /// Undo the primitive's effect. For some primitives (kill_node) this is a no-op
    /// because the effect is permanent until the soak loop re-spawns the node.
    async fn revert(
        &self,
        _ctx: &ChaosContext,
        _outcome: &ChaosOutcome,
    ) -> Result<(), ChaosError> {
        Ok(())
    }
}

/// Build a default ChaosContext using env vars + a seeded RNG.
pub fn default_context(seed: u64) -> ChaosContext {
    use rand::SeedableRng;
    let topology_ui_url = std::env::var("RAFKA_TOPOLOGY_UI_URL")
        .unwrap_or_else(|_| "http://localhost:19090".to_string());
    let jaeger_url = std::env::var("JAEGER_QUERY_URL")
        .unwrap_or_else(|_| "http://localhost:16686".to_string());
    ChaosContext {
        http: reqwest::Client::new(),
        topology_ui_url,
        jaeger_url,
        rng: Arc::new(tokio::sync::Mutex::new(ChaCha20Rng::seed_from_u64(seed))),
    }
}
