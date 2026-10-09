//! The path fence: before a Build creates a birth at a path, whatever the world still runs there
//! is asked, never assumed (silence never authorizes a replacement; only a provider inspection
//! that says the exact runtime exited is proof).
//!
//! The decision is here; the questions it asks of the world are a [`PathProbe`]: the running
//! admin answers them over Node RPC, the entry protocol and its provider, a functional test
//! answers them from a fixture. The spans are the same either way.

use crate::deployment::provider::DeploymentStatus;
use crate::model::{Node, PathName};

/// What fencing a path's previous birth found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceOutcome {
    /// Nothing runs there any more: a new birth may take the path. `gone` is the previous
    /// birth when the provider inspected its exact runtime as exited: that inspection is the
    /// proof its departure is published from. `None` when there was no birth, or this admin
    /// holds no runtime for it (no proof, so no departure is published).
    Clear {
        /// The previous birth, when its exact runtime was inspected as exited.
        gone: Option<Node>,
    },
    /// The previous birth answers directly: it is alive and keeps the path.
    Alive,
    /// The previous birth does not answer, but a runtime at the path still runs: silence is
    /// never death proof, so it is held and keeps the path. Only an explicit retire or replace
    /// may terminate a running runtime.
    Held,
}

/// The questions the fence asks of the world.
#[async_trait::async_trait]
pub trait PathProbe: Send + Sync {
    /// Does this birth answer directly (Node RPC Ping for an rpc node, an entry pull for an admin)?
    async fn answers(&self, node: &Node) -> bool;
    /// The provider's inspection of this birth's exact runtime; `Err` names why it has none.
    async fn inspect(&self, node: &Node) -> Result<DeploymentStatus, String>;
    /// Every runtime recorded at `path` in this provider's domain, as `(where, inspection)`.
    async fn recorded(&self, path: &PathName) -> Vec<(String, DeploymentStatus)>;
}

/// Fence `path` before a new birth, given the birth this view holds there (`prev`).
pub async fn fence(path: &PathName, prev: Option<Node>, probe: &dyn PathProbe) -> FenceOutcome {
    let Some(prev) = prev else {
        // The view holds no birth at the path: the world is asked, not assumed.
        let recorded = probe.recorded(path).await;
        if let Some((at, _)) = recorded.iter().find(|(_, s)| matches!(s, DeploymentStatus::Running)) {
            tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = "", outcome = "held-running-unheard", data_dir = %at)
                .in_scope(|| tracing::info!("the view holds no birth at this path, but a runtime recorded there still runs: held, never replaced on silence"));
            return FenceOutcome::Held;
        }
        tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = "", outcome = "no-predecessor-held", runtime_records = recorded.len())
            .in_scope(|| tracing::info!("the view holds no birth at this path and no recorded runtime there runs: nothing to fence"));
        return FenceOutcome::Clear { gone: None };
    };
    let incarnation = prev.incarnation_id.clone().map(|i| i.0).unwrap_or_default();
    if prev.incarnation_id.is_none() {
        tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = "", outcome = "no-birth-held")
            .in_scope(|| tracing::info!("the view holds a path with no birth at it: nothing to fence"));
        return FenceOutcome::Clear { gone: None };
    }
    // The plan was made on an older view: the path's birth is live here now (heard again through
    // a reborn forwarder, as the attempt ran). It keeps the path; a create over it is a duplicate.
    if prev.status.is_live() {
        tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = %incarnation, outcome = "predecessor-live")
            .in_scope(|| tracing::info!("the view holds a live birth at this path: it stays, never created over"));
        return FenceOutcome::Alive;
    }
    // Unheard is not dead: a healthy member's digests can stop arriving for a window (its
    // forwarder left, its connections are timing out). A predecessor that answers is alive.
    if probe.answers(&prev).await {
        tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = %incarnation, outcome = "answers")
            .in_scope(|| tracing::info!("the previous birth is unheard but answers directly: it stays"));
        return FenceOutcome::Alive;
    }
    let (outcome, proven) = match probe.inspect(&prev).await {
        Err(e) => (format!("not-found: {e}"), false),
        Ok(DeploymentStatus::Running) => {
            tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = %incarnation, outcome = "held-running")
                .in_scope(|| tracing::info!("the previous birth is unheard but its exact runtime still runs: held, never replaced on silence"));
            return FenceOutcome::Held;
        }
        // Only an inspection that says the runtime exited is proof; `Unknown` fences the path
        // (nothing answers there) but publishes no departure.
        Ok(other) => (format!("not-running: {other:?}"), matches!(other, DeploymentStatus::Exited { .. })),
    };
    tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = %incarnation, outcome = %outcome, proven_gone = proven)
        .in_scope(|| tracing::info!("the previous birth at this path is fenced before a new one"));
    FenceOutcome::Clear { gone: proven.then_some(prev) }
}
