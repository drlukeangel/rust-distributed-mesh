//! The path fence: before a Build creates a birth at a path, the birth the view holds there is asked,
//! never assumed. One that answers keeps the path; one that does not is decided by node
//! investigation's verdict (node-recovery.md step 3, Q111): it is deleted and the path re-created,
//! `TerminateRuntime` first when its runtime runs. Nothing holds and nothing waits for a proof of
//! termination.
//!
//! The decision is here; the questions it asks of the world are a [`PathProbe`]: the running
//! admin answers them over Node RPC and its provider, a functional test answers them from a
//! fixture. The spans are the same either way.

use crate::deployment::provider::Inspected;
use crate::model::{Node, PathName};

/// What fencing a path's previous birth found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceOutcome {
    /// A new birth may take the path. `gone` is the previous birth the view holds there, unheard and
    /// not answering, whatever the provider said of its runtime: the create deletes it first.
    /// `None` when the view holds no birth there.
    Clear {
        /// The previous birth, unheard and unanswering.
        gone: Option<Node>,
    },
    /// The previous birth answers directly: it is alive and keeps the path.
    Alive,
}

/// The questions the fence asks of the world.
#[async_trait::async_trait]
pub trait PathProbe: Send + Sync {
    /// Does this birth answer directly (Node RPC Ping for an rpc node, an entry pull for an admin)?
    async fn answers(&self, node: &Node) -> bool;
    /// node.runtime.inspect of this birth's exact runtime (node-runtime-inspect.md "Result"): its
    /// `unavailable` names why nothing could be told.
    async fn inspect(&self, node: &Node) -> Inspected;
}

/// Fence `path` before a new birth, given the birth this view holds there (`prev`).
pub async fn fence(path: &PathName, prev: Option<Node>, probe: &dyn PathProbe) -> FenceOutcome {
    let Some(prev) = prev.filter(|p| p.incarnation_id.is_some()) else {
        tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = "", outcome = "no-birth-held")
            .in_scope(|| tracing::info!("the view holds no birth at this path: nothing to fence"));
        return FenceOutcome::Clear { gone: None };
    };
    let incarnation = prev.incarnation_id.clone().map(|i| i.to_string()).unwrap_or_default();
    // The plan was made on an older view: the path's birth is live here now (heard again as the
    // attempt ran). It keeps the path; a create over it is a duplicate.
    if prev.status.is_live() {
        tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = %incarnation, outcome = "predecessor-live")
            .in_scope(|| tracing::info!("the view holds a live birth at this path: it stays, never created over"));
        return FenceOutcome::Alive;
    }
    // Reached: a predecessor that answers is commanded back, never created over (node-recovery.md
    // step 3).
    if probe.answers(&prev).await {
        tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = %incarnation, outcome = "answers")
            .in_scope(|| tracing::info!("the previous birth is unheard but answers directly: it stays"));
        return FenceOutcome::Alive;
    }
    let verdict = crate::admin::verdict_name(&probe.inspect(&prev).await);
    tracing::info_span!("rdm.node_admin.deployment.delete.via-fence", node = %path, incarnation = %incarnation, outcome = "unreached", verdict)
        .in_scope(|| tracing::info!("the previous birth at this path is unreached: it is deleted and the path re-created"));
    FenceOutcome::Clear { gone: Some(prev) }
}

