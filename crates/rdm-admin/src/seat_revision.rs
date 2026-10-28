//! The path target: before a Build creates a birth at a path, the birth the view holds there is asked,
//! never assumed. One that answers keeps the path; one that does not is decided by node
//! investigation's verdict (node-recovery.md step 3, Q111): it is deleted and the path re-created,
//! `TerminateRuntime` first when its runtime runs. Nothing holds and nothing waits for a proof of
//! termination.
//!
//! The decision is here; the questions it asks of the world are a [`PathProbe`]: the running
//! admin answers them over Node RPC and its provider, a functional test answers them from a
//! fixture. The spans are the same either way.

use crate::model::{Node, PathName};
use crate::node_investigation::Finding;

/// What targeting a path's previous birth found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetOutcome {
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

/// The questions the target asks of the world.
#[async_trait::async_trait]
pub trait PathProbe: Send + Sync {
    /// Node investigation of this birth (node-investigation.md; Q107 item 1): its one verdict.
    async fn investigate(&self, node: &Node) -> Finding;
}

/// Target `path` before a new birth, given the birth this view holds there (`prev`).
pub async fn investigate_path(path: &PathName, prev: Option<Node>, probe: &dyn PathProbe) -> TargetOutcome {
    let Some(prev) = prev.filter(|p| p.incarnation_id.is_some()) else {
        tracing::info_span!("rdm.node_admin.deployment.delete.via-path-investigation", node = %path, incarnation = "", outcome = "no-birth-held")
            .in_scope(|| tracing::info!("the view holds no birth at this path: nothing to target"));
        return TargetOutcome::Clear { gone: None };
    };
    let incarnation = prev.incarnation_id.clone().map(|i| i.to_string()).unwrap_or_default();
    // The plan was made on an older view: the path's birth is live here now (heard again as the
    // attempt ran). It keeps the path; a create over it is a duplicate.
    if prev.status.is_live() {
        tracing::info_span!("rdm.node_admin.deployment.delete.via-path-investigation", node = %path, incarnation = %incarnation, outcome = "predecessor-live")
            .in_scope(|| tracing::info!("the view holds a live birth at this path: it stays, never created over"));
        return TargetOutcome::Alive;
    }
    // Node investigation decides (node-recovery.md step 3): reached, the predecessor is commanded back
    // and never created over; any other verdict, it is deleted and the path re-created.
    let finding = probe.investigate(&prev).await;
    if let Finding::Reached { .. } = finding {
        tracing::info_span!("rdm.node_admin.deployment.delete.via-path-investigation", node = %path, incarnation = %incarnation, outcome = "answers", verdict = "reached")
            .in_scope(|| tracing::info!("the previous birth is unheard but reached: it stays"));
        return TargetOutcome::Alive;
    }
    let verdict = finding.name();
    tracing::info_span!("rdm.node_admin.deployment.delete.via-path-investigation", node = %path, incarnation = %incarnation, outcome = "unreached", verdict)
        .in_scope(|| tracing::info!("the previous birth at this path is unreached: it is deleted and the path re-created"));
    TargetOutcome::Clear { gone: Some(prev) }
}

