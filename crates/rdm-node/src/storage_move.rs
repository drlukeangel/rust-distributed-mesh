//! A node's move in a storage change (storage.md §2 "Changing it", step 4: "Each mesh-primary moves
//! its nodes to the new provider"): its mesh-primary commands the exact birth (node command op 0x22,
//! `StorageStep`), and the birth replaces the provider beneath its one storage handle, every caller
//! on the node moving with it, then answers. The command carries its change's `change_id`, and the
//! birth refuses by name (`rejected-stale-change`) a command of a change older than the one it acted
//! on last.

use futures_util::future::BoxFuture;
use rdm_core::node_command::NodeCommandReply;
use rdm_core::{BuildId, ChangeId, NodeId};
use std::sync::Arc;

/// A node's move span.
pub const MOVE_SPAN: &str = "rdm.node.build.update.via-storage-move";

/// What a node does to move: replace the provider beneath its storage handle with the one this
/// executable registers as the name given, or refuse by name.
pub type Mover = Arc<dyn Fn(String) -> BoxFuture<'static, Result<(), String>> + Send + Sync>;

/// A node's storage change moves: the changes it acted on, the latest last.
pub struct StorageMoves {
    node_id: NodeId,
    provider: Arc<dyn Fn() -> String + Send + Sync>,
    mover: Mover,
    acted: tokio::sync::Mutex<Vec<ChangeId>>,
}

impl StorageMoves {
    /// The moves of the node `node_id`, which writes `provider()` now and moves by `mover`.
    pub fn new(node_id: NodeId, provider: Arc<dyn Fn() -> String + Send + Sync>, mover: Mover) -> Self {
        Self { node_id, provider, mover, acted: tokio::sync::Mutex::new(Vec::new()) }
    }

    /// The refusal a command of `change_id` meets before the node accepts it: a change older than the
    /// one it acted on last.
    pub async fn refusal(&self, build_id: &BuildId, change_id: &ChangeId, provider: &str) -> Option<NodeCommandReply> {
        let acted = self.acted.lock().await;
        let latest = acted.last()?;
        if latest == change_id || !acted.contains(change_id) {
            return None;
        }
        let span = tracing::info_span!(MOVE_SPAN, build_id = %build_id.log(), change_id = %change_id, node_id = %self.node_id, provider = %provider, outcome = "rejected-stale-change", held = %latest);
        span.in_scope(|| tracing::info!("a command of a storage change older than the one this node acted on is refused by name"));
        Some(NodeCommandReply::RejectedStaleChange { held: latest.clone() })
    }

    /// Move onto `provider` for the change `change_id` of Build `build_id`.
    pub async fn serve(&self, build_id: &BuildId, change_id: &ChangeId, provider: &str) -> NodeCommandReply {
        let mut acted = self.acted.lock().await;
        if let Some(latest) = acted.last().filter(|l| *l != change_id && acted.contains(change_id)) {
            return NodeCommandReply::RejectedStaleChange { held: latest.clone() };
        }
        let span = tracing::info_span!(MOVE_SPAN, build_id = %build_id.log(), change_id = %change_id, node_id = %self.node_id, provider = %provider, from = %(self.provider)(), outcome = tracing::field::Empty, reason = tracing::field::Empty);
        if (self.provider)() != provider {
            if let Err(e) = (self.mover)(provider.to_string()).await {
                span.record("outcome", "refused").record("reason", e.as_str());
                span.in_scope(|| tracing::info!("the move is refused by name: the node writes the provider it wrote"));
                return NodeCommandReply::Failed { step: "storage-step".into(), reason: e, code: rdm_core::outcome::Code::FailedPrecondition };
            }
        }
        acted.retain(|c| c != change_id);
        acted.push(change_id.clone());
        span.record("outcome", "moved");
        span.in_scope(|| tracing::info!("this node writes the provider its change decided"));
        NodeCommandReply::StorageStepped { provider: provider.to_string() }
    }
}
