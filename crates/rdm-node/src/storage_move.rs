//! A node's move in a storage change (storage.md §2 "Changing it", step 4: "Each mesh-primary moves
//! its nodes to the new provider"): its mesh-primary commands the exact birth (node command op 0x22,
//! `StorageStep`), and the birth replaces the provider beneath its one storage handle, every caller
//! on the node moving with it, then answers. The command carries its change's `change_id`, and the
//! birth refuses by name (`rejected-stale-change`) a command of a change older than the one it acted
//! on last.

use futures_util::future::BoxFuture;
use rdm_core::node_command::NodeCommandReply;
use rdm_core::{ChangeId, NodeId};
use std::sync::Arc;

/// A node's move span.
pub const MOVE_SPAN: &str = "rdm.node.storage.update.via-move";

/// What a node does to move: replace the provider beneath its storage handle with the one this
/// executable registers as the name given, or refuse by name.
pub type Mover = Arc<dyn Fn(String) -> BoxFuture<'static, Result<(), String>> + Send + Sync>;

/// A node's storage change moves: the changes it acted on, the latest last.
#[derive(Clone)]
pub struct StorageMoves {
    node_id: NodeId,
    provider: Arc<dyn Fn() -> String + Send + Sync>,
    mover: Mover,
    state: Arc<tokio::sync::Mutex<MoveState>>,
    properties: Option<crate::node_records::NodePropertyWriter>,
}

#[derive(Default)]
struct MoveState {
    acted: Vec<CompletedMove>,
    blocked: Option<(ChangeId, crate::node_records::PropertyFreeze)>,
}

struct CompletedMove {
    change_id: ChangeId,
    provider: String,
}

impl StorageMoves {
    /// The moves of the node `node_id`, which writes `provider()` now and moves by `mover`.
    pub fn new(node_id: NodeId, provider: Arc<dyn Fn() -> String + Send + Sync>, mover: Mover) -> Self {
        Self { node_id, provider, mover, state: Default::default(), properties: None }
    }

    /// The node's existing own writer, frozen before copy and released after move or rollback.
    pub fn with_properties(mut self, properties: crate::node_records::NodePropertyWriter) -> Self {
        self.properties = Some(properties);
        self
    }

    /// Freeze admission and finish issued property writes. This retained transition owns its
    /// task through caller cancellation, just as an admitted property mutation does.
    pub async fn block(&self, change_id: &ChangeId) -> NodeCommandReply {
        let (moves, change_id) = (self.clone(), change_id.clone());
        tokio::spawn(async move {
            let mut state = moves.state.lock().await;
            if let Some((held_change, _)) = &state.blocked {
                return if *held_change == change_id { NodeCommandReply::StorageBlocked }
                    else { rdm_core::reply::node_command::rejected_stale_change(held_change.clone()) };
            }
            if let Some(completed) = state.acted.iter().find(|c| c.change_id == change_id) {
                return rdm_core::reply::node_command::rejected_stale_change(completed.change_id.clone());
            }
            let Some(properties) = moves.properties else {
                return rdm_core::reply::node_command::not_ready("this birth has no own property writer to block".into());
            };
            let guard = properties.freeze().await;
            state.blocked = Some((change_id.clone(), guard));
            tracing::info_span!("rdm.node.record.update.via-storage-block", node_id = %moves.node_id, change_id = %change_id, outcome = "blocked")
                .in_scope(|| tracing::info!("property admission is closed and earlier puts have ended"));
            NodeCommandReply::StorageBlocked
        }).await.expect("storage block task panicked")
    }

    /// The refusal a command of `change_id` meets before the node accepts it: a change older than the
    /// one it acted on last.
    pub async fn refusal(&self, change_id: &ChangeId, provider: &str) -> Option<NodeCommandReply> {
        let state = self.state.lock().await;
        if let Some((held_change, _)) = &state.blocked {
            if held_change != change_id {
                return Some(rdm_core::reply::node_command::rejected_stale_change(held_change.clone()));
            }
        }
        let acted = &state.acted;
        let latest = acted.last()?;
        if &latest.change_id == change_id && latest.provider == provider {
            return None;
        }
        if !acted.iter().any(|c| &c.change_id == change_id) {
            return None;
        }
        let span = tracing::info_span!("rdm.node.storage.update.via-move", change_id = %change_id, node_id = %self.node_id, provider = %provider, outcome = tracing::field::Empty, held = %latest.change_id);
        span.record("outcome", rdm_core::reply::node_command::rejected_stale_change(latest.change_id.clone()).name());
        span.in_scope(|| tracing::info!("a command of a storage change older than the one this node acted on is refused by name"));
        Some(rdm_core::reply::node_command::rejected_stale_change(latest.change_id.clone()))
    }

    /// Move onto `provider` for the storage change `change_id`.
    pub async fn serve(&self, change_id: &ChangeId, provider: &str) -> NodeCommandReply {
        let (moves, change_id, provider) = (self.clone(), change_id.clone(), provider.to_string());
        tokio::spawn(async move { moves.move_retained(&change_id, &provider).await })
            .await.expect("storage move task panicked")
    }

    async fn move_retained(&self, change_id: &ChangeId, provider: &str) -> NodeCommandReply {
        let mut state = self.state.lock().await;
        if let Some((held_change, _)) = &state.blocked {
            if held_change != change_id {
                return rdm_core::reply::node_command::rejected_stale_change(held_change.clone());
            }
        } else if self.properties.is_some() && (self.provider)() != provider
            && !state.acted.iter().any(|c| &c.change_id == change_id) {
            return rdm_core::reply::node_command::not_ready("the node's properties must be blocked before this storage move".into());
        }
        if let Some(done) = state.acted.iter().find(|c| &c.change_id == change_id) {
            let latest = state.acted.last().expect("a completed move exists");
            return if latest.change_id == *change_id && done.provider == provider {
                NodeCommandReply::StorageStepped { provider: done.provider.clone() }
            } else {
                rdm_core::reply::node_command::rejected_stale_change(latest.change_id.clone())
            };
        }
        let span = tracing::info_span!("rdm.node.storage.update.via-move", change_id = %change_id, node_id = %self.node_id, provider = %provider, from = %(self.provider)(), outcome = tracing::field::Empty, reason = tracing::field::Empty);
        if (self.provider)() != provider {
            if let Err(e) = (self.mover)(provider.to_string()).await {
                span.record("outcome", "refused").record("reason", e.as_str());
                span.in_scope(|| tracing::info!("the move is refused by name: the node writes the provider it wrote"));
                return rdm_core::reply::node_command::failed("storage-step".into(), e, rdm_core::outcome::Code::FailedPrecondition);
            }
        }
        state.acted.push(CompletedMove { change_id: change_id.clone(), provider: provider.to_string() });
        // Same-provider rollback also releases its exact freeze. Dropping the guard clears
        // the cached record before a waiting mutation may read the decided provider.
        state.blocked.take();
        span.record("outcome", "moved");
        span.in_scope(|| tracing::info!("this node writes the provider its change decided"));
        NodeCommandReply::StorageStepped { provider: provider.to_string() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_records::NodePropertyWriter;
    use crate::nodes_storage::{MemoryNodesStorage, NodeRecord, NodesStorage};
    use rdm_core::IncarnationId;

    /// A node's record at its start, its config `source = initial`.
    fn properties() -> NodeRecord {
        let mut record = crate::nodes_storage::fixture_record(&rdm_core::PathName::new("mesh1.node", "1").unwrap(), "node", NodeId::mint(), IncarnationId::mint(), crate::model::NodeStatus::Starting);
        record.config = [("source".into(), "initial".into())].into();
        record
    }

    // @feature: node-records
    /// A rollback releases its own block and late commands cannot release or recreate another one.
    #[tokio::test]
    async fn an_exact_storage_change_freezes_writes_until_move_or_rollback() {
        let store = Arc::new(MemoryNodesStorage::default());
        let writer = NodePropertyWriter::new(store.clone(), || "memory".into());
        let initial = properties();
        writer.bound(initial.clone(), |_| {}).await.unwrap();
        let moves = StorageMoves::new(initial.node_id.clone(), Arc::new(|| "file".into()), Arc::new(|_| Box::pin(async { panic!("same-provider rollback must not open another provider") }))).with_properties(writer.clone());
        let (a, b) = (ChangeId::mint(), ChangeId::mint());
        let adopted = ChangeId::mint();
        assert_eq!(moves.serve(&adopted, "file").await, NodeCommandReply::StorageStepped { provider: "file".into() }, "a fresh birth already on the decided provider completes without an old-provider freeze");
        assert!(matches!(moves.serve(&adopted, "other").await, NodeCommandReply::RejectedStaleChange { .. }), "the same completed change cannot choose another destination");
        assert_eq!(moves.block(&a).await, NodeCommandReply::StorageBlocked);
        assert_eq!(moves.block(&a).await, NodeCommandReply::StorageBlocked);
        let (w, seed) = (writer.clone(), initial.clone());
        let mut queued = Box::pin(w.update(seed, |r| r.config = [("source".into(), "queued".into())].into(), |_| {}));
        assert!(futures_util::poll!(&mut queued).is_pending());
        assert_eq!(store.node(&initial.node_id).await.unwrap().map(|l| l.record), Some(initial.clone()));
        assert_eq!(moves.serve(&a, "file").await, NodeCommandReply::StorageStepped { provider: "file".into() });
        queued.await.unwrap();
        assert!(matches!(moves.block(&a).await, NodeCommandReply::RejectedStaleChange { .. }), "a delayed block cannot refreeze a completed change");
        assert_eq!(moves.block(&b).await, NodeCommandReply::StorageBlocked);
        assert!(matches!(moves.serve(&a, "file").await, NodeCommandReply::RejectedStaleChange { held } if held == b));
        let (w, seed) = (writer.clone(), initial.clone());
        let mut next = Box::pin(w.update(seed, |r| { r.tags.insert("after-b".into()); }, |_| {}));
        assert!(futures_util::poll!(&mut next).is_pending());
        assert_eq!(moves.serve(&b, "file").await, NodeCommandReply::StorageStepped { provider: "file".into() });
        let final_record = next.await.unwrap().record;
        assert_eq!(final_record.config["source"], "queued");
        assert_eq!(final_record.tags, ["after-b".into()].into());
    }

    // @feature: node-records
    /// An accepted move retains its transition through caller cancellation after the handle changes.
    #[tokio::test]
    async fn a_cancelled_storage_move_still_releases_its_property_writer() {
        let writer = NodePropertyWriter::new(Arc::new(MemoryNodesStorage::default()), || "memory".into());
        let initial = properties();
        writer.bound(initial.clone(), |_| {}).await.unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let current = Arc::new(std::sync::Mutex::new("file".to_string()));
        let (provider, changed, e, r) = (current.clone(), current.clone(), entered.clone(), release.clone());
        let moves = StorageMoves::new(initial.node_id.clone(), Arc::new(move || provider.lock().unwrap().clone()), Arc::new(move |next| {
            let (changed, entered, release) = (changed.clone(), e.clone(), r.clone());
            Box::pin(async move { *changed.lock().unwrap() = next; entered.notify_one(); release.notified().await; Ok(()) })
        })).with_properties(writer.clone());
        let change = ChangeId::mint();
        assert_eq!(moves.block(&change).await, NodeCommandReply::StorageBlocked);
        let (m, c) = (moves.clone(), change.clone());
        let caller = tokio::spawn(async move { m.serve(&c, "target").await });
        entered.notified().await;
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        let mut pending = Box::pin(writer.update(initial, |r| { r.tags.insert("after-move".into()); }, |_| {}));
        assert!(futures_util::poll!(&mut pending).is_pending());
        release.notify_one();
        let written = tokio::time::timeout(std::time::Duration::from_secs(5), pending).await.expect("the retained move releases the writer").unwrap();
        assert_eq!(written.record.tags, ["after-move".into()].into());
        assert_eq!(*current.lock().unwrap(), "target");
        assert!(matches!(moves.block(&change).await, NodeCommandReply::RejectedStaleChange { .. }));
    }
}
