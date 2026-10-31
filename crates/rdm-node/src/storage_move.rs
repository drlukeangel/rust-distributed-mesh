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
    blocked: Option<(BuildId, ChangeId, crate::node_records::PropertyFreeze)>,
}

struct CompletedMove {
    build_id: BuildId,
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
    pub async fn block(&self, build_id: &BuildId, change_id: &ChangeId) -> NodeCommandReply {
        let (moves, build_id, change_id) = (self.clone(), build_id.clone(), change_id.clone());
        tokio::spawn(async move {
            let mut state = moves.state.lock().await;
            if let Some((held_build, held_change, _)) = &state.blocked {
                return if *held_build == build_id && *held_change == change_id { NodeCommandReply::StorageBlocked }
                    else { rdm_core::reply::node_command::rejected_stale_change(held_change.clone()) };
            }
            if let Some(completed) = state.acted.iter().find(|c| c.build_id == build_id && c.change_id == change_id) {
                return rdm_core::reply::node_command::rejected_stale_change(completed.change_id.clone());
            }
            let Some(properties) = moves.properties else {
                return rdm_core::reply::node_command::not_ready("this birth has no own property writer to block".into());
            };
            let guard = properties.freeze().await;
            state.blocked = Some((build_id.clone(), change_id.clone(), guard));
            tracing::info_span!("rdm.node.record.update.via-storage-block", node_id = %moves.node_id, build_id = %build_id.log(), change_id = %change_id, outcome = "blocked")
                .in_scope(|| tracing::info!("property admission is closed and earlier puts have ended"));
            NodeCommandReply::StorageBlocked
        }).await.expect("storage block task panicked")
    }

    /// The refusal a command of `change_id` meets before the node accepts it: a change older than the
    /// one it acted on last.
    pub async fn refusal(&self, build_id: &BuildId, change_id: &ChangeId, provider: &str) -> Option<NodeCommandReply> {
        let state = self.state.lock().await;
        if let Some((held_build, held_change, _)) = &state.blocked {
            if held_build != build_id || held_change != change_id {
                return Some(rdm_core::reply::node_command::rejected_stale_change(held_change.clone()));
            }
        }
        let acted = &state.acted;
        let latest = acted.last()?;
        if &latest.build_id == build_id && &latest.change_id == change_id && latest.provider == provider {
            return None;
        }
        if !acted.iter().any(|c| &c.build_id == build_id && &c.change_id == change_id) {
            return None;
        }
        let span = tracing::info_span!("rdm.node.build.update.via-storage-move", build_id = %build_id.log(), change_id = %change_id, node_id = %self.node_id, provider = %provider, outcome = tracing::field::Empty, held = %latest.change_id);
        span.record("outcome", rdm_core::reply::node_command::rejected_stale_change(latest.change_id.clone()).name());
        span.in_scope(|| tracing::info!("a command of a storage change older than the one this node acted on is refused by name"));
        Some(rdm_core::reply::node_command::rejected_stale_change(latest.change_id.clone()))
    }

    /// Move onto `provider` for the change `change_id` of Build `build_id`.
    pub async fn serve(&self, build_id: &BuildId, change_id: &ChangeId, provider: &str) -> NodeCommandReply {
        let (moves, build_id, change_id, provider) = (self.clone(), build_id.clone(), change_id.clone(), provider.to_string());
        tokio::spawn(async move { moves.move_retained(&build_id, &change_id, &provider).await })
            .await.expect("storage move task panicked")
    }

    async fn move_retained(&self, build_id: &BuildId, change_id: &ChangeId, provider: &str) -> NodeCommandReply {
        let mut state = self.state.lock().await;
        if let Some((held_build, held_change, _)) = &state.blocked {
            if held_build != build_id || held_change != change_id {
                return rdm_core::reply::node_command::rejected_stale_change(held_change.clone());
            }
        } else if self.properties.is_some() && (self.provider)() != provider
            && !state.acted.iter().any(|c| &c.build_id == build_id && &c.change_id == change_id) {
            return rdm_core::reply::node_command::not_ready("the node's properties must be blocked before this storage move".into());
        }
        if let Some(done) = state.acted.iter().find(|c| &c.build_id == build_id && &c.change_id == change_id) {
            let latest = state.acted.last().expect("a completed move exists");
            return if latest.build_id == *build_id && latest.change_id == *change_id && done.provider == provider {
                NodeCommandReply::StorageStepped { provider: done.provider.clone() }
            } else {
                rdm_core::reply::node_command::rejected_stale_change(latest.change_id.clone())
            };
        }
        let span = tracing::info_span!("rdm.node.build.update.via-storage-move", build_id = %build_id.log(), change_id = %change_id, node_id = %self.node_id, provider = %provider, from = %(self.provider)(), outcome = tracing::field::Empty, reason = tracing::field::Empty);
        if (self.provider)() != provider {
            if let Err(e) = (self.mover)(provider.to_string()).await {
                span.record("outcome", "refused").record("reason", e.as_str());
                span.in_scope(|| tracing::info!("the move is refused by name: the node writes the provider it wrote"));
                return rdm_core::reply::node_command::failed("storage-step".into(), e, rdm_core::outcome::Code::FailedPrecondition);
            }
        }
        state.acted.push(CompletedMove { build_id: build_id.clone(), change_id: change_id.clone(), provider: provider.to_string() });
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
    use crate::node_records::{MemoryNodeRecords, NodeProperties, NodePropertyWriter, NodeRecordsStorage, RecordBirth};
    use rdm_core::IncarnationId;

    fn properties() -> NodeProperties {
        NodeProperties { birth: RecordBirth { node_id: NodeId::mint(), incarnation_id: IncarnationId::mint() }, config: [("source".into(), "initial".into())].into(), tags: Default::default(), endpoint_id: None, transport_addr: None, listeners: vec![], app_types: None, held: Default::default(), applied: Default::default() }
    }

    // @feature: node-records
    /// A rollback releases its own block and late commands cannot release or recreate another one.
    #[tokio::test]
    async fn an_exact_storage_change_freezes_writes_until_move_or_rollback() {
        let store = Arc::new(MemoryNodeRecords::default());
        let writer = NodePropertyWriter::new(store.clone());
        let initial = properties();
        writer.bound(initial.clone(), |_| {}).await.unwrap();
        let moves = StorageMoves::new(initial.birth.node_id.clone(), Arc::new(|| "file".into()), Arc::new(|_| Box::pin(async { panic!("same-provider rollback must not open another provider") }))).with_properties(writer.clone());
        let (build, a, b) = (BuildId::mint(), ChangeId::mint(), ChangeId::mint());
        let adopted = ChangeId::mint();
        assert_eq!(moves.serve(&build, &adopted, "file").await, NodeCommandReply::StorageStepped { provider: "file".into() }, "a fresh birth already on the decided provider completes without an old-provider freeze");
        assert!(matches!(moves.serve(&build, &adopted, "other").await, NodeCommandReply::RejectedStaleChange { .. }), "the same completed change cannot choose another destination");
        assert_eq!(moves.block(&build, &a).await, NodeCommandReply::StorageBlocked);
        assert_eq!(moves.block(&build, &a).await, NodeCommandReply::StorageBlocked);
        let (w, seed) = (writer.clone(), initial.clone());
        let mut queued = Box::pin(w.update(seed, None, |r| r.config = [("source".into(), "queued".into())].into(), |_| {}));
        assert!(futures_util::poll!(&mut queued).is_pending());
        assert_eq!(store.properties(&initial.birth.node_id).await.unwrap(), Some(initial.clone()));
        assert_eq!(moves.serve(&build, &a, "file").await, NodeCommandReply::StorageStepped { provider: "file".into() });
        queued.await.unwrap();
        assert!(matches!(moves.block(&build, &a).await, NodeCommandReply::RejectedStaleChange { .. }), "a delayed block cannot refreeze a completed change");
        assert_eq!(moves.block(&build, &b).await, NodeCommandReply::StorageBlocked);
        assert!(matches!(moves.serve(&build, &a, "file").await, NodeCommandReply::RejectedStaleChange { held } if held == b));
        let (w, seed) = (writer.clone(), initial.clone());
        let mut next = Box::pin(w.update(seed, None, |r| { r.tags.insert("after-b".into()); }, |_| {}));
        assert!(futures_util::poll!(&mut next).is_pending());
        assert_eq!(moves.serve(&build, &b, "file").await, NodeCommandReply::StorageStepped { provider: "file".into() });
        let final_record = next.await.unwrap().record;
        assert_eq!(final_record.config["source"], "queued");
        assert_eq!(final_record.tags, ["after-b".into()].into());
    }

    // @feature: node-records
    /// An accepted move retains its transition through caller cancellation after the handle changes.
    #[tokio::test]
    async fn a_cancelled_storage_move_still_releases_its_property_writer() {
        let writer = NodePropertyWriter::new(Arc::new(MemoryNodeRecords::default()));
        let initial = properties();
        writer.bound(initial.clone(), |_| {}).await.unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let current = Arc::new(std::sync::Mutex::new("file".to_string()));
        let (provider, changed, e, r) = (current.clone(), current.clone(), entered.clone(), release.clone());
        let moves = StorageMoves::new(initial.birth.node_id.clone(), Arc::new(move || provider.lock().unwrap().clone()), Arc::new(move |next| {
            let (changed, entered, release) = (changed.clone(), e.clone(), r.clone());
            Box::pin(async move { *changed.lock().unwrap() = next; entered.notify_one(); release.notified().await; Ok(()) })
        })).with_properties(writer.clone());
        let (build, change) = (BuildId::mint(), ChangeId::mint());
        assert_eq!(moves.block(&build, &change).await, NodeCommandReply::StorageBlocked);
        let (m, b, c) = (moves.clone(), build.clone(), change.clone());
        let caller = tokio::spawn(async move { m.serve(&b, &c, "target").await });
        entered.notified().await;
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        let mut pending = Box::pin(writer.update(initial, None, |r| { r.tags.insert("after-move".into()); }, |_| {}));
        assert!(futures_util::poll!(&mut pending).is_pending());
        release.notify_one();
        let written = tokio::time::timeout(std::time::Duration::from_secs(5), pending).await.expect("the retained move releases the writer").unwrap();
        assert_eq!(written.record.tags, ["after-move".into()].into());
        assert_eq!(*current.lock().unwrap(), "target");
        assert!(matches!(moves.block(&build, &change).await, NodeCommandReply::RejectedStaleChange { .. }));
    }
}
