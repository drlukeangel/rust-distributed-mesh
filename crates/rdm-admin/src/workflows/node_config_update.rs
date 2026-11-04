//! Node config update workflow (`docs/architecture/lifecycles/node-config-update.md`).
//!
//! Updates configuration for a node without restarting it, writing updated rows and
//! notifying the target node.

pub use crate::node_config::{NodeConfigs, NodeConfigsSlot};

#[cfg(test)]
pub(crate) mod tests {
    use crate::model::{IncarnationId, NodeId, PathName};
    use crate::storage::{MemoryNodesStorage, NodeRecord, NodesStorage};

    pub(crate) struct RefusingNodesStorage(pub(crate) MemoryNodesStorage);

    #[async_trait::async_trait]
    impl NodesStorage for RefusingNodesStorage {
        async fn node(&self, node_id: &NodeId) -> Result<Option<crate::storage::NodeLine>, crate::storage_error::StorageError> {
            self.0.node(node_id).await
        }
        async fn nodes(&self, mesh: &str) -> Result<Vec<NodeRecord>, crate::storage_error::StorageError> {
            self.0.nodes(mesh).await
        }
        async fn put_node(&self, row: &NodeRecord) -> Result<(), crate::storage_error::StorageError> {
            Err(crate::storage_error::StorageError::Io {
                file: format!("nodes/records/{}", row.node_id),
                reason: "simulated disk refusal".into(),
            })
        }
        async fn records_at(&self, name: &PathName) -> Result<Vec<crate::storage::NodeRecord>, crate::storage_error::StorageError> {
            self.0.records_at(name).await
        }
    }

}
