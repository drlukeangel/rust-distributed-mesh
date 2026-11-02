//! Node config update workflow (`docs/architecture/lifecycles/node-config-update.md`).
//!
//! Updates configuration for a node without restarting it, writing updated rows and
//! notifying the target node.

pub use crate::node_config::{NodeConfigs, NodeConfigsSlot};

#[cfg(test)]
pub(crate) mod tests {
    use crate::model::{IncarnationId, NodeId, PathName};
    use crate::storage::{MemoryNodesStorage, NodeRow, NodesStorage};

    pub(crate) struct RefusingNodesStorage(pub(crate) MemoryNodesStorage);

    #[async_trait::async_trait]
    impl NodesStorage for RefusingNodesStorage {
        async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_runtime(row).await
        }
        async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.0.runtimes().await
        }
        async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, crate::storage_error::StorageError> {
            self.0.runtime(node_id, incarnation).await
        }
        async fn node(&self, node_id: &NodeId) -> Result<Option<NodeRow>, crate::storage_error::StorageError> {
            self.0.node(node_id).await
        }
        async fn nodes(&self, mesh: &str) -> Result<Vec<NodeRow>, crate::storage_error::StorageError> {
            self.0.nodes(mesh).await
        }
        async fn put_properties(&self, record: &rdm_node::node_records::NodeProperties) -> Result<(), crate::storage_error::StorageError> {
            self.0.put_properties(record).await
        }
        async fn properties(&self, node_id: &crate::model::NodeId) -> Result<Option<rdm_node::node_records::NodeProperties>, crate::storage_error::StorageError> {
            self.0.properties(node_id).await
        }
        async fn put_node(&self, row: &NodeRow) -> Result<(), crate::storage_error::StorageError> {
            Err(crate::storage_error::StorageError::Io {
                file: format!("nodes/records/{}", row.node_id),
                reason: "simulated disk refusal".into(),
            })
        }
        async fn name_claim(&self, name: &PathName) -> Result<Option<NodeId>, crate::storage_error::StorageError> {
            self.0.name_claim(name).await
        }
        async fn claim_node_name(&self, name: &PathName, node_id: &NodeId) -> Result<Result<(), crate::storage::NameHeld>, crate::storage_error::StorageError> {
            self.0.claim_node_name(name, node_id).await
        }
        async fn release_node_name(&self, name: &PathName, node_id: &NodeId) -> Result<(), crate::storage_error::StorageError> {
            self.0.release_node_name(name, node_id).await
        }
    }

}
