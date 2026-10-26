//! The storage backend a node runs on: its own connections rows (`connections.storage`) and the
//! open-traffic action it runs before it checks in. A node-admin's backend opens the admin's six boundaries (`rdm_admin::storage_backend`);
//! a node opens this one at its boot. RDM standalone's is [`FileNodeBackend`] (the `file-backend`
//! feature, the default), the rows as files on the fabric's store root. [`MemoryNodeBackend`] holds
//! one process's rows.

use crate::fabric_states::StateActions;
use crate::storage::ConnectionsStorage;
use async_trait::async_trait;
use rdm_core::launch::Launch;
use std::sync::Arc;

/// A node's storage, opened by its backend.
pub struct NodeStorage {
    /// The node's own connections rows.
    pub connections: Arc<dyn ConnectionsStorage>,
    /// The node's open-traffic action.
    pub state_actions: Arc<dyn StateActions>,
    /// Where the rows are written now, as the boot's span records it.
    pub writes_to: String,
}

/// The backend a node's storage is opened from.
#[async_trait]
pub trait NodeStorageBackend: Send + Sync {
    /// Opens the storage of the node `launch` names.
    async fn open(&self, launch: &Launch) -> Result<NodeStorage, String>;
}

/// RDM standalone's backend: the node's connection facts, its lines of the fabric's store root's
/// `connections.jsonl` (R14, R-SR1), written there from its open.
#[cfg(feature = "file-backend")]
#[derive(Debug, Default, Clone, Copy)]
pub struct FileNodeBackend;

#[cfg(feature = "file-backend")]
#[async_trait]
impl NodeStorageBackend for FileNodeBackend {
    async fn open(&self, launch: &Launch) -> Result<NodeStorage, String> {
        let root = crate::store_root::StoreRoot::open(&launch.store_dir, &launch.fabric_id).map_err(|e| format!("refusing to start: {e}"))?;
        let connections = crate::storage::FileConnectionsStorage::open(&root, &launch.name, &crate::store_log::LogWriter::new(&launch.node_id, &launch.incarnation)).await.map_err(|e| format!("connections storage: {e}"))?;
        Ok(NodeStorage {
            connections: Arc::new(connections),
            state_actions: Arc::new(crate::fabric_states::NoAction),
            writes_to: "store-root".into(),
        })
    }
}

#[cfg(feature = "file-backend")]
/// RDM standalone's node storage backend: the one a node's `start` variants and `run` construct.
pub fn file_node_backend() -> Arc<dyn NodeStorageBackend> {
    Arc::new(FileNodeBackend)
}

/// One process's rows: the node's connections held in memory, gone with its process.
#[derive(Debug, Default, Clone, Copy)]
pub struct MemoryNodeBackend;

#[async_trait]
impl NodeStorageBackend for MemoryNodeBackend {
    async fn open(&self, _launch: &Launch) -> Result<NodeStorage, String> {
        Ok(NodeStorage { connections: Arc::new(crate::storage::MemoryConnectionsStorage::default()), state_actions: Arc::new(crate::fabric_states::NoAction), writes_to: "memory".into() })
    }
}
