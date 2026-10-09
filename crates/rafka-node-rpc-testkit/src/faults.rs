//! Testkit-only storage faults: a decorator over a node's `ConnectionsStorage` that refuses the
//! next N index or history writes by name, armed and released through the originate door
//! ([`crate::originate`]). The product's storage carries no fault code; a scenario proves the
//! writer's reconciliation (connections.md §10) against this decorator in a real process.

use async_trait::async_trait;
use rafka_mesh_entity::connections::{ConnectionIndex, NodeConnection};
use rafka_node_admin_core::record_store::StorageError;
use rafka_node_admin_core::storage::ConnectionsStorage;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

/// The armed fault: how many index and history writes are still to be refused, and how many
/// were refused since the fault was armed.
#[derive(Debug, Default)]
pub struct StorageFault {
    refuse_index: AtomicU32,
    refuse_history: AtomicU32,
    /// History appends let through before `refuse_history` starts refusing.
    pass_history: AtomicU32,
    refused: AtomicU32,
}

impl StorageFault {
    /// Refuse the next `refuse_index` index writes and `refuse_history` history appends, after
    /// letting `pass_history` history appends through.
    pub fn arm(&self, refuse_index: u32, refuse_history: u32, pass_history: u32) {
        self.pass_history.store(pass_history, Ordering::SeqCst);
        self.refuse_index.store(refuse_index, Ordering::SeqCst);
        self.refuse_history.store(refuse_history, Ordering::SeqCst);
        self.refused.store(0, Ordering::SeqCst);
    }

    /// Release the fault: nothing more is refused. Returns how many writes it refused.
    pub fn release(&self) -> u32 {
        self.refuse_index.store(0, Ordering::SeqCst);
        self.refuse_history.store(0, Ordering::SeqCst);
        self.pass_history.store(0, Ordering::SeqCst);
        self.refused.load(Ordering::SeqCst)
    }

    /// How many writes the fault has refused since it was armed.
    pub fn refused(&self) -> u32 {
        self.refused.load(Ordering::SeqCst)
    }

    /// The index writes and history appends still to be refused.
    pub fn armed(&self) -> (u32, u32) {
        (self.refuse_index.load(Ordering::SeqCst), self.refuse_history.load(Ordering::SeqCst))
    }

    /// Take one refusal from `counter`, if any is left.
    fn take(&self, counter: &AtomicU32) -> bool {
        let took = counter.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok();
        if took {
            self.refused.fetch_add(1, Ordering::SeqCst);
        }
        took
    }
}

/// `ConnectionsStorage` that refuses the writes its fault names, by name, and passes the rest
/// through.
pub(crate) struct FaultedConnectionsStorage {
    inner: Arc<dyn ConnectionsStorage>,
    fault: Arc<StorageFault>,
}

impl FaultedConnectionsStorage {
    pub fn new(inner: Arc<dyn ConnectionsStorage>, fault: Arc<StorageFault>) -> Self {
        Self { inner, fault }
    }
}

#[async_trait]
impl ConnectionsStorage for FaultedConnectionsStorage {
    async fn put_connection(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        if self.fault.take(&self.fault.refuse_index) {
            return Err(StorageError::Io { file: "connections index".into(), reason: format!("testkit fault armed: index write refused ({} refused so far)", self.fault.refused()) });
        }
        self.inner.put_connection(fact).await
    }
    async fn connections(&self) -> Result<Vec<NodeConnection>, StorageError> {
        self.inner.connections().await
    }
    async fn remove_connection(&self, index: &ConnectionIndex) -> Result<(), StorageError> {
        self.inner.remove_connection(index).await
    }
    async fn append_history(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        if self.fault.pass_history.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok() {
            return self.inner.append_history(fact).await;
        }
        if self.fault.take(&self.fault.refuse_history) {
            return Err(StorageError::Io { file: "connections history".into(), reason: format!("testkit fault armed: history append refused ({} refused so far)", self.fault.refused()) });
        }
        self.inner.append_history(fact).await
    }
    async fn history(&self) -> Result<Vec<NodeConnection>, StorageError> {
        self.inner.history().await
    }
}
