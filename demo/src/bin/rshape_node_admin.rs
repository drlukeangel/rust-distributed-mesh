//! `rshape-node-admin`: the R-shape consumer's node-admin entry point, composed from RDM's public
//! node-admin seam (`rdm_admin::entry::run_on`). All configuration is the environment.
//!
//! It is the primer for the seam a product's node-admin plugs in besides its hooks and channels: its
//! own storage backend, handed over through `run_on`. [`ConsumerBackend`] is asked for every boundary
//! the admin opens (its own row, then the six boundaries), here by delegating to RDM's file backend; a
//! product's own backend (Rafka's topic-backed one) takes the file backend's place. A product's work on
//! a node's way into traffic enters only through the Fabric and node hooks (`pre_node_ready`,
//! `on_node_ready`, `on_mesh_ready`, `on_fabric_ready`; build.md §9).
use async_trait::async_trait;
use rdm_admin::model::{IncarnationId, ProviderKind};
use rdm_admin::storage::NodeRecord;
use rdm_admin::storage_backend::{StorageBackend, StorageBoundaries, StorageScope};
use rdm_admin::storage_error::StorageError;
use rdm_admin::wiring::{FabricHooks, Wiring};
use std::path::Path;
use std::sync::Arc;

const SERVICE: &str = "rshape-node-admin";

/// The consumer's storage backend: every boundary RDM's node-admin opens is asked of it.
struct ConsumerBackend(Arc<dyn StorageBackend>);

#[async_trait]
impl StorageBackend for ConsumerBackend {
    fn qualify(&self, provider: ProviderKind) -> Result<(), String> {
        self.0.qualify(provider)
    }

    async fn own_row(&self, data_dir: &Path, writer: &IncarnationId) -> Result<Option<NodeRecord>, StorageError> {
        self.0.own_row(data_dir, writer).await
    }

    async fn put_own_row(&self, data_dir: &Path, writer: &IncarnationId, record: &NodeRecord) -> Result<(), StorageError> {
        self.0.put_own_row(data_dir, writer, record).await
    }

    async fn open(&self, scope: &StorageScope) -> Result<StorageBoundaries, String> {
        // Where the boundaries write, as the boot's storage span records it: this backend, over the file
        // backend's own answer.
        self.0.open(scope).await.map(|b| StorageBoundaries { writes_to: format!("rshape-backend:{}", b.writes_to), ..b })
    }
}

#[tokio::main]
async fn main() {
    rdm_admin::entry::run_on(
        SERVICE,
        // Its backend wraps RDM's file backend, registered under that backend's name (storage.md §2).
        |cfg| rdm_admin::storage_backend::TopologyStorageProviders::new().register(rdm_core::launch::TOPOLOGY_STORAGE_FILE, Arc::new(ConsumerBackend(rdm_admin::storage_backend::file_backend(cfg.store_dir.clone())))),
        |cfg| Wiring { fabric_hooks: FabricHooks::no_app_work(), certs: rdm_admin::entry::material_or_exit(SERVICE, cfg), ..Wiring::default() },
    )
    .await
}
