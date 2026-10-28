//! `rshape-node-admin`: the R-shape consumer's node-admin entry point, composed from RDM's public
//! node-admin seam (`rdm_admin::entry::run_on`). All configuration is the environment.
//!
//! It is the primer for the seam a product's node-admin plugs in besides its hooks and channels: its
//! own storage backend, handed over through `run_on`. [`ConsumerBackend`] is asked for every boundary
//! the admin opens (its own row, then the six boundaries), here by delegating to RDM's file backend; a
//! product's own backend (Rafka's topic-backed one) takes the file backend's place. A product's work on
//! a node's way into traffic enters only through the Fabric and node hooks (`pre_node_ready`,
//! `on_node_ready`, `on_mesh_ready`, `on_fabric_ready`, `on_build_started`, `on_build_ended`; build.md §9).
//! This admin registers the four Fabric notices ([`notices`]), each one log line (target `rshape`) with
//! what it was told: the notices gate nothing, and a product puts its own work where the line is.
use async_trait::async_trait;
use rdm_admin::model::{IncarnationId, ProviderKind};
use rdm_admin::storage::NodeRecord;
use rdm_admin::storage_backend::{StorageBackend, StorageBoundaries, StorageScope};
use rdm_admin::storage_error::StorageError;
use rdm_admin::wiring::{BuildEndedCtx, BuildEndedNotice, BuildStartedCtx, BuildStartedNotice, FabricHooks, FabricReadyCtx, FabricReadyNotice, MeshReadyCtx, MeshReadyNotice, Wiring};
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

/// The Fabric notices this admin registers (fabric-node-hooks.md): each is fire and forget and writes one
/// log line naming what it was told. `on_mesh_ready` is called by a mesh-primary once its mesh is ready;
/// `on_fabric_ready`, `on_build_started` and `on_build_ended` by the fabric-primary.
fn notices() -> FabricHooks {
    let on_mesh_ready: MeshReadyNotice = Arc::new(|ready: MeshReadyCtx| {
        Box::pin(async move { tracing::info!(target: "rshape", fabric_id = %ready.fabric_id, mesh = %ready.mesh, mesh_id = %ready.mesh_id, "on_mesh_ready: the mesh is ready") })
    });
    let on_fabric_ready: FabricReadyNotice = Arc::new(|ready: FabricReadyCtx| {
        Box::pin(async move { tracing::info!(target: "rshape", fabric_id = %ready.fabric_id, build_id = %ready.build_id, attempt = ready.attempt, "on_fabric_ready: the fabric is ready") })
    });
    let on_build_started: BuildStartedNotice = Arc::new(|started: BuildStartedCtx| {
        Box::pin(async move {
            tracing::info!(target: "rshape", fabric_id = %started.fabric_id, build_id = %started.build_id, attempt = started.attempt, reason = started.reason.as_str(), "on_build_started: a Build started")
        })
    });
    let on_build_ended: BuildEndedNotice = Arc::new(|ended: BuildEndedCtx| {
        Box::pin(async move {
            match &ended.state {
                rdm_admin::wiring::BuildEndedState::Complete => {
                    tracing::info!(target: "rshape", fabric_id = %ended.fabric_id, build_id = %ended.build_id, attempt = ended.attempt, reason = ended.reason.as_str(), state = ended.state.as_str(), "on_build_ended: a Build ended")
                }
                rdm_admin::wiring::BuildEndedState::Failed { step, reason, code } => {
                    tracing::warn!(target: "rshape", fabric_id = %ended.fabric_id, build_id = %ended.build_id, attempt = ended.attempt, reason = ended.reason.as_str(), state = ended.state.as_str(), step = %step, failure = %reason, code = ?code, "on_build_ended: a Build ended")
                }
            }
        })
    });
    FabricHooks { on_mesh_ready: Some(on_mesh_ready), on_fabric_ready: Some(on_fabric_ready), on_build_started: Some(on_build_started), on_build_ended: Some(on_build_ended) }
}

#[tokio::main]
async fn main() {
    rdm_admin::entry::run_on(
        SERVICE,
        // Its backend wraps RDM's file backend, registered under that backend's name (storage.md §2).
        |cfg| rdm_admin::storage_backend::TopologyStorageProviders::new().register(rdm_core::launch::TOPOLOGY_STORAGE_FILE, Arc::new(ConsumerBackend(rdm_admin::storage_backend::file_backend(cfg.store_dir.clone())))),
        |cfg| Wiring { fabric_hooks: notices(), certs: rdm_admin::entry::material_or_exit(SERVICE, cfg), ..Wiring::default() },
    )
    .await
}
