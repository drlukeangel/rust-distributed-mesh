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
//! `on_fabric_ready` also does the replica's own day-0 work, as Rafka's does its system creations: it
//! produces one record to the `system` topic through a gateway ([`rshape_consumer::replica`]), under
//! an operation id of the ready Build, so a notice repeated for one Build appends nothing twice.
use async_trait::async_trait;
use rdm_admin::model::{IncarnationId, ProviderKind};
use rdm_admin::storage::NodeRecord;
use rdm_admin::storage_backend::{StorageBackend, StorageBoundaries, StorageScope};
use rdm_admin::storage_error::StorageError;
use rdm_admin::wiring::{BuildEndedCtx, BuildEndedNotice, BuildStartedCtx, BuildStartedNotice, FabricHooks, FabricReadyCtx, FabricReadyNotice, MeshReadyCtx, MeshReadyNotice, Wiring};
use rshape_consumer::replica::{Records, RecordsReply, RecordsRequest};
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

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

/// What the started admin hands over (`rdm_admin::entry::run_on_started`): its Node RPC client, its
/// topology view and its mesh, where `on_fabric_ready` finds a gateway and calls it.
type Own = Arc<OnceLock<(Arc<rdm_transport::NodeRpcClient>, rdm_transport::topology::TopologyView, String)>>;

/// The day-0 work of `ready`: one record to the `system` topic through a Ready gateway of this admin's
/// mesh, asked again until a gateway acknowledges it. The operation id is the ready Build's, so a
/// repeat of the notice for one Build is the same record.
async fn day_zero(own: Own, fabric_id: String, build_id: String) {
    let operation = format!("fabric-ready:{build_id}");
    let request = RecordsRequest::Produce { operation: operation.clone(), topic: "system".into(), record: format!("fabric {fabric_id} ready at Build {build_id}") };
    loop {
        if let Some((client, view, mesh)) = own.get() {
            let filter = rdm_core::filter::Filter::new().eq("mesh", [mesh.clone()]).eq("kind", ["gateway"]).eq("status", ["ready"]);
            if let Some(gateway) = view.nodes_get(&filter).ok().and_then(|g| g.into_iter().next()) {
                let (outcome, _) = client.call::<Records>(&rdm_transport::NodeTarget::ExactNode(gateway.node.node_id.clone()), &request, &rdm_transport::CallOptions::default()).await;
                if let rdm_core::outcome::RpcOutcome::Reply(reply) = outcome {
                    if let RecordsReply::Produced { offset, .. } = reply.value() {
                        return tracing::info!(target: "rshape", %operation, gateway = %gateway.node.name, offset, "on_fabric_ready: the day-0 record is produced through the gateway");
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// The Fabric notices this admin registers (fabric-node-hooks.md): each is fire and forget and writes one
/// log line naming what it was told. `on_mesh_ready` is called by a mesh-primary once its mesh is ready;
/// `on_fabric_ready`, `on_build_started` and `on_build_ended` by the fabric-primary.
fn notices(own: Own) -> FabricHooks {
    let on_mesh_ready: MeshReadyNotice = Arc::new(|ready: MeshReadyCtx| {
        Box::pin(async move { tracing::info!(target: "rshape", fabric_id = %ready.fabric_id, mesh = %ready.mesh, mesh_id = %ready.mesh_id, "on_mesh_ready: the mesh is ready") })
    });
    let on_fabric_ready: FabricReadyNotice = Arc::new(move |ready: FabricReadyCtx| {
        let own = own.clone();
        Box::pin(async move {
            tracing::info!(target: "rshape", fabric_id = %ready.fabric_id, build_id = %ready.build_id, attempt = ready.attempt, "on_fabric_ready: the fabric is ready");
            // The work runs on its own: a notice gates nothing.
            tokio::spawn(day_zero(own, ready.fabric_id.to_string(), ready.build_id.clone()));
        })
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
    let own: Own = Arc::default();
    let started = own.clone();
    rdm_admin::entry::run_on_started(
        SERVICE,
        // Its backend wraps RDM's file backend, registered under that backend's name (storage.md §2).
        |cfg| rdm_admin::storage_backend::TopologyStorageProviders::new().register(rdm_core::launch::TOPOLOGY_STORAGE_FILE, Arc::new(ConsumerBackend(rdm_admin::storage_backend::file_backend(cfg.store_dir.clone())))),
        |cfg| Wiring { fabric_hooks: notices(own), certs: rdm_admin::entry::material_or_exit(SERVICE, cfg), ..Wiring::default() },
        // The started admin hands its client and view over to the day-0 work.
        move |running| {
            let mesh = running.digest.lock().unwrap().node.name.mesh.clone();
            let _ = started.set((running.node_rpc.client.clone(), running.topology_view.clone(), mesh));
        },
    )
    .await
}
