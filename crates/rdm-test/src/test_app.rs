//! The testkit's application: it takes the fabric's notices the way an embedding does.
//!
//! `on_mesh_ready` and `on_fabric_ready` (Q175), `on_build_started` and `on_build_ended` (Q256) each only record a span. A scenario that holds a fabric
//! short of ready holds a node's `pre_node_ready` (`crate::write_pre_node_ready_knob`), as an
//! application holds its own node.
//!
//! It declares one secret (Q150), [`DECLARED_SECRET`], set on the broker and the gateway only: a
//! scenario sets its value in the bootstrap admin's environment and reads which processes hold it.
//!
//! It declares three listeners (Q151), [`LISTENERS`], on the gateway: a gateway binds them from its
//! launch, reports them at its join and its row publishes them.
//!
//! It registers a second topology storage provider (storage.md §2), [`TOPOLOGY_STORAGE_TARGET`], in
//! every executable of the testkit: RDM's file backend at a place of its own beside the fabric's store
//! dir ([`topology_storage_dir`]), a separate durable namespace standing in for an application's
//! backend; a third, [`TOPOLOGY_STORAGE_REFUSING`], the same backend at a place of its own whose
//! store lines are refused by name once a scenario writes its knob ([`refuse_list_knob`]: an
//! application's backend that answers its list with a named error), its claims answered; and a fourth,
//! [`TOPOLOGY_STORAGE_MEMORY`], RDM's memory backend, whose boundaries open and whose lines do not.

use rdm_admin::wiring::{BuildEndedCtx, BuildStartedCtx, FabricHooks, FabricReadyCtx, MeshReadyCtx};
use std::sync::Arc;
use tracing::Instrument;

/// The testkit application's declared secret: set in the process environment of [`DECLARED_KINDS`]
/// only.
pub const DECLARED_SECRET: &str = "TESTKIT_DECLARED_SECRET";

/// The kinds [`DECLARED_SECRET`] is set on.
pub const DECLARED_KINDS: [rdm_core::NodeKind; 2] = [rdm_core::NodeKind::Broker, rdm_core::NodeKind::Gateway];

/// The kind the testkit application declares listeners on (Q151).
pub const LISTENER_KIND: rdm_core::NodeKind = rdm_core::NodeKind::Gateway;

/// The listeners the testkit application declares on [`LISTENER_KIND`], as Rafka's gateway names its
/// own.
pub const LISTENERS: [&str; 3] = ["kafka", "rest", "proxy"];

/// The name the testkit registers its second topology storage provider under (storage.md §2: a name
/// is bound to its place at registration).
pub const TOPOLOGY_STORAGE_TARGET: &str = "test-target";

/// Where [`TOPOLOGY_STORAGE_TARGET`] keeps its stores for a fabric whose store dir
/// (`RDM_STORE_DIR`) is `store_dir`: a directory beside it, never inside it, so the file provider's
/// store dir holds only its own roots.
pub fn topology_storage_dir(store_dir: &std::path::Path) -> std::path::PathBuf {
    provider_dir(store_dir, TOPOLOGY_STORAGE_TARGET)
}

/// The name the testkit registers its third topology storage provider under: RDM's file backend at a
/// place of its own ([`provider_dir`]) that, while its knob stands ([`refuse_list_knob`]), answers a
/// read of its store lines with a named refusal
/// ([`StorageError::Refused`](rdm_node::storage_error::StorageError::Refused)), and its claims as the
/// file backend does.
pub const TOPOLOGY_STORAGE_REFUSING: &str = "test-refusing";

/// The name the testkit registers RDM's memory backend under: its boundaries open, and it answers no
/// lines to copy.
pub const TOPOLOGY_STORAGE_MEMORY: &str = "test-memory";

/// The knob of [`TOPOLOGY_STORAGE_REFUSING`] for a fabric whose store dir is `store_dir`: while this
/// file stands, every read of its store lines is refused by name.
pub fn refuse_list_knob(store_dir: &std::path::Path) -> std::path::PathBuf {
    provider_dir(store_dir, TOPOLOGY_STORAGE_REFUSING).join("refuse-list")
}

/// Where the testkit provider `provider` keeps its stores for a fabric whose store dir is `store_dir`:
/// a directory beside it, never inside it.
pub fn provider_dir(store_dir: &std::path::Path, provider: &str) -> std::path::PathBuf {
    let name = store_dir.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    store_dir.with_file_name(format!("{name}-{provider}"))
}

/// A backend whose copy lines refuse every store's list by name while `knob` stands; everything else
/// is `inner`'s.
#[cfg(feature = "file-backend")]
struct RefusesItsLines(Arc<dyn rdm_admin::storage_backend::StorageBackend>, std::path::PathBuf);

#[cfg(feature = "file-backend")]
#[async_trait::async_trait]
impl rdm_admin::storage_backend::StorageBackend for RefusesItsLines {
    fn qualify(&self, provider: rdm_admin::model::ProviderKind) -> Result<(), String> {
        self.0.qualify(provider)
    }
    async fn own_row(&self, data_dir: &std::path::Path, writer: &rdm_admin::model::IncarnationId) -> Result<Option<rdm_admin::storage::NodeRecord>, rdm_node::storage_error::StorageError> {
        self.0.own_row(data_dir, writer).await
    }
    async fn put_own_row(&self, data_dir: &std::path::Path, writer: &rdm_admin::model::IncarnationId, record: &rdm_admin::storage::NodeRecord) -> Result<(), rdm_node::storage_error::StorageError> {
        self.0.put_own_row(data_dir, writer, record).await
    }
    async fn open(&self, scope: &rdm_admin::storage_backend::StorageScope) -> Result<rdm_admin::storage_backend::StorageBoundaries, String> {
        self.0.open(scope).await
    }
    async fn delete_fabric(&self, data_dir: &std::path::Path, fabric_id: &rdm_admin::model::FabricId) -> Result<(), String> {
        self.0.delete_fabric(data_dir, fabric_id).await
    }
    async fn lines(&self, provider: &str, fabric_id: &rdm_admin::model::FabricId) -> Result<Arc<dyn rdm_node::storage_copy::ProviderLines>, rdm_node::storage_error::StorageError> {
        Ok(Arc::new(ListRefused { inner: self.0.lines(provider, fabric_id).await?, provider: provider.into(), knob: self.1.clone() }))
    }
}

/// A provider's lines whose every store's list is refused by name while `knob` stands.
#[cfg(feature = "file-backend")]
struct ListRefused {
    inner: Arc<dyn rdm_node::storage_copy::ProviderLines>,
    provider: String,
    knob: std::path::PathBuf,
}

#[cfg(feature = "file-backend")]
#[async_trait::async_trait]
impl rdm_node::storage_copy::ProviderLines for ListRefused {
    async fn lines(&self, store: rdm_node::storage_copy::Store) -> Result<Vec<rdm_node::store_log::Line>, rdm_node::storage_error::StorageError> {
        if !self.knob.exists() {
            return self.inner.lines(store).await;
        }
        Err(rdm_node::storage_error::StorageError::Refused { file: format!("the {store} store of `{}`", self.provider), reason: "this provider lists no lines".into() })
    }
    async fn append_line(&self, store: rdm_node::storage_copy::Store, line: &rdm_node::store_log::Line) -> Result<(), rdm_node::storage_error::StorageError> {
        self.inner.append_line(store, line).await
    }
    async fn claims(&self) -> Result<Vec<rdm_node::storage_copy::Claim>, rdm_node::storage_error::StorageError> {
        self.inner.claims().await
    }
    async fn insert_claim(&self, claim: &rdm_node::storage_copy::Claim) -> Result<rdm_node::storage_copy::ClaimTaken, rdm_node::storage_error::StorageError> {
        self.inner.insert_claim(claim).await
    }
    async fn remove_claim(&self, claim: &rdm_node::storage_copy::Claim) -> Result<bool, rdm_node::storage_error::StorageError> {
        self.inner.remove_claim(claim).await
    }
    async fn claim(&self, kind: rdm_node::storage_copy::ClaimKind, name: &str) -> Result<Option<Vec<u8>>, rdm_node::storage_error::StorageError> {
        self.inner.claim(kind, name).await
    }
}

/// The topology storage providers the testkit's node-admin registers (storage.md §2): RDM's `file`
/// backend on the fabric's store dir, [`TOPOLOGY_STORAGE_TARGET`] on its own place beside it
/// ([`topology_storage_dir`]), [`TOPOLOGY_STORAGE_REFUSING`] on its own ([`provider_dir`]), each made
/// here when absent, and [`TOPOLOGY_STORAGE_MEMORY`].
#[cfg(feature = "file-backend")]
pub fn topology_storage_providers(cfg: &rdm_admin::admin::AdminConfig) -> rdm_admin::storage_backend::TopologyStorageProviders {
    let target = topology_storage_dir(&cfg.store_dir);
    let refusing = provider_dir(&cfg.store_dir, TOPOLOGY_STORAGE_REFUSING);
    let _ = std::fs::create_dir_all(&target);
    let _ = std::fs::create_dir_all(&refusing);
    rdm_admin::storage_backend::TopologyStorageProviders::file(cfg.store_dir.clone())
        .register(TOPOLOGY_STORAGE_TARGET, rdm_admin::storage_backend::file_backend(target))
        .register(TOPOLOGY_STORAGE_REFUSING, Arc::new(RefusesItsLines(rdm_admin::storage_backend::file_backend(refusing), refuse_list_knob(&cfg.store_dir))))
        .register(TOPOLOGY_STORAGE_MEMORY, Arc::new(rdm_admin::storage_backend::MemoryBackend))
}

/// The testkit application's listener declarations, as its wiring registers them.
pub fn listeners() -> Vec<rdm_core::launch::DeclaredListeners> {
    vec![rdm_core::launch::DeclaredListeners { kind: LISTENER_KIND, names: LISTENERS.iter().map(|n| n.to_string()).collect() }]
}

/// The testkit application's declarations, as its wiring registers them.
pub fn declared() -> Vec<rdm_core::launch::DeclaredSecret> {
    vec![rdm_core::launch::DeclaredSecret { name: DECLARED_SECRET.into(), kinds: DECLARED_KINDS.to_vec() }]
}

/// The hooks of this application: each notice records a span.
pub fn hooks() -> FabricHooks {
    let on_mesh_ready: rdm_admin::wiring::MeshReadyNotice = Arc::new(|ready: MeshReadyCtx| {
        let span = tracing::info_span!("rdm.testkit.app.update.via-on-mesh-ready", fabric_id = %ready.fabric_id, mesh = %ready.mesh, mesh_id = %ready.mesh_id, "otel.kind" = "internal");
        Box::pin(async move { tracing::info!("the application was told the mesh is ready") }.instrument(span))
    });
    let on_fabric_ready: rdm_admin::wiring::FabricReadyNotice = Arc::new(|ready: FabricReadyCtx| {
        let span = tracing::info_span!("rdm.testkit.app.update.via-on-fabric-ready", fabric_id = %ready.fabric_id, build_id = %ready.build_id, attempt = ready.attempt, "otel.kind" = "internal");
        Box::pin(async move { tracing::info!("the application opens client traffic") }.instrument(span))
    });
    let on_build_started: rdm_admin::wiring::BuildStartedNotice = Arc::new(|started: BuildStartedCtx| {
        let span = tracing::info_span!("rdm.testkit.app.update.via-on-build-started", fabric_id = %started.fabric_id, build_id = %started.build_id, attempt = started.attempt, reason = started.reason.as_str(), "otel.kind" = "internal");
        Box::pin(async move { tracing::info!("the application was told the Build started") }.instrument(span))
    });
    let on_build_ended: rdm_admin::wiring::BuildEndedNotice = Arc::new(|ended: BuildEndedCtx| {
        let span = tracing::info_span!("rdm.testkit.app.update.via-on-build-ended", fabric_id = %ended.fabric_id, build_id = %ended.build_id, attempt = ended.attempt, reason = ended.reason.as_str(), state = ended.state.as_str(), "otel.kind" = "internal");
        Box::pin(async move { tracing::info!("the application was told the Build ended") }.instrument(span))
    });
    FabricHooks { on_mesh_ready: Some(on_mesh_ready), on_fabric_ready: Some(on_fabric_ready), on_build_started: Some(on_build_started), on_build_ended: Some(on_build_ended) }
}
