//! The storage backend a node-admin runs on (build.md: "production backends for the imported storage
//! interfaces"). It opens the six storage boundaries, `fabric.storage` ([`FabricStorage`]),
//! `meshes` ([`MeshStorage`]), `nodes.storage` ([`NodesStorage`]), `status.storage`
//! ([`StatusStorage`]), `connections.storage` ([`ConnectionsStorage`]) and the Build journal
//! ([`LocalBuildLog`]), and every durable read and write of the admin goes through them. The admin's
//! boot takes its backend beside its `Wiring` (`admin::start_on`); RDM standalone's is
//! [`FileBackend`] (the `file-backend` feature, the default), the boundaries as files on the fabric's
//! store root and the admin's data dir. [`MemoryBackend`] holds one process's rows.

use crate::build_state::LocalBuildLog;
use crate::fabric_handover::CutOffWrites;
use crate::fabric_rounds::RoundActions;
use crate::fabric_storage::FabricStorage;
use crate::model::{FabricId, IncarnationId, PathName, ProviderKind};
use crate::status_storage::StatusStorage;
use crate::storage::{ConnectionsStorage, MeshStorage, NodeRecord, NodesStorage};
use crate::storage_error::StorageError;
use async_trait::async_trait;
use futures_util::future::BoxFuture;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Where a node-admin's storage opens, decided by its boot before any boundary opens.
#[derive(Debug, Clone)]
pub struct StorageScope {
    /// The admin's own data dir.
    pub data_dir: PathBuf,
    /// The fabric's shared store dir (R14).
    pub store_dir: PathBuf,
    /// The Fabric the admin belongs to.
    pub fabric_id: FabricId,
    /// The mesh the admin belongs to: its Build lane, its status rows and its members' rows.
    pub own_mesh: String,
    /// The incarnation this process writes as.
    pub writer: IncarnationId,
}

/// A node's own connections rows, opened once its `path.name` is decided.
pub type OpenConnections = Arc<dyn Fn(PathName) -> BoxFuture<'static, Result<Arc<dyn ConnectionsStorage>, StorageError>> + Send + Sync>;

/// The storage a node-admin runs on, opened by its backend: the six boundaries, and what the
/// backend does at a commit-state round and at a seat transfer.
pub struct StorageBoundaries {
    /// `fabric.storage`: the Fabric record, its name's claim and the seat moves.
    pub fabric: Arc<dyn FabricStorage>,
    /// The Mesh rows and each mesh's in-flight row.
    pub meshes: Arc<dyn MeshStorage>,
    /// `nodes.storage`: the admin's own row, the runtimes it started, its mesh's Node rows and contacts.
    pub nodes: Arc<dyn NodesStorage>,
    /// `status.storage`: the status rows of the admin's mesh.
    pub status: Arc<dyn StatusStorage>,
    /// `connections.storage`: the admin's own connections rows, by its `path.name`.
    pub connections: OpenConnections,
    /// The Build journal: the Build facts, and each attempt's context.
    pub builds: Arc<dyn LocalBuildLog>,
    /// The node's commit-state and open-traffic actions: the backend's own, then the embedding's.
    pub round_actions: Arc<dyn RoundActions>,
    /// Applies a seat transfer's write cut-off to the stores the moving seats write (R14 item 4).
    pub cut_off_writes: CutOffWrites,
    /// Where the boundaries write now, as the boot's storage span records it.
    pub writes_to: String,
}

/// The backend a node-admin's boundaries are opened from.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Refuses, by name, a deployment provider whose runtimes this backend cannot serve the
    /// fabric's rows to (asked by the bootstrap admin before anything opens).
    fn qualify(&self, provider: ProviderKind) -> Result<(), String>;
    /// The admin's own row as its data dir holds it, read before any boundary opens: an inherited
    /// row names the Fabric and the mesh the boundaries open on (R-FI1). `None` for a new admin.
    async fn own_row(&self, data_dir: &Path, writer: &IncarnationId) -> Result<Option<NodeRecord>, StorageError>;
    /// Opens the six boundaries of `scope`; `embedding` is the embedding's own round actions, run
    /// after the backend's.
    async fn open(&self, scope: &StorageScope, embedding: Option<Arc<dyn RoundActions>>) -> Result<StorageBoundaries, String>;
}

/// RDM standalone's backend: the boundaries as files, the shared ones on the fabric's store root
/// (`<store_dir>/<fabric_id>/`, R14), the admin's own in its data dir.
#[cfg(feature = "file-backend")]
#[derive(Debug, Default, Clone, Copy)]
pub struct FileBackend;

#[cfg(feature = "file-backend")]
#[async_trait]
impl StorageBackend for FileBackend {
    fn qualify(&self, provider: ProviderKind) -> Result<(), String> {
        crate::store_root::qualify(provider)
    }

    async fn own_row(&self, data_dir: &Path, writer: &IncarnationId) -> Result<Option<NodeRecord>, StorageError> {
        crate::storage::FileNodesStorage::own_in(data_dir, writer).await
    }

    async fn open(&self, scope: &StorageScope, embedding: Option<Arc<dyn RoundActions>>) -> Result<StorageBoundaries, String> {
        let storage_err = |e: StorageError| e.to_string();
        let root = crate::store_root::StoreRoot::open(&scope.store_dir, &scope.fabric_id).map_err(|e| format!("refusing to start: {e}"))?;
        // Before the fabric's first state-commit every row this admin writes is held in its
        // scratchpad; its commit-state action publishes them (states.md, Rounds).
        let writes_to = root.writes_to().to_string();
        let round_actions: Arc<dyn RoundActions> = Arc::new(crate::fabric_rounds::StorePublication::new(root.scratchpad().cloned(), embedding));
        let fabric = Arc::new(crate::fabric_storage::FileFabricStorage::open(&root, &scope.writer).await.map_err(storage_err)?);
        let meshes = Arc::new(crate::storage::FileMeshStorage::open(&root, &scope.writer).await.map_err(storage_err)?);
        let nodes = Arc::new(crate::storage::FileNodesStorage::open(&scope.data_dir, &root, &scope.own_mesh, &scope.writer).await.map_err(storage_err)?);
        let builds = Arc::new(crate::build_state::FileJournal::open_on_root(&root, &scope.data_dir, &scope.own_mesh, &scope.writer).await.map_err(|e| e.to_string())?);
        let status = Arc::new(crate::status_storage::FileStatusStorage::open(&root, &scope.own_mesh, &scope.writer).await.map_err(storage_err)?);
        let cut_off_writes = Arc::new(crate::fabric_handover::SeatStores { mesh: meshes.clone(), nodes: nodes.clone(), fabric: fabric.clone(), status: status.clone() }).cut_off_writes();
        let connections: OpenConnections = {
            let (root, writer) = (root.clone(), scope.writer.clone());
            Arc::new(move |name: PathName| {
                let (root, writer) = (root.clone(), writer.clone());
                Box::pin(async move { Ok(Arc::new(crate::storage::FileConnectionsStorage::open(&root, &name, &writer).await?) as Arc<dyn ConnectionsStorage>) })
            })
        };
        Ok(StorageBoundaries { fabric, meshes, nodes, status, connections, builds, round_actions, cut_off_writes, writes_to })
    }
}

/// One process's rows: every boundary held in memory, written by this admin alone and gone with its
/// process. An admin on it is never started on an inherited root, and a seat it holds moves to no
/// other writer of its rows.
#[derive(Debug, Default, Clone, Copy)]
pub struct MemoryBackend;

#[async_trait]
impl StorageBackend for MemoryBackend {
    fn qualify(&self, provider: ProviderKind) -> Result<(), String> {
        match provider {
            ProviderKind::Process => Ok(()),
            ProviderKind::Container => Err("the memory storage backend holds one process's rows, and the container provider is qualified only for a shared store namespace (R14 item 6)".into()),
        }
    }

    /// A memory admin's data dir holds no row: every start is a new admin's.
    async fn own_row(&self, _data_dir: &Path, _writer: &IncarnationId) -> Result<Option<NodeRecord>, StorageError> {
        Ok(None)
    }

    async fn open(&self, _scope: &StorageScope, embedding: Option<Arc<dyn RoundActions>>) -> Result<StorageBoundaries, String> {
        let connections: OpenConnections = Arc::new(|_name: PathName| Box::pin(async { Ok(Arc::new(crate::storage::MemoryConnectionsStorage::default()) as Arc<dyn ConnectionsStorage>) }));
        Ok(StorageBoundaries {
            fabric: Arc::new(crate::fabric_storage::MemoryFabricStorage::new()),
            meshes: Arc::new(crate::storage::MemoryMeshStorage::default()),
            nodes: Arc::new(crate::storage::MemoryNodesStorage::default()),
            status: Arc::new(crate::status_storage::MemoryStatusStorage::default()),
            connections,
            builds: Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
            // Nothing is held for a later publication: the embedding's actions alone, or none.
            round_actions: embedding.unwrap_or_else(|| Arc::new(crate::fabric_rounds::NoScratchpad)),
            // One process's rows: the seat's next holder is another process, which never writes them.
            cut_off_writes: Arc::new(|_| Box::pin(async {})),
            writes_to: "memory".into(),
        })
    }
}
