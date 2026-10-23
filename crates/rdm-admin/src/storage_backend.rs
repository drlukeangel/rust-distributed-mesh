//! The storage backend a node-admin runs on (build.md: "production backends for the imported storage
//! interfaces"). It opens the five storage boundaries, `fabric.storage` ([`FabricStorage`]),
//! `mesh.storage` ([`MeshStorage`]), `nodes.storage` ([`NodesStorage`]), `connections.storage`
//! ([`ConnectionsStorage`]) and the Build journal ([`LocalBuildLog`]), and every durable read and
//! write of the admin goes through them. A fact lives in the store of the object it describes. The admin's
//! boot takes its backend beside its `Wiring` (`admin::start_on`); RDM standalone's is
//! [`FileBackend`] (the `file-backend` feature, the default), the boundaries as files on the fabric's
//! store root and the admin's data dir. [`MemoryBackend`] holds one process's rows.

use crate::build_state::LocalBuildLog;
use crate::fabric_handover::FinishWrites;
use crate::fabric_rounds::RoundActions;
use crate::fabric_storage::FabricStorage;
use crate::model::{FabricId, IncarnationId, PathName, ProviderKind};
use crate::storage::{ConnectionsStorage, MeshStorage, NodeRecord, NodesStorage};
use crate::storage_error::StorageError;
use async_trait::async_trait;
use futures_util::future::BoxFuture;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Where a node-admin's storage opens, decided by its boot before any boundary opens.
#[derive(Debug, Clone)]
pub struct StorageScope {
    /// The admin's own data dir: its local identity, on every backend.
    pub data_dir: PathBuf,
    /// The Fabric the admin belongs to.
    pub fabric_id: FabricId,
    /// The mesh the admin belongs to: its Build lane and its members' rows.
    pub own_mesh: String,
    /// The incarnation this process writes as.
    pub writer: IncarnationId,
}

/// A node's own connections rows, opened once its `path.name` is decided.
pub type OpenConnections = Arc<dyn Fn(PathName) -> BoxFuture<'static, Result<Arc<dyn ConnectionsStorage>, StorageError>> + Send + Sync>;

/// The storage a node-admin runs on, opened by its backend: the five boundaries, what the backend does
/// at a commit-state round, and how it finishes the writes it has issued.
pub struct StorageBoundaries {
    /// `fabric.storage`: the Fabric record, its name's claim and its events.
    pub fabric: Arc<dyn FabricStorage>,
    /// `mesh.storage`: the Mesh rows, each mesh's in-flight row and status facts.
    pub meshes: Arc<dyn MeshStorage>,
    /// `nodes.storage`: the admin's own row, the runtimes it started, every mesh's Node rows and
    /// contacts, and the facts on each birth.
    pub nodes: Arc<dyn NodesStorage>,
    /// `connections.storage`: the admin's own connections rows, by its `path.name`.
    pub connections: OpenConnections,
    /// The Build journal: the Build facts, and each attempt's context.
    pub builds: Arc<dyn LocalBuildLog>,
    /// The node's commit-state and open-traffic actions: the backend's own, then the embedding's.
    pub round_actions: Arc<dyn RoundActions>,
    /// Finishes every write the boundaries have issued: a primary runs it before it calls its successor
    /// (node-primary-update.md step 3).
    pub finish_writes: FinishWrites,
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
    /// Opens the five boundaries of `scope`; `embedding` is the embedding's own round actions, run
    /// after the backend's.
    async fn open(&self, scope: &StorageScope, embedding: Option<Arc<dyn RoundActions>>) -> Result<StorageBoundaries, String>;
}

/// RDM standalone's backend: the boundaries as files, the shared ones on the fabric's store root
/// (`<store_dir>/<fabric_id>/`, R14), the admin's own in its data dir.
#[cfg(feature = "file-backend")]
#[derive(Debug, Clone)]
pub struct FileBackend {
    /// The fabric's shared store dir (`RDM_STORE_DIR`): every fabric's root is under it.
    pub store_dir: PathBuf,
}

#[cfg(feature = "file-backend")]
impl FileBackend {
    /// The file backend on the shared store dir `store_dir`.
    pub fn at(store_dir: impl Into<PathBuf>) -> Self {
        Self { store_dir: store_dir.into() }
    }
}

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
        let root = crate::store_root::StoreRoot::open(&self.store_dir, &scope.fabric_id).map_err(|e| format!("refusing to start: {e}"))?;
        // Before the fabric's first state-commit every row this admin writes is held in its
        // scratchpad; its commit-state action publishes them (states.md, Rounds).
        let writes_to = root.writes_to().to_string();
        let round_actions: Arc<dyn RoundActions> = Arc::new(crate::fabric_rounds::StorePublication::new(root.scratchpad().cloned(), embedding));
        let fabric = Arc::new(crate::fabric_storage::FileFabricStorage::open(&root, &scope.writer).await.map_err(storage_err)?);
        let meshes = Arc::new(crate::storage::FileMeshStorage::open(&root, &scope.writer).await.map_err(storage_err)?);
        let nodes = Arc::new(crate::storage::FileNodesStorage::open(&scope.data_dir, &root, &scope.own_mesh, &scope.writer).await.map_err(storage_err)?);
        let builds = Arc::new(crate::build_state::FileJournal::open_on_root(&root, &scope.data_dir, &scope.writer).await.map_err(|e| e.to_string())?);
        let finish_writes: FinishWrites = {
            let (meshes, nodes, fabric) = (meshes.clone(), nodes.clone(), fabric.clone());
            Arc::new(move || {
                let (meshes, nodes, fabric) = (meshes.clone(), nodes.clone(), fabric.clone());
                Box::pin(async move {
                    meshes.finish_issued_writes().await;
                    nodes.finish_issued_writes().await;
                    fabric.finish_issued_writes().await;
                })
            })
        };
        let connections: OpenConnections = {
            let (root, writer) = (root.clone(), scope.writer.clone());
            Arc::new(move |name: PathName| {
                let (root, writer) = (root.clone(), writer.clone());
                Box::pin(async move { Ok(Arc::new(crate::storage::FileConnectionsStorage::open(&root, &name, &writer).await?) as Arc<dyn ConnectionsStorage>) })
            })
        };
        Ok(StorageBoundaries { fabric, meshes, nodes, connections, builds, round_actions, finish_writes, writes_to })
    }
}

#[cfg(feature = "file-backend")]
/// RDM standalone's storage backend on the shared store dir `store_dir`: the one a node-admin's
/// `start`, `start_with` and `entry::run` construct.
pub fn file_backend(store_dir: impl Into<PathBuf>) -> Arc<dyn StorageBackend> {
    Arc::new(FileBackend::at(store_dir))
}

/// One process's rows: every boundary held in memory, written by this admin alone and gone with its
/// process. An admin on it is never started on an inherited root.
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

    async fn open(&self, scope: &StorageScope, embedding: Option<Arc<dyn RoundActions>>) -> Result<StorageBoundaries, String> {
        let connections: OpenConnections = Arc::new(|_name: PathName| Box::pin(async { Ok(Arc::new(crate::storage::MemoryConnectionsStorage::default()) as Arc<dyn ConnectionsStorage>) }));
        // A memory write is one step under its store's lock: every write issued has finished.
        let finish_writes: FinishWrites = Arc::new(|| Box::pin(async {}));
        Ok(StorageBoundaries {
            fabric: Arc::new(crate::fabric_storage::MemoryFabricStorage::new()),
            meshes: Arc::new(crate::storage::MemoryMeshStorage::default()),
            nodes: Arc::new(crate::storage::MemoryNodesStorage::default()),
            connections,
            builds: Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
            // Nothing is held for a later publication: the embedding's actions alone, or none.
            round_actions: embedding.unwrap_or_else(|| Arc::new(crate::fabric_rounds::NoScratchpad)),
            finish_writes,
            writes_to: "memory".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::NodeId;

    /// The same script against any backend that keeps its rows across opens: a birth's runtime rows
    /// and attempt contexts are the data dir's, so the admin re-born at the same path.name over the
    /// inherited data dir (a new node.id and incarnation, `inherited = true`) reads its predecessor's;
    /// an admin over another data dir reads none of them.
    async fn a_birth_re_born_over_its_inherited_data_dir_reads_its_predecessors_runtimes_and_contexts(backend: &dyn StorageBackend, first: &StorageScope, reborn: &StorageScope, other: &StorageScope) {
        let runtime = crate::storage::RuntimeRow {
            node_id: NodeId::mint(),
            name: "mesh1.rpc.2".parse().unwrap(),
            incarnation_id: IncarnationId::mint(),
            runtime: rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-1")).unwrap(),
            data_dir: Some("/tmp/mesh1.rpc.2".into()),
        };
        let build = crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b1")).unwrap();
        let context = rdm_core::context::CallContext { caller_system: Some("rdm".into()), traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()), ..Default::default() };
        {
            let b = backend.open(first, None).await.unwrap_or_else(|e| panic!("the first birth opens: {e}"));
            b.nodes.put_runtime(&runtime).await.unwrap();
            b.builds.put_attempt_context(&build, Some("mesh1"), 2, &context).await.unwrap();
        }
        let b = backend.open(reborn, None).await.unwrap_or_else(|e| panic!("the re-born birth opens over the inherited data dir: {e}"));
        assert_eq!(b.nodes.runtimes().await.unwrap(), vec![runtime.clone()], "the re-born admin holds its predecessor's runtime rows");
        assert_eq!(b.nodes.runtime(&runtime.node_id, &runtime.incarnation_id).await.unwrap(), Some(runtime.clone()), "a point read by the birth finds it");
        assert_eq!(b.builds.attempt_context(&build, Some("mesh1"), 2).await.unwrap(), Some(context.clone()), "and its predecessor's attempt context");
        let o = backend.open(other, None).await.unwrap_or_else(|e| panic!("another admin opens over its own data dir: {e}"));
        assert!(o.nodes.runtimes().await.unwrap().is_empty(), "another data dir holds none of them");
        assert_eq!(o.builds.attempt_context(&build, Some("mesh1"), 2).await.unwrap(), None, "nor the attempt context");
    }

    /// CONTRACT (node-recovery.md: recovery retains the inherited data dir and creates a new node.id
    /// and incarnation; R-FM2 `inherited = true`): on the file backend, the runtime rows and attempt
    /// contexts an admin wrote are its data dir's, read back by the admin re-born at the same
    /// path.name over that data dir, so it still holds the handles of the runtimes it must inspect and
    /// terminate; another admin's data dir reads none. Must NOT happen: a re-birth losing them because
    /// they were keyed by the birth that wrote them. Rafka's backend runs the same script.
    #[cfg(feature = "file-backend")]
    #[tokio::test]
    async fn a_birth_re_born_over_its_inherited_data_dir_reads_its_predecessors_runtimes_and_contexts_on_the_file_backend() {
        let store = crate::test_support::tempdir("file-backend-reborn-store");
        let data_dir = crate::test_support::tempdir("file-backend-reborn-data");
        let fabric_id = FabricId::mint();
        let at = |data_dir: PathBuf| StorageScope { data_dir, fabric_id: fabric_id.clone(), own_mesh: "mesh1".into(), writer: IncarnationId::mint() };
        a_birth_re_born_over_its_inherited_data_dir_reads_its_predecessors_runtimes_and_contexts(&FileBackend::at(&store), &at(data_dir.clone()), &at(data_dir), &at(crate::test_support::tempdir("file-backend-reborn-other"))).await;
    }
}
