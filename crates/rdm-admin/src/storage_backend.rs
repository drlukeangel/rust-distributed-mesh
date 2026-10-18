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
    /// The admin's own data dir: its local identity, on every backend.
    pub data_dir: PathBuf,
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
/// process. An admin on it is never started on an inherited root. A seat transfer cuts off the
/// moving seats' writes and the journal refuses a stale seat's facts by name, as on the file backend.
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
        // One cut-off every memory store reads at its writes: a seat transfer cuts off the moving
        // seats' scopes and refuses each later write by name, as the file stores do.
        let cut_off = Arc::new(crate::fabric_handover::SeatCutOff::default());
        let connections: OpenConnections = Arc::new(|_name: PathName| Box::pin(async { Ok(Arc::new(crate::storage::MemoryConnectionsStorage::default()) as Arc<dyn ConnectionsStorage>) }));
        let cut_off_writes: CutOffWrites = {
            let cut_off = cut_off.clone();
            Arc::new(move |c| {
                cut_off.apply(&c);
                Box::pin(async {})
            })
        };
        Ok(StorageBoundaries {
            fabric: Arc::new(crate::fabric_storage::MemoryFabricStorage::cut_off_by(cut_off.clone())),
            meshes: Arc::new(crate::storage::MemoryMeshStorage::cut_off_by(cut_off.clone())),
            nodes: Arc::new(crate::storage::MemoryNodesStorage::of_mesh(&scope.own_mesh, cut_off.clone())),
            status: Arc::new(crate::status_storage::MemoryStatusStorage::of_mesh(&scope.own_mesh, cut_off)),
            connections,
            builds: Arc::new(crate::build_state::MemoryBuildStateAdapter::of_mesh(&scope.own_mesh)),
            // Nothing is held for a later publication: the embedding's actions alone, or none.
            round_actions: embedding.unwrap_or_else(|| Arc::new(crate::fabric_rounds::NoScratchpad)),
            cut_off_writes,
            writes_to: "memory".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fabric_handover::WriteCutOff;
    use crate::fabric_storage::FabricIdentity;
    use crate::model::{MeshId, NodeId};
    use crate::status_storage::NodeExitRow;
    use crate::storage::{MeshRecord, NodeRow};

    fn row(name: &str) -> NodeRow {
        NodeRow { name: name.parse().unwrap(), node_id: NodeId::mint(), status: crate::model::NodeStatus::ReadyForTraffic, config: Default::default(), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None }
    }

    fn exit(name: &str) -> NodeExitRow {
        NodeExitRow { node_id: NodeId::mint(), name: name.parse().unwrap(), incarnation_id: IncarnationId::mint(), exit_code: Some(0), proof: "provider".into(), observed_at_ms: 1 }
    }

    /// The same script against any backend: a seat transfer's cut-off refuses, naming the transfer,
    /// every write in the moving seats' scopes; a lift gives the writes back.
    async fn a_moving_seats_writes_are_refused_by_name_until_lifted(backend: &dyn StorageBackend, scope: &StorageScope) {
        let b = backend.open(scope, None).await.unwrap_or_else(|e| panic!("the backend opens: {e}"));
        let reason = "take-seats:b1, successor mesh2.admin.1".to_string();
        (b.cut_off_writes)(WriteCutOff::Set { reason: reason.clone(), mesh_seat: true, fabric_seat: true }).await;
        let refused = |r: Result<(), StorageError>, what: &str| match r {
            Err(StorageError::CutOff { reason: why, .. }) => assert_eq!(why, reason, "{what} is refused naming the transfer"),
            other => panic!("{what}: a write in a moving seat's scope is refused by the cut-off, not {other:?}"),
        };
        let mesh2 = MeshRecord { mesh_id: MeshId::mint(), name: "mesh2".into(), dead: false };
        let identity = FabricIdentity { fabric_id: scope.fabric_id.clone(), name: format!("fabric-{}", scope.fabric_id) };
        refused(b.meshes.claim_mesh(&mesh2).await.map(drop), "the Mesh row");
        refused(b.nodes.put_node_row(&row("mesh1.rpc.1")).await.map(drop), "the admin's own mesh's node row");
        refused(b.nodes.put_node_row(&row("mesh2.rpc.1")).await.map(drop), "another mesh's node row");
        refused(b.status.put_node_exit(&exit("mesh1.rpc.1")).await, "the admin's own mesh's status row");
        refused(b.status.put_node_exit_of("mesh2", &exit("mesh2.rpc.1")).await, "another mesh's status row");
        refused(b.fabric.claim_identity(&identity).await.map(drop), "the Fabric identity");
        (b.cut_off_writes)(WriteCutOff::Lift).await;
        assert!(b.meshes.claim_mesh(&mesh2).await.unwrap().is_ok(), "the Mesh row is written once the cut-off is lifted");
        assert!(b.nodes.put_node_row(&row("mesh1.rpc.1")).await.unwrap().is_ok(), "the own mesh's node row is written once lifted");
        assert!(b.nodes.put_node_row(&row("mesh2.rpc.1")).await.unwrap().is_ok(), "another mesh's node row is written once lifted");
        b.status.put_node_exit(&exit("mesh1.rpc.1")).await.unwrap();
        b.status.put_node_exit_of("mesh2", &exit("mesh2.rpc.1")).await.unwrap();
        assert!(b.fabric.claim_identity(&identity).await.unwrap().is_ok(), "the Fabric identity is written once lifted");
    }

    fn scope(data_dir: PathBuf) -> StorageScope {
        StorageScope { data_dir, fabric_id: FabricId::mint(), own_mesh: "mesh1".into(), writer: IncarnationId::mint() }
    }

    /// CONTRACT (R14 item 4; 1b, Arch's decision 3): a seat transfer's cut-off is the backend's, not
    /// the file store's: on the memory backend, as on the file backend, every write in the moving
    /// mesh-primary and fabric-primary seats' scopes (the Mesh rows, the admin's own mesh's node and
    /// status rows, another mesh's, the Fabric identity) is refused `CutOff`, naming the transfer, and
    /// each is written again once the transfer lifts it. Must NOT happen: a write a moved seat no
    /// longer authorizes reaching a memory store.
    #[tokio::test]
    async fn the_memory_backend_cuts_off_a_moving_seats_writes_as_the_file_backend_does() {
        a_moving_seats_writes_are_refused_by_name_until_lifted(&MemoryBackend, &scope(std::env::temp_dir().join(format!("memory-backend-{}", NodeId::mint())))).await;
        #[cfg(feature = "file-backend")]
        {
            let store = crate::test_support::tempdir("file-backend-store");
            a_moving_seats_writes_are_refused_by_name_until_lifted(&FileBackend::at(&store), &scope(crate::test_support::tempdir("file-backend-data"))).await;
        }
    }
}
