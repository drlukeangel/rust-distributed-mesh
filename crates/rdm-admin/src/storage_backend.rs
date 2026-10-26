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
    /// The node id this process writes as: with `writer`, the writer every store line's `op` names
    /// (R-SR3).
    pub node_id: crate::model::NodeId,
}

impl StorageScope {
    /// The writer of every store file line this process appends.
    #[cfg(feature = "file-backend")]
    pub fn log_writer(&self) -> crate::store_log::LogWriter {
        crate::store_log::LogWriter::new(&self.node_id, &self.writer)
    }
}

/// A node's own connections rows, opened once its `path.name` is decided.
pub type OpenConnections = Arc<dyn Fn(PathName) -> BoxFuture<'static, Result<Arc<dyn ConnectionsStorage>, StorageError>> + Send + Sync>;

/// The storage a node-admin runs on, opened by its backend: the five boundaries and how it finishes
/// the writes it has issued.
pub struct StorageBoundaries {
    /// `fabric.storage`: the Fabric record, its name's claim and its events.
    pub fabric: Arc<dyn FabricStorage>,
    /// `mesh.storage`: the Mesh rows, each mesh's in-flight row and status facts.
    pub meshes: Arc<dyn MeshStorage>,
    /// `nodes.storage`: the runtimes the admin started, every mesh's Node rows and the facts on each
    /// birth, its address and primaries included.
    pub nodes: Arc<dyn NodesStorage>,
    /// `nodes.storage`'s runtime rows on the fabric's shared store root, in no admin's data dir: where an
    /// externally started node-admin (the Day-0 admin, an adoption launch), whose runtime no launcher
    /// recorded, keeps its own runtime row, and where a later adoption launch on the same store root
    /// reads it (deployment-provider.md §3, the externally started Day-0 adoption and the successor's
    /// management cut). `None` on a backend whose rows no other process reads.
    pub root_runtimes: Option<Arc<dyn NodesStorage>>,
    /// `connections.storage`: the admin's own connections rows, by its `path.name`.
    pub connections: OpenConnections,
    /// The Build journal: the Build facts, and each attempt's context.
    pub builds: Arc<dyn LocalBuildLog>,
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
    /// Keep `record` as the admin's own row in its data dir, where [`Self::own_row`] reads it: the own
    /// seed, written by the process born as `writer` once its birth and its Mesh are decided, so a
    /// restart on this data dir is the same logical node in the same Fabric and Mesh (R-FI1). Its
    /// address and primaries are on its node's record, written by its mesh-primary.
    async fn put_own_row(&self, data_dir: &Path, writer: &IncarnationId, record: &NodeRecord) -> Result<(), StorageError>;
    /// Opens the five boundaries of `scope`.
    async fn open(&self, scope: &StorageScope) -> Result<StorageBoundaries, String>;
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

    async fn put_own_row(&self, data_dir: &Path, writer: &IncarnationId, record: &NodeRecord) -> Result<(), StorageError> {
        crate::storage::FileNodesStorage::put_own_in(data_dir, writer, record).await
    }

    async fn open(&self, scope: &StorageScope) -> Result<StorageBoundaries, String> {
        let storage_err = |e: StorageError| e.to_string();
        let root = crate::store_root::StoreRoot::open(&self.store_dir, &scope.fabric_id).map_err(|e| format!("refusing to start: {e}"))?;
        let writes_to = "store-root".to_string();
        let fabric = Arc::new(crate::fabric_storage::FileFabricStorage::open(&root, &scope.log_writer()).await.map_err(storage_err)?);
        let meshes = Arc::new(crate::storage::FileMeshStorage::open(&root, &scope.log_writer()).await.map_err(storage_err)?);
        let nodes = Arc::new(crate::storage::FileNodesStorage::open(&scope.data_dir, &root, &scope.own_mesh, &scope.log_writer()).await.map_err(storage_err)?);
        // Every runtime row is a line of the root's nodes.jsonl: the root's rows are the nodes store's.
        let root_runtimes: Option<Arc<dyn NodesStorage>> = Some(nodes.clone() as Arc<dyn NodesStorage>);
        let builds = Arc::new(crate::build_state::FileJournal::open_on_root(&root, &scope.data_dir, &scope.log_writer()).await.map_err(|e| e.to_string())?);
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
            let (root, writer) = (root.clone(), scope.log_writer());
            Arc::new(move |name: PathName| {
                let (root, writer) = (root.clone(), writer.clone());
                Box::pin(async move { Ok(Arc::new(crate::storage::FileConnectionsStorage::open(&root, &name, &writer).await?) as Arc<dyn ConnectionsStorage>) })
            })
        };
        Ok(StorageBoundaries { fabric, meshes, nodes, root_runtimes, connections, builds, finish_writes, writes_to })
    }
}

#[cfg(feature = "file-backend")]
/// RDM standalone's storage backend on the shared store dir `store_dir`: the one a node-admin's
/// `start`, `start_with` and `entry::run` construct.
pub fn file_backend(store_dir: impl Into<PathBuf>) -> Arc<dyn StorageBackend> {
    Arc::new(FileBackend::at(store_dir))
}

/// The topology storage providers a node-admin executable registers in its wiring, by name (storage.md
/// §2): each a backend of the five record stores, its name bound to its place at registration. The
/// admin opens the one its `topology_storage_provider` names (`RDM_TOPOLOGY_STORAGE_PROVIDER` on Day 0,
/// its launch's when launched); as the fabric-primary it refuses at acceptance a Build naming one this
/// registry does not hold.
#[derive(Clone, Default)]
pub struct TopologyStorageProviders {
    registered: Vec<(String, Arc<dyn StorageBackend>)>,
}

impl TopologyStorageProviders {
    /// No provider registered.
    pub fn new() -> Self {
        Self::default()
    }

    /// RDM's file backend on the shared store dir `store_dir`, registered as `file`: what every RDM
    /// node-admin executable registers.
    #[cfg(feature = "file-backend")]
    pub fn file(store_dir: impl Into<PathBuf>) -> Self {
        Self::new().register(rdm_core::launch::TOPOLOGY_STORAGE_FILE, file_backend(store_dir))
    }

    /// These providers with `backend` registered as `name`. A name registered twice refuses the
    /// admin's start by name ([`Self::named`]).
    pub fn register(mut self, name: &str, backend: Arc<dyn StorageBackend>) -> Self {
        self.registered.push((name.to_string(), backend));
        self
    }

    /// The registered names, in registration order.
    pub fn names(&self) -> Vec<String> {
        self.registered.iter().map(|(n, _)| n.clone()).collect()
    }

    /// The backend registered as `name`, refused by name when none is, or when a name is registered
    /// twice.
    pub fn named(&self, name: &str) -> Result<Arc<dyn StorageBackend>, String> {
        let names = self.names();
        if let Some(twice) = names.iter().enumerate().find(|(i, n)| names[..*i].contains(n)).map(|(_, n)| n) {
            return Err(format!("the topology storage provider `{twice}` is registered twice: each name is bound to one place"));
        }
        self.registered.iter().find(|(n, _)| n == name).map(|(_, b)| b.clone()).ok_or_else(|| {
            format!("{} names topology storage provider `{name}`, which this executable does not register (it registers: {})", rdm_core::launch::ENV_TOPOLOGY_STORAGE_PROVIDER, names.join(", "))
        })
    }
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

    /// A memory admin's data dir keeps no row: its next start is a new admin's.
    async fn put_own_row(&self, _data_dir: &Path, _writer: &IncarnationId, _record: &NodeRecord) -> Result<(), StorageError> {
        Ok(())
    }

    async fn open(&self, _scope: &StorageScope) -> Result<StorageBoundaries, String> {
        let connections: OpenConnections = Arc::new(|_name: PathName| Box::pin(async { Ok(Arc::new(crate::storage::MemoryConnectionsStorage::default()) as Arc<dyn ConnectionsStorage>) }));
        // A memory write is one step under its store's lock: every write issued has finished.
        let finish_writes: FinishWrites = Arc::new(|| Box::pin(async {}));
        Ok(StorageBoundaries {
            fabric: Arc::new(crate::fabric_storage::MemoryFabricStorage::new()),
            meshes: Arc::new(crate::storage::MemoryMeshStorage::default()),
            nodes: Arc::new(crate::storage::MemoryNodesStorage::default()),
            root_runtimes: None,
            connections,
            builds: Arc::new(crate::build_state::MemoryBuildStateAdapter::new()),
            finish_writes,
            writes_to: "memory".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT (storage.md §2: a provider is "registered in its wiring by name"; a name is bound to its
    /// place at registration): the registry answers the backend a name was registered with, and refuses
    /// by name a name it does not register (naming the ones it does) and a name registered twice.
    #[test]
    fn a_provider_is_opened_by_its_registered_name_and_an_unregistered_one_is_refused_by_name() {
        let providers = TopologyStorageProviders::new().register("file", Arc::new(MemoryBackend)).register("app", Arc::new(MemoryBackend));
        assert_eq!(providers.names(), ["file", "app"]);
        assert!(providers.named("app").is_ok());
        let refused = providers.named("other").err().expect("an unregistered name is refused");
        assert!(refused.contains("`other`") && refused.contains("file, app") && refused.contains(rdm_core::launch::ENV_TOPOLOGY_STORAGE_PROVIDER), "{refused}");
        let twice = providers.register("app", Arc::new(MemoryBackend)).named("file").err().expect("a name registered twice is refused");
        assert!(twice.contains("`app` is registered twice"), "{twice}");
    }
    use crate::model::NodeId;

    /// The same script against any backend that keeps its rows across opens: the admin re-born at the
    /// same path.name over the inherited data dir (a new node.id and incarnation, `inherited = true`)
    /// reads its predecessor's runtime rows and attempt contexts. The attempt contexts are the data
    /// dir's, so an admin over another data dir reads none of them; the runtime rows are the fabric's
    /// lines in its store root's `nodes.jsonl` (R-SR1), so it reads them too.
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
            let b = backend.open(first).await.unwrap_or_else(|e| panic!("the first birth opens: {e}"));
            b.nodes.put_runtime(&runtime).await.unwrap();
            b.builds.put_attempt_context(&build, Some("mesh1"), 2, &context).await.unwrap();
        }
        let b = backend.open(reborn).await.unwrap_or_else(|e| panic!("the re-born birth opens over the inherited data dir: {e}"));
        assert_eq!(b.nodes.runtimes().await.unwrap(), vec![runtime.clone()], "the re-born admin holds its predecessor's runtime rows");
        assert_eq!(b.nodes.runtime(&runtime.node_id, &runtime.incarnation_id).await.unwrap(), Some(runtime.clone()), "a point read by the birth finds it");
        assert_eq!(b.builds.attempt_context(&build, Some("mesh1"), 2).await.unwrap(), Some(context.clone()), "and its predecessor's attempt context");
        let o = backend.open(other).await.unwrap_or_else(|e| panic!("another admin opens over its own data dir: {e}"));
        assert_eq!(o.nodes.runtimes().await.unwrap(), vec![runtime.clone()], "the runtime rows are the fabric's, on its store root");
        assert_eq!(o.builds.attempt_context(&build, Some("mesh1"), 2).await.unwrap(), None, "nor the attempt context");
    }

    /// CONTRACT (R-FI1: a restarting node-admin's own row names the Fabric whose root it opens, read
    /// before any store opens): the own row is the admin's own seed in its data dir: the file backend
    /// reads back what it was given there, by another process of the data dir too, and its nodes store,
    /// opened over that data dir, holds no copy of it; another data dir holds none. A memory admin keeps
    /// none: its next start is a new admin's. Must NOT happen: the own row kept anywhere but the data dir.
    #[tokio::test]
    async fn the_own_row_is_the_admins_own_seed_in_its_data_dir() {
        let own = crate::storage::NodeRecord {
            node_id: NodeId::mint(),
            fabric_id: FabricId::mint(),
            name: "mesh1.admin.1".parse().unwrap(),
            mesh_id: crate::model::MeshId::mint(),
            incarnation_id: IncarnationId::mint(),
            endpoint_id: crate::model::EndpointId(iroh::SecretKey::generate().public().to_string()),
            transport_addr: "127.0.0.1:41001".parse().unwrap(),
            listeners: vec![("control".into(), "127.0.0.1:41002".parse().unwrap())],
            is_primary: None,
            is_fabric_primary: None,
        };
        let memory = MemoryBackend;
        memory.put_own_row(Path::new("unused"), &own.incarnation_id, &own).await.unwrap();
        assert_eq!(memory.own_row(Path::new("unused"), &IncarnationId::mint()).await.unwrap(), None, "a memory admin keeps no own row");
        #[cfg(feature = "file-backend")]
        {
            let (store, data_dir, other) = (crate::test_support::tempdir("own-seed-store"), crate::test_support::tempdir("own-seed-data"), crate::test_support::tempdir("own-seed-other"));
            let file = FileBackend::at(&store);
            assert_eq!(file.own_row(&data_dir, &IncarnationId::mint()).await.unwrap(), None, "a new data dir holds no own row");
            file.put_own_row(&data_dir, &own.incarnation_id, &own).await.unwrap();
            assert_eq!(file.own_row(&data_dir, &IncarnationId::mint()).await.unwrap(), Some(own.clone()), "the next process of the data dir reads it back whole");
            assert_eq!(file.own_row(&other, &IncarnationId::mint()).await.unwrap(), None, "another data dir holds none");
            let scope = StorageScope { data_dir: data_dir.clone(), fabric_id: own.fabric_id.clone(), own_mesh: "mesh1".into(), writer: IncarnationId::mint(), node_id: NodeId::mint() };
            let opened = file.open(&scope).await.unwrap_or_else(|e| panic!("the boundaries open: {e}"));
            assert!(opened.nodes.nodes("mesh1").await.unwrap().is_empty(), "the nodes store holds no copy of the own row");
        }
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
        let at = |data_dir: PathBuf| StorageScope { data_dir, fabric_id: fabric_id.clone(), own_mesh: "mesh1".into(), writer: IncarnationId::mint(), node_id: NodeId::mint() };
        a_birth_re_born_over_its_inherited_data_dir_reads_its_predecessors_runtimes_and_contexts(&FileBackend::at(&store), &at(data_dir.clone()), &at(data_dir), &at(crate::test_support::tempdir("file-backend-reborn-other"))).await;
    }
}
