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

/// A node-admin's one access point to its topology storage (storage.md §2): every reader and writer
/// on the admin reaches the five stores through it, and a provider change replaces the provider
/// beneath them.
pub struct TopologyStorageHandle {
    opened: StorageBoundaries,
    provider: String,
}

impl TopologyStorageHandle {
    /// Open the provider `providers` registers as `provider` on `scope`, else refused by name.
    pub async fn open(providers: &TopologyStorageProviders, provider: &str, scope: StorageScope) -> Result<Arc<Self>, String> {
        let opened = providers.named(provider)?.open(&scope).await?;
        Ok(Arc::new(Self { opened, provider: provider.to_string() }))
    }

    /// The provider the admin writes now.
    pub fn provider(&self) -> String {
        self.provider.clone()
    }

    /// Where the boundaries write now, as the boot's storage span records it.
    pub fn writes_to(&self) -> String {
        self.opened.writes_to.clone()
    }

    /// `fabric.storage`, through this handle.
    pub fn fabric(self: &Arc<Self>) -> Arc<dyn FabricStorage> {
        self.opened.fabric.clone()
    }

    /// `mesh.storage`, through this handle.
    pub fn meshes(self: &Arc<Self>) -> Arc<dyn MeshStorage> {
        self.opened.meshes.clone()
    }

    /// `nodes.storage`, through this handle.
    pub fn nodes(self: &Arc<Self>) -> Arc<dyn NodesStorage> {
        self.opened.nodes.clone()
    }

    /// `nodes.storage`'s runtime rows on the fabric's shared store root, through this handle; `None`
    /// on a provider whose rows no other process reads.
    pub fn root_runtimes(self: &Arc<Self>) -> Option<Arc<dyn NodesStorage>> {
        self.opened.root_runtimes.clone()
    }

    /// The Build journal, through this handle.
    pub fn builds(self: &Arc<Self>) -> Arc<dyn LocalBuildLog> {
        self.opened.builds.clone()
    }

    /// The admin's own connections rows at `name`, through this handle.
    pub async fn connections(self: &Arc<Self>, name: PathName) -> Result<Arc<dyn ConnectionsStorage>, StorageError> {
        (self.opened.connections)(name).await
    }

    /// Finishes every write the provider the admin writes now has issued.
    pub fn finish_writes(self: &Arc<Self>) -> FinishWrites {
        self.opened.finish_writes.clone()
    }

    /// Replace the provider with the one registered as `provider`.
    pub async fn replace(&self, _provider: &str) -> Result<(), String> {
        Ok(())
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

    /// Whether any file under `dir`, by its path or its contents, names `needle`.
    #[cfg(feature = "file-backend")]
    fn holds(dir: &Path, needle: &str) -> bool {
        std::fs::read_dir(dir).into_iter().flatten().flatten().any(|e| {
            let p = e.path();
            p.to_string_lossy().contains(needle) || if p.is_dir() { holds(&p, needle) } else { std::fs::read_to_string(&p).unwrap_or_default().contains(needle) }
        })
    }

    /// The scope of an admin on a new data dir in a new fabric.
    #[cfg(feature = "file-backend")]
    fn scope_in(data_dir: PathBuf) -> StorageScope {
        StorageScope { data_dir, fabric_id: FabricId::mint(), own_mesh: "mesh1".into(), writer: IncarnationId::mint(), node_id: NodeId::mint() }
    }

    /// CONTRACT (storage.md §2, "Changing it", Unblock: "Each node moves to the new provider"; i149 R4:
    /// "Every active reader and writer uses the same replaceable provider handle on its node"): what a
    /// node-admin's boot took from its handle, each of the five stores and its own connections rows,
    /// goes to the new provider for every call once the provider is replaced: every write lands under
    /// the new provider's store dir and none under the old one's, every read answers the new
    /// provider's records, and the derivation installed on the Build journal before the change runs
    /// over the facts the new journal takes. Must NOT happen: an owner holding what the boot handed it
    /// still reading or writing the old provider after the change.
    #[cfg(feature = "file-backend")]
    #[tokio::test]
    async fn a_node_uses_the_replaced_provider_for_every_storage_call() {
        use crate::fabric_storage::{FabricIdentity, FabricRecord};
        use crate::storage::{MeshRecord, NodeRow, RuntimeRow};
        let (old, new) = (crate::test_support::tempdir("handle-old"), crate::test_support::tempdir("handle-new"));
        let providers = TopologyStorageProviders::new().register("file", Arc::new(FileBackend::at(&old))).register("file-new", Arc::new(FileBackend::at(&new)));
        let scope = scope_in(crate::test_support::tempdir("handle-data"));
        let handle = TopologyStorageHandle::open(&providers, "file", scope.clone()).await.unwrap_or_else(|e| panic!("the admin opens its provider: {e}"));
        // What the boot hands its owners: taken once, before the change.
        let (fabric, meshes, nodes, builds) = (handle.fabric(), handle.meshes(), handle.nodes(), handle.builds());
        let name: PathName = "mesh1.admin.1".parse().unwrap();
        let connections = handle.connections(name.clone()).await.unwrap();
        let before = MeshRecord::new(crate::model::MeshId::mint(), "meshbefore");
        meshes.put_mesh(&before).await.unwrap();
        assert!(holds(&old, before.mesh_id.as_str()), "before the change the admin writes the provider it opened");
        let seen = Arc::new(std::sync::Mutex::new(Vec::<crate::build::BuildId>::new()));
        let derived = seen.clone();
        builds.derive_on_every_fact(crate::build_state::FactDerivation::new(Vec::new, move |facts| *derived.lock().unwrap() = facts.iter().map(|f| f.build_id().clone()).collect())).await;

        handle.replace("file-new").await.unwrap_or_else(|e| panic!("the provider is replaced: {e}"));

        assert_eq!(meshes.mesh(&before.mesh_id).await.unwrap(), None, "a read after the change answers the new provider's records, which do not hold the old one's");
        let identity = FabricIdentity { fabric_id: scope.fabric_id.clone(), name: "fabricafter".into() };
        fabric.put_fabric(&FabricRecord::new(identity.clone())).await.unwrap();
        fabric.put_name_claim(&identity).await.unwrap();
        let mesh = MeshRecord::new(crate::model::MeshId::mint(), "meshafter");
        meshes.put_mesh(&mesh).await.unwrap();
        meshes.claim_mesh_name("meshafter", &mesh.mesh_id).await.unwrap().unwrap();
        let row = NodeRow::new("meshafter.rpc.7".parse().unwrap(), NodeId::mint(), crate::model::NodeStatus::ReadyForTraffic);
        nodes.put_node(&row).await.unwrap();
        nodes.claim_node_name(&row.name, &row.node_id).await.unwrap().unwrap();
        let runtime = RuntimeRow { node_id: NodeId::mint(), name: "meshafter.rpc.8".parse().unwrap(), incarnation_id: IncarnationId::mint(), runtime: rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-after")).unwrap(), data_dir: None };
        nodes.put_runtime(&runtime).await.unwrap();
        let build = crate::build_state::BuildAccepted {
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("after")).unwrap(),
            topology: crate::accepted::FabricTopology::root("fabric1", "meshafter", mesh.mesh_id.clone()),
            submitted_change: None,
            submitted_at_ms: 1,
        };
        builds.publish_accepted(&build).await.unwrap();
        let fact = rdm_core::connections::NodeConnection {
            id: rdm_core::ConnectionId::mint(),
            source: rdm_core::connections::ConnectionEnd { name: name.clone(), node_id: scope.node_id.clone(), incarnation: Some(scope.writer.clone()) },
            destination: rdm_core::connections::ConnectionEnd { name: "mesh1.rpc.2".parse().unwrap(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()) },
            kind: rdm_core::connections::ConnectionKind::Direct,
            alpn: b"storage-provider/1".to_vec(),
            state: rdm_core::connections::ConnectionState::Connected,
            carrier: None,
            recovery: None,
            reason: None,
            logged_at_ms: 1,
        };
        connections.put_connection(&fact).await.unwrap();

        assert_eq!(fabric.fabric().await.unwrap().map(|f| f.name), Some("fabricafter".to_string()), "fabric.storage reads the new provider");
        assert_eq!(fabric.name_claim("fabricafter").await.unwrap(), Some(identity.clone()), "and its name claim");
        assert_eq!(meshes.mesh_named("meshafter").await.unwrap(), Some(mesh.clone()), "mesh.storage reads the new provider");
        assert_eq!(nodes.node(&row.node_id).await.unwrap().map(|r| r.node_id), Some(row.node_id.clone()), "nodes.storage reads the new provider");
        assert_eq!(nodes.name_claim(&row.name).await.unwrap(), Some(row.node_id.clone()), "and its name claims");
        assert_eq!(nodes.runtime(&runtime.node_id, &runtime.incarnation_id).await.unwrap(), Some(runtime.clone()), "and its runtime rows");
        assert_eq!(builds.read_build(&build.build_id).await.unwrap().build_id, build.build_id, "the Build journal reads the new provider");
        assert!(seen.lock().unwrap().contains(&build.build_id), "the derivation installed before the change runs over the new journal's facts");
        assert_eq!(connections.connections().await.unwrap(), vec![fact.clone()], "connections.storage reads the new provider");
        for (store, needle) in [("fabric", "fabricafter".to_string()), ("mesh", mesh.mesh_id.to_string()), ("mesh name", "meshafter".to_string()), ("nodes", row.node_id.to_string()), ("runtime", runtime.node_id.to_string()), ("builds", build.build_id.to_string()), ("connections", fact.id.to_string())] {
            assert!(holds(&new, &needle), "the {store} write after the change lands on the new provider");
            assert!(!holds(&old, &needle), "and nothing of the {store} write after the change on the old one");
        }
        assert_eq!(handle.provider(), "file-new", "the handle names the provider the admin writes now");
    }

    /// A fabric store whose record writes stop at `gate` until it opens, telling `arrived` when one is
    /// there: a write issued and not yet landed.
    #[cfg(feature = "file-backend")]
    struct StalledFabric {
        inner: Arc<dyn FabricStorage>,
        gate: tokio::sync::watch::Sender<bool>,
        arrived: Arc<tokio::sync::Notify>,
    }

    #[cfg(feature = "file-backend")]
    #[async_trait]
    impl FabricStorage for StalledFabric {
        async fn fabric(&self) -> Result<Option<crate::fabric_storage::FabricRecord>, crate::fabric_storage::FabricStorageError> {
            self.inner.fabric().await
        }
        async fn put_fabric(&self, record: &crate::fabric_storage::FabricRecord) -> Result<(), crate::fabric_storage::FabricStorageError> {
            self.arrived.notify_one();
            let _ = self.gate.subscribe().wait_for(|open| *open).await;
            self.inner.put_fabric(record).await
        }
        async fn name_claim(&self, name: &str) -> Result<Option<crate::fabric_storage::FabricIdentity>, crate::fabric_storage::FabricStorageError> {
            self.inner.name_claim(name).await
        }
        async fn put_name_claim(&self, identity: &crate::fabric_storage::FabricIdentity) -> Result<(), crate::fabric_storage::FabricStorageError> {
            self.inner.put_name_claim(identity).await
        }
        async fn remove_name_claim(&self, name: &str) -> Result<(), crate::fabric_storage::FabricStorageError> {
            self.inner.remove_name_claim(name).await
        }
    }

    /// The file provider whose fabric record writes stall at the gate.
    #[cfg(feature = "file-backend")]
    struct StalledFile {
        file: FileBackend,
        gate: tokio::sync::watch::Sender<bool>,
        arrived: Arc<tokio::sync::Notify>,
    }

    #[cfg(feature = "file-backend")]
    #[async_trait]
    impl StorageBackend for StalledFile {
        fn qualify(&self, provider: ProviderKind) -> Result<(), String> {
            self.file.qualify(provider)
        }
        async fn own_row(&self, data_dir: &Path, writer: &IncarnationId) -> Result<Option<NodeRecord>, StorageError> {
            self.file.own_row(data_dir, writer).await
        }
        async fn put_own_row(&self, data_dir: &Path, writer: &IncarnationId, record: &NodeRecord) -> Result<(), StorageError> {
            self.file.put_own_row(data_dir, writer, record).await
        }
        async fn open(&self, scope: &StorageScope) -> Result<StorageBoundaries, String> {
            let opened = self.file.open(scope).await?;
            Ok(StorageBoundaries { fabric: Arc::new(StalledFabric { inner: opened.fabric.clone(), gate: self.gate.clone(), arrived: self.arrived.clone() }), ..opened })
        }
    }

    /// CONTRACT (storage.md §2, "Changing it", Finish outstanding writes: "Each node brings every write
    /// and claim operation it issued before the block to a durable outcome on the old provider"; i149
    /// e2: "No replacement succeeds while an unaccounted issued write can complete"): a write issued
    /// through a node-admin's handle and not yet landed holds the change; the change returns only once
    /// that write is on the old provider, and the new provider holds none of it. Must NOT happen: the
    /// change returning while a write issued before it can still land on the old provider.
    #[cfg(feature = "file-backend")]
    #[tokio::test]
    async fn a_provider_change_waits_for_already_issued_writes() {
        use crate::fabric_storage::{FabricIdentity, FabricRecord};
        let (old, new) = (crate::test_support::tempdir("handle-wait-old"), crate::test_support::tempdir("handle-wait-new"));
        let (gate, arrived) = (tokio::sync::watch::channel(false).0, Arc::new(tokio::sync::Notify::new()));
        let stalled = StalledFile { file: FileBackend::at(&old), gate: gate.clone(), arrived: arrived.clone() };
        let providers = TopologyStorageProviders::new().register("file", Arc::new(stalled)).register("file-new", Arc::new(FileBackend::at(&new)));
        let scope = scope_in(crate::test_support::tempdir("handle-wait-data"));
        let handle = TopologyStorageHandle::open(&providers, "file", scope.clone()).await.unwrap_or_else(|e| panic!("the admin opens its provider: {e}"));
        let fabric = handle.fabric();
        let write = tokio::spawn({
            let (fabric, record) = (fabric.clone(), FabricRecord::new(FabricIdentity { fabric_id: scope.fabric_id.clone(), name: "fabricissued".into() }));
            async move { fabric.put_fabric(&record).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), arrived.notified()).await.expect("the write reaches the old provider and stalls there");

        let change = tokio::spawn({
            let handle = handle.clone();
            async move { handle.replace("file-new").await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert!(!change.is_finished(), "the change waits for the write already issued through the old provider");

        gate.send_replace(true);
        tokio::time::timeout(std::time::Duration::from_secs(5), change).await.expect("the change returns once the issued write has landed").unwrap().unwrap_or_else(|e| panic!("the provider is replaced: {e}"));
        assert!(holds(&old, "fabricissued"), "when the change returns the issued write has landed on the old provider");
        assert!(!holds(&new, "fabricissued"), "and nothing of it on the new one");
        write.await.unwrap().expect("the issued write is answered once it has landed");
        assert_eq!(handle.provider(), "file-new");
    }

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
