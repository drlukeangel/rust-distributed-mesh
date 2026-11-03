//! The storage backend a node-admin runs on (build.md: "production backends for the imported storage
//! interfaces"). It opens the six storage boundaries, `fabric.storage` ([`FabricStorage`]),
//! `mesh.storage` ([`MeshStorage`]), `nodes.storage` ([`NodesStorage`]), `connections.storage`
//! ([`ConnectionsStorage`]), the Build journal ([`LocalBuildLog`]) and `events.storage`
//! ([`EventsStorage`]), and every durable read and
//! write of the admin goes through them. A fact lives in the store of the object it describes. The admin's
//! boot takes its backend beside its `Wiring` (`admin::start_on`); RDM standalone's is
//! [`FileBackend`] (the `file-backend` feature, the default), the boundaries as files on the fabric's
//! store root and the admin's data dir. [`MemoryBackend`] holds one process's rows.

use crate::build_state::LocalBuildLog;
use crate::events_storage::EventsStorage;
use crate::fabric_handover::FinishWrites;
use crate::fabric_storage::FabricStorage;
use crate::model::{FabricId, IncarnationId, NodeId, PathName, ProviderKind};
use crate::storage::{ConnectionsStorage, MeshStorage, NodeRecord, NodesStorage};
use crate::storage_error::StorageError;
use async_trait::async_trait;
use futures_util::future::BoxFuture;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::Instrument;

/// Where a node-admin's storage opens, decided by its boot before any boundary opens
/// (`docs/architecture/storage.md` §2).
#[derive(Debug, Clone)]
pub struct StorageScope {
    /// The admin's own data dir: its local identity, on every backend.
    pub data_dir: PathBuf,
    /// The Fabric the admin belongs to.
    pub fabric_id: FabricId,
    /// The mesh the admin belongs to: its Build section and its nodes' rows
    /// (`docs/architecture/storage.md` §2, `docs/architecture/build.md` §5.3).
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

/// A node's own connections rows, opened by its globally unique node ID.
pub type OpenConnections = Arc<dyn Fn(NodeId) -> BoxFuture<'static, Result<Arc<dyn ConnectionsStorage>, StorageError>> + Send + Sync>;

/// The storage a node-admin runs on, opened by its backend (`docs/architecture/storage.md` §2): the
/// six boundaries and how it finishes the writes it has issued.
pub struct StorageBoundaries {
    /// `fabric.storage`: the Fabric record, its name's claim and its events.
    pub fabric: Arc<dyn FabricStorage>,
    /// `mesh.storage`: the Mesh rows, each mesh's in-flight row and status facts.
    pub meshes: Arc<dyn MeshStorage>,
    /// `nodes.storage`: the runtimes the admin started, every node's record in its two parts (Q333),
    /// its mesh-primary's row with the facts on each birth, primaries included, and the node's own part.
    pub nodes: Arc<dyn NodesStorage>,
    /// `nodes.storage`'s runtime rows on the fabric's shared store root, in no admin's data dir: where an
    /// externally started node-admin (the Day-0 admin, an adoption launch), whose runtime no launcher
    /// recorded, keeps its own runtime row, and where a later adoption launch on the same store root
    /// reads it (deployment-provider.md §3, the externally started Day-0 adoption and the successor's
    /// management cut). `None` on a backend whose rows no other process reads.
    pub root_runtimes: Option<Arc<dyn NodesStorage>>,
    /// `connections.storage`: the admin's own connections rows, by its `(path, name)`.
    pub connections: OpenConnections,
    /// The Build journal: the Build facts, and each attempt's context.
    pub builds: Arc<dyn LocalBuildLog>,
    /// `events.storage`: what happened while executing each Build, and each node's investigations
    /// (build.md §5).
    pub events: Arc<dyn EventsStorage>,
    /// Finishes every write the boundaries have issued: a primary runs it before it calls its successor
    /// (node-primary-update.md step 3).
    pub finish_writes: FinishWrites,
    /// Where the boundaries write now, as the boot's storage span records it.
    pub writes_to: String,
}

/// The backend a node-admin's boundaries are opened from (`docs/architecture/storage.md` §2).
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
    /// Opens the six boundaries of `scope`.
    async fn open(&self, scope: &StorageScope) -> Result<StorageBoundaries, String>;
    /// Delete the topology storage of the Fabric `fabric_id`: its six stores and the admin's own row and
    /// Build attempt contexts in `data_dir`. Persistence only: the admin's boot decides when (Q200). A
    /// backend that deletes nothing refuses by name.
    async fn delete_fabric(&self, data_dir: &Path, fabric_id: &FabricId) -> Result<(), String> {
        let _ = (data_dir, fabric_id);
        Err("this topology storage provider deletes no fabric's storage".into())
    }
    /// This backend's six stores as their lines and its claims for the fabric `fabric_id`, registered
    /// as `provider`: what a storage change's copy reads from the old provider and appends to the new
    /// one (storage.md §2 Copy, [`rdm_node::storage_copy`]). A backend that answers none refuses by name
    /// ([`StorageError::Refused`]).
    #[cfg(feature = "file-backend")]
    async fn lines(&self, provider: &str, fabric_id: &FabricId) -> Result<Arc<dyn rdm_node::storage_copy::ProviderLines>, StorageError> {
        let _ = fabric_id;
        Err(StorageError::Refused { file: format!("the topology storage provider `{provider}`"), reason: "it answers no lines and claims to copy (storage.md §2 Copy)".into() })
    }
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
        crate::storage::own_in(data_dir, writer).await
    }

    async fn put_own_row(&self, data_dir: &Path, writer: &IncarnationId, record: &NodeRecord) -> Result<(), StorageError> {
        crate::storage::put_own_in(data_dir, writer, record).await
    }

    async fn delete_fabric(&self, data_dir: &Path, fabric_id: &FabricId) -> Result<(), String> {
        // The fabric's root on the store dir (R14), then the admin's own stores in its data dir: its own
        // row (`nodes/`). Custody (`node-key`, `ca-secret`, `fabric-ca`,
        // `declared-secrets`) is no topology storage and stays.
        let dirs = [self.store_dir.join(fabric_id.to_string()), data_dir.join("nodes")];
        crate::blocking::blocking_io(move || {
            for dir in &dirs {
                match std::fs::remove_dir_all(dir) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(format!("{}: {e}", dir.display())),
                }
            }
            Ok(())
        })
        .await
    }

    async fn lines(&self, provider: &str, fabric_id: &FabricId) -> Result<Arc<dyn rdm_node::storage_copy::ProviderLines>, StorageError> {
        Ok(Arc::new(rdm_node::storage_copy::FileProviderLines::open(provider, &self.store_dir, fabric_id).await?))
    }

    async fn open(&self, scope: &StorageScope) -> Result<StorageBoundaries, String> {
        let storage_err = |e: StorageError| e.to_string();
        // The root's directory and layout marker are made and fsynced on the blocking pool: no worker
        // thread waits on the disk (rdm-node/src/blocking.rs).
        let (store_dir, fabric_id) = (self.store_dir.clone(), scope.fabric_id.clone());
        let root = crate::blocking::blocking_io(move || crate::store_root::StoreRoot::open(&store_dir, &fabric_id)).await.map_err(|e| format!("refusing to start: {e}"))?;
        let writes_to = "store-root".to_string();
        let fabric = Arc::new(crate::fabric_storage::FileFabricStorage::open(&root, &scope.log_writer()).await.map_err(storage_err)?);
        let meshes = Arc::new(crate::storage::FileMeshStorage::open(&root, &scope.log_writer()).await.map_err(storage_err)?);
        let nodes = Arc::new(crate::storage::FileNodesStorage::open(&scope.data_dir, &root, &scope.own_mesh, &scope.log_writer()).await.map_err(storage_err)?);
        // Every runtime row is a line of the root's nodes.jsonl: the root's rows are the nodes store's.
        let root_runtimes: Option<Arc<dyn NodesStorage>> = Some(nodes.clone() as Arc<dyn NodesStorage>);
        let events = Arc::new(crate::events_storage::FileEventsStorage::open(&root, &scope.log_writer()).await.map_err(storage_err)?);
        let builds = Arc::new(crate::build_state::FileJournal::open_on_root(&root, &scope.log_writer(), events.clone()).await.map_err(|e| e.to_string())?);
        let finish_writes: FinishWrites = {
            let (meshes, nodes, fabric, events) = (meshes.clone(), nodes.clone(), fabric.clone(), events.clone());
            Arc::new(move || {
                let (meshes, nodes, fabric, events) = (meshes.clone(), nodes.clone(), fabric.clone(), events.clone());
                Box::pin(async move {
                    meshes.finish_issued_writes().await;
                    nodes.finish_issued_writes().await;
                    fabric.finish_issued_writes().await;
                    events.finish_issued_writes().await;
                })
            })
        };
        let connections: OpenConnections = {
            let (root, writer) = (root.clone(), scope.log_writer());
            Arc::new(move |node_id: NodeId| {
                let (root, writer) = (root.clone(), writer.clone());
                Box::pin(async move { if node_id != writer.node_id { return Err(StorageError::Refused { file: "connections.storage".into(), reason: "connection store node ID differs from its writer".into() }); } Ok(Arc::new(crate::storage::FileConnectionsStorage::open(&root, &writer).await?) as Arc<dyn ConnectionsStorage>) })
            })
        };
        Ok(StorageBoundaries { fabric, meshes, nodes, root_runtimes, connections, builds, events, finish_writes, writes_to })
    }
}

#[cfg(feature = "file-backend")]
/// RDM standalone's storage backend on the shared store dir `store_dir`
/// (`docs/architecture/storage.md` §2): the one a node-admin's `start`, `start_with` and `entry::run`
/// construct.
pub fn file_backend(store_dir: impl Into<PathBuf>) -> Arc<dyn StorageBackend> {
    Arc::new(FileBackend::at(store_dir))
}

/// The topology storage providers a node-admin executable registers in its wiring, by name
/// (`docs/architecture/storage.md` §2): each a backend of the six record stores, its name bound to
/// its place at registration. The admin opens the one its `topology_storage_provider` names
/// (`RDM_TOPOLOGY_STORAGE_PROVIDER` on Day 0, its launch's when launched, its own row's on a start
/// with no launcher); as the fabric-primary it refuses at acceptance a Build naming one this registry
/// does not hold or that does not open.
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

    /// The lines and claims of the provider registered as `name` for the fabric `fabric_id`: a storage
    /// change's copy reads the old provider's and appends to the new one's (storage.md §2 Copy,
    /// [`rdm_node::storage_copy::copy`]). Refused by name ([`StorageError::Refused`]) as [`Self::named`]
    /// refuses, or when the provider answers none; an error opening them is the backend's, its kind kept.
    #[cfg(feature = "file-backend")]
    pub async fn lines(&self, name: &str, fabric_id: &FabricId) -> Result<Arc<dyn rdm_node::storage_copy::ProviderLines>, StorageError> {
        let backend = self.named(name).map_err(|reason| StorageError::Refused { file: format!("the topology storage provider `{name}`"), reason })?;
        backend.lines(name, fabric_id).await
    }
}

/// The stores one provider opened for a node-admin: its boundaries, and its connections rows at each
/// node ID the admin opened them for.
struct AdminStores {
    boundaries: StorageBoundaries,
    connections: std::sync::Mutex<std::collections::HashMap<NodeId, Arc<dyn ConnectionsStorage>>>,
}

impl AdminStores {
    fn connections_at(&self, node_id: &NodeId) -> Result<Arc<dyn ConnectionsStorage>, StorageError> {
        self.connections.lock().unwrap_or_else(|e| e.into_inner()).get(node_id).cloned().ok_or_else(|| StorageError::Io { file: "connections.storage".into(), reason: format!("no connections rows are open for node {node_id}") })
    }
}

/// A node-admin's one access point to its topology storage (storage.md §2): every reader and writer
/// on the admin reaches the six stores through it, never through a store held from its boot, so a
/// provider change replaces the provider beneath all of them ("Each node moves to the new provider").
/// The writers' own copies of their records (`crate::record_cache::Caches`) are theirs, outside it,
/// and a replacement leaves them as they are.
pub struct TopologyStorageHandle {
    current: crate::storage_handle::StorageHandle<AdminStores>,
    /// The providers the admin's executable registers: a replacement opens one of them.
    providers: TopologyStorageProviders,
    /// Where the admin's stores open.
    scope: StorageScope,
    /// The path and names the admin's own connections rows were opened at: a replacement opens each on
    /// the new provider.
    connection_nodes: std::sync::Mutex<Vec<NodeId>>,
    /// The derivation installed on the Build journal (`LocalBuildLog::derive_on_every_fact`): a
    /// replacement installs it on the new provider's journal, which derives over each fact as the fact
    /// enters its log (its first read of the store root takes the facts the store holds).
    derivation: std::sync::Mutex<Option<crate::build_state::FactDerivation>>,
    /// Whether the admin reads the Build records now (`LocalBuildLog::read_records_while`, Q227): a
    /// replacement sets it on the new provider's journal.
    records_reader: std::sync::Mutex<Option<crate::build_state::RecordsReader>>,
    /// One replacement at a time, and no connections open or derivation installed during one.
    replacing: tokio::sync::Mutex<()>,
    /// Told the provider the admin writes each time it moves to another.
    provider_written: std::sync::Mutex<Option<Box<dyn Fn(&str) + Send + Sync>>>,
    /// The admin's own row in its data dir, as it last wrote it ([`Self::put_own_row`]): a move writes
    /// it again naming the provider it moves onto.
    own_row: std::sync::Mutex<Option<NodeRecord>>,
}

impl TopologyStorageHandle {
    /// Where the admin's stores open: its fabric, mesh and the writer its lines name.
    pub fn scope(&self) -> &StorageScope {
        &self.scope
    }

    /// Open the provider `providers` registers as `provider` on `scope`, else the admin's start is
    /// refused by name.
    pub async fn open(providers: &TopologyStorageProviders, provider: &str, scope: StorageScope) -> Result<Arc<Self>, String> {
        let boundaries = providers.named(provider).map_err(|e| format!("refusing to start: {e}"))?.open(&scope).await?;
        Ok(Arc::new(Self {
            current: crate::storage_handle::StorageHandle::new(provider, AdminStores { boundaries, connections: Default::default() }),
            providers: providers.clone(),
            scope,
            connection_nodes: Default::default(),
            derivation: Default::default(),
            records_reader: Default::default(),
            replacing: tokio::sync::Mutex::new(()),
            provider_written: Default::default(),
            own_row: Default::default(),
        }))
    }

    /// The provider the admin writes now.
    pub fn provider(&self) -> String {
        self.current.read().provider().to_string()
    }

    /// Where the boundaries write now, as the boot's storage span records it.
    pub fn writes_to(&self) -> String {
        self.current.read().stores().boundaries.writes_to.clone()
    }

    /// `fabric.storage`, through this handle.
    pub fn fabric(self: &Arc<Self>) -> Arc<dyn FabricStorage> {
        Arc::new(Through(self.clone()))
    }

    /// `mesh.storage`, through this handle.
    pub fn meshes(self: &Arc<Self>) -> Arc<dyn MeshStorage> {
        Arc::new(Through(self.clone()))
    }

    /// `nodes.storage`, through this handle.
    pub fn nodes(self: &Arc<Self>) -> Arc<dyn NodesStorage> {
        Arc::new(Through(self.clone()))
    }

    /// `nodes.storage`'s runtime rows on the fabric's shared store root, through this handle; `None`
    /// on a provider whose rows no other process reads.
    pub fn root_runtimes(self: &Arc<Self>) -> Option<Arc<dyn NodesStorage>> {
        self.current.read().stores().boundaries.root_runtimes.as_ref().map(|_| Arc::new(RootRuntimes(self.clone())) as Arc<dyn NodesStorage>)
    }

    /// The Build journal, through this handle.
    pub fn builds(self: &Arc<Self>) -> Arc<dyn LocalBuildLog> {
        Arc::new(Through(self.clone()))
    }

    /// `events.storage`, through this handle.
    pub fn events(self: &Arc<Self>) -> Arc<dyn EventsStorage> {
        Arc::new(Through(self.clone()))
    }

    /// The admin's own connections rows for `node_id`, opened on the provider it writes now, through this
    /// handle.
    pub async fn connections(self: &Arc<Self>, node_id: NodeId) -> Result<Arc<dyn ConnectionsStorage>, StorageError> {
        let _one = self.replacing.lock().await;
        let current = self.current.read();
        let rows = (current.stores().boundaries.connections)(node_id.clone()).await?;
        current.stores().connections.lock().unwrap_or_else(|e| e.into_inner()).insert(node_id.clone(), rows);
        self.connection_nodes.lock().unwrap_or_else(|e| e.into_inner()).push(node_id.clone());
        Ok(Arc::new(ConnectionsThrough { handle: self.clone(), node_id }))
    }

    /// Finishes every write the provider the admin writes at the call has issued (node-primary-update.md
    /// step 3).
    pub fn finish_writes(self: &Arc<Self>) -> FinishWrites {
        let handle = self.clone();
        Arc::new(move || {
            let current = handle.current.read();
            Box::pin(async move { (current.stores().boundaries.finish_writes)().await })
        })
    }

    /// Keep `record` as the admin's own row in its data dir, naming the provider it writes now (storage.md
    /// §2: a start with no launcher opens "its own row in its data dir"'s provider), through the
    /// provider it writes; each later move writes it again naming the provider it moves onto.
    pub async fn put_own_row(&self, mut record: NodeRecord) -> Result<(), StorageError> {
        let _one = self.replacing.lock().await;
        let provider = self.provider();
        record.topology_storage_provider = Some(provider.clone());
        let backend = self.providers.named(&provider).map_err(|reason| StorageError::Io { file: "nodes (own row)".into(), reason })?;
        backend.put_own_row(&self.scope.data_dir, &self.scope.writer, &record).await?;
        *self.own_row.lock().unwrap_or_else(|e| e.into_inner()) = Some(record);
        Ok(())
    }

    /// Open the provider this executable registers as `provider` on this admin's scope, and close it
    /// again: what the fabric-primary's acceptance of a storage change asks before anything is accepted
    /// (storage.md §2 "Changing it": "Acceptance refuses by name a provider the fabric-primary's wiring
    /// does not register or cannot open"). Opened for the change is opened for its copy too: the
    /// provider's lines are opened and every store the copy reads, and its claims, are read. A name not
    /// registered, a provider that does not open, or lines that do not open or are not answered (a named
    /// refusal and no answer alike: acceptance is a request, and no block waits on it) are refused by
    /// name.
    pub async fn opens(&self, provider: &str) -> Result<(), String> {
        let boundaries = self.providers.named(provider)?.open(&self.scope).await?;
        drop(boundaries);
        self.copy_opens(provider).await
    }

    /// The lines of `provider` a storage change's copy reads, opened and read once.
    #[cfg(feature = "file-backend")]
    async fn copy_opens(&self, provider: &str) -> Result<(), String> {
        let lines = self.providers.lines(provider, &self.scope.fabric_id).await.map_err(|e| format!("its lines do not open for the copy: {e}"))?;
        for store in rdm_node::storage_copy::Store::ALL {
            lines.lines(store).await.map_err(|e| format!("its {store} store's lines are not read for the copy: {e}"))?;
        }
        Ok(())
    }

    /// A build without the file backend copies no provider's lines (`storage_change::copy_stores`).
    #[cfg(not(feature = "file-backend"))]
    async fn copy_opens(&self, provider: &str) -> Result<(), String> {
        Err(format!("this build copies no provider's lines, `{provider}`'s included"))
    }

    /// Call `written` with the provider the admin writes each time it moves to another, so the rows it
    /// writes record it (storage.md §2: "The same name on a node's row is the provider that node
    /// writes").
    pub fn on_provider_written(&self, written: impl Fn(&str) + Send + Sync + 'static) {
        *self.provider_written.lock().unwrap_or_else(|e| e.into_inner()) = Some(Box::new(written));
    }

    /// Replace the provider with the one this executable registers as `provider`, opened now, off the
    /// admin's boot, with the admin's connections rows for each node ID it opened them for and the Build
    /// journal's derivation installed: from the swap every call goes to it, and this returns once every
    /// write admitted to the old provider has been answered there and the old provider has finished
    /// the writes it issued ("Finish outstanding writes"). A name the executable does not register, or
    /// a provider that does not open, is refused by name and nothing moves.
    pub async fn replace(&self, provider: &str) -> Result<(), String> {
        if self.provider() == provider {
            return Ok(());
        }
        let _one = self.replacing.lock().await;
        let span = tracing::info_span!("rdm.node_admin.storage.update.via-open", node_id = %self.scope.node_id, provider = %provider, outcome = tracing::field::Empty);
        let opened = async {
            let boundaries = self.providers.named(provider)?.open(&self.scope).await?;
            let mut connections = std::collections::HashMap::new();
            let node_ids = self.connection_nodes.lock().unwrap_or_else(|e| e.into_inner()).clone();
            for node_id in node_ids {
                let rows = (boundaries.connections)(node_id.clone()).await.map_err(|e| format!("connections storage for node {node_id}: {e}"))?;
                connections.insert(node_id, rows);
            }
            let derivation = self.derivation.lock().unwrap_or_else(|e| e.into_inner()).clone();
            if let Some(derivation) = derivation {
                boundaries.builds.derive_on_every_fact(derivation).await;
            }
            // The new journal holds what the old one held: an admin that reads no Build record (Q227)
            // takes none from the new provider's store, and the fabric-primary reads on from it.
            let reader = self.records_reader.lock().unwrap_or_else(|e| e.into_inner()).clone();
            if let Some(reader) = reader {
                boundaries.builds.read_records_while(reader).await;
            }
            let held = self.current.read().stores().boundaries.builds.facts().await.map_err(|e| format!("the Build facts this admin holds could not be read for the provider moved onto: {e}"))?;
            boundaries.builds.absorb_build(&held).await;
            // The own row in the data dir names the provider moved onto before the move answers, so a
            // start with no launcher opens it (storage.md §2 "The property").
            let own = self.own_row.lock().unwrap_or_else(|e| e.into_inner()).clone();
            if let Some(mut own) = own {
                own.topology_storage_provider = Some(provider.to_string());
                self.providers.named(provider)?.put_own_row(&self.scope.data_dir, &self.scope.writer, &own).await.map_err(|e| format!("the own row in {}: {e}", self.scope.data_dir.display()))?;
                *self.own_row.lock().unwrap_or_else(|e| e.into_inner()) = Some(own);
            }
            Ok::<_, String>(AdminStores { boundaries, connections: std::sync::Mutex::new(connections) })
        }
        .instrument(span.clone())
        .await;
        let opened = match opened {
            Ok(opened) => opened,
            Err(e) => {
                span.record("outcome", format!("refused: {e}").as_str());
                return Err(e);
            }
        };
        span.record("outcome", "opened");
        let old = self.current.replace(provider, opened).await;
        (old.stores().boundaries.finish_writes)().await;
        if let Some(written) = &*self.provider_written.lock().unwrap_or_else(|e| e.into_inner()) {
            written(provider);
        }
        Ok(())
    }
}

/// One of the six stores through a node-admin's [`TopologyStorageHandle`]: every read goes to the
/// provider the admin writes at that call, and every write is admitted to it.
struct Through(Arc<TopologyStorageHandle>);

impl Through {
    fn read(&self) -> Arc<crate::storage_handle::Opened<AdminStores>> {
        self.0.current.read()
    }
    fn write(&self) -> crate::storage_handle::Admitted<AdminStores> {
        self.0.current.write()
    }
}

#[async_trait]
impl FabricStorage for Through {
    async fn fabric(&self) -> Result<Option<crate::fabric_storage::FabricRecord>, crate::fabric_storage::FabricStorageError> {
        self.read().stores().boundaries.fabric.fabric().await
    }
    async fn put_fabric(&self, record: &crate::fabric_storage::FabricRecord) -> Result<(), crate::fabric_storage::FabricStorageError> {
        let w = self.write();
        w.boundaries.fabric.put_fabric(record).await
    }
}

#[async_trait]
impl MeshStorage for Through {
    async fn mesh(&self, mesh_id: &crate::model::MeshId) -> Result<Option<crate::storage::MeshRecord>, StorageError> {
        self.read().stores().boundaries.meshes.mesh(mesh_id).await
    }
    async fn mesh_named(&self, name: &str) -> Result<Option<crate::storage::MeshRecord>, StorageError> {
        self.read().stores().boundaries.meshes.mesh_named(name).await
    }
    async fn put_mesh(&self, record: &crate::storage::MeshRecord) -> Result<(), StorageError> {
        let w = self.write();
        w.boundaries.meshes.put_mesh(record).await
    }
    async fn meshes(&self) -> Result<Vec<crate::storage::MeshRecord>, StorageError> {
        self.read().stores().boundaries.meshes.meshes().await
    }
}

/// `nodes.storage`'s calls, delegated to `$nodes` of the provider the admin writes at each call.
macro_rules! nodes_through {
    ($ty:ty, $nodes:ident) => {
        #[async_trait]
        impl NodesStorage for $ty {
            async fn put_runtime(&self, row: &crate::storage::RuntimeRow) -> Result<(), StorageError> {
                let w = self.0.current.write();
                $nodes(&w.boundaries)?.put_runtime(row).await
            }
            async fn runtimes(&self) -> Result<Vec<crate::storage::RuntimeRow>, StorageError> {
                let r = self.0.current.read();
                $nodes(&r.stores().boundaries)?.runtimes().await
            }
            async fn runtime(&self, node_id: &crate::model::NodeId, incarnation: &IncarnationId) -> Result<Option<crate::storage::RuntimeRow>, StorageError> {
                let r = self.0.current.read();
                $nodes(&r.stores().boundaries)?.runtime(node_id, incarnation).await
            }
            async fn node(&self, node_id: &crate::model::NodeId) -> Result<Option<crate::storage::NodeRow>, StorageError> {
                let r = self.0.current.read();
                $nodes(&r.stores().boundaries)?.node(node_id).await
            }
            async fn nodes(&self, mesh: &str) -> Result<Vec<crate::storage::NodeRow>, StorageError> {
                let r = self.0.current.read();
                $nodes(&r.stores().boundaries)?.nodes(mesh).await
            }
            async fn put_node(&self, row: &crate::storage::NodeRow) -> Result<(), StorageError> {
                let w = self.0.current.write();
                $nodes(&w.boundaries)?.put_node(row).await
            }
            async fn put_properties(&self, record: &rdm_node::node_records::NodeProperties) -> Result<(), StorageError> {
                let w = self.0.current.write();
                $nodes(&w.boundaries)?.put_properties(record).await
            }
            async fn properties(&self, node_id: &crate::model::NodeId) -> Result<Option<rdm_node::node_records::NodeProperties>, StorageError> {
                let r = self.0.current.read();
                $nodes(&r.stores().boundaries)?.properties(node_id).await
            }
            async fn records_at(&self, name: &PathName) -> Result<Vec<crate::storage::NodeRow>, StorageError> {
                let r = self.0.current.read();
                $nodes(&r.stores().boundaries)?.records_at(name).await
            }
        }
    };
}

/// `nodes.storage` of `b`.
fn nodes_of(b: &StorageBoundaries) -> Result<Arc<dyn NodesStorage>, StorageError> {
    Ok(b.nodes.clone())
}

nodes_through!(Through, nodes_of);

/// `nodes.storage`'s runtime rows on the store root through a node-admin's [`TopologyStorageHandle`].
struct RootRuntimes(Arc<TopologyStorageHandle>);

/// `nodes.storage`'s runtime rows on the store root of `b`, refused by name on a provider that keeps none.
fn root_runtimes_of(b: &StorageBoundaries) -> Result<Arc<dyn NodesStorage>, StorageError> {
    b.root_runtimes.clone().ok_or_else(|| StorageError::Io { file: "nodes.storage".into(), reason: "the topology storage provider the admin writes keeps no runtime rows on a shared store root".into() })
}

nodes_through!(RootRuntimes, root_runtimes_of);

#[async_trait]
impl crate::build_state::BuildStateAdapter for Through {
    async fn publish_accepted(&self, accepted: &crate::build_state::BuildAccepted) -> Result<(), crate::build_state::BuildStateError> {
        let w = self.write();
        w.boundaries.builds.publish_accepted(accepted).await
    }
    async fn read_build(&self, build_id: &crate::build::BuildId) -> Result<crate::build_state::BuildProjection, crate::build_state::BuildStateError> {
        self.read().stores().boundaries.builds.read_build(build_id).await
    }
    async fn current_build(&self) -> Result<crate::build_state::CurrentBuild, crate::build_state::BuildStateError> {
        self.read().stores().boundaries.builds.current_build().await
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, crate::build_state::BuildStateError> {
        self.read().stores().boundaries.builds.newest_accepted_at_ms().await
    }
    async fn accepted_newest_first(&self) -> Result<Vec<crate::build::BuildId>, crate::build_state::BuildStateError> {
        self.read().stores().boundaries.builds.accepted_newest_first().await
    }
    async fn forget(&self, build_id: &crate::build::BuildId) -> Result<(), crate::build_state::BuildStateError> {
        let w = self.write();
        w.boundaries.builds.forget(build_id).await
    }
    async fn note_blocked(&self, build_id: &crate::build::BuildId, operation: &rdm_core::OperationId, step: &str, reason: &str) {
        self.read().stores().boundaries.builds.note_blocked(build_id, operation, step, reason).await
    }
    async fn write_step(&self, step: &crate::build_state::StepWrite) -> Result<(), crate::build_state::BuildStateError> {
        self.read().stores().boundaries.builds.write_step(step).await
    }
    fn events(&self) -> Arc<dyn crate::events_storage::EventsStorage> {
        self.read().stores().boundaries.builds.events()
    }
    fn held_name(&self) -> Option<PathName> {
        self.read().stores().boundaries.builds.held_name()
    }
}

#[async_trait]
impl LocalBuildLog for Through {
    async fn absorb_facts(&self, facts: &[crate::build_state::BuildFact]) {
        let w = self.write();
        w.boundaries.builds.absorb_facts(facts).await
    }
    async fn derive_on_every_fact(&self, derive: crate::build_state::FactDerivation) {
        let _one = self.0.replacing.lock().await;
        *self.0.derivation.lock().unwrap_or_else(|e| e.into_inner()) = Some(derive.clone());
        self.read().stores().boundaries.builds.derive_on_every_fact(derive).await
    }
    async fn absorb_build(&self, facts: &[crate::build_state::BuildFact]) {
        let w = self.write();
        w.boundaries.builds.absorb_build(facts).await
    }
    async fn facts(&self) -> Result<Vec<crate::build_state::BuildFact>, crate::build_state::BuildStateError> {
        self.read().stores().boundaries.builds.facts().await
    }
    async fn committed_facts(&self, build: Option<&crate::build::BuildId>) -> Result<Vec<crate::build_state::BuildFact>, crate::build_state::BuildStateError> {
        self.read().stores().boundaries.builds.committed_facts(build).await
    }
    async fn held_build(&self, build_id: &crate::build::BuildId) -> Result<crate::build_state::BuildProjection, crate::build_state::BuildStateError> {
        self.read().stores().boundaries.builds.held_build(build_id).await
    }
    async fn held_current_build(&self) -> Result<crate::build_state::CurrentBuild, crate::build_state::BuildStateError> {
        self.read().stores().boundaries.builds.held_current_build().await
    }
    async fn read_records_now(&self, op: &'static str, node: &str) -> Result<(), crate::build_state::BuildStateError> {
        self.read().stores().boundaries.builds.read_records_now(op, node).await
    }
    async fn read_records_while(&self, reader: crate::build_state::RecordsReader) {
        let _one = self.0.replacing.lock().await;
        *self.0.records_reader.lock().unwrap_or_else(|e| e.into_inner()) = Some(reader.clone());
        self.read().stores().boundaries.builds.read_records_while(reader).await
    }
}

#[async_trait]
impl EventsStorage for Through {
    async fn append(&self, event: &crate::events_storage::Event) -> Result<(), StorageError> {
        let w = self.write();
        w.boundaries.events.append(event).await
    }
    async fn event(&self, id: &crate::events_storage::EventId) -> Result<Option<crate::events_storage::Event>, StorageError> {
        self.read().stores().boundaries.events.event(id).await
    }
    async fn of_build(&self, build_id: &crate::build::BuildId) -> Result<Vec<crate::events_storage::Event>, StorageError> {
        self.read().stores().boundaries.events.of_build(build_id).await
    }
    async fn of_node(&self, node_id: &NodeId) -> Result<Vec<crate::events_storage::Event>, StorageError> {
        self.read().stores().boundaries.events.of_node(node_id).await
    }
    async fn of_storage_changes(&self) -> Result<Vec<crate::events_storage::Event>, StorageError> {
        self.read().stores().boundaries.events.of_storage_changes().await
    }
}

/// The admin's own connections rows by node ID through its [`TopologyStorageHandle`].
struct ConnectionsThrough {
    handle: Arc<TopologyStorageHandle>,
    node_id: NodeId,
}

#[async_trait]
impl ConnectionsStorage for ConnectionsThrough {
    async fn put_connection(&self, fact: &rdm_core::connections::NodeConnection) -> Result<(), StorageError> {
        let w = self.handle.current.write();
        w.connections_at(&self.node_id)?.put_connection(fact).await
    }
    async fn connections(&self) -> Result<Vec<rdm_core::connections::NodeConnection>, StorageError> {
        let rows = self.handle.current.read().stores().connections_at(&self.node_id)?;
        rows.connections().await
    }
    async fn remove_connection(&self, index: &rdm_core::connections::ConnectionIndex) -> Result<(), StorageError> {
        let w = self.handle.current.write();
        w.connections_at(&self.node_id)?.remove_connection(index).await
    }
    async fn append_history(&self, fact: &rdm_core::connections::NodeConnection) -> Result<(), StorageError> {
        let w = self.handle.current.write();
        w.connections_at(&self.node_id)?.append_history(fact).await
    }
    async fn history(&self) -> Result<Vec<rdm_core::connections::NodeConnection>, StorageError> {
        let rows = self.handle.current.read().stores().connections_at(&self.node_id)?;
        rows.history().await
    }
    async fn connections_to(&self, destination: &crate::model::NodeId) -> Result<Vec<rdm_core::connections::NodeConnection>, StorageError> {
        let rows = self.handle.current.read().stores().connections_at(&self.node_id)?;
        rows.connections_to(destination).await
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
        let connections: OpenConnections = Arc::new(|_node_id: NodeId| Box::pin(async { Ok(Arc::new(crate::storage::MemoryConnectionsStorage::default()) as Arc<dyn ConnectionsStorage>) }));
        // A memory write is one step under its store's lock: every write issued has finished.
        let finish_writes: FinishWrites = Arc::new(|| Box::pin(async {}));
        // One events store: the Build store folds a Build's state from the events the fabric-primary
        // writes through the boundary, as the file backend opens its journal over its events store.
        let builds = Arc::new(crate::build_state::MemoryBuildStateAdapter::new());
        let events = crate::build_state::BuildStateAdapter::events(&*builds);
        Ok(StorageBoundaries {
            fabric: Arc::new(crate::fabric_storage::MemoryFabricStorage::new()),
            meshes: Arc::new(crate::storage::MemoryMeshStorage::default()),
            nodes: Arc::new(crate::storage::MemoryNodesStorage::default()),
            root_runtimes: None,
            connections,
            builds,
            events,
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
    /// CONTRACT (storage.md §2 Copy: the fabric-primary "appends every line of every store on the old
    /// provider to the new one"): the registry answers the lines of a provider by its registered name,
    /// the provider's fold spans naming it; a provider whose backend answers none, and a name the
    /// executable does not register, are refused by name. Must NOT happen: a copy opened on a provider
    /// that cannot answer its lines, or on an unregistered name.
    #[cfg(feature = "file-backend")]
    #[tokio::test]
    async fn a_registered_provider_answers_its_lines_by_its_name_and_one_that_answers_none_is_refused_by_name() {
        let store = crate::test_support::tempdir("provider-lines-store");
        let providers = TopologyStorageProviders::file(&store).register("memory", Arc::new(MemoryBackend));
        let fabric = FabricId::mint();
        let lines = providers.lines("file", &fabric).await.unwrap_or_else(|e| panic!("the file provider answers its lines: {e}"));
        assert!(lines.lines(rdm_node::storage_copy::Store::Fabric).await.unwrap().is_empty(), "a new root holds no line");
        let memory = providers.lines("memory", &fabric).await.err().expect("a provider that answers no lines is refused");
        assert!(matches!(memory, StorageError::Refused { .. }) && memory.to_string().contains("`memory`"), "{memory}");
        let other = providers.lines("other", &fabric).await.err().expect("an unregistered name is refused");
        assert!(matches!(other, StorageError::Refused { .. }) && other.to_string().contains("`other`"), "{other}");
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

    /// CONTRACT (rdm-node/src/blocking.rs: a disk call "runs on tokio's blocking pool. No worker thread
    /// waits on the disk"): the file backend's open makes the fabric's store root (its directory and
    /// layout marker, fsynced) on the blocking pool. On a runtime whose one blocking thread is held,
    /// the open waits and the root is not there yet; once the thread is let go the open completes and
    /// the root stands. Must NOT happen: the root made, its fsyncs run, on the thread polling the open
    /// (seen as a node-admin's main thread in `jbd2_log_wait_commit` at its boot, rdm#117).
    #[cfg(feature = "file-backend")]
    #[test]
    fn the_file_backend_makes_the_fabrics_store_root_on_the_blocking_pool() {
        let (store, data) = (crate::test_support::tempdir("open-on-pool-store"), crate::test_support::tempdir("open-on-pool-data"));
        let scope = scope_in(data);
        let root = store.join(scope.fabric_id.to_string());
        let rt = tokio::runtime::Builder::new_current_thread().max_blocking_threads(1).enable_all().build().unwrap();
        rt.block_on(async move {
            let (held_tx, held_rx) = tokio::sync::oneshot::channel::<()>();
            let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
            let holder = tokio::task::spawn_blocking(move || {
                let _ = held_tx.send(());
                let _ = release_rx.recv();
            });
            held_rx.await.unwrap();
            let backend = FileBackend::at(&store);
            let open = tokio::spawn(async move { backend.open(&scope).await.map(|_| ()) });
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            assert!(!open.is_finished(), "the open waits on the held blocking pool");
            assert!(!root.exists(), "the fabric's store root {} was made on the thread polling the open, not on the blocking pool", root.display());
            release_tx.send(()).unwrap();
            holder.await.unwrap();
            open.await.unwrap().unwrap_or_else(|e| panic!("the open completes once the pool's thread is let go: {e}"));
            assert!(root.is_dir(), "the fabric's store root stands once the open completes");
        });
    }

    /// CONTRACT (storage.md §2, "Changing it", Unblock: "Each node moves to the new provider"; i149 R4:
    /// "Every active reader and writer uses the same replaceable provider handle on its node"): what a
    /// node-admin's boot took from its handle, each of the six stores and its own connections rows,
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
        let (fabric, meshes, nodes, builds, events) = (handle.fabric(), handle.meshes(), handle.nodes(), handle.builds(), handle.events());
        let name: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
        let connections = handle.connections(scope.node_id.clone()).await.unwrap();
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
        let mesh = MeshRecord::new(crate::model::MeshId::mint(), "meshafter");
        meshes.put_mesh(&mesh).await.unwrap();
        let row = NodeRow::new(rdm_core::PathName::new("meshafter.node", "7").unwrap(), "meshafter", "node", NodeId::mint(), crate::model::NodeStatus::Ready);
        nodes.put_node(&row).await.unwrap();
        let runtime = RuntimeRow { r#type: "node".into(), mesh: "meshafter".into(), node_id: NodeId::mint(), path: "meshafter.node".into(), name: "8".into(), incarnation_id: IncarnationId::mint(), runtime: rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-after")).unwrap(), data_dir: None };
        nodes.put_runtime(&runtime).await.unwrap();
        let build = crate::build_state::BuildAccepted {
            build_id: crate::build::BuildId::parse(&rdm_core::fixture_crockford60("after")).unwrap(),
            topology: crate::accepted::FabricTopology::root("fabric1", "meshafter", mesh.mesh_id.clone()),
            change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() },
            source: None, time: 1,
        };
        builds.publish_accepted(&build).await.unwrap();
        let fact = rdm_core::connections::NodeConnection {
            id: rdm_core::ConnectionId::mint(),
            source: rdm_core::connections::ConnectionEnd { r#type: "node-admin".into(), mesh: "mesh1".into(), path: name.path.clone(), name: name.name.clone(), node_id: scope.node_id.clone(), incarnation: Some(scope.writer.clone()) },
            destination: rdm_core::connections::ConnectionEnd { mesh: "mesh1".into(), r#type: "node".into(), path: "mesh1.node".into(), name: "2".into(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()) },
            kind: rdm_core::connections::ConnectionKind::Direct,
            alpn: b"storage-provider/1".to_vec(),
            state: rdm_core::connections::ConnectionState::Connected,
            carrier: None,
            recovery: None,
            reason: None,
            logged_at_ms: 1,
        };
        connections.put_connection(&fact).await.unwrap();
        let source = crate::events_storage::EventSource { fabric_id: scope.fabric_id.clone(), node_id: scope.node_id.clone(), incarnation: scope.writer.clone() };
        let event = crate::events_storage::Event::build_started(&source, 1, &build.build_id);
        events.append(&event).await.unwrap();

        assert_eq!(fabric.fabric().await.unwrap().map(|f| f.name), Some("fabricafter".to_string()), "fabric.storage reads the new provider");
        assert_eq!(meshes.mesh_named("meshafter").await.unwrap(), Some(mesh.clone()), "mesh.storage reads the new provider");
        assert_eq!(nodes.node(&row.node_id).await.unwrap().map(|r| r.node_id), Some(row.node_id.clone()), "nodes.storage reads the new provider");
        assert_eq!(crate::node_rows::record_at(&*nodes, &row.location()).await.unwrap().map(|r| r.node_id), Some(row.node_id.clone()), "and the record at its path and name");
        assert_eq!(nodes.runtime(&runtime.node_id, &runtime.incarnation_id).await.unwrap(), Some(runtime.clone()), "and its runtime rows");
        assert_eq!(builds.read_build(&build.build_id).await.unwrap().build_id, build.build_id, "the Build journal reads the new provider");
        assert!(seen.lock().unwrap().contains(&build.build_id), "the derivation installed before the change runs over the new journal's facts");
        assert_eq!(connections.connections().await.unwrap(), vec![fact.clone()], "connections.storage reads the new provider");
        assert_eq!(events.of_build(&build.build_id).await.unwrap(), vec![event.clone()], "events.storage reads the new provider");
        for (store, needle) in [("events", event.id.to_string()), ("fabric", "fabricafter".to_string()), ("mesh", mesh.mesh_id.to_string()), ("mesh name", "meshafter".to_string()), ("nodes", row.node_id.to_string()), ("runtime", runtime.node_id.to_string()), ("builds", build.build_id.to_string()), ("connections", fact.id.to_string())] {
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
    /// same path and name over the inherited data dir (a new node.id and incarnation, `inherited = true`)
    /// reads its predecessor's runtime rows: they are the fabric's lines in its store root's
    /// `nodes.jsonl` (R-SR1), so another admin over another data dir reads them too.
    async fn a_birth_re_born_over_its_inherited_data_dir_reads_its_predecessors_runtimes(backend: &dyn StorageBackend, first: &StorageScope, re_born: &StorageScope, other: &StorageScope) {
        let runtime = crate::storage::RuntimeRow { r#type: "node".into(), mesh: "mesh1".into(),
            node_id: NodeId::mint(),
            path: "mesh1.node".into(), name: "2".into(),
            incarnation_id: IncarnationId::mint(),
            runtime: rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("dep-1")).unwrap(),
            data_dir: Some("/tmp/mesh1.node.2".into()),
        };
        {
            let b = backend.open(first).await.unwrap_or_else(|e| panic!("the first birth opens: {e}"));
            b.nodes.put_runtime(&runtime).await.unwrap();
        }
        let b = backend.open(re_born).await.unwrap_or_else(|e| panic!("the re-born birth opens over the inherited data dir: {e}"));
        assert_eq!(b.nodes.runtimes().await.unwrap(), vec![runtime.clone()], "the re-born admin holds its predecessor's runtime rows");
        assert_eq!(b.nodes.runtime(&runtime.node_id, &runtime.incarnation_id).await.unwrap(), Some(runtime.clone()), "a point read by the birth finds it");
        let o = backend.open(other).await.unwrap_or_else(|e| panic!("another admin opens over its own data dir: {e}"));
        assert_eq!(o.nodes.runtimes().await.unwrap(), vec![runtime.clone()], "the runtime rows are the fabric's, on its store root");
    }

    /// CONTRACT (R-FI1: a restarting node-admin's own row names the Fabric whose root it opens, read
    /// before any store opens): the own row is the admin's own seed in its data dir: the file backend
    /// reads back what it was given there, by another process of the data dir too, and its nodes store,
    /// opened over that data dir, holds no copy of it; another data dir holds none. A memory admin keeps
    /// none: its next start is a new admin's. Must NOT happen: the own row kept anywhere but the data dir.
    #[tokio::test]
    async fn the_own_row_is_the_admins_own_seed_in_its_data_dir() {
        let own = crate::storage::NodeRecord { mesh: "mesh1".into(),
            node_id: NodeId::mint(),
            fabric_id: FabricId::mint(),
            path: "mesh1.node-admin".into(), name: "1".into(),
            mesh_id: crate::model::MeshId::mint(),
            incarnation_id: IncarnationId::mint(),
            endpoint_id: crate::model::EndpointId(iroh::SecretKey::generate().public().to_string()),
            transport_addr: "127.0.0.1:41001".parse().unwrap(),
            listeners: vec![("control".into(), "127.0.0.1:41002".parse().unwrap())],
            is_primary: None,
            is_fabric_primary: None,
            topology_storage_provider: None,
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
    /// path and name over that data dir, so it still holds the handles of the runtimes it must inspect and
    /// terminate; another admin's data dir reads none. Must NOT happen: a re-birth losing them because
    /// they were keyed by the birth that wrote them. Rafka's backend runs the same script.
    #[cfg(feature = "file-backend")]
    #[tokio::test]
    async fn a_birth_re_born_over_its_inherited_data_dir_reads_its_predecessors_runtimes_on_the_file_backend() {
        let store = crate::test_support::tempdir("file-backend-re-born-store");
        let data_dir = crate::test_support::tempdir("file-backend-re-born-data");
        let fabric_id = FabricId::mint();
        let at = |data_dir: PathBuf| StorageScope { data_dir, fabric_id: fabric_id.clone(), own_mesh: "mesh1".into(), writer: IncarnationId::mint(), node_id: NodeId::mint() };
        a_birth_re_born_over_its_inherited_data_dir_reads_its_predecessors_runtimes(&FileBackend::at(&store), &at(data_dir.clone()), &at(data_dir), &at(crate::test_support::tempdir("file-backend-re-born-other"))).await;
    }
}
