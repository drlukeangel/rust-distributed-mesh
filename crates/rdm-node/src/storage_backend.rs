//! The storage backend a node runs on: its own connections rows (`connections.storage`). A node-admin's backend opens the admin's six boundaries (`rdm_admin::storage_backend`);
//! a node opens this one at its boot. RDM standalone's is [`FileNodeBackend`] (the `file-backend`
//! feature, the default), the rows as files on the fabric's store root. [`MemoryNodeBackend`] holds
//! one process's rows.

use crate::storage::ConnectionsStorage;
use async_trait::async_trait;
use rdm_core::launch::Launch;
use std::sync::Arc;

/// A node's storage, opened by its backend.
pub struct NodeStorage {
    /// The node's own connections rows.
    pub connections: Arc<dyn ConnectionsStorage>,
    /// Where the rows are written now, as the boot's span records it.
    pub writes_to: String,
}

/// The backend a node's storage is opened from.
#[async_trait]
pub trait NodeStorageBackend: Send + Sync {
    /// Opens the storage of the node `launch` names.
    async fn open(&self, launch: &Launch) -> Result<NodeStorage, String>;
    /// Whether this provider holds the `commit` record `commit` (storage.md §2 step 4): what a node's
    /// start that reads a block from its data dir decides by. `Err`: the provider did not answer, and
    /// the block holds.
    async fn commit(&self, launch: &Launch, commit: &rdm_core::build::StorageCommit) -> Result<bool, String> {
        let _ = (launch, commit);
        Err("this provider answers no commit record (storage.md §2 step 4)".into())
    }
}

/// RDM standalone's backend: the node's connection facts, its lines of the fabric's store root's
/// `connections.jsonl` (R14, R-SR1), written there from its open. Its place is the launch's store dir
/// (`RDM_STORE_DIR`), or the one it was registered at ([`Self::at`]).
#[cfg(feature = "file-backend")]
#[derive(Debug, Default, Clone)]
pub struct FileNodeBackend {
    store_dir: Option<std::path::PathBuf>,
}

#[cfg(feature = "file-backend")]
impl FileNodeBackend {
    /// The file backend at the store dir `store_dir`, whatever the launch's.
    pub fn at(store_dir: impl Into<std::path::PathBuf>) -> Self {
        Self { store_dir: Some(store_dir.into()) }
    }
}

#[cfg(feature = "file-backend")]
#[async_trait]
impl NodeStorageBackend for FileNodeBackend {
    async fn open(&self, launch: &Launch) -> Result<NodeStorage, String> {
        let store_dir = self.store_dir.as_ref().unwrap_or(&launch.store_dir);
        let root = crate::store_root::StoreRoot::open(store_dir, &launch.fabric_id).map_err(|e| format!("refusing to start: {e}"))?;
        let connections = crate::storage::FileConnectionsStorage::open(&root, &launch.name, &crate::store_log::LogWriter::new(&launch.node_id, &launch.incarnation)).await.map_err(|e| format!("connections storage: {e}"))?;
        Ok(NodeStorage {
            connections: Arc::new(connections),
            writes_to: "store-root".into(),
        })
    }

    async fn commit(&self, launch: &Launch, commit: &rdm_core::build::StorageCommit) -> Result<bool, String> {
        let store_dir = self.store_dir.as_ref().unwrap_or(&launch.store_dir);
        let locks = crate::store_log::StoreLocks::at(&store_dir.join(launch.fabric_id.to_string()), &crate::store_log::LogWriter::new(&launch.node_id, &launch.incarnation));
        locks.read(crate::store_log::LockKind::Commit, &commit.name()).await.map(|held| held.is_some()).map_err(|e| e.to_string())
    }
}

#[cfg(feature = "file-backend")]
/// RDM standalone's node storage backend: the one a node's `start` variants and `run` construct.
pub fn file_node_backend() -> Arc<dyn NodeStorageBackend> {
    Arc::new(FileNodeBackend::default())
}

/// The topology storage providers a node executable registers in its wiring, by name (storage.md §2):
/// the node opens the one its launch names (`RDM_TOPOLOGY_STORAGE_PROVIDER`, written by its launching
/// admin), and a name it does not register refuses its start by name.
#[derive(Clone, Default)]
pub struct NodeStorageProviders {
    registered: Vec<(String, Arc<dyn NodeStorageBackend>)>,
}

impl NodeStorageProviders {
    /// No provider registered.
    pub fn new() -> Self {
        Self::default()
    }

    /// RDM's file backend registered as `file`: what every RDM node executable registers.
    #[cfg(feature = "file-backend")]
    pub fn file() -> Self {
        Self::new().register(rdm_core::launch::TOPOLOGY_STORAGE_FILE, file_node_backend())
    }

    /// These providers with `backend` registered as `name`.
    pub fn register(mut self, name: &str, backend: Arc<dyn NodeStorageBackend>) -> Self {
        self.registered.push((name.to_string(), backend));
        self
    }

    /// The registered names, in registration order.
    pub fn names(&self) -> Vec<String> {
        self.registered.iter().map(|(n, _)| n.clone()).collect()
    }

    /// The backend registered as `name`, refused by name when none is or a name is registered twice.
    pub fn named(&self, name: &str) -> Result<Arc<dyn NodeStorageBackend>, String> {
        let names = self.names();
        if let Some(twice) = names.iter().enumerate().find(|(i, n)| names[..*i].contains(n)).map(|(_, n)| n) {
            return Err(format!("the topology storage provider `{twice}` is registered twice: each name is bound to one place"));
        }
        self.registered.iter().find(|(n, _)| n == name).map(|(_, b)| b.clone()).ok_or_else(|| {
            format!("{} names topology storage provider `{name}`, which this executable does not register (it registers: {})", rdm_core::launch::ENV_TOPOLOGY_STORAGE_PROVIDER, names.join(", "))
        })
    }
}

/// A node's one access point to its topology storage (storage.md §2): every reader and writer on the
/// node reaches its stores through it, and a provider change replaces the provider beneath them
/// ("Each node moves to the new provider"), whatever each caller took from it at the node's open.
pub struct NodeStorageHandle {
    current: crate::storage_handle::StorageHandle<NodeStorage>,
    /// The providers the node's executable registers: a replacement opens one of them.
    providers: NodeStorageProviders,
    /// The launch the node's storage is opened for.
    launch: Launch,
    /// One replacement at a time.
    replacing: tokio::sync::Mutex<()>,
    /// The provider a storage change's block opened beside the one the node writes.
    beside: std::sync::Mutex<Option<(String, NodeStorage)>>,
}

impl NodeStorageHandle {
    /// Open the provider `launch` names (`topology_storage_provider`), one `providers` registers, else the
    /// node's start is refused by name.
    pub async fn open(providers: &NodeStorageProviders, launch: &Launch) -> Result<Arc<Self>, String> {
        let opened = providers.named(&launch.topology_storage_provider).map_err(|e| format!("refusing to start: {e}"))?.open(launch).await?;
        Ok(Arc::new(Self {
            current: crate::storage_handle::StorageHandle::new(&launch.topology_storage_provider, opened),
            providers: providers.clone(),
            launch: launch.clone(),
            replacing: tokio::sync::Mutex::new(()),
            beside: Default::default(),
        }))
    }

    /// The provider the node writes now.
    pub fn provider(&self) -> String {
        self.current.read().provider().to_string()
    }

    /// Where the rows are written now, as the boot's span records it.
    pub fn writes_to(&self) -> String {
        self.current.read().stores().writes_to.clone()
    }

    /// The node's connections rows, through this handle: every call goes to the provider the node
    /// writes at that call.
    pub fn connections(self: &Arc<Self>) -> Arc<dyn ConnectionsStorage> {
        Arc::new(ConnectionsThroughHandle(self.clone()))
    }

    /// Replace the provider with the one this executable registers as `provider`, opened now, off the
    /// node's boot: from the swap every call goes to it, and this returns once every write admitted to
    /// the old provider has been answered there ("Finish outstanding writes"). A name the executable
    /// does not register, or a provider that does not open, is refused by name and nothing moves.
    pub async fn replace(&self, provider: &str) -> Result<(), String> {
        self.replace_for(provider, None).await
    }

    /// [`Self::replace`] as a step of the storage change `change`: the provider its block opened beside
    /// the old one is the one moved to; a replacement onto the provider the node writes moves nothing.
    pub async fn replace_for(&self, provider: &str, change: Option<(&rdm_core::BuildId, &rdm_core::ChangeId)>) -> Result<(), String> {
        let _one = self.replacing.lock().await;
        if self.provider() == provider {
            return Ok(());
        }
        let beside = self.beside.lock().unwrap_or_else(|e| e.into_inner()).take().filter(|(p, _)| p == provider).map(|(_, s)| s);
        let opened = match beside {
            Some(stores) => stores,
            None => self.open_stores(provider, change).await?,
        };
        self.current.replace(provider, opened).await;
        Ok(())
    }

    /// Open the provider this executable registers as `provider` beside the one the node writes, for
    /// the storage change `change` (storage.md §2 step 1): kept until the node moves to it or its change
    /// ends. A name the executable does not register, or a provider that does not open, is refused by
    /// name.
    pub async fn open_beside(&self, provider: &str, change: (&rdm_core::BuildId, &rdm_core::ChangeId)) -> Result<(), String> {
        let _one = self.replacing.lock().await;
        if self.provider() == provider || self.beside.lock().unwrap_or_else(|e| e.into_inner()).as_ref().is_some_and(|(p, _)| p == provider) {
            return Ok(());
        }
        let stores = self.open_stores(provider, Some(change)).await?;
        *self.beside.lock().unwrap_or_else(|e| e.into_inner()) = Some((provider.to_string(), stores));
        Ok(())
    }

    /// Close the provider opened beside the one the node writes, if any.
    pub fn close_beside(&self) {
        self.beside.lock().unwrap_or_else(|e| e.into_inner()).take();
    }

    /// Block (storage.md §2 step 1): no write is admitted from here; each waits as owed. Answers how many
    /// writes were issued before the block and not answered.
    pub fn block(&self) -> usize {
        self.current.block()
    }

    /// Resolves once every write admitted before the block has been answered.
    pub async fn finish_issued(&self) {
        self.current.finish_issued().await
    }

    /// Lift the block: every owed write is admitted to the provider the node writes now.
    pub fn unblock(&self) {
        self.current.unblock()
    }

    /// How many writes wait on the block now.
    pub fn owed(&self) -> usize {
        self.current.owed()
    }

    async fn open_stores(&self, provider: &str, change: Option<(&rdm_core::BuildId, &rdm_core::ChangeId)>) -> Result<NodeStorage, String> {
        let span = tracing::info_span!("rdm.node_admin.build.update.via-storage-open", build_id = tracing::field::Empty, change_id = tracing::field::Empty, node_id = %self.launch.node_id, provider = %provider, outcome = tracing::field::Empty);
        if let Some((build_id, change_id)) = change {
            span.record("build_id", build_id.log().to_string().as_str()).record("change_id", change_id.as_str());
        }
        let opened = match self.providers.named(provider) {
            Ok(backend) => backend.open(&Launch { topology_storage_provider: provider.to_string(), ..self.launch.clone() }).await,
            Err(e) => Err(e),
        };
        match &opened {
            Ok(_) => span.record("outcome", "opened"),
            Err(e) => span.record("outcome", format!("refused: {e}").as_str()),
        };
        opened
    }
}

/// `connections.storage` through a node's [`NodeStorageHandle`].
struct ConnectionsThroughHandle(Arc<NodeStorageHandle>);

#[async_trait]
impl ConnectionsStorage for ConnectionsThroughHandle {
    async fn put_connection(&self, fact: &rdm_core::connections::NodeConnection) -> Result<(), crate::storage_error::StorageError> {
        let w = self.0.current.write().await;
        w.connections.put_connection(fact).await
    }
    async fn connections(&self) -> Result<Vec<rdm_core::connections::NodeConnection>, crate::storage_error::StorageError> {
        self.0.current.read().stores().connections.connections().await
    }
    async fn remove_connection(&self, index: &rdm_core::connections::ConnectionIndex) -> Result<(), crate::storage_error::StorageError> {
        let w = self.0.current.write().await;
        w.connections.remove_connection(index).await
    }
    async fn append_history(&self, fact: &rdm_core::connections::NodeConnection) -> Result<(), crate::storage_error::StorageError> {
        let w = self.0.current.write().await;
        w.connections.append_history(fact).await
    }
    async fn history(&self) -> Result<Vec<rdm_core::connections::NodeConnection>, crate::storage_error::StorageError> {
        self.0.current.read().stores().connections.history().await
    }
    async fn connections_to(&self, destination: &crate::model::NodeId) -> Result<Vec<rdm_core::connections::NodeConnection>, crate::storage_error::StorageError> {
        self.0.current.read().stores().connections.connections_to(destination).await
    }
    async fn fleet_directs(&self) -> Result<Vec<rdm_core::connections::NodeConnection>, crate::storage_error::StorageError> {
        self.0.current.read().stores().connections.fleet_directs().await
    }
}

/// One process's rows: the node's connections held in memory, gone with its process.
#[derive(Debug, Default, Clone, Copy)]
pub struct MemoryNodeBackend;

#[async_trait]
impl NodeStorageBackend for MemoryNodeBackend {
    async fn open(&self, _launch: &Launch) -> Result<NodeStorage, String> {
        Ok(NodeStorage { connections: Arc::new(crate::storage::MemoryConnectionsStorage::default()), writes_to: "memory".into() })
    }
}
