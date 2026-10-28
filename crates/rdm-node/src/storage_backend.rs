//! The storage backend a node runs on: its own connections rows (`connections.storage`) and its point
//! reads of other nodes' rows (`nodes.storage`, query.md §1, Q237). A node-admin's backend opens the admin's six boundaries (`rdm_admin::storage_backend`);
//! a node opens this one at its boot. RDM standalone's is [`FileNodeBackend`] (the `file-backend`
//! feature, the default), the rows as files on the fabric's store root. [`MemoryNodeBackend`] holds
//! one process's rows.

use crate::nodes_storage::NodeRowsStorage;
use crate::storage::ConnectionsStorage;
use async_trait::async_trait;
use rdm_core::launch::Launch;
use std::sync::Arc;

/// A node's storage, opened by its backend.
pub struct NodeStorage {
    /// The node's own connections rows.
    pub connections: Arc<dyn ConnectionsStorage>,
    /// The nodes' rows, a point read by id or by `path.name` (query.md §1, Q237): the backend opens
    /// them on the first read, never at the node's start, join or create (Q227).
    pub node_rows: Arc<dyn NodeRowsStorage>,
    /// Where the rows are written now, as the boot's span records it.
    pub writes_to: String,
}

/// The backend a node's storage is opened from.
#[async_trait]
pub trait NodeStorageBackend: Send + Sync {
    /// Opens the storage of the node `launch` names.
    async fn open(&self, launch: &Launch) -> Result<NodeStorage, String>;
}

/// RDM standalone's backend: the node's connection facts, its lines of the fabric's store root's
/// `connections.jsonl` (R14, R-SR1), appended there from an open that reads none of it (Q227), and the
/// nodes' rows of the root's `nodes.jsonl`, folded on the first read of one (Q237). Its place is the launch's store dir
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
        // The root's directory and layout marker are made and fsynced on the blocking pool: no worker
        // thread waits on the disk (crate::blocking).
        let (store_dir, fabric_id) = (self.store_dir.clone().unwrap_or_else(|| launch.store_dir.clone()), launch.fabric_id.clone());
        let root = crate::blocking::blocking_io(move || crate::store_root::StoreRoot::open(&store_dir, &fabric_id)).await.map_err(|e| format!("refusing to start: {e}"))?;
        let connections = crate::storage::FileConnectionsStorage::open(&root, &launch.name, &crate::store_log::LogWriter::new(&launch.node_id, &launch.incarnation)).await.map_err(|e| format!("connections storage: {e}"))?;
        Ok(NodeStorage {
            connections: Arc::new(connections),
            node_rows: Arc::new(crate::nodes_storage::FileNodeRowsOnFirstRead::at(root)),
            writes_to: "store-root".into(),
        })
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

    /// The nodes' rows, through this handle: every read goes to the provider the node writes at that
    /// read, opened on its first read (query.md §1, Q237).
    pub fn node_rows(self: &Arc<Self>) -> Arc<dyn NodeRowsStorage> {
        Arc::new(NodeRowsThroughHandle(self.clone()))
    }

    /// Replace the provider with the one this executable registers as `provider`, opened now, off the
    /// node's boot: from the swap every call goes to it, and this returns once every write admitted to
    /// the old provider has been answered there ("Finish outstanding writes"). A name the executable
    /// does not register, or a provider that does not open, is refused by name and nothing moves.
    pub async fn replace(&self, provider: &str) -> Result<(), String> {
        let _one = self.replacing.lock().await;
        let opened = self.providers.named(provider)?.open(&Launch { topology_storage_provider: provider.to_string(), ..self.launch.clone() }).await?;
        self.current.replace(provider, opened).await;
        Ok(())
    }
}

/// `connections.storage` through a node's [`NodeStorageHandle`].
struct ConnectionsThroughHandle(Arc<NodeStorageHandle>);

#[async_trait]
impl ConnectionsStorage for ConnectionsThroughHandle {
    async fn put_connection(&self, fact: &rdm_core::connections::NodeConnection) -> Result<(), crate::storage_error::StorageError> {
        let w = self.0.current.write();
        w.connections.put_connection(fact).await
    }
    async fn connections(&self) -> Result<Vec<rdm_core::connections::NodeConnection>, crate::storage_error::StorageError> {
        self.0.current.read().stores().connections.connections().await
    }
    async fn remove_connection(&self, index: &rdm_core::connections::ConnectionIndex) -> Result<(), crate::storage_error::StorageError> {
        let w = self.0.current.write();
        w.connections.remove_connection(index).await
    }
    async fn append_history(&self, fact: &rdm_core::connections::NodeConnection) -> Result<(), crate::storage_error::StorageError> {
        let w = self.0.current.write();
        w.connections.append_history(fact).await
    }
    async fn history(&self) -> Result<Vec<rdm_core::connections::NodeConnection>, crate::storage_error::StorageError> {
        self.0.current.read().stores().connections.history().await
    }
    async fn connections_to(&self, destination: &crate::model::NodeId) -> Result<Vec<rdm_core::connections::NodeConnection>, crate::storage_error::StorageError> {
        self.0.current.read().stores().connections.connections_to(destination).await
    }
}

/// `nodes.storage`'s point reads through a node's [`NodeStorageHandle`].
struct NodeRowsThroughHandle(Arc<NodeStorageHandle>);

#[async_trait]
impl NodeRowsStorage for NodeRowsThroughHandle {
    async fn node(&self, node_id: &crate::model::NodeId) -> Result<Option<crate::nodes_storage::NodeRow>, crate::storage_error::StorageError> {
        self.0.current.read().stores().node_rows.node(node_id).await
    }
    async fn node_at(&self, name: &crate::model::PathName) -> Result<Option<crate::nodes_storage::NodeRow>, crate::storage_error::StorageError> {
        self.0.current.read().stores().node_rows.node_at(name).await
    }
}

/// The nodes' rows of a process that holds none: every read is refused by name.
struct NoNodeRows;

#[async_trait]
impl NodeRowsStorage for NoNodeRows {
    async fn node(&self, _: &crate::model::NodeId) -> Result<Option<crate::nodes_storage::NodeRow>, crate::storage_error::StorageError> {
        Err(no_node_rows())
    }
    async fn node_at(&self, _: &crate::model::PathName) -> Result<Option<crate::nodes_storage::NodeRow>, crate::storage_error::StorageError> {
        Err(no_node_rows())
    }
}

fn no_node_rows() -> crate::storage_error::StorageError {
    crate::storage_error::StorageError::Refused { file: "nodes.storage".into(), reason: "the memory backend holds this process's own connections only: no node's row is read from it".into() }
}

/// One process's rows: the node's connections held in memory, gone with its process; it holds no
/// node's row, and a read of one is refused by name.
#[derive(Debug, Default, Clone, Copy)]
pub struct MemoryNodeBackend;

#[async_trait]
impl NodeStorageBackend for MemoryNodeBackend {
    async fn open(&self, _launch: &Launch) -> Result<NodeStorage, String> {
        Ok(NodeStorage { connections: Arc::new(crate::storage::MemoryConnectionsStorage::default()), node_rows: Arc::new(NoNodeRows), writes_to: "memory".into() })
    }
}

#[cfg(all(test, feature = "file-backend"))]
mod tests {
    use super::*;
    use rdm_core::launch as env;

    /// Run `op` on a single-thread runtime whose one blocking thread is held while `op` first runs, and
    /// answer whether `root` already stood while `op` was waiting (made on the thread polling `op`, not
    /// on the blocking pool), and `op`'s answer once the thread is let go.
    fn root_made_while_waiting<T: Send + 'static>(root: std::path::PathBuf, op: std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>) -> (bool, T) {
        let rt = tokio::runtime::Builder::new_current_thread().max_blocking_threads(1).enable_all().build().unwrap();
        rt.block_on(async move {
            let (held_tx, held_rx) = tokio::sync::oneshot::channel::<()>();
            let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
            let holder = tokio::task::spawn_blocking(move || {
                let _ = held_tx.send(());
                let _ = release_rx.recv();
            });
            held_rx.await.unwrap();
            let task = tokio::spawn(op);
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            assert!(!task.is_finished(), "the open waits on the held blocking pool");
            let made = root.exists();
            release_tx.send(()).unwrap();
            holder.await.unwrap();
            (made, task.await.unwrap())
        })
    }

    /// CONTRACT (rdm-node/src/blocking.rs: a disk call "runs on tokio's blocking pool. No worker thread
    /// waits on the disk"): a launched node's file storage makes the fabric's store root on the
    /// blocking pool. Must NOT happen: the root's directory and marker made, and fsynced, on the
    /// thread polling the open (rdm#117).
    #[test]
    fn a_launched_nodes_file_storage_makes_the_store_root_on_the_blocking_pool() {
        let (store, data) = (crate::record_store::tempdir("node-open-store"), crate::record_store::tempdir("node-open-data"));
        let fabric_id = crate::model::FabricId::mint();
        let vars = std::collections::BTreeMap::from([
            (env::ENV_FABRIC, "fabric1".to_string()),
            (env::ENV_FABRIC_ID, fabric_id.to_string()),
            (env::ENV_NODE_NAME, "mesh1.rpc.1".to_string()),
            (env::ENV_NODE_ID, crate::model::NodeId::mint().to_string()),
            (env::ENV_INCARNATION, crate::model::IncarnationId::mint().to_string()),
            (env::ENV_TRANSPORT_ADDR, "127.0.0.1:0".to_string()),
            (env::ENV_DATA_DIR, data.display().to_string()),
            (env::ENV_STORE_DIR, store.display().to_string()),
        ]);
        let launch = Launch::from_env(|k| vars.get(k).cloned()).expect("the launch reads");
        let root = store.join(fabric_id.to_string());
        let (made, opened) = root_made_while_waiting(root.clone(), Box::pin(async move { FileNodeBackend::default().open(&launch).await.map(|_| ()) }));
        assert!(!made, "the store root {} was made on the thread polling the open, not on the blocking pool", root.display());
        opened.unwrap_or_else(|e| panic!("the open completes once the pool's thread is let go: {e}"));
        assert!(root.is_dir(), "the store root stands once the open completes");
    }

    /// CONTRACT (as above, storage.md §2 Copy's provider lines): the file provider's lines open makes
    /// the fabric's store root on the blocking pool. Must NOT happen: the root made on the thread
    /// polling the open.
    #[test]
    fn the_file_providers_lines_make_the_store_root_on_the_blocking_pool() {
        let store = crate::record_store::tempdir("lines-open-store");
        let fabric_id = crate::model::FabricId::mint();
        let root = store.join(fabric_id.to_string());
        let at = store.clone();
        let (made, opened) = root_made_while_waiting(root.clone(), Box::pin(async move { crate::storage_copy::FileProviderLines::open("file", &at, &fabric_id).await.map(|_| ()) }));
        assert!(!made, "the store root {} was made on the thread polling the open, not on the blocking pool", root.display());
        opened.unwrap_or_else(|e| panic!("the open completes once the pool's thread is let go: {e}"));
        assert!(root.is_dir(), "the store root stands once the open completes");
    }
}
