//! A node reaches its topology storage through one replaceable handle (storage.md §2, "Changing it":
//! "Each node moves to the new provider"; i149 R4): every storage call, through whatever its caller
//! took from the handle before the change, goes to the provider the handle holds now, and a change
//! returns only once every write already issued through the old provider has landed there
//! ("Finish outstanding writes": "Each node brings every write ... it issued before the block to a
//! durable outcome on the old provider").
//!
//! - `a_node_uses_the_replaced_provider_for_every_storage_call`: after the change, every
//!   `ConnectionsStorage` call through the storage the node's writer was handed at its open reads and
//!   writes the new provider's file, and the old provider's file holds none of it.
//! - `a_provider_change_waits_for_already_issued_writes`: a write issued through the old provider and
//!   stalled in it holds the change; the change returns only once that write's line is in the old
//!   provider's file, and the new provider's file holds none of it.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rdm_core::connections::{ConnectionEnd, ConnectionIndex, ConnectionKind, ConnectionState, NodeConnection};
use rdm_core::launch::Launch;
use rdm_core::{ConnectionId, FabricId, IncarnationId, NodeId};
use rdm_node::storage::ConnectionsStorage;
use rdm_node::storage_backend::{FileNodeBackend, NodeStorage, NodeStorageBackend, NodeStorageHandle, NodeStorageProviders};
use rdm_node::storage_error::StorageError;

fn store_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("storage-provider-{tag}-{}", NodeId::mint()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn launch(provider: &str, data_dir: &Path) -> Launch {
    Launch {
        topology_storage_provider: provider.into(),
        fabric: "fabric1".into(),
        fabric_id: FabricId::mint(),
        name: "mesh1.rpc.2".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
        launcher: None,
        seeds: vec![],
        data_dir: data_dir.to_path_buf(),
        store_dir: data_dir.to_path_buf(),
        mesh_id: None,
        issuing: None,
        roots: vec![],
        connections_delete_deadline: Duration::from_secs(5),
        declared: vec![],
    }
}

/// A Direct Connected fact of `launch`'s node toward `mesh1.rpc.<ordinal>`: its own index key.
fn fact(launch: &Launch, ordinal: u32) -> NodeConnection {
    NodeConnection {
        id: ConnectionId::mint(),
        source: ConnectionEnd { name: launch.name.clone(), node_id: launch.node_id.clone(), incarnation: Some(launch.incarnation.clone()) },
        destination: ConnectionEnd { name: format!("mesh1.rpc.{ordinal}").parse().unwrap(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()) },
        kind: ConnectionKind::Direct,
        alpn: b"storage-provider/1".to_vec(),
        state: ConnectionState::Connected,
        carrier: None,
        recovery: None,
        reason: None,
        logged_at_ms: 1,
    }
}

/// The lines of the provider at `store_dir`'s `connections.jsonl` for `launch`'s fabric naming `id`.
fn lines_naming(store_dir: &Path, launch: &Launch, id: &ConnectionId) -> usize {
    let file = store_dir.join(launch.fabric_id.to_string()).join("connections.jsonl");
    std::fs::read_to_string(file).unwrap_or_default().lines().filter(|l| l.contains(&id.to_string())).count()
}

/// CONTRACT (storage.md §2, "Changing it", Unblock: "Each node moves to the new provider"; i149 R4:
/// "Every active reader and writer uses the same replaceable provider handle on its node"): the
/// storage a node's writer was handed at its open goes, once the provider is replaced, to the new
/// provider for every `ConnectionsStorage` call: its writes land in the new provider's file and none
/// in the old one's, and its reads answer the new provider's rows alone. Must NOT happen: a caller
/// holding what the boot handed it still writing or reading the old provider after the change.
#[tokio::test]
async fn a_node_uses_the_replaced_provider_for_every_storage_call() {
    let (old, new, data) = (store_dir("old"), store_dir("new"), store_dir("data"));
    let providers = NodeStorageProviders::new().register("file", Arc::new(FileNodeBackend::at(&old))).register("file-new", Arc::new(FileNodeBackend::at(&new)));
    let launch = launch("file", &data);
    let handle = NodeStorageHandle::open(&providers, &launch).await.unwrap_or_else(|e| panic!("the node opens its launch's provider: {e}"));
    // What the node's connections writer holds: taken once, at its open.
    let storage = handle.connections();
    let before = fact(&launch, 3);
    storage.put_connection(&before).await.unwrap();
    assert_eq!(lines_naming(&old, &launch, &before.id), 1, "before the change the node writes the provider its launch names");

    handle.replace("file-new").await.unwrap_or_else(|e| panic!("the provider is replaced: {e}"));

    let after = fact(&launch, 4);
    storage.put_connection(&after).await.unwrap();
    storage.append_history(&after).await.unwrap();
    assert_eq!(storage.connections().await.unwrap(), vec![after.clone()], "the node's rows are read from the new provider alone");
    assert_eq!(storage.history().await.unwrap(), vec![after.clone()], "its history likewise");
    assert_eq!(storage.connections_to(&after.destination.node_id).await.unwrap(), vec![after.clone()], "the by-destination read likewise");
    assert!(storage.connections_to(&before.destination.node_id).await.unwrap().is_empty(), "the old provider's row is not read after the change");
    assert_eq!(storage.fleet_directs().await.unwrap(), vec![after.clone()], "the fleet's Direct rows likewise");
    let index: ConnectionIndex = after.index();
    storage.remove_connection(&index).await.unwrap();
    assert!(storage.connections().await.unwrap().is_empty(), "the removal is the new provider's");
    assert_eq!(lines_naming(&new, &launch, &after.id), 2, "the new provider's file holds the write and the removal");
    assert_eq!(lines_naming(&old, &launch, &after.id), 0, "the old provider's file holds nothing written after the change");
    assert_eq!(lines_naming(&old, &launch, &before.id), 1, "and keeps what was written before it");
    assert_eq!(handle.provider(), "file-new", "the handle names the provider the node writes now");
}

/// A connections store whose writes stop at `gate` until it opens, telling `arrived` when one is there:
/// a write issued and not yet landed.
struct Stalled {
    rows: Arc<dyn ConnectionsStorage>,
    gate: tokio::sync::watch::Sender<bool>,
    arrived: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl ConnectionsStorage for Stalled {
    async fn put_connection(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        self.arrived.notify_one();
        let _ = self.gate.subscribe().wait_for(|open| *open).await;
        self.rows.put_connection(fact).await
    }
    async fn connections(&self) -> Result<Vec<NodeConnection>, StorageError> {
        self.rows.connections().await
    }
    async fn remove_connection(&self, index: &ConnectionIndex) -> Result<(), StorageError> {
        self.rows.remove_connection(index).await
    }
    async fn append_history(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        self.rows.append_history(fact).await
    }
    async fn history(&self) -> Result<Vec<NodeConnection>, StorageError> {
        self.rows.history().await
    }
    async fn connections_to(&self, destination: &NodeId) -> Result<Vec<NodeConnection>, StorageError> {
        self.rows.connections_to(destination).await
    }
}

/// The file provider at `store_dir` whose writes stall at the gate.
struct StalledFile {
    file: FileNodeBackend,
    gate: tokio::sync::watch::Sender<bool>,
    arrived: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl NodeStorageBackend for StalledFile {
    async fn open(&self, launch: &Launch) -> Result<NodeStorage, String> {
        let opened = self.file.open(launch).await?;
        Ok(NodeStorage { connections: Arc::new(Stalled { rows: opened.connections, gate: self.gate.clone(), arrived: self.arrived.clone() }), writes_to: opened.writes_to })
    }
}

/// CONTRACT (storage.md §2, "Changing it", Finish outstanding writes: "Each node brings every write
/// and claim operation it issued before the block to a durable outcome on the old provider"; i149
/// e2: "No replacement succeeds while an unaccounted issued write can complete"): a write issued
/// through the node's handle and not yet landed holds the change; the change returns only once that
/// write's line is in the old provider's file, and the new provider's file holds none of it. Must NOT
/// happen: the change returning while a write issued before it can still land on the old provider.
#[tokio::test]
async fn a_provider_change_waits_for_already_issued_writes() {
    let (old, new, data) = (store_dir("old"), store_dir("new"), store_dir("data"));
    let (gate, arrived) = (tokio::sync::watch::channel(false).0, Arc::new(tokio::sync::Notify::new()));
    let stalled = StalledFile { file: FileNodeBackend::at(&old), gate: gate.clone(), arrived: arrived.clone() };
    let providers = NodeStorageProviders::new().register("file", Arc::new(stalled)).register("file-new", Arc::new(FileNodeBackend::at(&new)));
    let launch = launch("file", &data);
    let handle = NodeStorageHandle::open(&providers, &launch).await.unwrap_or_else(|e| panic!("the node opens its launch's provider: {e}"));
    let storage = handle.connections();
    let issued = fact(&launch, 3);
    let write = tokio::spawn({
        let (storage, issued) = (storage.clone(), issued.clone());
        async move { storage.put_connection(&issued).await }
    });
    tokio::time::timeout(Duration::from_secs(5), arrived.notified()).await.expect("the write reaches the old provider and stalls there");

    let change = tokio::spawn({
        let handle = handle.clone();
        async move { handle.replace("file-new").await }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!change.is_finished(), "the change waits for the write already issued through the old provider");

    gate.send_replace(true);
    tokio::time::timeout(Duration::from_secs(5), change).await.expect("the change returns once the issued write has landed").unwrap().unwrap_or_else(|e| panic!("the provider is replaced: {e}"));
    assert_eq!(lines_naming(&old, &launch, &issued.id), 1, "when the change returns the issued write has landed on the old provider");
    assert_eq!(lines_naming(&new, &launch, &issued.id), 0, "and nothing of it on the new one");
    write.await.unwrap().expect("the issued write is answered once it has landed");
    assert_eq!(handle.provider(), "file-new");
}
