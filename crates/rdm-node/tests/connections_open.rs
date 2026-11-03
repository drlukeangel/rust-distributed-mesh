//! A node reads no connection fact (Q227): its connections store's open appends and folds nothing,
//! and its connections writer is born holding no connection, its held set complete, reading nothing
//! back from the store.
//!
//! - `a_nodes_connections_store_opens_over_a_file_it_cannot_fold_and_appends_its_fact`: the fabric's
//!   `connections.jsonl` ends in a line no fold can read; the node's open of its provider succeeds,
//!   its fact is appended, and only a reader that folds the file (the mesh-primary's node
//!   investigation, when it runs) meets the refusal.
//! - `a_connections_writer_is_born_complete_and_reads_nothing_back`: a writer over a store that
//!   counts every read is complete from its birth, resolves a destination it never observed as a
//!   direct dial, writes its observed fact, and has made no read.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use rdm_core::connections::{resolve_for_carriers, ConnectionEnd, ConnectionIndex, ConnectionKind, ConnectionState, ConnectionsHeld, EffectiveRoute, NodeConnection};
use rdm_core::launch::Launch;
use rdm_core::{ConnectionId, FabricId, IncarnationId, NodeId};
use rdm_node::connections_writer::ConnectionsWriter;
use rdm_node::storage::{ConnectionsStorage, MemoryConnectionsStorage};
use rdm_node::storage_backend::{FileNodeBackend, NodeStorageHandle, NodeStorageProviders};
use rdm_node::storage_error::StorageError;

fn store_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("connections-open-{tag}-{}", NodeId::mint()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn launch(store: &Path) -> Launch {
    Launch {
        initial_properties: Default::default(),
        topology_storage_provider: "file".into(),
        fabric: "fabric1".into(),
        fabric_id: FabricId::mint(),
        mesh: "mesh1".into(),
        r#type: "node".into(),
        path: "mesh1.node".into(), name: "2".into(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
        launcher: None,
        seeds: vec![],
        data_dir: store.to_path_buf(),
        store_dir: store.to_path_buf(),
        mesh_id: None,
        issuing: None,
        roots: vec![],
        connections_delete_deadline: Duration::from_secs(5),
        declared: vec![],
    }
}

fn fact(source: ConnectionEnd, ordinal: u32) -> NodeConnection {
    NodeConnection {
        id: ConnectionId::mint(),
        source,
        destination: ConnectionEnd { mesh: "mesh1".into(), r#type: "node".into(), path: "mesh1.node".into(), name: ordinal.to_string(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()) },
        kind: ConnectionKind::Direct,
        alpn: rdm_core::connections::ALPN.to_vec(),
        state: ConnectionState::Connected,
        carrier: None,
        recovery: None,
        reason: None,
        logged_at_ms: 1,
    }
}

/// CONTRACT (Q227: no node reads anything from the store on create, start or rejoin; the mesh-primary's
/// node investigation reads the connection facts when it runs): the fabric's `connections.jsonl` ends in
/// an unended line no fold can read (another writer's write in flight, #3222). A node's open of the
/// provider its launch names succeeds over it, and the fact the node then writes is appended to the
/// file, which keeps every byte it held. Must NOT happen: a node's start refused, or held, by a read of
/// other nodes' connection lines it never needed.
#[tokio::test]
async fn a_nodes_connections_store_opens_over_a_file_it_cannot_fold_and_appends_its_fact() {
    let store = store_dir("unfoldable");
    let launch = launch(&store);
    let root = rdm_node::store_root::StoreRoot::open(&store, &launch.fabric_id).unwrap();
    let file = root.dir().join(rdm_node::store_root::CONNECTIONS_FILE);
    let unended = b"{\"op\":{\"node_id\":\"another\",\"incarnation_id\":\"in-flight\",\"seq\":1},\"format\":\"node-connection/2\",\"key\":\"half";
    std::fs::write(&file, unended).unwrap();

    let providers = NodeStorageProviders::new().register("file", Arc::new(FileNodeBackend::at(&store)));
    let handle = NodeStorageHandle::open(&providers, &launch).await.unwrap_or_else(|e| panic!("the node opens its connections store without reading it: {e}"));
    let written = fact(ConnectionEnd { mesh: launch.mesh.clone(), r#type: launch.r#type.clone(), path: launch.path.clone(), name: launch.name.clone(), node_id: launch.node_id.clone(), incarnation: Some(launch.incarnation.clone()) }, 3);
    handle.connections().put_connection(&written).await.unwrap_or_else(|e| panic!("the node appends its fact: {e}"));

    let bytes = std::fs::read(&file).unwrap();
    assert!(bytes.starts_with(unended), "the file keeps every byte another writer wrote");
    assert_eq!(String::from_utf8_lossy(&bytes).matches(&written.id.to_string()).count(), 1, "the node's fact is appended once");
}

/// A connections store that answers every read with a refusal and counts it, and keeps its writes.
#[derive(Default)]
struct ReadsCounted {
    rows: MemoryConnectionsStorage,
    reads: AtomicUsize,
}

impl ReadsCounted {
    fn read(&self) -> StorageError {
        self.reads.fetch_add(1, Ordering::SeqCst);
        StorageError::Io { file: "connections".into(), reason: "a node read its connections store".into() }
    }
}

#[async_trait]
impl ConnectionsStorage for ReadsCounted {
    async fn put_connection(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        self.rows.put_connection(fact).await
    }
    async fn connections(&self) -> Result<Vec<NodeConnection>, StorageError> {
        Err(self.read())
    }
    async fn remove_connection(&self, index: &ConnectionIndex) -> Result<(), StorageError> {
        self.rows.remove_connection(index).await
    }
    async fn append_history(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        self.rows.append_history(fact).await
    }
    async fn history(&self) -> Result<Vec<NodeConnection>, StorageError> {
        Err(self.read())
    }
    async fn connections_to(&self, _destination: &NodeId) -> Result<Vec<NodeConnection>, StorageError> {
        Err(self.read())
    }
}

/// CONTRACT (Q227; connections.md §4.2: a restart closes every connection the node held, so a node is
/// born holding none and loads none): a connections writer is complete from its birth, with this node as
/// the source of its own facts; a destination it never observed resolves as a direct dial; the fact it
/// then records lands in its store and in its held set; and the store has answered no read. Must NOT
/// happen: a writer that needs a read of the store before it can answer a route.
#[tokio::test]
async fn a_connections_writer_is_born_complete_and_reads_nothing_back() {
    let own = ConnectionEnd { mesh: "mesh1".into(), r#type: "node".into(), path: "mesh1.node".into(), name: "1".into(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()) };
    let store = Arc::new(ReadsCounted::default());
    let writer = ConnectionsWriter::new(own.clone(), store.clone(), Arc::new(Mutex::new(ConnectionsHeld::new())));
    {
        let held = writer.held();
        let held = held.lock().unwrap();
        assert!(held.is_complete(), "the writer holds every connection this birth has, none, from its birth");
        let unseen = NodeId::mint();
        let route = resolve_for_carriers(&held, &own.node_id, &unseen, rdm_core::connections::ALPN, false, &[]).route;
        assert_eq!(route, EffectiveRoute::Direct { known: false }, "a destination never observed is dialled");
    }
    let row = fact(own.clone(), 4);
    writer.record(row.clone()).await.unwrap_or_else(|e| panic!("the writer records its fact: {e}"));
    assert_eq!(writer.held_routes().await, vec![row.clone()], "the fact is held once written");
    assert_eq!(store.rows.connections().await.unwrap(), vec![row], "and is in the store");
    assert_eq!(store.reads.load(Ordering::SeqCst), 0, "the writer read nothing from its store");
}


/// Persistent ownership is independent of name reuse and rename.
#[tokio::test]
async fn connection_history_is_scoped_by_source_id_and_explicit_writer() {
    use rdm_node::{storage::FileConnectionsStorage, store_log::LogWriter, store_root::StoreRoot};
    let directory = store_dir("source-id");
    let root = StoreRoot::open(&directory, &FabricId::mint()).unwrap();
    let location = rdm_core::PathName::new("apps.east", "worker").unwrap();
    let predecessor = ConnectionEnd { mesh: "explicit-mesh".into(), r#type: "node".into(),
        path: location.path.clone(), name: location.name.clone(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()) };
    let successor = ConnectionEnd { node_id: NodeId::mint(), ..predecessor.clone() };
    let old_writer = LogWriter::new(&predecessor.node_id, predecessor.incarnation.as_ref().unwrap());
    let new_writer = LogWriter::new(&successor.node_id, successor.incarnation.as_ref().unwrap());
    let old_store = FileConnectionsStorage::open(&root, &old_writer).await.unwrap();
    let first = fact(predecessor.clone(), 1);
    old_store.put_connection(&first).await.unwrap();
    let new_store = FileConnectionsStorage::open(&root, &new_writer).await.unwrap();
    assert!(new_store.connections().await.unwrap().is_empty());
    assert!(new_store.history().await.unwrap().is_empty());
    let new_fact = fact(successor, 2);
    new_store.put_connection(&new_fact).await.unwrap();
    let retained = ConnectionEnd { path: location.path.clone(), name: location.with_name("renamed").unwrap().name, ..predecessor };
    let later = NodeConnection { id: ConnectionId::mint(), source: retained, logged_at_ms: 2, ..first.clone() };
    old_store.put_connection(&later).await.unwrap();
    let reopened = FileConnectionsStorage::open(&root, &old_writer).await.unwrap();
    assert_eq!(reopened.connections().await.unwrap(), vec![later.clone()]);
    assert_eq!(reopened.history().await.unwrap(), vec![first, later]);
    assert_eq!(new_store.connections().await.unwrap(), vec![new_fact.clone()]);
    assert_eq!(new_store.history().await.unwrap(), vec![new_fact]);
    drop((reopened, old_store, new_store, root));
    std::fs::remove_dir_all(directory).unwrap();
}

/// A location-keyed connection format cannot be folded as the ID-keyed format.
#[tokio::test]
async fn location_keyed_connection_formats_are_refused_when_read() {
    use rdm_node::{storage::FileConnectionsStorage, store_log::{encode_line, LogWriter, Op}, store_root::StoreRoot};
    for format in ["node-connection/1", "node-connection/2"] {
        let directory = store_dir("old-format");
        let root = StoreRoot::open(&directory, &FabricId::mint()).unwrap();
        let writer = LogWriter::mint();
        let source = ConnectionEnd { mesh: "mesh1".into(), r#type: "node".into(),
            path: "mesh1.node".into(), name: "1".into(), node_id: writer.node_id.clone(), incarnation: Some(writer.incarnation_id.clone()) };
        let row = fact(source, 2);
        let op = Op { node_id: writer.node_id.to_string(), incarnation_id: writer.incarnation_id.to_string(), seq: 1 };
        let bytes = encode_line(&op, format, "obsolete-location-index", &serde_json::to_vec(&row).unwrap());
        std::fs::write(root.dir().join(rdm_node::store_root::CONNECTIONS_FILE), bytes).unwrap();
        let store = FileConnectionsStorage::open(&root, &writer).await.expect("append-only open does not fold history");
        for result in [store.connections().await, store.history().await, store.connections_to(&row.destination.node_id).await] {
            assert!(matches!(result, Err(StorageError::Unrecognised { reason, .. }) if reason.contains(format)), "{format} must be refused by name");
        }
        drop((store, root));
        std::fs::remove_dir_all(directory).unwrap();
    }
}
