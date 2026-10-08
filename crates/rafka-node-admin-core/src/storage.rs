//! `mesh.storage`, `nodes.storage` and `connections.storage`: node-admin control state behind
//! storage boundaries, beside `fabric.storage` (`crate::fabric_storage`) and `builds.storage`
//! (`crate::build_state::FileJournal`).
//!
//! Each boundary is a trait with a memory implementation (runs that need no restart survival) and
//! a file implementation in the admin's own data dir (`crate::record_store`: one record per file,
//! temp + fsync + rename, an unrecognised file refused by name). Rafka binds its own production
//! backends to the same traits.
//!
//! - `mesh.storage`: the admin's own Mesh (id and name), so a restarted admin rejoins the same
//!   Mesh channel rather than minting a new one.
//! - `nodes.storage`: the admin's own row (NodeId, path.name, the incarnation it last ran, its
//!   transport identity), so a restart is the same logical node; the last-known
//!   births it heard, as bootstrap contacts (a stored contact is a hint: never topology, never
//!   death proof); and, when this admin is a birth's authority, the lifecycle state that birth
//!   declared and the admin applied (`status_rpc`).
//! - `connections.storage`: the latest connection fact per (source, destination, kind).

use async_trait::async_trait;
use crate::model::{IncarnationId, MeshId, NodeId, PathName, EndpointId};
use crate::record_store::{FileRecords, StorageError};
use rafka_mesh_entity::connections::{ConnectionIndex, NodeConnection};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

// ---------------------------------------------------------------- mesh.storage

/// A Mesh's identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshRecord {
    pub mesh_id: MeshId,
    pub name: String,
}

#[async_trait]
pub trait MeshStorage: Send + Sync {
    async fn mesh(&self, name: &str) -> Result<Option<MeshRecord>, StorageError>;
    async fn put_mesh(&self, record: &MeshRecord) -> Result<(), StorageError>;
    async fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError>;
}

#[derive(Debug, Default)]
pub struct MemoryMeshStorage(Mutex<BTreeMap<String, MeshRecord>>);

#[async_trait]
impl MeshStorage for MemoryMeshStorage {
    async fn mesh(&self, name: &str) -> Result<Option<MeshRecord>, StorageError> {
        Ok(self.0.lock().unwrap().get(name).cloned())
    }
    async fn put_mesh(&self, record: &MeshRecord) -> Result<(), StorageError> {
        self.0.lock().unwrap().insert(record.name.clone(), record.clone());
        Ok(())
    }
    async fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError> {
        Ok(self.0.lock().unwrap().values().cloned().collect())
    }
}

const MESH_FORMAT: &str = "mesh-record/1";

/// `<data dir>/meshes/<name>.json`.
#[derive(Debug)]
pub struct FileMeshStorage(FileRecords);

impl FileMeshStorage {
    pub fn open(own_data_dir: &Path) -> Result<Self, StorageError> {
        Ok(Self(FileRecords::open(own_data_dir, "meshes")?))
    }
}

#[async_trait]
impl MeshStorage for FileMeshStorage {
    async fn mesh(&self, name: &str) -> Result<Option<MeshRecord>, StorageError> {
        self.0.read(name, MESH_FORMAT)
    }
    async fn put_mesh(&self, record: &MeshRecord) -> Result<(), StorageError> {
        self.0.write(&record.name, MESH_FORMAT, record)
    }
    async fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError> {
        self.0.list(MESH_FORMAT)
    }
}

// ---------------------------------------------------------------- nodes.storage

/// One birth as last known: the admin's own row, or a contact it heard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub node_id: NodeId,
    pub name: PathName,
    /// The incarnation this birth last ran (a restart supersedes it).
    pub incarnation_id: IncarnationId,
    pub endpoint_id: EndpointId,
    /// The one address of the birth's Iroh endpoint.
    pub transport_addr: std::net::SocketAddr,
    /// Non-Iroh listeners (the own row of a node-admin: its `control` API).
    #[serde(default)]
    pub listeners: Vec<(String, std::net::SocketAddr)>,
    /// The lifecycle state this birth declared and its authority applied (`status_rpc`), when
    /// this admin is that authority. Never liveness: membership still says what is heard.
    #[serde(default)]
    pub declared: Option<String>,
    /// The status the mesh primary holds for this birth while it is silent (fabric-node-lifecycle.md
    /// §7.3): `PendingReconnect`, then `Dead` once its offline tickle found no path on two rounds a
    /// staleness floor apart. `None` while the birth is heard. Never a reason to restart or delete it.
    #[serde(default)]
    pub status: Option<crate::model::NodeStatus>,
}

impl NodeRecord {
    /// Where this birth's gossip was last reachable: its transport address, as membership dials.
    pub fn gossip_addr(&self) -> Option<iroh::EndpointAddr> {
        let key = self.endpoint_id.0.parse::<iroh::PublicKey>().ok()?;
        Some(iroh::EndpointAddr::new(key).with_ip_addr(self.transport_addr))
    }
}

#[async_trait]
pub trait NodesStorage: Send + Sync {
    /// This admin's own row; `None` before its first start completed.
    async fn own(&self) -> Result<Option<NodeRecord>, StorageError>;
    async fn put_own(&self, record: &NodeRecord) -> Result<(), StorageError>;
    /// The births this admin last heard, by NodeId (bootstrap contacts).
    async fn contacts(&self) -> Result<Vec<NodeRecord>, StorageError>;
    async fn put_contact(&self, record: &NodeRecord) -> Result<(), StorageError>;
    async fn remove_contact(&self, node_id: &NodeId) -> Result<(), StorageError>;
}

#[derive(Debug, Default)]
pub struct MemoryNodesStorage {
    own: Mutex<Option<NodeRecord>>,
    contacts: Mutex<BTreeMap<NodeId, NodeRecord>>,
}

#[async_trait]
impl NodesStorage for MemoryNodesStorage {
    async fn own(&self) -> Result<Option<NodeRecord>, StorageError> {
        Ok(self.own.lock().unwrap().clone())
    }
    async fn put_own(&self, record: &NodeRecord) -> Result<(), StorageError> {
        *self.own.lock().unwrap() = Some(record.clone());
        Ok(())
    }
    async fn contacts(&self) -> Result<Vec<NodeRecord>, StorageError> {
        Ok(self.contacts.lock().unwrap().values().cloned().collect())
    }
    async fn put_contact(&self, record: &NodeRecord) -> Result<(), StorageError> {
        self.contacts.lock().unwrap().insert(record.node_id.clone(), record.clone());
        Ok(())
    }
    async fn remove_contact(&self, node_id: &NodeId) -> Result<(), StorageError> {
        self.contacts.lock().unwrap().remove(node_id);
        Ok(())
    }
}

const NODE_FORMAT: &str = "node-record/1";

/// `<data dir>/nodes/self.json` and `<data dir>/nodes/contacts/<node id>.json`.
#[derive(Debug)]
pub struct FileNodesStorage {
    own: FileRecords,
    contacts: FileRecords,
}

impl FileNodesStorage {
    pub fn open(own_data_dir: &Path) -> Result<Self, StorageError> {
        Ok(Self { own: FileRecords::open(own_data_dir, "nodes")?, contacts: FileRecords::open(own_data_dir, "nodes/contacts")? })
    }
}

#[async_trait]
impl NodesStorage for FileNodesStorage {
    async fn own(&self) -> Result<Option<NodeRecord>, StorageError> {
        self.own.read("self", NODE_FORMAT)
    }
    async fn put_own(&self, record: &NodeRecord) -> Result<(), StorageError> {
        self.own.write("self", NODE_FORMAT, record)
    }
    async fn contacts(&self) -> Result<Vec<NodeRecord>, StorageError> {
        self.contacts.list(NODE_FORMAT)
    }
    async fn put_contact(&self, record: &NodeRecord) -> Result<(), StorageError> {
        self.contacts.write(record.node_id.as_str(), NODE_FORMAT, record)
    }
    async fn remove_contact(&self, node_id: &NodeId) -> Result<(), StorageError> {
        self.contacts.remove(node_id.as_str())
    }
}

// ---------------------------------------------------------------- connections.storage

#[async_trait]
pub trait ConnectionsStorage: Send + Sync {
    /// Keep `fact` as the latest for its (source, destination, kind).
    async fn put_connection(&self, fact: &NodeConnection) -> Result<(), StorageError>;
    async fn connections(&self) -> Result<Vec<NodeConnection>, StorageError>;
    async fn remove_connection(&self, index: &ConnectionIndex) -> Result<(), StorageError>;
    /// Append `fact` to the raw connection log (connections.md section 3): the history every
    /// fact joins, while the index above holds one entry per (source, destination, kind).
    async fn append_history(&self, fact: &NodeConnection) -> Result<(), StorageError>;
    /// The raw log, oldest first.
    async fn history(&self) -> Result<Vec<NodeConnection>, StorageError>;
}

#[derive(Debug, Default)]
pub struct MemoryConnectionsStorage(Mutex<BTreeMap<ConnectionIndex, NodeConnection>>, Mutex<Vec<NodeConnection>>);

#[async_trait]
impl ConnectionsStorage for MemoryConnectionsStorage {
    async fn put_connection(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        self.0.lock().unwrap().insert(fact.index(), fact.clone());
        Ok(())
    }
    async fn connections(&self) -> Result<Vec<NodeConnection>, StorageError> {
        Ok(self.0.lock().unwrap().values().cloned().collect())
    }
    async fn remove_connection(&self, index: &ConnectionIndex) -> Result<(), StorageError> {
        self.0.lock().unwrap().remove(index);
        Ok(())
    }
    async fn append_history(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        self.1.lock().unwrap().push(fact.clone());
        Ok(())
    }
    async fn history(&self) -> Result<Vec<NodeConnection>, StorageError> {
        Ok(self.1.lock().unwrap().clone())
    }
}

const CONNECTION_FORMAT: &str = "node-connection/1";

fn connection_key(i: &ConnectionIndex) -> String {
    let kind = match i.kind {
        rafka_mesh_entity::connections::ConnectionKind::Direct => "direct",
        rafka_mesh_entity::connections::ConnectionKind::Proxy => "proxy",
    };
    format!("{}--{}--{kind}", i.source, i.destination)
}

/// `<data dir>/connections/<source>--<destination>--<kind>.json`.
#[derive(Debug)]
pub struct FileConnectionsStorage(FileRecords, std::path::PathBuf, Mutex<()>);

impl FileConnectionsStorage {
    pub fn open(own_data_dir: &Path) -> Result<Self, StorageError> {
        let records = FileRecords::open(own_data_dir, "connections")?;
        Ok(Self(records, own_data_dir.join("connections").join("history.jsonl"), Mutex::new(())))
    }

    fn append_history_now(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        use std::io::Write as _;
        let _g = self.2.lock().unwrap();
        let io = |e: std::io::Error| StorageError::Io { file: self.1.display().to_string(), reason: e.to_string() };
        let mut line = serde_json::to_vec(fact).map_err(|e| StorageError::Io { file: self.1.display().to_string(), reason: e.to_string() })?;
        line.push(b'\n');
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&self.1).map_err(io)?;
        f.write_all(&line).map_err(io)?;
        f.sync_all().map_err(io)
    }
}

#[async_trait]
impl ConnectionsStorage for FileConnectionsStorage {
    async fn put_connection(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        self.0.write(&connection_key(&fact.index()), CONNECTION_FORMAT, fact)
    }
    async fn connections(&self) -> Result<Vec<NodeConnection>, StorageError> {
        self.0.list(CONNECTION_FORMAT)
    }
    async fn remove_connection(&self, index: &ConnectionIndex) -> Result<(), StorageError> {
        self.0.remove(&connection_key(index))
    }
    async fn append_history(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        crate::record_store::off_the_runtime(|| self.append_history_now(fact))
    }
    async fn history(&self) -> Result<Vec<NodeConnection>, StorageError> {
        match std::fs::read_to_string(&self.1) {
            Ok(text) => text
                .lines()
                .filter(|l| !l.trim().is_empty())
                .map(|l| serde_json::from_str(l).map_err(|e| StorageError::Unrecognised { file: self.1.display().to_string(), reason: e.to_string() }))
                .collect(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(StorageError::Io { file: self.1.display().to_string(), reason: e.to_string() }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rafka_mesh_entity::connections::{ConnectionEnd, ConnectionKind, ConnectionState};

    fn node(name: &str) -> NodeRecord {
        NodeRecord {
            node_id: NodeId::mint(),
            name: name.parse().unwrap(),
            incarnation_id: IncarnationId::mint(),
            endpoint_id: EndpointId(iroh::SecretKey::generate().public().to_string()),
            transport_addr: "127.0.0.1:41001".parse().unwrap(),
            listeners: vec![],
            declared: None,
            status: None,
        }
    }

    fn end(name: &str) -> ConnectionEnd {
        ConnectionEnd { name: name.parse().unwrap(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()) }
    }

    fn fact(state: ConnectionState) -> NodeConnection {
        NodeConnection {
            source: end("mesh1.admin.1"),
            destination: end("mesh1.rpc.1"),
            kind: ConnectionKind::Direct,
            state,
            carrier: None,
            recovery: None,
            reason: None,
            logged_at_ms: 1,
        }
    }

    /// Every boundary behaves the same in memory and on disk, and the disk copy survives reopening.
    #[tokio::test]
    async fn mesh_nodes_and_connections_hold_their_records_in_memory_and_across_a_reopen() {
        let d = crate::record_store::tempdir("storage");
        let mesh = MeshRecord { mesh_id: MeshId::mint(), name: "mesh1".into() };
        let (own, peer) = (node("mesh1.admin.1"), node("mesh1.rpc.1"));
        let (up, down) = (fact(ConnectionState::Connected), fact(ConnectionState::Disconnected));
        let check = async |m: &dyn MeshStorage, n: &dyn NodesStorage, c: &dyn ConnectionsStorage, fresh: bool| {
            if fresh {
                assert_eq!(m.mesh("mesh1").await.unwrap(), None);
                assert_eq!(n.own().await.unwrap(), None);
                m.put_mesh(&mesh).await.unwrap();
                n.put_own(&own).await.unwrap();
                n.put_contact(&peer).await.unwrap();
                c.put_connection(&up).await.unwrap();
                c.put_connection(&down).await.unwrap();
            }
            assert_eq!(m.mesh("mesh1").await.unwrap(), Some(mesh.clone()));
            assert_eq!(m.meshes().await.unwrap(), [mesh.clone()]);
            assert_eq!(n.own().await.unwrap(), Some(own.clone()));
            assert_eq!(n.contacts().await.unwrap(), [peer.clone()]);
            assert_eq!(c.connections().await.unwrap(), [down.clone()], "the latest fact per (source, destination, kind)");
        };
        let mem = (MemoryMeshStorage::default(), MemoryNodesStorage::default(), MemoryConnectionsStorage::default());
        check(&mem.0, &mem.1, &mem.2, true).await;
        check(&FileMeshStorage::open(&d).unwrap(), &FileNodesStorage::open(&d).unwrap(), &FileConnectionsStorage::open(&d).unwrap(), true).await;
        let (m, n, c) = (FileMeshStorage::open(&d).unwrap(), FileNodesStorage::open(&d).unwrap(), FileConnectionsStorage::open(&d).unwrap());
        check(&m, &n, &c, false).await;
        n.remove_contact(&peer.node_id).await.unwrap();
        c.remove_connection(&down.index()).await.unwrap();
        assert!(n.contacts().await.unwrap().is_empty() && c.connections().await.unwrap().is_empty());
        assert_eq!(peer.gossip_addr().map(|a| a.id.to_string()), Some(peer.endpoint_id.0.clone()));
        std::fs::write(d.join("meshes").join("mesh1.json"), br#"{"format":"mesh-record/9","record":{}}"#).unwrap();
        assert!(matches!(m.mesh("mesh1").await, Err(StorageError::Unrecognised { .. })));
    }
}
