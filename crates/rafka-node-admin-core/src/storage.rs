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
//!   transport identity and endpoints), so a restart is the same logical node; and the last-known
//!   births it heard, as bootstrap contacts. A stored contact is a hint: never topology, never
//!   death proof.
//! - `connections.storage`: the latest connection fact per (source, destination, kind).

use crate::model::{IncarnationId, MeshId, NodeId, PathName, TransportId};
use crate::record_store::{FileRecords, StorageError};
use rafka_mesh_entity::connections::{ConnectionIndex, NodeConnection};
use rafka_mesh_entity::EndpointSlot;
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

pub trait MeshStorage: Send + Sync {
    fn mesh(&self, name: &str) -> Result<Option<MeshRecord>, StorageError>;
    fn put_mesh(&self, record: &MeshRecord) -> Result<(), StorageError>;
    fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError>;
}

#[derive(Debug, Default)]
pub struct MemoryMeshStorage(Mutex<BTreeMap<String, MeshRecord>>);

impl MeshStorage for MemoryMeshStorage {
    fn mesh(&self, name: &str) -> Result<Option<MeshRecord>, StorageError> {
        Ok(self.0.lock().unwrap().get(name).cloned())
    }
    fn put_mesh(&self, record: &MeshRecord) -> Result<(), StorageError> {
        self.0.lock().unwrap().insert(record.name.clone(), record.clone());
        Ok(())
    }
    fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError> {
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

impl MeshStorage for FileMeshStorage {
    fn mesh(&self, name: &str) -> Result<Option<MeshRecord>, StorageError> {
        self.0.read(name, MESH_FORMAT)
    }
    fn put_mesh(&self, record: &MeshRecord) -> Result<(), StorageError> {
        self.0.write(&record.name, MESH_FORMAT, record)
    }
    fn meshes(&self) -> Result<Vec<MeshRecord>, StorageError> {
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
    pub transport_id: TransportId,
    /// The one address of the birth's Iroh endpoint.
    pub transport_addr: std::net::SocketAddr,
    pub endpoints: Vec<EndpointSlot>,
    /// Non-Iroh listeners (the own row of a node-admin: its `control` API).
    #[serde(default)]
    pub listeners: Vec<(String, std::net::SocketAddr)>,
}

impl NodeRecord {
    /// Where this birth's gossip was last reachable: its transport address, as membership dials.
    pub fn gossip_addr(&self) -> Option<iroh::EndpointAddr> {
        let key = self.transport_id.0.parse::<iroh::PublicKey>().ok()?;
        Some(iroh::EndpointAddr::new(key).with_ip_addr(self.transport_addr))
    }
}

pub trait NodesStorage: Send + Sync {
    /// This admin's own row; `None` before its first start completed.
    fn own(&self) -> Result<Option<NodeRecord>, StorageError>;
    fn put_own(&self, record: &NodeRecord) -> Result<(), StorageError>;
    /// The births this admin last heard, by NodeId (bootstrap contacts).
    fn contacts(&self) -> Result<Vec<NodeRecord>, StorageError>;
    fn put_contact(&self, record: &NodeRecord) -> Result<(), StorageError>;
    fn remove_contact(&self, node_id: &NodeId) -> Result<(), StorageError>;
}

#[derive(Debug, Default)]
pub struct MemoryNodesStorage {
    own: Mutex<Option<NodeRecord>>,
    contacts: Mutex<BTreeMap<NodeId, NodeRecord>>,
}

impl NodesStorage for MemoryNodesStorage {
    fn own(&self) -> Result<Option<NodeRecord>, StorageError> {
        Ok(self.own.lock().unwrap().clone())
    }
    fn put_own(&self, record: &NodeRecord) -> Result<(), StorageError> {
        *self.own.lock().unwrap() = Some(record.clone());
        Ok(())
    }
    fn contacts(&self) -> Result<Vec<NodeRecord>, StorageError> {
        Ok(self.contacts.lock().unwrap().values().cloned().collect())
    }
    fn put_contact(&self, record: &NodeRecord) -> Result<(), StorageError> {
        self.contacts.lock().unwrap().insert(record.node_id.clone(), record.clone());
        Ok(())
    }
    fn remove_contact(&self, node_id: &NodeId) -> Result<(), StorageError> {
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

impl NodesStorage for FileNodesStorage {
    fn own(&self) -> Result<Option<NodeRecord>, StorageError> {
        self.own.read("self", NODE_FORMAT)
    }
    fn put_own(&self, record: &NodeRecord) -> Result<(), StorageError> {
        self.own.write("self", NODE_FORMAT, record)
    }
    fn contacts(&self) -> Result<Vec<NodeRecord>, StorageError> {
        self.contacts.list(NODE_FORMAT)
    }
    fn put_contact(&self, record: &NodeRecord) -> Result<(), StorageError> {
        self.contacts.write(record.node_id.as_str(), NODE_FORMAT, record)
    }
    fn remove_contact(&self, node_id: &NodeId) -> Result<(), StorageError> {
        self.contacts.remove(node_id.as_str())
    }
}

// ---------------------------------------------------------------- connections.storage

pub trait ConnectionsStorage: Send + Sync {
    /// Keep `fact` as the latest for its (source, destination, kind).
    fn put_connection(&self, fact: &NodeConnection) -> Result<(), StorageError>;
    fn connections(&self) -> Result<Vec<NodeConnection>, StorageError>;
    fn remove_connection(&self, index: &ConnectionIndex) -> Result<(), StorageError>;
}

#[derive(Debug, Default)]
pub struct MemoryConnectionsStorage(Mutex<BTreeMap<ConnectionIndex, NodeConnection>>);

impl ConnectionsStorage for MemoryConnectionsStorage {
    fn put_connection(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        self.0.lock().unwrap().insert(fact.index(), fact.clone());
        Ok(())
    }
    fn connections(&self) -> Result<Vec<NodeConnection>, StorageError> {
        Ok(self.0.lock().unwrap().values().cloned().collect())
    }
    fn remove_connection(&self, index: &ConnectionIndex) -> Result<(), StorageError> {
        self.0.lock().unwrap().remove(index);
        Ok(())
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
pub struct FileConnectionsStorage(FileRecords);

impl FileConnectionsStorage {
    pub fn open(own_data_dir: &Path) -> Result<Self, StorageError> {
        Ok(Self(FileRecords::open(own_data_dir, "connections")?))
    }
}

impl ConnectionsStorage for FileConnectionsStorage {
    fn put_connection(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        self.0.write(&connection_key(&fact.index()), CONNECTION_FORMAT, fact)
    }
    fn connections(&self) -> Result<Vec<NodeConnection>, StorageError> {
        self.0.list(CONNECTION_FORMAT)
    }
    fn remove_connection(&self, index: &ConnectionIndex) -> Result<(), StorageError> {
        self.0.remove(&connection_key(index))
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
            transport_id: TransportId(iroh::SecretKey::generate().public().to_string()),
            transport_addr: "127.0.0.1:41001".parse().unwrap(),
            endpoints: vec![EndpointSlot::fresh("rpc-0")],
            listeners: vec![],
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
    #[test]
    fn mesh_nodes_and_connections_hold_their_records_in_memory_and_across_a_reopen() {
        let d = crate::record_store::tempdir("storage");
        let mesh = MeshRecord { mesh_id: MeshId::mint(), name: "mesh1".into() };
        let (own, peer) = (node("mesh1.admin.1"), node("mesh1.rpc.1"));
        let (up, down) = (fact(ConnectionState::Connected), fact(ConnectionState::Disconnected));
        let check = |m: &dyn MeshStorage, n: &dyn NodesStorage, c: &dyn ConnectionsStorage, fresh: bool| {
            if fresh {
                assert_eq!(m.mesh("mesh1").unwrap(), None);
                assert_eq!(n.own().unwrap(), None);
                m.put_mesh(&mesh).unwrap();
                n.put_own(&own).unwrap();
                n.put_contact(&peer).unwrap();
                c.put_connection(&up).unwrap();
                c.put_connection(&down).unwrap();
            }
            assert_eq!(m.mesh("mesh1").unwrap(), Some(mesh.clone()));
            assert_eq!(m.meshes().unwrap(), [mesh.clone()]);
            assert_eq!(n.own().unwrap(), Some(own.clone()));
            assert_eq!(n.contacts().unwrap(), [peer.clone()]);
            assert_eq!(c.connections().unwrap(), [down.clone()], "the latest fact per (source, destination, kind)");
        };
        let mem = (MemoryMeshStorage::default(), MemoryNodesStorage::default(), MemoryConnectionsStorage::default());
        check(&mem.0, &mem.1, &mem.2, true);
        check(&FileMeshStorage::open(&d).unwrap(), &FileNodesStorage::open(&d).unwrap(), &FileConnectionsStorage::open(&d).unwrap(), true);
        let (m, n, c) = (FileMeshStorage::open(&d).unwrap(), FileNodesStorage::open(&d).unwrap(), FileConnectionsStorage::open(&d).unwrap());
        check(&m, &n, &c, false);
        n.remove_contact(&peer.node_id).unwrap();
        c.remove_connection(&down.index()).unwrap();
        assert!(n.contacts().unwrap().is_empty() && c.connections().unwrap().is_empty());
        assert_eq!(peer.gossip_addr().map(|a| a.id.to_string()), Some(peer.transport_id.0.clone()));
        std::fs::write(d.join("meshes").join("mesh1.json"), br#"{"format":"mesh-record/9","record":{}}"#).unwrap();
        assert!(matches!(m.mesh("mesh1"), Err(StorageError::Unrecognised { .. })));
    }
}
