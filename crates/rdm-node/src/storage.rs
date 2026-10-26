//! Durable storage for a node's `connections` table (`rdm-node`).

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use rdm_core::connections::{ConnectionIndex, NodeConnection};

#[cfg(feature = "file-backend")]
use crate::model::PathName;
use crate::storage_error::StorageError;

// ---------------------------------------------------------------- connections.storage

/// The durable record of the connection facts a node holds as a source: one resident entry per
/// index and a raw history.
#[async_trait]
pub trait ConnectionsStorage: Send + Sync {
    /// Keep `fact` as the latest for its (source, destination, kind).
    async fn put_connection(&self, fact: &NodeConnection) -> Result<(), StorageError>;
    /// Every resident connection fact.
    async fn connections(&self) -> Result<Vec<NodeConnection>, StorageError>;
    /// Forget the resident entry at `index`.
    async fn remove_connection(&self, index: &ConnectionIndex) -> Result<(), StorageError>;
    /// Append `fact` to the raw connection log (connections.md section 3): the history every
    /// fact joins, while the index above holds one entry per (source, destination, kind).
    async fn append_history(&self, fact: &NodeConnection) -> Result<(), StorageError>;
    /// The raw log, oldest first.
    async fn history(&self) -> Result<Vec<NodeConnection>, StorageError>;
    /// Every current connection row, in any mesh and of any source, whose destination is the node
    /// `destination` (every incarnation of it): one read of the store's by-destination index
    /// (connections.md §3.1). Who holds a connection to a node, never whether it is reachable: a
    /// row nobody marked dropped survives a cut or a crashed source.
    async fn connections_to(&self, destination: &crate::model::NodeId) -> Result<Vec<NodeConnection>, StorageError>;
    /// Every resident Direct connection fact across the shared connections store (`connections.md`
    /// §3.2, §4.1: each source writes under `connections/<node>/`; carrier discovery reads the
    /// fleet-wide active Direct graph from the store).
    async fn fleet_directs(&self) -> Result<Vec<NodeConnection>, StorageError> {
        Ok(self
            .connections()
            .await?
            .into_iter()
            .filter(|r| r.kind == rdm_core::connections::ConnectionKind::Direct)
            .collect())
    }
}

/// A connections store held in memory.
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
    async fn connections_to(&self, destination: &crate::model::NodeId) -> Result<Vec<NodeConnection>, StorageError> {
        Ok(self.0.lock().unwrap().values().filter(|r| &r.destination.node_id == destination).cloned().collect())
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

#[cfg(feature = "file-backend")]
/// A connection fact's line in the root's `connections.jsonl`: the whole fact, at its index's key.
const CONNECTION_FORMAT: &str = "node-connection/2";
#[cfg(feature = "file-backend")]
const CONNECTION_KIND: &str = "node-connection";

#[cfg(feature = "file-backend")]
fn connection_key(i: &ConnectionIndex) -> String {
    let kind = match i.kind {
        rdm_core::connections::ConnectionKind::Direct => "direct",
        rdm_core::connections::ConnectionKind::Proxy => "proxy",
    };
    format!("{}--{}--{}--{kind}", i.source, i.destination, hex::encode(&i.alpn))
}

#[cfg(feature = "file-backend")]
/// One line of `connections.jsonl`: a fact, or (`removed`) the fact whose index entry was forgotten,
/// so the key's latest line holds no current entry. Nothing is removed from the file (R-SR2).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ConnectionLine {
    #[serde(flatten)]
    fact: NodeConnection,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    removed: bool,
}

#[cfg(feature = "file-backend")]
/// Every connection fact of every source, one line each of the root's `connections.jsonl` (R-SR1,
/// connections.md §3): the raw log and the current view are one file, the view the fold's latest line
/// per (source, destination, alpn, kind), and the by-destination index the same fold keyed the other
/// way. This store is one node's: it writes that node's facts, held in the root's scratchpad until
/// the fabric's first state-commit, reads them back as its own (by its writer, or by the path.name it
/// was opened at), and reads every source's for the fleet's Direct rows and the by-destination index.
#[derive(Debug)]
pub struct FileConnectionsStorage {
    log: crate::store_log::StoreLog,
    source: PathName,
    writer: crate::store_log::LogWriter,
}

#[cfg(feature = "file-backend")]
impl FileConnectionsStorage {
    /// Open the store of the node `node`, written by `writer`, on the fabric's store root.
    pub async fn open(root: &crate::store_root::StoreRoot, node: &PathName, writer: &crate::store_log::LogWriter) -> Result<Self, StorageError> {
        let log = root.log(crate::store_root::CONNECTIONS_FILE, crate::store_log::Formats::new().with::<ConnectionLine>(CONNECTION_FORMAT)).await?;
        Ok(Self { log, source: node.clone(), writer: writer.clone() })
    }

    /// Whether `line`, read as `fact`, is this node's: written by this store's writer (under any name
    /// the node held since: a rename keeps its rows), held by this store, or a fact whose source is the
    /// path.name this store was opened at (a birth's predecessor's at that name).
    fn own(&self, line: &crate::store_log::Line, fact: &NodeConnection) -> bool {
        match &line.op {
            None => true,
            Some(op) => op.node_id == self.writer.node_id.to_string() || fact.source.name == self.source,
        }
    }

    /// The current entry of every key (the latest line of each that is not removed), of every source
    /// (`all`) or of this node's only.
    async fn current(&self, all: bool) -> Result<Vec<NodeConnection>, StorageError> {
        let mut out = Vec::new();
        for l in self.log.latest_of(CONNECTION_KIND).await? {
            let line: ConnectionLine = l.decode(self.log.path())?;
            if !line.removed && (all || self.own(&l, &line.fact)) {
                out.push(line.fact);
            }
        }
        Ok(out)
    }

    /// Every line of this node, in file order.
    async fn own_lines(&self) -> Result<Vec<ConnectionLine>, StorageError> {
        let mut out = Vec::new();
        for l in self.log.lines().await? {
            let line: ConnectionLine = l.decode(self.log.path())?;
            if self.own(&l, &line.fact) {
                out.push(line);
            }
        }
        Ok(out)
    }

    /// Finishes every write this store has issued.
    pub async fn finish_issued_writes(&self) {
        self.log.finish_issued_writes().await;
    }
}

#[cfg(feature = "file-backend")]
#[async_trait]
impl ConnectionsStorage for FileConnectionsStorage {
    /// One line: the fact is the raw log's line and its key's latest entry at once.
    async fn put_connection(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        self.log.append(&self.writer, CONNECTION_FORMAT, &connection_key(&fact.index()), &ConnectionLine { fact: fact.clone(), removed: false }).await
    }
    async fn connections(&self) -> Result<Vec<NodeConnection>, StorageError> {
        self.current(false).await
    }
    async fn connections_to(&self, destination: &crate::model::NodeId) -> Result<Vec<NodeConnection>, StorageError> {
        Ok(self.current(true).await?.into_iter().filter(|f| &f.destination.node_id == destination).collect())
    }
    /// The key's entry is forgotten by a line of its last fact marked removed: the raw log keeps the fact.
    async fn remove_connection(&self, index: &ConnectionIndex) -> Result<(), StorageError> {
        let key = connection_key(index);
        let Some(latest) = self.log.latest(CONNECTION_KIND, &key).await? else { return Ok(()) };
        let line: ConnectionLine = latest.decode(self.log.path())?;
        if line.removed {
            return Ok(());
        }
        self.log.append(&self.writer, CONNECTION_FORMAT, &key, &ConnectionLine { fact: line.fact, removed: true }).await
    }
    /// The raw log is the file: [`Self::put_connection`]'s line is the fact's log line.
    async fn append_history(&self, _fact: &NodeConnection) -> Result<(), StorageError> {
        Ok(())
    }
    async fn fleet_directs(&self) -> Result<Vec<NodeConnection>, StorageError> {
        Ok(self.current(true).await?.into_iter().filter(|r| r.kind == rdm_core::connections::ConnectionKind::Direct).collect())
    }
    /// This source's facts, oldest first.
    async fn history(&self) -> Result<Vec<NodeConnection>, StorageError> {
        Ok(self.own_lines().await?.into_iter().filter(|l| !l.removed).map(|l| l.fact).collect())
    }
}

#[cfg(feature = "file-backend")]
/// The raw connection log of the node at `source` in the store root `root_dir`'s `connections.jsonl`:
/// every fact a node that wrote at that path.name wrote, under any name it held (a rename keeps its
/// log), oldest first, read on the calling thread (a cell's look at a root while its nodes write it).
/// A missing file holds none; a malformed line is refused by name.
pub fn connection_history(root_dir: &std::path::Path, source: &str) -> Result<Vec<NodeConnection>, StorageError> {
    let file = root_dir.join(crate::store_root::CONNECTIONS_FILE);
    let mut facts = Vec::new();
    for l in crate::store_log::read_file(&file)? {
        if l.kind() == CONNECTION_KIND {
            let line: ConnectionLine = l.decode(&file)?;
            if !line.removed {
                facts.push(line.fact);
            }
        }
    }
    let nodes: std::collections::BTreeSet<crate::model::NodeId> = facts.iter().filter(|f| f.source.name.to_string() == source).map(|f| f.source.node_id.clone()).collect();
    Ok(facts.into_iter().filter(|f| nodes.contains(&f.source.node_id)).collect())
}
