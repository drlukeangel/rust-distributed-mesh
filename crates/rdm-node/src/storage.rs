//! Durable storage for a node's `connections` table (`rdm-node`).

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use rdm_core::connections::{ConnectionIndex, NodeConnection};

use crate::model::{IncarnationId, PathName};
use crate::record_store::{FileRecords, StorageError};

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
        rdm_core::connections::ConnectionKind::Direct => "direct",
        rdm_core::connections::ConnectionKind::Proxy => "proxy",
    };
    format!("{}--{}--{}--{kind}", i.source, i.destination, hex::encode(&i.alpn))
}

/// `<store root>/connections/<node>/<source>--<destination>--<kind>.json` and its raw log
/// `history.jsonl`: the node's own connections rows, written by the node itself.
#[derive(Debug)]
pub struct FileConnectionsStorage(FileRecords, std::sync::Arc<HistoryLog>, std::path::PathBuf);

/// A node's raw connection log, `history.jsonl`: appended one line per fact, one appender at a
/// time; held in the scratchpad, like the node's connections rows, until its publication.
#[derive(Debug)]
struct HistoryLog {
    path: std::path::PathBuf,
    /// The store's directory under the store root: how a publication names it.
    store: String,
    append: tokio::sync::Mutex<()>,
    /// `Some` while the log holds its lines in the scratchpad: the lines held, oldest first.
    held: Mutex<Option<Vec<Vec<u8>>>>,
}

impl HistoryLog {
    /// Append `lines` to the file and fsync it, on the blocking pool.
    async fn append_to_file(&self, lines: Vec<Vec<u8>>) -> Result<(), StorageError> {
        use std::io::Write as _;
        let path = self.path.clone();
        crate::record_store::blocking_io(move || {
            let io = |e: std::io::Error| StorageError::Io { file: path.display().to_string(), reason: e.to_string() };
            let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&path).map_err(io)?;
            for line in &lines {
                f.write_all(line).map_err(io)?;
            }
            f.sync_all().map_err(io)
        })
        .await
    }
}

#[async_trait]
impl crate::record_store::HeldStore for HistoryLog {
    async fn publish(&self) -> Result<crate::record_store::StorePublished, StorageError> {
        let _g = self.append.lock().await;
        let lines = self.held.lock().unwrap_or_else(|e| e.into_inner()).clone().unwrap_or_default();
        let written = lines.len();
        if written > 0 {
            self.append_to_file(lines).await?;
        }
        *self.held.lock().unwrap_or_else(|e| e.into_inner()) = None;
        Ok(crate::record_store::StorePublished { store: self.store.clone(), written, ..Default::default() })
    }
}

impl FileConnectionsStorage {
    /// Open the store of the node `node` under `connections/<node>/` of the fabric's store root,
    /// written by the process born as `writer`: held in the root's scratchpad until its publication.
    pub async fn open(root: &crate::store_root::StoreRoot, node: &PathName, writer: &IncarnationId) -> Result<Self, StorageError> {
        let dir = root.connections(node);
        let connections_root = root.dir().join("connections");
        let records = root.records(&dir, "", writer).await?;
        let log = std::sync::Arc::new(HistoryLog {
            path: dir.join("history.jsonl"),
            store: format!("connections/{node}/history"),
            append: tokio::sync::Mutex::new(()),
            held: Mutex::new(Some(Vec::new())),
        });
        if !root.scratchpad().is_some_and(|s| s.enrol(log.clone())) {
            *log.held.lock().unwrap_or_else(|e| e.into_inner()) = None;
        }
        Ok(Self(records, log, connections_root))
    }

    /// Append `fact` to the raw log, one appender at a time: held while the log holds its lines,
    /// else written and fsynced on the blocking pool.
    async fn append_history_line(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        let path = &self.1.path;
        let mut line = serde_json::to_vec(fact).map_err(|e| StorageError::Io { file: path.display().to_string(), reason: e.to_string() })?;
        line.push(b'\n');
        let _g = self.1.append.lock().await;
        if let Some(held) = self.1.held.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
            held.push(line);
            return Ok(());
        }
        self.1.append_to_file(vec![line]).await
    }
}

#[async_trait]
impl ConnectionsStorage for FileConnectionsStorage {
    async fn put_connection(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        self.0.write(&connection_key(&fact.index()), CONNECTION_FORMAT, fact).await
    }
    async fn connections(&self) -> Result<Vec<NodeConnection>, StorageError> {
        self.0.list(CONNECTION_FORMAT).await
    }
    async fn remove_connection(&self, index: &ConnectionIndex) -> Result<(), StorageError> {
        self.0.remove(&connection_key(index)).await.map(|_| ())
    }
    async fn append_history(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        self.append_history_line(fact).await
    }
    async fn fleet_directs(&self) -> Result<Vec<NodeConnection>, StorageError> {
        let mut by_index: BTreeMap<ConnectionIndex, NodeConnection> = self
            .connections()
            .await?
            .into_iter()
            .filter(|r| r.kind == rdm_core::connections::ConnectionKind::Direct)
            .map(|r| (r.index(), r))
            .collect();
        let connections_root = self.2.clone();
        let disk_rows = crate::record_store::blocking_io(move || {
            let mut rows = Vec::new();
            let node_dirs = match std::fs::read_dir(&connections_root) {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(rows),
                Err(e) => {
                    return Err(StorageError::Io {
                        file: connections_root.display().to_string(),
                        reason: e.to_string(),
                    })
                }
            };
            let mut dirs: Vec<std::path::PathBuf> = node_dirs
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.is_dir())
                .collect();
            dirs.sort();
            for dir in dirs {
                let Ok(entries) = std::fs::read_dir(&dir) else {
                    continue;
                };
                let mut files: Vec<std::path::PathBuf> = entries
                    .filter_map(|e| e.ok().map(|e| e.path()))
                    .filter(|p| {
                        p.file_name()
                            .and_then(|n| n.to_str())
                            .is_some_and(|n| !n.starts_with('.') && n.ends_with("--direct.json"))
                    })
                    .collect();
                files.sort();
                for path in files {
                    match std::fs::read(&path) {
                        Ok(bytes) => {
                            let row = FileRecords::decode::<NodeConnection>(&path, &bytes, CONNECTION_FORMAT)?;
                            if row.kind == rdm_core::connections::ConnectionKind::Direct {
                                rows.push(row);
                            }
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => {
                            return Err(StorageError::Io {
                                file: path.display().to_string(),
                                reason: e.to_string(),
                            })
                        }
                    }
                }
            }
            Ok(rows)
        })
        .await?;
        for row in disk_rows {
            by_index.entry(row.index()).or_insert(row);
        }
        Ok(by_index.into_values().collect())
    }
    async fn history(&self) -> Result<Vec<NodeConnection>, StorageError> {
        let path = &self.1.path;
        // One reader or appender at a time: a publication moving the held lines to the file never
        // runs between the two reads below.
        let _g = self.1.append.lock().await;
        let held = self.1.held.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let file = path.clone();
        let mut text = match crate::record_store::blocking_io(move || std::fs::read_to_string(&file)).await {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(e) => return Err(StorageError::Io { file: path.display().to_string(), reason: e.to_string() }),
        };
        if let Some(lines) = held {
            for l in lines {
                text.push_str(&String::from_utf8_lossy(&l));
            }
        }
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).map_err(|e| StorageError::Unrecognised { file: path.display().to_string(), reason: e.to_string() }))
            .collect()
    }
}

