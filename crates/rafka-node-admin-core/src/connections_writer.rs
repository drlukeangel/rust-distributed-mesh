//! The source-owned connections writer (connections.md sections 3, 4.2, 9 and 10; i143.e6.s11):
//! every node writes its own Direct and Proxy facts, and holds its own projection of them.
//!
//! The Node RPC client reports what it observes of this node's pooled connections through
//! [`ConnectionObserver`]; this writer turns each observation into one row, appends it to the raw
//! log, keeps it as the latest of its pair in the index, and applies it to the held projection the
//! route resolver reads. At birth the held projection is hydrated from the index, so a restart
//! keeps its latest Proxy and the backoff of its latest Direct failure. The writer decides no
//! route and chooses no carrier: it records facts.

use crate::storage::ConnectionsStorage;
use rafka_mesh_entity::connections::{ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, NodeConnection};
use rafka_mesh_entity::reconnect::{first_failure, owed_retirements};
use rafka_node_rpc::{ConnectionObserver, ResolvedNode};
use std::sync::{Arc, Mutex};

pub struct ConnectionsWriter {
    own: ConnectionEnd,
    storage: Arc<dyn ConnectionsStorage>,
    held: Arc<Mutex<ConnectionsHeld>>,
    runtime: tokio::runtime::Handle,
    /// The last stamp this writer gave a row: every row it writes carries a later one, so two
    /// facts in one millisecond never tie (the held projection keeps one stamp per key).
    last_stamp: Mutex<u64>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn end_of(node: &ResolvedNode) -> ConnectionEnd {
    ConnectionEnd { name: node.name.clone(), node_id: node.node_id.clone(), incarnation: Some(node.incarnation.clone()) }
}

impl ConnectionsWriter {
    /// A writer for `own`, over its storage, holding `held` (shared with the route resolver).
    pub fn new(own: ConnectionEnd, storage: Arc<dyn ConnectionsStorage>, held: Arc<Mutex<ConnectionsHeld>>) -> Self {
        Self { own, storage, held, runtime: tokio::runtime::Handle::current(), last_stamp: Mutex::new(0) }
    }

    pub fn held(&self) -> Arc<Mutex<ConnectionsHeld>> {
        self.held.clone()
    }

    /// Hydrate the held projection from the index: every latest row this node wrote (its own
    /// Direct and Proxy facts) is applied, so the first call after a restart resolves over the
    /// latest Proxy and the reconnect series continues from the latest failure.
    pub async fn hydrate(&self) -> Result<usize, String> {
        let rows = self.storage.connections().await.map_err(|e| e.to_string())?;
        let mut held = self.held.lock().unwrap();
        held.set_own_source(self.own.name.clone());
        let mut applied = 0;
        for row in rows {
            if held.apply(row).is_ok() {
                applied += 1;
            }
        }
        held.mark_complete();
        Ok(applied)
    }

    /// Record one fact: the raw log first, then the index, then the held projection.
    pub async fn record(&self, row: NodeConnection) -> Result<(), String> {
        self.storage.append_history(&row).await.map_err(|e| e.to_string())?;
        self.storage.put_connection(&row).await.map_err(|e| e.to_string())?;
        let _ = self.held.lock().unwrap().apply(row);
        Ok(())
    }

    /// The latest Direct row this node holds toward `destination`, if any.
    fn latest_direct(&self, destination: &ConnectionEnd) -> Option<NodeConnection> {
        self.held.lock().unwrap().own_latest_directs().into_iter().find(|d| d.destination.name == destination.name).cloned()
    }

    fn direct(&self, destination: ConnectionEnd, state: ConnectionState, reason: Option<String>) -> NodeConnection {
        NodeConnection {
            source: self.own.clone(),
            destination,
            kind: ConnectionKind::Direct,
            state,
            carrier: None,
            recovery: None,
            reason,
            logged_at_ms: now_ms(),
        }
    }

    /// Every Proxy this node owes a retirement for (a Direct Connected sits beside it), written
    /// durably as `Disconnected(direct-restored)`; new calls cut back only once these land.
    pub async fn retire_owed(&self) -> Result<usize, String> {
        let at = self.stamp();
        let owed = owed_retirements(&self.held.lock().unwrap(), at);
        let n = owed.len();
        for row in owed {
            self.record(row).await?;
        }
        Ok(n)
    }

    /// A strictly increasing stamp for this writer's rows.
    fn stamp(&self) -> u64 {
        let mut last = self.last_stamp.lock().unwrap();
        *last = now_ms().max(*last + 1);
        *last
    }

    /// An observed fact: applied to the held projection at once (the next observation and the
    /// next resolution read it), then written to the raw log and the index.
    fn observed(&self, mut row: NodeConnection) {
        row.logged_at_ms = self.stamp();
        let _ = self.held.lock().unwrap().apply(row.clone());
        let storage = self.storage.clone();
        self.runtime.spawn(async move {
            if let Err(e) = storage.append_history(&row).await {
                tracing::warn!(error = %e, "connection history append failed");
            }
            if let Err(e) = storage.put_connection(&row).await {
                tracing::warn!(error = %e, "connection index write failed");
            }
        });
    }
}

impl ConnectionObserver for ConnectionsWriter {
    fn direct_connected(&self, node: &ResolvedNode) {
        self.observed(self.direct(end_of(node), ConnectionState::Connected, None));
    }

    fn direct_failed(&self, node: &ResolvedNode, reason: &str) {
        let destination = end_of(node);
        let previous = self.latest_direct(&destination);
        // A failing series continues in its epoch, one ordinal up; anything else (never tried,
        // connected, dropped) is a new incident: a new epoch at ordinal one.
        let row = match previous.as_ref() {
            Some(p) if p.state == ConnectionState::Failed => match p.recovery {
                Some(r) => NodeConnection {
                    recovery: Some(rafka_mesh_entity::connections::DirectRecovery { recovery_epoch: r.recovery_epoch, attempt_ordinal: r.attempt_ordinal.saturating_add(1) }),
                    reason: Some(reason.to_string()),
                    logged_at_ms: now_ms(),
                    ..p.clone()
                },
                None => first_failure(self.own.clone(), destination, previous.as_ref(), reason, now_ms()),
            },
            _ => first_failure(self.own.clone(), destination, previous.as_ref(), reason, now_ms()),
        };
        self.observed(row);
    }

    fn direct_broken(&self, node: &ResolvedNode, reason: &str) {
        self.observed(self.direct(end_of(node), ConnectionState::Disconnected, Some(reason.to_string())));
    }
}
