//! The node row at the fabric-primary (ops-naming "Node data: meta, config and exe"; the delete
//! point; lifecycles/node-decommission.md step 3).
//!
//! The fabric-primary is the only writer of a node's row. It writes the whole row from the record
//! it holds (its topology), a blind put of the `path.name` key, and never reads the store on that
//! write. Two moments of a Build it drives write a row, each read off the live `Step` frame the
//! executor streams:
//!
//! - `node.delete` (`retire-node:<path>`): at `NodeDeleted` the row is written with status
//!   `Deleted`, the record and its config kept. The record is the one held when the executor
//!   announced the removal (`NodeDeleting`): the node is still found and live in every view then,
//!   while by `NodeDeleted` its departure may already have left the view.
//! - `node.create` (`create-node:<path>`): at `Complete` the new birth's row is claimed at its
//!   `path.name` (insert-and-fail), its config seeded from the previous row at that `path.name`.

use crate::model::{Node, NodeStatus, PathName};
use crate::storage::{NodeRow, NodesStorage};
use crate::topology::Topology;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

/// The fabric-primary's writer of node rows.
pub struct NodeRows {
    /// This admin's `path.name`: it writes only while its view names it the fabric-primary.
    me: PathName,
    storage: Arc<dyn NodesStorage>,
    /// The record held when each removal was announced (`NodeDeleting`), by operation, until its
    /// `NodeDeleted` writes it.
    announced: Mutex<HashMap<String, Node>>,
}

impl NodeRows {
    /// The writer of the admin `me` over `storage`, the fabric-primary's nodes store.
    pub fn new(me: PathName, storage: Arc<dyn NodesStorage>) -> Self {
        Self { me, storage, announced: Mutex::new(HashMap::new()) }
    }

    /// The store the rows are written to.
    pub fn storage(&self) -> &Arc<dyn NodesStorage> {
        &self.storage
    }

    /// A live `Step` frame of a Build the fabric-primary `me` drives, over its held view.
    pub async fn on_step(&self, me: &PathName, view: &RwLock<Topology>, operation: &str, step: &str) {
        let Some((verb, path)) = operation.split_once(':') else { return };
        let Ok(name) = path.parse::<PathName>() else { return };
        match (verb, step) {
            ("retire-node", "NodeDeleting") => {
                if let Some(n) = held_at(view, &name).await {
                    self.announced.lock().unwrap().insert(operation.to_string(), n);
                }
            }
            ("retire-node", "NodeDeleted") => self.deleted(me, view, operation, &name).await,
            ("create-node", "Complete") => self.created(me, view, &name).await,
            _ => {}
        }
    }

    /// `NodeDeleted`: the row with status `Deleted`, the record and config kept, a blind put.
    async fn deleted(&self, me: &PathName, view: &RwLock<Topology>, operation: &str, name: &PathName) {
        let span = tracing::info_span!(
            "rdm.node_admin.node.delete.via-node-row",
            node = %me,
            subject = %name,
            operation,
            node_id = tracing::field::Empty,
            keys = tracing::field::Empty,
            held_from = tracing::field::Empty,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let announced = self.announced.lock().unwrap().remove(operation);
        let (held, from) = match announced {
            Some(n) => (Some(n), "node-deleting"),
            // A fabric-primary that took the seat after the announcement holds no record of it:
            // the view as it is now, while it still holds the node.
            None => (held_at(view, name).await, "view-at-node-deleted"),
        };
        let Some(n) = held else {
            let detail = format!("{me}: the removal of {name} reached NodeDeleted and this fabric-primary holds no record of the node, neither from its NodeDeleting nor in its view now; no row is written");
            span.record("outcome", "no-held-record").record("detail", detail.as_str());
            span.in_scope(|| tracing::info!("node.delete: no row written"));
            return;
        };
        span.record("node_id", n.node_id.as_str()).record("keys", n.config.len() as u64).record("held_from", from);
        let row = NodeRow { name: name.clone(), node_id: n.node_id.clone(), status: NodeStatus::Deleted, config: n.config.clone() };
        match self.storage.put_node_row(&row).await {
            Ok(()) => {
                span.record("outcome", "row-deleted");
                span.in_scope(|| tracing::info!("node.delete: the row says Deleted; the record and its config stay"));
            }
            Err(e) => {
                let detail = format!("{me}: nodes.storage refused the Deleted row of {name} ({}): {e}", n.node_id);
                span.record("outcome", "row-refused").record("detail", detail.as_str());
                span.in_scope(|| tracing::info!("node.delete: the Deleted row was refused"));
            }
        }
    }

    /// The create's `Complete`: claim the new birth's row at its `path.name`, its config seeded
    /// from the previous row there (a `Deleted` row, or the same node's own row on a restart). The
    /// previous row is a point read through the `path.name` key (ops-naming "Node data": "from the
    /// fabric-primary's held record or a point read through the `path.name` index").
    async fn created(&self, me: &PathName, view: &RwLock<Topology>, name: &PathName) {
        let span = tracing::info_span!(
            "rdm.node_admin.node.create.via-node-row",
            node = %me,
            subject = %name,
            node_id = tracing::field::Empty,
            seeded_from = tracing::field::Empty,
            keys = tracing::field::Empty,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let Some(birth) = held_at(view, name).await else {
            let detail = format!("{me}: the create of {name} completed and this fabric-primary's view holds no birth at {name}; no row is claimed");
            span.record("outcome", "not-in-view").record("detail", detail.as_str());
            span.in_scope(|| tracing::info!("node.create: no row claimed"));
            return;
        };
        span.record("node_id", birth.node_id.as_str());
        let previous = match self.storage.node_row_at(name).await {
            Ok(p) => p,
            Err(e) => {
                let detail = format!("{me}: the previous row at {name} could not be read for the create of {}: {e}", birth.node_id);
                span.record("outcome", "row-unreadable").record("detail", detail.as_str());
                span.in_scope(|| tracing::info!("node.create: no row claimed"));
                return;
            }
        };
        let seed = previous.filter(|p| p.status == NodeStatus::Deleted || p.node_id == birth.node_id);
        let config = seed.as_ref().map_or_else(|| birth.config.clone(), |p| p.config.clone());
        if let Some(p) = &seed {
            span.record("seeded_from", p.node_id.as_str());
        }
        span.record("keys", config.len() as u64);
        let row = NodeRow { name: name.clone(), node_id: birth.node_id.clone(), status: birth.status, config };
        match self.storage.claim_node_row(&row).await {
            Ok(Ok(())) => {
                span.record("outcome", if seed.is_some() { "row-seeded" } else { "row-claimed" });
                span.in_scope(|| tracing::info!("node.create: the birth holds the row at its path.name"));
            }
            Ok(Err(held)) => {
                let detail = format!("{me}: the create of {name} ({}) claimed a held name: {held}", birth.node_id);
                span.record("outcome", "name-held").record("detail", detail.as_str());
                span.in_scope(|| tracing::info!("node.create: the name is held"));
            }
            Err(e) => {
                let detail = format!("{me}: nodes.storage refused the row of {name} ({}): {e}", birth.node_id);
                span.record("outcome", "row-refused").record("detail", detail.as_str());
                span.in_scope(|| tracing::info!("node.create: the row was refused"));
            }
        }
    }
}

/// Why `node.update` refused a rename at the fabric-primary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenameRefused {
    /// The body differs from what RDM holds in a field RDM owns (a live node's name among them).
    RdmOwned(rafka_node_admin_client::MetaFieldIsRdmOwned),
    /// The new name is held by another node, parked or live.
    NameHeld(rafka_node_admin_client::NameHeld),
    /// The rename could not be decided or written: why, whole.
    Unwritten(String),
}

impl NodeRows {
    /// `node.update` of a stopped node's name (to or from its `.old` form): the body is checked
    /// against what RDM holds, then the new name is claimed for the node, insert-and-fail on its
    /// key, then the old key is released. Whether the node is stopped is the held record's
    /// (`parked`), never the caller's.
    pub async fn rename(&self, me: &PathName, view: &RwLock<Topology>, held: &rafka_node_admin_client::NodeMeta, body: &rafka_node_admin_client::NodeMeta) -> Result<(), RenameRefused> {
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-rename",
            node = %me,
            node_id = %held.node_id,
            from = %held.name,
            to = %body.name,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let r = tracing::Instrument::instrument(self.rename_steps(me, view, held, body), span.clone()).await;
        let (outcome, detail) = match &r {
            Ok(()) => ("renamed", None),
            Err(RenameRefused::RdmOwned(e)) => ("meta-field-is-rdm-owned", Some(e.to_string())),
            Err(RenameRefused::NameHeld(e)) => ("name-held", Some(format!("{me}: the rename of {} ({}) to {}: {e}", held.name, held.node_id, body.name))),
            Err(RenameRefused::Unwritten(why)) => ("unwritten", Some(why.clone())),
        };
        span.record("outcome", outcome);
        if let Some(d) = &detail {
            span.record("detail", d.as_str());
        }
        span.in_scope(|| tracing::info!("node.update: the rename decided"));
        r
    }

    async fn rename_steps(&self, me: &PathName, view: &RwLock<Topology>, held: &rafka_node_admin_client::NodeMeta, body: &rafka_node_admin_client::NodeMeta) -> Result<(), RenameRefused> {
        // The held record: whether the node is stopped, its status and its config. It is the
        // fabric-primary's own, never the caller's body.
        let Some(record) = view.read().await.nodes.iter().find(|n| n.node_id == held.node_id).cloned() else {
            return Err(RenameRefused::Unwritten(format!("{me}: node {} is not in the fabric-primary's view; its name cannot be changed", held.node_id)));
        };
        body.check_put(held, record.parked).map_err(RenameRefused::RdmOwned)?;
        if body.name == held.name {
            return Ok(());
        }
        let row = NodeRow { name: body.name.clone(), node_id: record.node_id.clone(), status: record.status, config: record.config.clone() };
        match self.storage.claim_node_row(&row).await {
            Ok(Ok(())) => {}
            Ok(Err(name_held)) => return Err(RenameRefused::NameHeld(name_held)),
            Err(e) => return Err(RenameRefused::Unwritten(format!("{me}: nodes.storage refused the claim of {} for {}: {e}", body.name, record.node_id))),
        }
        // The new name is the node's: the old key is let go.
        self.storage
            .remove_node_row(&held.name)
            .await
            .map_err(|e| RenameRefused::Unwritten(format!("{me}: {} holds {} and nodes.storage refused the release of {}: {e}", record.node_id, body.name, held.name)))
    }
}

/// The record the view holds at `name`.
async fn held_at(view: &RwLock<Topology>, name: &PathName) -> Option<Node> {
    view.read().await.nodes.iter().find(|n| &n.name == name).cloned()
}
