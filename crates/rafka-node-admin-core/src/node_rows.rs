//! The node row at the fabric-primary (ops-naming "Node data: meta, config and exe"; the delete
//! point; lifecycles/node-decommission.md step 3).
//!
//! The fabric-primary is the only writer of a node's row. It writes the whole row from the record
//! it holds (its topology), a blind put of the `path.name` key, and never reads the store on that
//! write. A row is written at two moments:
//!
//! - `node.delete` (`retire-node:<path>`), read off the live `Step` frame the executor streams: at
//!   `NodeDeleted` the row is written with status `Deleted`, the record and its config kept. The
//!   record is the one held when the executor announced the removal (`NodeDeleting`): the node is
//!   still found and live in every view then, while by `NodeDeleted` its departure may already
//!   have left the view.
//! - A birth the fabric-primary's own view holds for the first time (`Records::install_projected`):
//!   its row is claimed at its `path.name` (insert-and-fail) from that held record, its config
//!   seeded from the previous row at that `path.name`. A birth of a peer mesh reaches the
//!   fabric-primary's view through the forwarded aggregate, after its create's last frame, so the
//!   row follows the view, never the create's frames. An admin that takes the seat claims every
//!   member's row; a row a node already holds is never overwritten.

use crate::model::{Node, NodeStatus, PathName};
use crate::storage::{Claimed, NodeRow, NodesStorage};
use crate::topology::Topology;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

/// The fabric-primary's writer of node rows.
pub struct NodeRows {
    /// This admin's `path.name`: it writes only while its view names it the fabric-primary.
    me: PathName,
    storage: Arc<dyn NodesStorage>,
    /// The config door a seeded birth is told its config through (`crate::node_config`).
    configs: Option<crate::node_config::NodeConfigsSlot>,
    /// The record held when each removal was announced (`NodeDeleting`), by operation, until its
    /// `NodeDeleted` writes it.
    announced: Mutex<HashMap<String, Node>>,
}

impl NodeRows {
    /// The writer of the admin `me` over `storage`, the fabric-primary's nodes store.
    pub fn new(me: PathName, storage: Arc<dyn NodesStorage>) -> Self {
        Self { me, storage, configs: None, announced: Mutex::new(HashMap::new()) }
    }

    /// Tell each birth whose row is seeded its config through `configs`, the fabric-primary's
    /// config door.
    pub fn with_configs(mut self, configs: crate::node_config::NodeConfigsSlot) -> Self {
        self.configs = Some(configs);
        self
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
            ("retire-node" | "replace-node", "NodeDeleting") => {
                if let Some(n) = held_at(view, &name).await {
                    self.announced.lock().unwrap().insert(operation.to_string(), n);
                }
            }
            ("retire-node", "NodeDeleted") => self.deleted(me, view, operation, &name).await,
            // A replace: the predecessor gives up its path.name (its row moves to `<path>.old`), and
            // its departure is written there.
            ("replace-node", "RenamePredecessor") => self.moved_to_old(me, view, operation, &name).await,
            ("replace-node", "NodeDeleted") => self.deleted(me, view, operation, &name.renamed()).await,
            _ => {}
        }
    }

    /// A replace's `RenamePredecessor`: the predecessor's row moves from `name` to `<name>.old`, from
    /// the record held at its `NodeDeleting` (or the view now, for a fabric-primary that took the
    /// seat since), insert-and-fail on the `.old` key, then `name` is released for the successor.
    async fn moved_to_old(&self, me: &PathName, view: &RwLock<Topology>, operation: &str, name: &PathName) {
        let to = name.renamed();
        let announced = self.announced.lock().unwrap().get(operation).cloned();
        let record = match announced {
            Some(n) => Some(n),
            None => held_at(view, name).await,
        };
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-rename",
            node = %me,
            node_id = tracing::field::Empty,
            from = %name,
            to = %to,
            operation,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let Some(record) = record else {
            let detail = format!("{me}: the replace of {name} reached RenamePredecessor and this fabric-primary holds no record of the predecessor; no row is moved");
            span.record("outcome", "no-held-record").record("detail", detail.as_str());
            span.in_scope(|| tracing::info!("node.replace: no row moved"));
            return;
        };
        span.record("node_id", record.node_id.as_str());
        let r = tracing::Instrument::instrument(self.move_row(me, &record, name, &to), span.clone()).await;
        let (outcome, detail) = match &r {
            Ok(()) => ("renamed", None),
            Err(RenameRefused::NameHeld(e)) => ("name-held", Some(format!("{me}: the replace of {name} ({}) to {to}: {e}", record.node_id))),
            Err(RenameRefused::RdmOwned(e)) => ("meta-field-is-rdm-owned", Some(e.to_string())),
            Err(RenameRefused::Unwritten(why)) => ("unwritten", Some(why.clone())),
        };
        span.record("outcome", outcome);
        if let Some(d) = &detail {
            span.record("detail", d.as_str());
        }
        span.in_scope(|| tracing::info!("node.replace: the predecessor's row moved"));
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

    /// The births of `now`, the view being installed over `prev`, whose rows this admin claims:
    /// none unless `now` names it the fabric-primary; every member when `prev` did not (the seat was
    /// just taken: the rebuild after a handover, which never overwrites a row a node holds); else
    /// each member birth `prev` did not hold. Read under the view lock, so of two installs each
    /// sees the arrivals its own change made.
    pub fn arrivals(&self, prev: &Topology, now: &Topology) -> Vec<Node> {
        if now.fabric_primary().map(|n| &n.name) != Some(&self.me) {
            return Vec::new();
        }
        let took_seat = prev.fabric_primary().map(|n| &n.name) != Some(&self.me);
        now.members()
            .filter(|n| n.incarnation_id.is_some())
            .filter(|n| took_seat || !prev.members().any(|p| p.node_id == n.node_id && p.incarnation_id == n.incarnation_id))
            .cloned()
            .collect()
    }

    /// Claim the row of each birth in `arrivals`, the fabric-primary's view `now` having first held
    /// it; a birth whose row is seeded from a previous row is told that config.
    pub async fn claim_arrivals(&self, now: &Topology, arrivals: Vec<Node>) {
        for birth in arrivals {
            let span = tracing::info_span!(
                "rdm.node_admin.node.create.via-node-row",
                node = %self.me,
                subject = %birth.name,
                node_id = %birth.node_id,
                seeded_from = tracing::field::Empty,
                keys = tracing::field::Empty,
                outcome = tracing::field::Empty,
                told = tracing::field::Empty,
                detail = tracing::field::Empty,
                "otel.kind" = "internal",
            );
            if let Some(row) = tracing::Instrument::instrument(self.claim(&birth, &span), span.clone()).await {
                self.tell(now, &birth, row, &span);
            }
        }
    }

    /// Tell a seeded birth its config (ops-naming "Node data": the node applies it and carries it in
    /// its next digest) through the fabric-primary's config door, which writes the record and calls
    /// the birth through its mesh-primary. On its own task, under the claim's span: the view install
    /// that found the birth never waits on a call to it (the join door installs the view before it
    /// answers the joiner).
    fn tell(&self, now: &Topology, birth: &Node, row: NodeRow, span: &tracing::Span) {
        let Some(door) = self.configs.as_ref().and_then(|slot| slot.get().cloned()) else {
            span.record("told", "no-config-door");
            return;
        };
        let Some(incarnation) = birth.incarnation_id.clone() else { return };
        span.record("told", "config-door");
        let req = rafka_node_rpc_contract::status::StatusRequest::UpdateNodeConfig { node_id: birth.node_id.clone(), incarnation, config: row.config };
        let (me, view, storage) = (self.me.clone(), now.clone(), self.storage.clone());
        tokio::spawn(tracing::Instrument::instrument(
            async move {
                door.update(&me, &view, &*storage, &req).await;
            },
            span.clone(),
        ));
    }

    /// The birth's row at its `path.name`, claimed from the record the view holds (ops-naming "Node
    /// data": "writes the whole row from that held record"), its config seeded from the previous
    /// row at that `path.name` (a `Deleted` row), read through the `path.name` key ("from the
    /// fabric-primary's held record or a point read through the `path.name` index"). A row the
    /// same node holds already is left as it is.
    async fn claim(&self, birth: &Node, span: &tracing::Span) -> Option<NodeRow> {
        let (me, name) = (&self.me, &birth.name);
        let previous = match self.storage.node_row_at(name).await {
            Ok(p) => p,
            Err(e) => {
                let detail = format!("{me}: the previous row at {name} could not be read for the birth {}: {e}", birth.node_id);
                span.record("outcome", "row-unreadable").record("detail", detail.as_str());
                span.in_scope(|| tracing::info!("node.create: no row claimed"));
                return None;
            }
        };
        // The previous row at the path.name: a `Deleted` row there, or, when the key is free, the
        // `<name>.old` row a replace moved the predecessor's row to.
        let seed = match previous {
            Some(p) => Some(p).filter(|p| p.status == NodeStatus::Deleted && p.node_id != birth.node_id),
            None => match self.storage.node_row_at(&name.renamed()).await {
                Ok(old) => old.filter(|p| p.node_id != birth.node_id),
                Err(e) => {
                    let detail = format!("{me}: the .old row at {} could not be read for the birth {}: {e}", name.renamed(), birth.node_id);
                    span.record("outcome", "row-unreadable").record("detail", detail.as_str());
                    span.in_scope(|| tracing::info!("node.create: no row claimed"));
                    return None;
                }
            },
        };
        let config = seed.as_ref().map_or_else(|| birth.config.clone(), |p| p.config.clone());
        if let Some(p) = &seed {
            span.record("seeded_from", p.node_id.as_str());
        }
        span.record("keys", config.len() as u64);
        let row = NodeRow { name: name.clone(), node_id: birth.node_id.clone(), status: birth.status, config };
        match self.storage.claim_node_row(&row).await {
            Ok(Ok(Claimed::Written)) => {
                span.record("outcome", if seed.is_some() { "row-seeded" } else { "row-claimed" });
                span.in_scope(|| tracing::info!("node.create: the birth holds the row at its path.name"));
                seed.map(|_| row)
            }
            Ok(Ok(Claimed::HeldByNode)) => {
                span.record("outcome", "row-held-by-node");
                span.in_scope(|| tracing::info!("node.create: the node already holds its row; nothing is written"));
                None
            }
            Ok(Err(held)) => {
                let detail = format!("{me}: the birth {} at {name} claimed a held name: {held}", birth.node_id);
                span.record("outcome", "name-held").record("detail", detail.as_str());
                span.in_scope(|| tracing::info!("node.create: the name is held"));
                None
            }
            Err(e) => {
                let detail = format!("{me}: nodes.storage refused the row of {name} ({}): {e}", birth.node_id);
                span.record("outcome", "row-refused").record("detail", detail.as_str());
                span.in_scope(|| tracing::info!("node.create: the row was refused"));
                None
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
        self.move_row(me, &record, &held.name, &body.name).await
    }

    /// The node's row moves from `from` to `to`: the new name is claimed for the node from its held
    /// `record` (insert-and-fail, `NameHeld` naming another holder), then the old key is released.
    /// The one writer of a rename: `node.update` and a replace's `RenamePredecessor` both move here.
    async fn move_row(&self, me: &PathName, record: &Node, from: &PathName, to: &PathName) -> Result<(), RenameRefused> {
        if from == to {
            return Ok(());
        }
        let row = NodeRow { name: to.clone(), node_id: record.node_id.clone(), status: record.status, config: record.config.clone() };
        match self.storage.claim_node_row(&row).await {
            Ok(Ok(_)) => {}
            Ok(Err(name_held)) => return Err(RenameRefused::NameHeld(name_held)),
            Err(e) => return Err(RenameRefused::Unwritten(format!("{me}: nodes.storage refused the claim of {to} for {}: {e}", record.node_id))),
        }
        // The new name is the node's: the old key is let go.
        self.storage.remove_node_row(from).await.map_err(|e| RenameRefused::Unwritten(format!("{me}: {} holds {to} and nodes.storage refused the release of {from}: {e}", record.node_id)))
    }
}

/// The record the view holds at `name`.
async fn held_at(view: &RwLock<Topology>, name: &PathName) -> Option<Node> {
    view.read().await.nodes.iter().find(|n| &n.name == name).cloned()
}
