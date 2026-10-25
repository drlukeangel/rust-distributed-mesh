//! `node.config.update` at the node's mesh-primary (ops-naming "Node data: meta, config and exe").
//!
//! Config is a field of the node record. The mesh-primary of the node's mesh is the only writer of
//! that mesh's rows (fabric-node-lifecycle §4.3): the door refuses by name at any admin that does not
//! hold that mesh's primary seat. It checks the cap, writes the node's whole row from what it holds (a
//! blind put of the `path.name` key; it never reads the store on the write path), tells the exact
//! birth directly, within its mesh, with `ApplyNodeConfig`, which the node holds and carries in its
//! next digest, and answers `node.config.updated` only after that apply answered. A stopped node is
//! told nothing and the command completes on the row: the node gets its config back in its join
//! answer at its next start (node-rpc-envelope.md "Node properties"). A caller that cannot
//! reach the mesh-primary directly reaches it through a carrier (Forward `0x1A`). A single call, never
//! a Build, and never refused because a Build runs: updates go through this one door one at a time.

use crate::storage::NodeRow;
use crate::topology::Topology;
use rdm_client::{CallEnd, ExactBirth, NodeRpc};
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget};
use rdm_core::status::{NotPrimary, StatusReply, StatusRequest};
use std::sync::Arc;

/// A mesh-primary's config door: the client it tells a birth of its mesh through, what holds this
/// admin's own config (the mesh-primary is a member of its mesh and the writer of its own row too),
/// and the lock that makes every config change one at a time.
pub struct NodeConfigs {
    client: Arc<NodeRpcClient>,
    own: crate::status_rpc::Configure,
    one_at_a_time: tokio::sync::Mutex<()>,
    /// The record this admin's writer of its mesh's rows holds (`crate::node_rows::WrittenRows`):
    /// each row the door puts is held there, so the node's next row carries the config put.
    written: Arc<crate::node_rows::WrittenRows>,
}

/// The slot an admin fills once its node-RPC client exists.
pub type NodeConfigsSlot = Arc<std::sync::OnceLock<Arc<NodeConfigs>>>;

impl NodeConfigs {
    /// The door over `client`; `own` holds this admin's config when it is the subject; `written` is
    /// the record the writer of this admin's mesh rows holds (`NodeRows::with_written`).
    pub fn new(client: Arc<NodeRpcClient>, own: crate::status_rpc::Configure, written: Arc<crate::node_rows::WrittenRows>) -> Self {
        Self { client, own, one_at_a_time: tokio::sync::Mutex::new(()), written }
    }

    /// The client this door tells births through: the mesh-primary's node.name.update tells a birth its
    /// name through it as well (`crate::node_name_update`).
    pub fn client(&self) -> &NodeRpcClient {
        &self.client
    }

    /// The record this admin's writer of its mesh's rows holds: node.name.update puts its rows through it
    /// as this door does, so each row carries every field the writer knows.
    pub fn written(&self) -> &Arc<crate::node_rows::WrittenRows> {
        &self.written
    }

    /// The turn of one change to a node record at this mesh-primary (node-config-update.md:
    /// "every change to a node record goes through its mesh-primary's one held record, one at a
    /// time"): this door's puts and node.name.update's take it.
    pub async fn one_at_a_time(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.one_at_a_time.lock().await
    }

    /// Serve an `UpdateNodeConfig` at the admin `me` (born as `me_id`) deciding over `view`, writing to `storage`. One
    /// span, opened first, holds the whole put: the row write and the call that tells the birth are
    /// inside it, and its outcome is the reply's name. A refusal's reply carries its detail cut to the
    /// reply bound; the span carries it whole (`answered`, `detail`), so nothing of it is lost.
    pub async fn update(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, view: &Topology, storage: &dyn crate::storage::NodesStorage, req: &StatusRequest) -> StatusReply {
        let StatusRequest::UpdateNodeConfig { node_id, incarnation, config } = req else { unreachable!("update is called for UpdateNodeConfig only") };
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-config",
            node = %me,
            subject = tracing::field::Empty,
            node_id = %node_id,
            incarnation_id = %incarnation.as_str(),
            keys = config.len(),
            tags = tracing::field::Empty,
            row_status = tracing::field::Empty,
            apply = tracing::field::Empty,
            outcome = tracing::field::Empty,
            answered = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let reply = tracing::Instrument::instrument(self.put(me, me_id, view, storage, node_id, incarnation, config, &span), span.clone()).await;
        span.record("outcome", reply.name());
        span.in_scope(|| tracing::info!("node.config.update decided"));
        reply
    }

    #[allow(clippy::too_many_arguments)]
    async fn put(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, view: &Topology, storage: &dyn crate::storage::NodesStorage, node_id: &rdm_core::NodeId, incarnation: &rdm_core::IncarnationId, config: &std::collections::BTreeMap<String, String>, span: &tracing::Span) -> StatusReply {
        if let Err(over) = rdm_core::check_config(config) {
            return rdm_core::reply::status::rejected_config(over);
        }
        // The subject is the exact birth the call names, wherever its row stands: a node whose row its
        // mesh-primary moved to `<path>.old` is still that birth (never found by path.name, its shape
        // included: `Topology::own`). A subject this receiver does not hold is the receiver's own fact
        // (node-rpc-envelope.md: "`SubjectUnknown` when the receiver does not hold the subject at all"),
        // never a peer that did not resolve.
        let Some(subject) = view.own(node_id) else {
            span.record("detail", format!("{me}: node {node_id} holds no birth in this admin's view; its config cannot be put").as_str());
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown);
        };
        span.record("subject", subject.name.to_string().as_str());
        span.record("row_status", format!("{:?}", subject.status).as_str());
        // The writer of the row is the mesh-primary of the node's mesh, and no other admin
        // (fabric-node-lifecycle §4.3); a caller that sent it elsewhere is told whose seat it needs.
        if !view.is_mesh_primary(&subject.mesh, me_id) {
            let held_by = view.node_type_primary(&subject.mesh, rdm_core::NodeKind::NodeAdmin).map_or_else(|| "held by no single live node-admin in this view".to_string(), |p| format!("held by {}", p.name));
            let needed = format!("{}'s primary node-admin, the writer of {}'s row ({held_by}; {me} does not hold it)", subject.mesh, subject.name);
            span.record("detail", needed.as_str());
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed });
        }
        match subject.incarnation_id.as_ref() {
            Some(held) if held != incarnation => return rdm_core::reply::stale(held.clone()),
            None => {
                span.record("detail", format!("{me}: {} has no known birth; its config cannot be put", subject.name).as_str());
                return rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown);
            }
            Some(_) => {}
        }
        let _one = self.one_at_a_time.lock().await;
        // The record is the one this writer holds, its tags, facts and pending release kept: a config
        // put changes the config (storage-whole-records.md, "The rule").
        match self.written.known_row(storage, node_id).await {
            // A writer that meets the record finishes the release a rename left pending
            // (node-name-update.md); one that fails again stays pending, named on its span.
            Ok(Some(r)) => {
                let _ = crate::node_rows::finish_release(storage, &self.written, me, &r, "config-put").await;
                span.record("tags", crate::node_rows::labels(&r.tags).as_str());
            }
            Ok(None) => {}
            Err(why) => {
                let detail = format!("{me}: {why}: the row of {} is not written without the tags it carries", subject.name);
                span.record("detail", detail.as_str());
                return rdm_core::reply::status::bounded_config_row_refused(&subject.name.to_string(), &detail);
            }
        }
        let base = NodeRow { config: config.clone(), ..NodeRow::new(subject.name.clone(), node_id.clone(), subject.status) };
        let status = subject.status;
        let refused = match self.written.update(storage, node_id, Some(base), |r| {
            r.status = status;
            r.config = config.clone();
        }).await {
            Ok(_) => None,
            Err(why) => Some(format!("{me}: {why}")),
        };
        if let Some(detail) = refused {
            span.record("detail", detail.as_str());
            return rdm_core::reply::status::bounded_config_row_refused(&subject.name.to_string(), &detail);
        }
        // A stopped node is told nothing: its config is a field of its row, and it gets it back in its
        // join answer at its next start (node-rpc-envelope.md "Node properties"), as node.name.update
        // completes a stopped node on the row.
        if subject.stopped {
            span.record("apply", "stopped");
            return StatusReply::ConfigUpdated { node_id: node_id.clone(), incarnation: incarnation.clone() };
        }
        // The node applies it. This admin as the subject applies it in-process, through what holds
        // its config (the same hand-off its `ApplyNodeConfig` door uses); any other member of its
        // mesh is called directly.
        let told = if subject.node_id == *me_id {
            span.record("apply", "in-process");
            match self.own.get() {
                Some(configure) => {
                    configure(config.clone()).await;
                    Ok(rdm_core::reply::applied())
                }
                None => Ok(rdm_core::reply::status::not_ready(format!("{me}: its digest does not exist yet; the config is not held"))),
            }
        } else {
            span.record("apply", "direct");
            let birth = ExactBirth { target: NodeTarget::ExactNode(node_id.clone()), node_id: node_id.clone(), incarnation: incarnation.clone() };
            NodeRpc::new(&self.client).config_apply(&birth, config.clone(), &CallOptions::default()).await
        };
        let (answered, detail) = match told {
            Ok(StatusReply::Applied | StatusReply::AlreadyApplied) => return StatusReply::ConfigUpdated { node_id: node_id.clone(), incarnation: incarnation.clone() },
            Ok(refused) => (refused.name(), format!("{me} wrote the row; the birth answered {refused:?}")),
            Err(end) => (end.outcome(), format!("{me} wrote the row; apply-node-config ended {}", describe(&end))),
        };
        span.record("answered", answered);
        span.record("detail", detail.as_str());
        rdm_core::reply::status::bounded_config_not_applied(&subject.name.to_string(), answered, &detail)
    }
}

fn describe(end: &CallEnd) -> String {
    format!("{end:?}")
}
