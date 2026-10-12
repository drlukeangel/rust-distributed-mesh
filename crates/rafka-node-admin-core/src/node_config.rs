//! `node.config.update` at the node's mesh-primary (ops-naming "Node data: meta, config and exe").
//!
//! Config is a field of the node record. The mesh-primary of the node's mesh is the only writer of
//! that mesh's rows (fabric-node-lifecycle §4.3): the door refuses by name at any admin that does not
//! hold that mesh's primary seat. It checks the cap, writes the node's whole row from what it holds (a
//! blind put of the `path.name` key; it never reads the store on the write path), tells the exact
//! birth directly, within its mesh, with `ApplyNodeConfig`, which the node holds and carries in its
//! next digest, and answers `node.config.updated` only after that apply answered. A caller that cannot
//! reach the mesh-primary directly reaches it through a carrier (Forward `0x1A`). A single call, never
//! a Build, and never refused because a Build runs: updates go through this one door one at a time.

use crate::storage::NodeRow;
use crate::topology::Topology;
use rafka_node_admin_client::{CallEnd, ExactBirth, NodeRpc};
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget};
use rafka_node_rpc_contract::status::{NotAuthority, StatusReply, StatusRequest};
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

    /// The client this door tells births through: the mesh-primary's node.update tells a birth its
    /// name through it as well (`crate::node_update`).
    pub fn client(&self) -> &NodeRpcClient {
        &self.client
    }

    /// The record this admin's writer of its mesh's rows holds: node.update puts its rows through it
    /// as this door does, so each row carries every field the writer knows.
    pub fn written(&self) -> &Arc<crate::node_rows::WrittenRows> {
        &self.written
    }

    /// The turn of one change to a node record at this mesh-primary (node-update.md "Config update":
    /// "every change to a node record goes through its mesh-primary's one held record, one at a
    /// time"): this door's puts and node.update's take it.
    pub async fn one_at_a_time(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.one_at_a_time.lock().await
    }

    /// Serve an `UpdateNodeConfig` at the admin `me` (born as `me_id`) deciding over `view`, writing to `storage`. One
    /// span, opened first, holds the whole put: the row write and the call that tells the birth are
    /// inside it, and its outcome is the reply's name. A refusal's reply carries its detail cut to the
    /// reply bound; the span carries it whole (`answered`, `detail`), so nothing of it is lost.
    pub async fn update(&self, me: &crate::model::PathName, me_id: &rafka_mesh_entity::NodeId, view: &Topology, storage: &dyn crate::storage::NodesStorage, req: &StatusRequest) -> StatusReply {
        let StatusRequest::UpdateNodeConfig { node_id, incarnation, config } = req else { unreachable!("update is called for UpdateNodeConfig only") };
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-config",
            node = %me,
            subject = tracing::field::Empty,
            node_id = %node_id,
            incarnation_id = %incarnation.0,
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
    async fn put(&self, me: &crate::model::PathName, me_id: &rafka_mesh_entity::NodeId, view: &Topology, storage: &dyn crate::storage::NodesStorage, node_id: &rafka_mesh_entity::NodeId, incarnation: &rafka_mesh_entity::IncarnationId, config: &std::collections::BTreeMap<String, String>, span: &tracing::Span) -> StatusReply {
        if let Err(over) = rafka_mesh_entity::check_config(config) {
            return StatusReply::RejectedConfig { over };
        }
        // The subject is the exact birth the call names, wherever its row stands: a node whose row its
        // mesh-primary moved to `<path>.old` is still that birth (never found by path.name, its shape
        // included: `Topology::own`). A subject this receiver does not hold is the receiver's own fact
        // (node-rpc-envelope.md: "`SubjectUnknown` when the receiver does not hold the subject at all"),
        // never a peer that did not resolve.
        let Some(subject) = view.own(node_id) else {
            span.record("detail", format!("{me}: node {node_id} holds no birth in this admin's view; its config cannot be put").as_str());
            return StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown };
        };
        span.record("subject", subject.name.to_string().as_str());
        span.record("row_status", format!("{:?}", subject.status).as_str());
        // The writer of the row is the mesh-primary of the node's mesh, and no other admin
        // (fabric-node-lifecycle §4.3); a caller that sent it elsewhere is told whose seat it needs.
        if !view.holds_mesh_seat(&subject.mesh, me_id) {
            let held_by = view.cohort_primary(&subject.mesh, rafka_mesh_entity::NodeKind::NodeAdmin).map_or_else(|| "held by no single live node-admin in this view".to_string(), |p| format!("held by {}", p.name));
            let needed = format!("{}'s primary node-admin, the writer of {}'s row ({held_by}; {me} does not hold it)", subject.mesh, subject.name);
            span.record("detail", needed.as_str());
            return StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed } };
        }
        match subject.incarnation_id.as_ref() {
            Some(held) if held != incarnation => return StatusReply::RejectedStaleIncarnation { held: held.clone() },
            None => {
                span.record("detail", format!("{me}: {} has no known birth; its config cannot be put", subject.name).as_str());
                return StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown };
            }
            Some(_) => {}
        }
        let _one = self.one_at_a_time.lock().await;
        // The row carries the node's tags and its caller-owned group as this writer knows them: a
        // config put changes the config.
        let (tags, otel_debug) = match self.written.known_row(storage, &subject.mesh, node_id).await {
            Ok(row) => row.map(|r| (r.tags, r.otel_debug)).unwrap_or_default(),
            Err(why) => {
                let detail = format!("{me}: {why}: the row of {} is not written without the tags it carries", subject.name);
                span.record("detail", detail.as_str());
                return StatusReply::config_row_refused(&subject.name.to_string(), &detail);
            }
        };
        span.record("tags", crate::node_rows::labels(&tags).as_str());
        let row = NodeRow { name: subject.name.clone(), node_id: node_id.clone(), status: subject.status, config: config.clone(), tags, otel_debug, allocated: None };
        let refused = match storage.put_node_row(&row).await {
            Ok(Ok(())) => {
                self.written.hold(&row);
                None
            }
            Ok(Err(held)) => Some(format!("{me}: nodes.storage refused the row of {node_id}: {held}")),
            Err(e) => Some(format!("{me}: nodes.storage refused the row of {node_id}: {e}")),
        };
        if let Some(detail) = refused {
            span.record("detail", detail.as_str());
            return StatusReply::config_row_refused(&subject.name.to_string(), &detail);
        }
        // The node applies it. This admin as the subject applies it in-process, through what holds
        // its config (the same hand-off its `ApplyNodeConfig` door uses); any other member of its
        // mesh is called directly.
        let told = if subject.node_id == *me_id {
            span.record("apply", "in-process");
            match self.own.get() {
                Some(configure) => {
                    configure(config.clone()).await;
                    Ok(StatusReply::Applied)
                }
                None => Ok(StatusReply::NotReady { reason: format!("{me}: its digest does not exist yet; the config is not held") }),
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
        StatusReply::config_not_applied(&subject.name.to_string(), answered, &detail)
    }
}

fn describe(end: &CallEnd) -> String {
    format!("{end:?}")
}
