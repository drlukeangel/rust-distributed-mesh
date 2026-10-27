//! `node.config.update` (node-config-update.md; ops-naming "Node data: meta, config and exe").
//!
//! The caller sends it to the fabric-primary (step 1, Q203): it writes the map to the node in its
//! `FabricAtlas` and sends the command to the node's mesh-primary (in process when it is that
//! mesh-primary too; Forward `0x1A` through another node-admin of the mesh when the mesh-primary is not
//! directly reachable). An admin that is neither refuses it by name (`ReceiverNotPrimary`).
//!
//! Config is a field of the node record. The mesh-primary of the node's mesh is the only writer of
//! that mesh's rows (fabric-node-lifecycle §4.3). It writes the node's whole row from what it holds (a
//! blind put of the `path.name` key; it never reads the store on the write path), tells the exact
//! birth directly, within its mesh, with `ApplyNodeConfig`, which the node holds and applies, and
//! answers `node.config.updated` only after that apply answered. A stopped node is told nothing and
//! the command completes on the row: the node gets its config back in its join answer at its next
//! start (node-rpc-envelope.md "Node properties"). A single call, never a Build, and never refused
//! because a Build runs: updates go through this one door one at a time.

use crate::storage::NodeRow;
use crate::topology::Topology;
use rdm_client::{CallEnd, ExactBirth, NodeRpc};
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget};
use rdm_core::status::{NotPrimary, Status, StatusReply, StatusRequest};
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
    /// The tick of this admin's installed view ([`Self::watch_views`]).
    views: std::sync::OnceLock<tokio::sync::watch::Receiver<u64>>,
}

/// How a command the fabric-primary sends down ended ([`NodeConfigs::send_down`]).
pub enum Down {
    /// The node's mesh-primary answered it.
    Answered(StatusReply),
    /// This admin holds the node's mesh-primary in this view: the command is served in process.
    InProcess(Topology),
    /// This view holds the birth no more.
    Gone(Topology),
}

/// The slot an admin fills once its node-RPC client exists.
pub type NodeConfigsSlot = Arc<std::sync::OnceLock<Arc<NodeConfigs>>>;

impl NodeConfigs {
    /// The door over `client`; `own` holds this admin's config when it is the subject; `written` is
    /// the record the writer of this admin's mesh rows holds (`NodeRows::with_written`).
    pub fn new(client: Arc<NodeRpcClient>, own: crate::status_rpc::Configure, written: Arc<crate::node_rows::WrittenRows>) -> Self {
        Self { client, own, one_at_a_time: tokio::sync::Mutex::new(()), written, views: Default::default() }
    }

    /// The client this door tells births through: the mesh-primary's node.name.update tells a birth its
    /// name through it as well (`crate::node_name_update`).
    pub fn client(&self) -> &NodeRpcClient {
        &self.client
    }

    /// [`Self::client`], shared: for a call made after the caller's turn ends.
    pub fn client_arc(&self) -> Arc<NodeRpcClient> {
        self.client.clone()
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

    /// Hold `views`, the tick of this admin's installed view: what a command sent down and refused
    /// because the mesh's seat is in transit waits on before it is sent again ([`Self::serve`]).
    pub fn watch_views(&self, views: tokio::sync::watch::Receiver<u64>) {
        let _ = self.views.set(views);
    }

    /// A caller's `UpdateNodeConfig` at the admin `me` (born as `me_id`), deciding over `view`, the
    /// view `topology` holds (node-config-update.md steps 1-3): at the fabric-primary, the map written
    /// to the node in `atlas` and the command sent to the node's mesh-primary ([`Self::update`] in
    /// process when it is that mesh-primary too); at the node's mesh-primary, the command served
    /// ([`Self::update`]); at any other admin refused `ReceiverNotPrimary`, naming the fabric-primary.
    ///
    /// The command sent down is owed until the mesh-primary answers it (node-rpc.md §43; D12): one
    /// `Budget::Owed` call to the holder this admin's view names, direct first and then through each
    /// other live node-admin of the mesh on Forward `0x1A`. A holder that answers `RejectedNotPrimary`
    /// (its seat in transit: a newly elected admin that has not taken it yet) or is proven gone does
    /// not fail the caller: the owning workflow retargets it to the holder the view names when the
    /// view next changes, or after the resend schedule's next delay, whichever is first. A command the
    /// mesh-primary refused before writing the row puts the atlas's config back.
    #[allow(clippy::too_many_arguments)]
    pub async fn serve(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, view: &Topology, topology: &tokio::sync::RwLock<Topology>, storage: &dyn crate::storage::NodesStorage, atlas: Option<&crate::fabric_atlas::FabricAtlas>, req: &StatusRequest) -> StatusReply {
        let StatusRequest::UpdateNodeConfig { node_id, config, .. } = req else { unreachable!("serve is called for UpdateNodeConfig only") };
        let fabric_primary = view.is_fabric_primary(me_id);
        let Some(subject) = view.own(node_id).cloned() else {
            return self.update(me, me_id, view, storage, req).await;
        };
        if !fabric_primary {
            if view.is_mesh_primary(&subject.mesh, me_id) {
                return self.update(me, me_id, view, storage, req).await;
            }
            let held_by = view.fabric_primary().map_or_else(|| "held by no single live node-admin in this view".to_string(), |p| format!("held by {}", p.name));
            let needed = format!("the fabric-primary, where node.config.update of {} goes ({held_by}; {me} does not hold it)", subject.name);
            tracing::info_span!("rdm.node_admin.node.reject.via-not-primary", node = %me, subject = %subject.name, op = req.op(), detail = %needed, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("node.config.update refused: this admin holds neither the fabric-primary nor the node's mesh-primary"));
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed });
        }
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-fabric-primary",
            node = %me,
            subject = %subject.name,
            node_id = %node_id,
            keys = config.len(),
            mesh_primary = tracing::field::Empty,
            atlas = tracing::field::Empty,
            route = tracing::field::Empty,
            sends = tracing::field::Empty,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let prior = atlas.and_then(|a| a.write_config(node_id, config));
        span.record("atlas", if prior.is_some() { "written" } else { "no-record" });
        let reply = tracing::Instrument::instrument(
            async {
                match self.send_down(me, me_id, view, topology, &subject, req).await {
                    Down::Answered(reply) => reply,
                    Down::InProcess(now) | Down::Gone(now) => self.update(me, me_id, &now, storage, req).await,
                }
            },
            span.clone(),
        )
        .await;
        // The op's result is the atlas's (Q204: each op writes it): a written row's map once more, over
        // any fill made while the command was down; a command whose row was not written leaves the
        // record as it was, the atlas's config too.
        if let Some(a) = atlas {
            if matches!(reply, StatusReply::ConfigUpdated { .. } | StatusReply::ConfigNotApplied { .. }) {
                a.write_config(node_id, config);
            } else if let Some(prior) = prior {
                a.restore_config(node_id, config, prior);
            }
        }
        span.record("outcome", reply.name());
        span.in_scope(|| tracing::info!("node.config.update at the fabric-primary decided"));
        reply
    }

    /// The fabric-primary's command `req` about `subject` down to `subject`'s mesh-primary, owed until
    /// it answers ([`Self::serve`]): the mesh-primary's answer, or the view in which this admin holds
    /// that seat itself (served in process) or no longer holds the birth. Each send is one span event
    /// naming the holder, the route and the outcome.
    pub async fn send_down(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, first: &Topology, topology: &tokio::sync::RwLock<Topology>, subject: &crate::model::Node, req: &StatusRequest) -> Down {
        let node_id = &subject.node_id;
        let mut views = self.views.get().cloned();
        let mut view = first.clone();
        let mut sends: u32 = 0;
        let mut waits: u32 = 0;
        loop {
            if view.own(node_id).is_none() {
                // The birth left the view while its command was owed: the answer a door gives a birth it does not hold.
                return Down::Gone(view);
            }
            if view.is_mesh_primary(&subject.mesh, me_id) {
                tracing::Span::current().record("route", "in-process");
                tracing::Span::current().record("mesh_primary", me.to_string().as_str());
                return Down::InProcess(view);
            }
            let holder = view.node_type_primary(&subject.mesh, rdm_core::NodeKind::NodeAdmin).map(|p| (p.name.clone(), p.node_id.clone()));
            let ended = match holder {
                None => format!("{} has no mesh-primary in {me}'s view", subject.mesh),
                Some((name, mp)) => {
                    tracing::Span::current().record("mesh_primary", name.to_string().as_str());
                    // Direct first; once an attempt sends nothing, through each other live node-admin of the mesh in turn (Forward `0x1A`).
                    let carriers: Vec<(rdm_core::NodeId, crate::model::PathName)> = view.births().filter(|n| n.mesh == subject.mesh && n.kind == rdm_core::NodeKind::NodeAdmin && n.node_id != mp && n.node_id != *me_id && n.status.is_live()).map(|n| (n.node_id.clone(), n.name.clone())).collect();
                    let policy = rdm_transport::RoutePolicy(Arc::new(move |attempt: u32| match carriers.get(attempt.saturating_sub(1) as usize % carriers.len().max(1)) {
                        Some((carrier, path)) if attempt > 0 => rdm_transport::route::RouteChoice::ViaPeer { carrier: carrier.clone(), path: path.clone() },
                        _ => rdm_transport::route::RouteChoice::Direct,
                    }));
                    let opts = CallOptions { budget: rdm_transport::Budget::Owed, route: Some(policy), ..CallOptions::default() };
                    sends += 1;
                    let (out, _) = self.client.call::<Status>(&NodeTarget::ExactNode(mp.clone()), req, &opts).await;
                    let outcome = match &out {
                        rdm_core::outcome::RpcOutcome::Reply(r) => r.value().name().to_string(),
                        other => other.name().to_string(),
                    };
                    tracing::Span::current().record("sends", sends);
                    tracing::Span::current().record("route", "owed");
                    tracing::info!(to = %name, send = sends, outcome = %outcome, "node.config.update sent down to the mesh-primary");
                    match out {
                        rdm_core::outcome::RpcOutcome::Reply(r) => match r.into_value() {
                            StatusReply::RejectedNotPrimary { why } => format!("{name} answered rejected-not-primary ({})", why.as_str()),
                            answered => return Down::Answered(answered),
                        },
                        // An Owed call ends without a reply only when its receiver is proven gone, or
                        // before anything was owed: the command is owed to the holder the view names next.
                        other => format!("the call to {name} ended {} with no reply", other.name()),
                    }
                }
            };
            // Retargeting is the owning workflow's (node-rpc.md §43): the holder the view names when it
            // next changes, or after the resend schedule's next delay.
            waits += 1;
            let delay = self.client.delivery().pacing().after(waits);
            tracing::Span::current().record("detail", ended.as_str());
            tracing::info!(detail = %ended, wait_ms = delay.as_millis() as u64, "node.config.update stays owed: sent again to the holder the view names next");
            match views.as_mut() {
                Some(rx) => {
                    let _ = tokio::time::timeout(delay, rx.changed()).await;
                }
                None => tokio::time::sleep(delay).await,
            }
            view = topology.read().await.clone();
        }
    }

    /// Serve an `UpdateNodeConfig` at the admin `me` (born as `me_id`) deciding over `view`, writing to `storage`. One
    /// span, opened first, holds the whole put: the row write and the call that tells the birth are
    /// inside it, and its outcome is the reply's name. A refusal's reply carries its detail cut to the
    /// reply bound; the span carries it whole (`answered`, `detail`), so nothing of it is lost.
    pub async fn update(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, view: &Topology, storage: &dyn crate::storage::NodesStorage, req: &StatusRequest) -> StatusReply {
        self.update_with(me, me_id, view, storage, req, None).await
    }

    /// [`Self::update`] of a Build's create spec (build.md §1, "The Build applies the diff"): the same
    /// put, the spec's `storage` and `placement` (`onto_row`) written onto the row in the same write as
    /// the map, effective at the node's next birth.
    pub async fn update_with(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, view: &Topology, storage: &dyn crate::storage::NodesStorage, req: &StatusRequest, onto_row: Option<(rdm_core::meta::StorageMeta, rdm_core::meta::PlacementMeta)>) -> StatusReply {
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
        let reply = tracing::Instrument::instrument(self.put(me, me_id, view, storage, node_id, incarnation, config, onto_row, &span), span.clone()).await;
        span.record("outcome", reply.name());
        span.in_scope(|| tracing::info!("node.config.update decided"));
        reply
    }

    #[allow(clippy::too_many_arguments)]
    async fn put(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, view: &Topology, storage: &dyn crate::storage::NodesStorage, node_id: &rdm_core::NodeId, incarnation: &rdm_core::IncarnationId, config: &std::collections::BTreeMap<String, String>, onto_row: Option<(rdm_core::meta::StorageMeta, rdm_core::meta::PlacementMeta)>, span: &tracing::Span) -> StatusReply {
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
        let (row_storage, row_placement) = onto_row.map_or((None, None), |(s, p)| (Some(s), Some(p)));
        let base = NodeRow { config: config.clone(), storage_persistence: row_storage, placement: row_placement, ..NodeRow::new(subject.name.clone(), node_id.clone(), subject.status) };
        // A config put moves no status (node-status.md: "never a meta or config put"): the record's stands.
        let refused = match self.written.update(storage, node_id, Some(base), |r| {
            r.config = config.clone();
            if onto_row.is_some() {
                (r.storage_persistence, r.placement) = (row_storage, row_placement);
            }
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
