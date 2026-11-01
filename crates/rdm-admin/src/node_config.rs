//! Property commands use the subject node's durable property writer.
//! Caller commands go through the fabric-primary and mesh-primary to the exact birth.
//! Local changes commit at the node without an upward property copy.

use crate::topology::Topology;
use rdm_client::{CallEnd, ExactBirth, NodeRpc};
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget};
use rdm_core::status::{NotPrimary, Status, StatusReply, StatusRequest};
use std::sync::Arc;

/// A mesh-primary's config door: the client it tells a birth of its mesh through, what holds this
/// admin's own config (the mesh-primary is a member of its mesh and the writer of its own row too),
/// and the lock that makes every config change one at a time.
pub struct NodeConfigs {
    records: std::sync::OnceLock<Arc<dyn rdm_node::node_records::NodeRecordsStorage>>,
    own_properties: std::sync::OnceLock<rdm_node::node_records::NodePropertyWriter>,
    own_channels: std::sync::OnceLock<rdm_transport::topology::TopologyView>,
    client: Arc<NodeRpcClient>,
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

/// How a caller's property command an admin carries up ended ([`NodeConfigs::forward_up`]).
pub enum Up {
    /// The fabric-primary answered it: the answer the caller is handed.
    Answered(StatusReply),
    /// This admin holds the fabric-primary in this view: the command is served in process.
    InProcess(Topology),
}

/// Who sent a property command (`node.config.update`, `node.tag.*`) to an admin, as its view names the
/// call's endpoint: what decides whether an admin that is not the fabric-primary carries it up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SentBy {
    /// A node of the fabric that is not a node-admin, about another node: a caller's command, carried up
    /// to the fabric-primary by an admin that is not it (node-config-update.md, a caller's update).
    Caller,
    /// A node-admin over Node RPC (the fabric-primary's command sent down, or a carrier of it), or an
    /// endpoint the view names no birth for: served by the node's mesh-primary, refused by name at an
    /// admin that is neither primary.
    Other,
}

impl SentBy {


    /// The sender of a command a caller sends to the node's mesh-primary (`node.name.update`,
    /// node-name-update.md step 1; `node.tag.*`): every endpoint that is not a node-admin the view
    /// holds, a caller outside the fabric (an endpoint the view holds no birth for) included, is the
    /// caller's; a node-admin's is the fabric-primary's command sent down, or a carrier of it.
    pub fn of_writer_call(sender: Option<&crate::model::Node>) -> Self {
        match sender {
            Some(n) if n.kind == rdm_core::NodeKind::NodeAdmin => Self::Other,
            _ => Self::Caller,
        }
    }
}

/// The primary an owed property command goes to ([`NodeConfigs::owed`]).
pub(crate) enum Hop<'a> {
    /// Down to `subject`'s mesh-primary (the fabric-primary's command).
    Down(&'a crate::model::Node),
    /// Up to the fabric-primary (a caller's command at an admin that is not it).
    Up,
    /// Up to the fabric-primary for its atlas (`fabric.atlas.get`), a refused caller's verification at
    /// the root (Q207, node-investigation.md): a fabric-primary holding no atlas yet answers it once it
    /// holds one.
    Root,
}

/// How an owed property command ended ([`NodeConfigs::owed`]).
pub(crate) enum Owed {
    Answered(StatusReply),
    InProcess(Topology),
    Gone(Topology),
}

/// The slot an admin fills once its node-RPC client exists.
pub type NodeConfigsSlot = Arc<std::sync::OnceLock<Arc<NodeConfigs>>>;

impl NodeConfigs {
    /// The door over `client`; `written` is the record the writer of this admin's mesh rows holds
    /// (`NodeRows::with_written`).
    pub fn new(client: Arc<NodeRpcClient>, written: Arc<crate::node_rows::WrittenRows>) -> Self {
        Self { own_channels: Default::default(), records: Default::default(), own_properties: Default::default(), client, one_at_a_time: tokio::sync::Mutex::new(()), written, views: Default::default() }
    }

    /// Bind the configured provider's node records. Every access follows provider migration.
    pub fn uses_records(&self, records: Arc<dyn rdm_node::node_records::NodeRecordsStorage>, channels: rdm_transport::topology::TopologyView) {
        let _ = self.own_channels.set(channels);
        let _ = self.own_properties.set(rdm_node::node_records::NodePropertyWriter::new(records.clone()));
        let _ = self.records.set(records);
    }

    /// The shared records used for current property reads and exact lifecycle resolution.
    pub fn records(&self) -> Option<&Arc<dyn rdm_node::node_records::NodeRecordsStorage>> {
        self.records.get()
    }

    /// This admin's own serialized property writer, shared by local and downward updates.
    pub fn own_properties(&self) -> Option<&rdm_node::node_records::NodePropertyWriter> {
        self.own_properties.get()
    }

    fn own_snapshot(&self, subject: &crate::model::Node, config: std::collections::BTreeMap<String, String>) -> Option<rdm_node::node_records::NodeProperties> {
        let channels = self.own_channels.get()?;
        let row = channels.own_row();
        Some(rdm_node::node_records::NodeProperties {
            birth: rdm_node::node_records::RecordBirth { node_id: subject.node_id.clone(), incarnation_id: subject.incarnation_id.clone()? },
            config, tags: row.as_ref().map(|r| r.tags.clone()).unwrap_or_default(),
            endpoint_id: subject.endpoint_id.clone(), transport_addr: subject.transport_addr, listeners: subject.listeners.clone(),
            app_types: Some(rdm_node::nodes_storage::AppTypes { incarnation: subject.incarnation_id.clone()?, types: channels.app_types() }),
            held: channels.held_types(), applied: Default::default(),
        })
    }

    /// Persist this admin's actual binding before it may become Ready. Retained properties
    /// take precedence for the same node ID; only the bound metadata is refreshed.
    pub async fn bound_own(&self, subject: &crate::model::Node, properties: rdm_core::launch::InitialProperties) -> Result<(), String> {
        let mut initial = self.own_snapshot(subject, properties.config)
            .ok_or_else(|| format!("{}: own property writer is not bound", subject.node_id))?;
        initial.tags = properties.tags;
        initial.applied = properties.applied;
        let channels = self.own_channels.get().expect("own snapshot requires channels").clone();
        self.own_properties.get().expect("production own writer is bound").bound(initial,
            move |record| { channels.set_config(record.config.clone()); channels.set_tags(record.tags.clone()); })
            .await.map(|_| ()).map_err(|e| e.to_string())
    }

    /// An admin writes its own map locally through the same writer as downward commands.
    pub async fn own_config(&self, subject: &crate::model::Node, config: std::collections::BTreeMap<String, String>) -> Option<Result<StatusReply, crate::storage_error::StorageError>> {
        let initial = self.own_snapshot(subject, config.clone())?;
        let writer = self.own_properties.get()?;
        let channels = self.own_channels.get()?.clone();
        Some(writer.update(initial, None, move |record| record.config = config,
            move |record| channels.set_config(record.config.clone())).await.map(|written| StatusReply::ConfigUpdated {
                node_id: written.record.birth.node_id, incarnation: written.record.birth.incarnation_id,
                config: written.record.config, revision: 0,
            }))
    }

    /// A Build's complete entry uses this admin's ordinary property writer.
    pub async fn own_entry(&self, subject: &crate::model::Node, entry: rdm_core::properties::EntryProperties) -> Option<Result<StatusReply, crate::storage_error::StorageError>> {
        let initial = self.own_snapshot(subject, entry.config.clone())?;
        let channels = self.own_channels.get()?.clone();
        Some(self.own_properties.get()?.entry(initial, entry,
            move |record| { channels.set_config(record.config.clone()); channels.set_tags(record.tags.clone()); })
            .await.map(|written| if written.already_applied { rdm_core::reply::already_applied() } else { rdm_core::reply::applied() }))
    }

    /// A downward tag command to this admin is a local write, with no command to its primary.
    pub async fn own_tags(&self, subject: &crate::model::Node, change: rdm_node::node_records::TagChange) -> Option<Result<StatusReply, crate::storage_error::StorageError>> {
        let channels = self.own_channels.get()?.clone();
        let initial = self.own_snapshot(subject, channels.config().unwrap_or_default())?;
        Some(self.own_properties.get()?.tags(initial, change, None,
            move |record| channels.set_tags(record.tags.clone())).await)
    }

    /// Read entry knowledge from the exact node's shared property record.
    pub async fn entry_applied(&self, node_id: &rdm_core::NodeId, application: &rdm_core::properties::PropertyApplication) -> Result<bool, String> {
        Ok(self.records().ok_or_else(|| "node records are not bound".to_string())?
            .properties(node_id).await.map_err(|error| error.to_string())?
            .is_some_and(|record| record.applied.contains(application)))
    }

    /// Carry a Build entry from its mesh-primary to the pinned node, then finish the
    /// lifecycle metadata owed by that entry. Recognition at the node never skips this write.
    pub async fn apply_entry(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, view: &Topology, storage: &dyn crate::storage::NodesStorage, birth: &crate::build::PinnedBirth, entry: rdm_core::properties::EntryProperties, placed: (rdm_core::meta::StorageMeta, rdm_core::meta::PlacementMeta)) -> Result<(), String> {
        let subject = view.own(&birth.node_id).ok_or_else(|| format!("{me_id}: property target {} is not held", birth.node_id))?;
        if !birth.is(subject) {
            return Err(format!("{me_id}: property target {} has a different incarnation", birth.node_id));
        }
        if !view.is_mesh_primary(&subject.mesh, me_id) {
            return Err(format!("{me_id}: property target {} is not in its mesh-primary's view", birth.node_id));
        }
        let applied = self.entry_applied(&birth.node_id, &entry.application).await?;
        if !applied {
            let reply = if birth.node_id == *me_id {
                self.own_entry(subject, entry).await.ok_or_else(|| format!("{me_id}: own property writer is not bound"))?
                    .map_err(|error| error.to_string())?
            } else {
                let exact = ExactBirth { target: NodeTarget::ExactNode(birth.node_id.clone()), node_id: birth.node_id.clone(), incarnation: birth.incarnation.clone() };
                NodeRpc::new(&self.client).properties_apply(&exact, entry, &CallOptions::default()).await.map_err(|error| error.to_string())?
            };
            if !matches!(reply, StatusReply::Applied | StatusReply::AlreadyApplied) {
                return Err(format!("{me_id}: property target {} answered {}", birth.node_id, reply.name()));
            }
        }
        self.written.update(storage, &birth.node_id, None, |row| {
            row.storage_persistence = Some(placed.0);
            row.placement = Some(placed.1);
        }).await?.ok_or_else(|| format!("{me_id}: lifecycle record of {} is absent after property apply", birth.node_id))
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
    /// because the mesh's primary is changing waits on before it is sent again ([`Self::serve`]).
    pub fn watch_views(&self, views: tokio::sync::watch::Receiver<u64>) {
        let _ = self.views.set(views);
    }



    /// A caller's config command goes through its primaries to the subject's writer.
    /// The command sent down is owed until the mesh-primary answers it (node-rpc.md §43; D12): one
    /// `Budget::Owed` call to the mesh-primary this admin's view names, direct first and then through each
    /// other live node-admin of the mesh on Forward `0x1A`. A mesh-primary that answers `RejectedNotPrimary`
    /// (the primary changing: a newly elected admin that does not hold it yet) or is proven gone does
    /// not fail the caller: the owning workflow retargets it to the mesh-primary the view names when the
    /// view next changes, or after the resend schedule's next delay, whichever is first.
    #[allow(clippy::too_many_arguments)]
    pub async fn serve(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, view: &Topology, topology: &Arc<tokio::sync::RwLock<Topology>>, storage: &dyn crate::storage::NodesStorage, _atlas: Option<&crate::atlas::Atlas>, sent_by: SentBy, req: &StatusRequest) -> StatusReply {
        let StatusRequest::UpdateNodeConfig { node_id, config, .. } = req else { unreachable!("serve is called for UpdateNodeConfig only") };
        let mut held = None;
        if sent_by == SentBy::Caller && !view.is_fabric_primary(me_id) {
            match self.forward_up(me, me_id, view, topology, req).await {
                Up::Answered(reply) => return reply,
                Up::InProcess(now) => held = Some(now),
            }
        }
        let view = held.as_ref().unwrap_or(view);
        let fabric_primary = view.is_fabric_primary(me_id);
        let Some(subject) = view.own(node_id).cloned() else {
            return self.update(me, me_id, view, storage, req).await;
        };
        if !fabric_primary {
            if view.is_mesh_primary(&subject.mesh, me_id) {
                return self.update(me, me_id, view, storage, req).await;
            }
            let held_by = view.fabric_primary().map_or_else(|| "held by no single live node-admin in this view".to_string(), |p| format!("held by {}", p.node_id));
            let needed = format!("the fabric-primary, where node.config.update of {} goes ({held_by}; {me_id} does not hold it)", subject.node_id);
            tracing::info_span!("rdm.node_admin.node.reject.via-not-primary", node_path = %me.path, node_name = %me.name, subject_path = %subject.name.path, subject_name = %subject.name.name, op = req.op(), detail = %needed, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("node.config.update refused: this admin holds neither the fabric-primary nor the node's mesh-primary"));
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed });
        }
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-fabric-primary",
            receiver_node_id = %me_id,
            subject_path = %subject.name.path, subject_name = %subject.name.name,
            node_id = %node_id,
            keys = config.len(),
            mesh_primary = tracing::field::Empty,
            revision = tracing::field::Empty,
            route = tracing::field::Empty,
            sends = tracing::field::Empty,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
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
        span.record("outcome", reply.name());
        span.in_scope(|| tracing::info!("node.config.update at the fabric-primary decided"));
        reply
    }





    /// The fabric-primary's command `req` about `subject` down to `subject`'s mesh-primary, owed until
    /// it answers ([`Self::serve`]): the mesh-primary's answer, or the view in which this admin holds
    /// that primary itself (served in process) or no longer holds the birth. Each send is one span event
    /// naming the mesh-primary, the route and the outcome.
    pub async fn send_down(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, first: &Topology, topology: &tokio::sync::RwLock<Topology>, subject: &crate::model::Node, req: &StatusRequest) -> Down {
        match self.owed(me, me_id, first, topology, req, Hop::Down(subject)).await {
            Owed::Answered(reply) => Down::Answered(reply),
            Owed::InProcess(view) => Down::InProcess(view),
            Owed::Gone(view) => Down::Gone(view),
        }
    }

    /// A caller's property command `req` at this admin, which `first` does not name the fabric-primary,
    /// carried up to the fabric-primary (node-config-update.md step 1; commands go down from it, so a
    /// caller's goes up to it first), owed until it answers (node-rpc.md §43): the fabric-primary's
    /// answer, handed back to the caller as it is, or the view in which this admin holds the
    /// fabric-primary itself (served in process). The fabric-primary is the one this admin's view names
    /// from its records, never a digest; one `Budget::Owed` call, direct first and then through each other
    /// live node-admin of its mesh on Forward `0x1A`. A fabric-primary that answers `ReceiverNotPrimary`
    /// (it no longer holds it, or its successor does not yet) or is proven gone does not fail the
    /// caller: the command is sent again to the fabric-primary the view names when it next changes, or
    /// after the resend schedule's next delay. One span, `rdm.node_admin.node.update.via-forward`, names
    /// the fabric-primary, the sends and the answer.
    pub async fn forward_up(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, first: &Topology, topology: &tokio::sync::RwLock<Topology>, req: &StatusRequest) -> Up {
        let (node_id, subject) = match req {
            StatusRequest::UpdateNodeConfig { node_id, .. } | StatusRequest::CreateTags { node_id, .. } | StatusRequest::DeleteTags { node_id, .. } | StatusRequest::UpdateName { node_id, .. } => (node_id, first.own(node_id).map(|n| n.name.clone())),
            other => unreachable!("forward_up carries a node's property command only, never {}", other.op()),
        };
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-forward",
            node_path = %me.path, node_name = %me.name,
            receiver_node_id = %me_id,
            op = req.op(),
            subject_path = ?subject.as_ref().map(|location| location.path.as_str()), subject_name = ?subject.as_ref().map(|location| location.name.as_str()),
            node_id = %node_id,
            fabric_primary = tracing::field::Empty,
            route = tracing::field::Empty,
            sends = tracing::field::Empty,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let up = tracing::Instrument::instrument(self.owed(me, me_id, first, topology, req, Hop::Up), span.clone()).await;
        let up = match up {
            Owed::Answered(reply) => {
                span.record("outcome", reply.name());
                Up::Answered(reply)
            }
            Owed::InProcess(view) | Owed::Gone(view) => {
                span.record("outcome", "in-process");
                Up::InProcess(view)
            }
        };
        span.in_scope(|| tracing::info!("a caller's property command carried up to the fabric-primary ended"));
        up
    }

    /// `req` owed to the primary `hop` names in this admin's view until it answers: down to the subject's
    /// mesh-primary, or up to the fabric-primary. One `Budget::Owed` call per send to the primary the view
    /// names then, direct first and then through each other live node-admin of that primary's mesh on
    /// Forward `0x1A`; a refusal that it is not the primary, or a call ended with no reply, sends it again
    /// to the primary the view names when it next changes, or after the resend schedule's next delay
    /// (retargeting is the owning workflow's, node-rpc.md §43).
    #[allow(clippy::too_many_arguments)]
    async fn owed(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, first: &Topology, topology: &tokio::sync::RwLock<Topology>, req: &StatusRequest, hop: Hop<'_>) -> Owed {
        owed(&self.client, self.views.get().cloned(), me, me_id, first, topology, req, hop).await
    }
}

/// The route of a call to `primary`, a primary of `mesh` in `view`: direct first; once an attempt sends
/// nothing, through each other live node-admin of `mesh` in turn (Forward `0x1A`), never `me_id`.
fn primary_route(view: &Topology, mesh: &str, primary: &rdm_core::NodeId, me_id: &rdm_core::NodeId) -> rdm_transport::RoutePolicy {
    let carriers: Vec<(rdm_core::NodeId, crate::model::PathName)> = view.births().filter(|n| n.mesh == mesh && n.kind == rdm_core::NodeKind::NodeAdmin && n.node_id != *primary && n.node_id != *me_id && n.status.is_live()).map(|n| (n.node_id.clone(), n.name.clone())).collect();
    rdm_transport::RoutePolicy(Arc::new(move |attempt: u32| match carriers.get(attempt.saturating_sub(1) as usize % carriers.len().max(1)) {
        Some((carrier, path)) if attempt > 0 => rdm_transport::route::RouteChoice::ViaPeer { carrier: carrier.clone() },
        _ => rdm_transport::route::RouteChoice::Direct,
    }))
}

impl NodeConfigs {
    /// A plural read (query.md §1) this admin does not answer from an atlas, read from the primary `to`
    /// names in `view`: at a mesh-primary that is not the fabric-primary, a read its atlas does not hold
    /// (a read of another mesh, a read naming no mesh, `fabric.get`; Q224) is carried up to the
    /// fabric-primary, whose atlas holds every mesh; at a node-admin that is not a primary, which keeps
    /// no atlas, every read is forwarded to its own mesh-primary and never to the fabric-primary (Q243).
    /// Read on op `0x23` (Q285), the rows collected whole from the primary's stream and handed back as
    /// they are: one call with the caller's default budget, never owed (node-rpc.md, Owed: "A command
    /// with an owner's bound keeps a finite budget"; the read's owner is the caller's call), direct, a
    /// stream being never forwarded. A view naming no such primary, or a read that ends with no valid
    /// `End`, is `NotReady` naming it; the primary's refusal of the filter is handed back as it is. One
    /// span, `rdm.node_admin.<entity>.serve.via-forward`, names the read, its pairs, the primary
    /// (`fabric_primary` or `mesh_primary`) and the answer.
    pub async fn carry_read(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, view: &Topology, read: &rdm_core::status::Read, to: ReadTo) -> Result<rdm_core::read::Rows, rdm_core::read::ReadReply> {
        use rdm_core::filter::Entity;
        use rdm_core::read::ReadReply;
        let op = read.entity.read();
        let pairs = crate::views::pairs(&read.filter);
        let empty = tracing::field::Empty;
        let span = match read.entity {
            Entity::Nodes => tracing::info_span!("rdm.node_admin.node.serve.via-forward", node_path = %me.path, node_name = %me.name, read = op, pairs = %pairs, fabric_primary = empty, mesh_primary = empty, route = empty, outcome = empty, detail = empty, "otel.kind" = "internal"),
            Entity::Meshes => tracing::info_span!("rdm.node_admin.mesh.serve.via-forward", node_path = %me.path, node_name = %me.name, read = op, pairs = %pairs, fabric_primary = empty, mesh_primary = empty, route = empty, outcome = empty, detail = empty, "otel.kind" = "internal"),
            _ => tracing::info_span!("rdm.node_admin.fabric.serve.via-forward", node_path = %me.path, node_name = %me.name, read = op, pairs = %pairs, fabric_primary = empty, mesh_primary = empty, route = empty, outcome = empty, detail = empty, "otel.kind" = "internal"),
        };
        let whom = to.whom();
        let carried = async {
            let primary = match to {
                ReadTo::FabricPrimary => view.fabric_primary(),
                ReadTo::MeshPrimary => view.own(me_id).and_then(|own| view.node_type_primary(&own.mesh, rdm_core::NodeKind::NodeAdmin)),
            };
            let Some(primary) = primary else {
                return Err(ReadReply::NotReady { reason: format!("{me_id} answers no {op} from an atlas, and its view names none to send this one on to ({whom})") });
            };
            span.record(to.field(), primary.node_id.to_string().as_str());
            span.record("route", "direct");
            match rdm_client::NodeRpc::new(&self.client).read(&NodeTarget::ExactNode(primary.node_id.clone()), read.entity, &read.filter, &CallOptions::default()).await {
                Ok(rows) => Ok(rows),
                Err(rdm_client::CallEnd::Rejected { reason, detail }) if reason == "rejected-filter" => Err(ReadReply::RejectedFilter { reason: detail }),
                Err(e) => Err(ReadReply::NotReady { reason: format!("{me_id} read {op} from {whom} {}: the read ended {} ({e})", primary.node_id, e.outcome()) }),
            }
        };
        let answer = tracing::Instrument::instrument(carried, span.clone()).await;
        match &answer {
            // The answer by the data it carries, as the read's reply names it (`nodes`, `meshes`, `fabrics`).
            Ok(rows) => span.record("outcome", rows_name(rows)).record("detail", format!("{} rows", rows.len()).as_str()),
            Err(r) => span.record("outcome", r.name()).record("detail", format!("{r:?}").as_str()),
        };
        span.in_scope(|| match to {
            ReadTo::FabricPrimary => tracing::info!("a read this mesh-primary's atlas does not hold, carried up to the fabric-primary"),
            ReadTo::MeshPrimary => tracing::info!("a read at a node-admin that is not a primary, forwarded to its own mesh-primary"),
        });
        answer
    }
}

/// A read's rows by the reply that carries them: `nodes`, `meshes`, `fabrics` or `connections`.
pub(crate) fn rows_name(rows: &rdm_core::read::Rows) -> &'static str {
    match rows {
        rdm_core::read::Rows::Nodes(_) => "nodes",
        rdm_core::read::Rows::Meshes(_) => "meshes",
        rdm_core::read::Rows::Fabrics(_) => "fabrics",
        rdm_core::read::Rows::Connections(_) => "connections",
    }
}

/// Where [`NodeConfigs::carry_read`] sends a read on to (query.md §1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadTo {
    /// Up to the fabric-primary, from a mesh-primary whose atlas does not hold it (Q224).
    FabricPrimary,
    /// To its own mesh-primary, from a node-admin that is not a primary (Q243).
    MeshPrimary,
}

impl ReadTo {
    /// The primary, as a reason names it.
    pub fn whom(self) -> &'static str {
        match self {
            ReadTo::FabricPrimary => "the fabric-primary",
            ReadTo::MeshPrimary => "its mesh-primary",
        }
    }

    /// The span field naming the primary.
    fn field(self) -> &'static str {
        match self {
            ReadTo::FabricPrimary => "fabric_primary",
            ReadTo::MeshPrimary => "mesh_primary",
        }
    }
}

/// [`NodeConfigs::owed`] over `client`, woken by `views`, the tick of the admin's installed view.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn owed(client: &NodeRpcClient, mut views: Option<tokio::sync::watch::Receiver<u64>>, me: &crate::model::PathName, me_id: &rdm_core::NodeId, first: &Topology, topology: &tokio::sync::RwLock<Topology>, req: &StatusRequest, hop: Hop<'_>) -> Owed {
    {
        let (field, to_whom) = match hop {
            Hop::Down(_) => ("mesh_primary", "the mesh-primary"),
            Hop::Up | Hop::Root => ("fabric_primary", "the fabric-primary"),
        };
        // A clone remembers the version its source last saw: only a view installed from now on wakes a resend.
        if let Some(rx) = views.as_mut() {
            rx.borrow_and_update();
        }
        let mut view = first.clone();
        let mut sends: u32 = 0;
        let mut waits: u32 = 0;
        loop {
            let primary = match hop {
                Hop::Down(subject) => {
                    if view.own(&subject.node_id).is_none() {
                        // The birth left the view while its command was owed: the answer a door gives a birth it does not hold.
                        return Owed::Gone(view);
                    }
                    if view.is_mesh_primary(&subject.mesh, me_id) {
                        tracing::Span::current().record("route", "in-process");
                        tracing::Span::current().record(field, me_id.as_str());
                        return Owed::InProcess(view);
                    }
                    view.node_type_primary(&subject.mesh, rdm_core::NodeKind::NodeAdmin).map(|p| (p.node_id.clone(), p.mesh.clone())).ok_or_else(|| format!("{} has no mesh-primary in {me_id}'s view", subject.mesh))
                }
                Hop::Up | Hop::Root => {
                    if view.is_fabric_primary(me_id) {
                        tracing::Span::current().record("route", "in-process");
                        tracing::Span::current().record(field, me_id.as_str());
                        return Owed::InProcess(view);
                    }
                    view.fabric_primary().map(|p| (p.node_id.clone(), p.mesh.clone())).ok_or_else(|| format!("no single live node-admin holds the fabric-primary in {me_id}'s view"))
                }
            };
            let ended = match primary {
                Err(none) => none,
                Ok((primary, mesh)) => {
                    tracing::Span::current().record(field, primary.as_str());
                    let opts = CallOptions { budget: rdm_transport::Budget::Owed, route: Some(primary_route(&view, &mesh, &primary, me_id)), ..CallOptions::default() };
                    sends += 1;
                    let (out, _) = client.call::<Status>(&NodeTarget::ExactNode(primary.clone()), req, &opts).await;
                    let outcome = match &out {
                        rdm_core::outcome::RpcOutcome::Reply(r) => r.value().name().to_string(),
                        other => other.name().to_string(),
                    };
                    tracing::Span::current().record("sends", sends);
                    tracing::Span::current().record("route", "owed");
                    match hop {
                        Hop::Down(_) => tracing::info!(to_node_id = %primary, send = sends, outcome = %outcome, "{} sent down to the mesh-primary", req.op()),
                        Hop::Up | Hop::Root => tracing::info!(to_node_id = %primary, send = sends, outcome = %outcome, "{} carried up to the fabric-primary", req.op()),
                    }
                    match out {
                        rdm_core::outcome::RpcOutcome::Reply(r) => match (r.into_value(), &hop) {
                            // Down: any refusal that the receiver is not the primary is the primary changing. Up: only
                            // `ReceiverNotPrimary` is; any other answer (a subject the fabric-primary does not hold) is the caller's.
                            (StatusReply::RejectedNotPrimary { why }, Hop::Down(_)) => format!("{primary} answered rejected-not-primary ({})", why.as_str()),
                            (StatusReply::RejectedNotPrimary { why: why @ NotPrimary::ReceiverNotPrimary { .. } }, Hop::Up | Hop::Root) => format!("{primary} answered rejected-not-primary ({})", why.as_str()),
                            // A fabric-primary holding no atlas yet takes the push, and answers the read, once it holds one.
                            (StatusReply::NotReady { reason }, Hop::Root) => format!("{primary} answered not-ready ({reason})"),
                            (answered, _) => return Owed::Answered(answered),
                        },
                        // An Owed call ends without a reply only when its receiver is proven gone, or
                        // before anything was owed: the command is owed to the primary the view names next.
                        other => format!("the call to {primary} ended {} with no reply", other.name()),
                    }
                }
            };
            waits += 1;
            let delay = client.delivery().pacing().after(waits);
            tracing::Span::current().record("detail", ended.as_str());
            tracing::info!(detail = %ended, wait_ms = delay.as_millis() as u64, "{} stays owed: sent again to {to_whom} the view names next", req.op());
            match views.as_mut() {
                Some(rx) => {
                    let _ = tokio::time::timeout(delay, rx.changed()).await;
                }
                None => tokio::time::sleep(delay).await,
            }
            view = topology.read().await.clone();
        }
    }
}

impl NodeConfigs {
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
            node_path = %me.path, node_name = %me.name,
            receiver_node_id = %me_id,
            subject_path = tracing::field::Empty, subject_name = tracing::field::Empty,
            node_id = %node_id,
            incarnation_id = %incarnation.as_str(),
            keys = config.len(),
            tags = tracing::field::Empty,
            row_status = tracing::field::Empty,
            revision = tracing::field::Empty,
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
        // mesh-primary moved to the predecessor name `<name>-old` at the same path is still that birth (never found by path and name, its shape
        // included: `Topology::own`). A subject this receiver does not hold is the receiver's own fact
        // (node-rpc-envelope.md: "`SubjectUnknown` when the receiver does not hold the subject at all"),
        // never a peer that did not resolve.
        let Some(subject) = view.own(node_id) else {
            span.record("detail", format!("{me_id}: node {node_id} holds no birth in this admin's view; its config cannot be put").as_str());
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown);
        };
        span.record("subject_path", subject.name.path.as_str()).record("subject_name", subject.name.name.as_str());
        span.record("row_status", format!("{:?}", subject.status).as_str());
        // The writer of the row is the mesh-primary of the node's mesh, and no other admin
        // (fabric-node-lifecycle §4.3); a caller that sent it elsewhere is told whose role it needs.
        if !view.is_mesh_primary(&subject.mesh, me_id) {
            let held_by = view.node_type_primary(&subject.mesh, rdm_core::NodeKind::NodeAdmin).map_or_else(|| "held by no single live node-admin in this view".to_string(), |p| format!("held by {}", p.node_id));
            let needed = format!("{}'s primary node-admin, the writer of {}'s row ({held_by}; {me_id} does not hold it)", subject.mesh, subject.node_id);
            span.record("detail", needed.as_str());
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed });
        }
        match subject.incarnation_id.as_ref() {
            Some(held) if held != incarnation => return rdm_core::reply::stale(held.clone()),
            None => {
                span.record("detail", format!("{me_id}: {} has no known birth; its config cannot be put", subject.node_id).as_str());
                return rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown);
            }
            Some(_) => {}
        }
        if self.records().is_some() {
            // The node's own property writer puts the map: this admin's in-process, any other node's
            // through `ApplyNodeConfig`, called directly.
            let applied = if node_id == me_id {
                span.record("apply", "in-process");
                match self.own_config(subject, config.clone()).await {
                    Some(Ok(reply)) => Ok(reply),
                    Some(Err(error)) => {
                        span.record("detail", error.to_string().as_str());
                        return rdm_core::reply::status::bounded_config_row_refused(subject.node_id.as_str(), &error.to_string());
                    }
                    None => return rdm_core::reply::status::not_ready(format!("{me_id} has no own property writer yet")),
                }
            } else {
                span.record("apply", "direct");
                let birth = ExactBirth { target: NodeTarget::ExactNode(node_id.clone()), node_id: node_id.clone(), incarnation: incarnation.clone() };
                NodeRpc::new(&self.client).config_apply(&birth, config.clone(), 0, &CallOptions::default()).await
            };
            let (answered, detail) = match applied {
                Ok(StatusReply::Applied | StatusReply::AlreadyApplied | StatusReply::ConfigUpdated { .. }) => (None, String::new()),
                Ok(refused) => (Some(refused.name()), format!("{me_id}: the birth answered {refused:?}")),
                Err(end) => (Some(end.outcome()), format!("{me_id}: apply-node-config ended {}", describe(&end))),
            };
            if let Some(answered) = answered {
                span.record("answered", answered).record("detail", detail.as_str());
                return rdm_core::reply::status::bounded_config_not_applied(subject.node_id.as_str(), answered, &detail);
            }
            if let Some((storage_persistence, placement)) = onto_row {
                match self.written.update(storage, node_id, None, |row| {
                    row.storage_persistence = Some(storage_persistence);
                    row.placement = Some(placement);
                }).await {
                    Ok(Some(_)) => {},
                    Ok(None) => return rdm_core::reply::status::bounded_node_row_refused(&subject.node_id.to_string(), "lifecycle record is absent after property apply"),
                    Err(error) => return rdm_core::reply::status::bounded_node_row_refused(&subject.node_id.to_string(), &error),
                }
            }
            return StatusReply::ConfigUpdated { node_id: node_id.clone(), incarnation: incarnation.clone(), config: config.clone(), revision: 0 };
        }
        rdm_core::reply::status::not_ready(format!("{me_id}: node records are not bound; no property was written"))
    }
}

impl NodeConfigs {





}

fn describe(end: &CallEnd) -> String {
    format!("{end:?}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_rows::RecordAt;
    use crate::model::{Fabric, FabricId, IncarnationId, Mesh, MeshId, Node, NodeId, NodeStatus, ProviderKind, ScopeStatus};
    use crate::storage::{MemoryNodesStorage, NodeRow};
    use rdm_transport::StaticResolver;

    // @feature: node-records
    /// CONTRACT (i150 R8): a property effect may precede a failed metadata write. A repeat
    /// finishes metadata without contacting the node or overwriting its later own properties.
    #[tokio::test]
    async fn an_applied_entry_finishes_failed_metadata_without_repeating_the_node_effect() {
        use rdm_core::meta::{PersistentDisposition, PlacementMeta, StorageMeta};
        use rdm_core::properties::{EntryProperties, PropertyApplication};
        use rdm_node::node_records::{MemoryNodeRecords, NodeProperties, NodeRecordsStorage, RecordBirth};
        let mut admin = Node::allocated(rdm_core::PathName::new("mesh1.admin", "1").unwrap(), "mesh1", rdm_core::NodeKind::NodeAdmin);
        admin.is_primary = true; admin.is_fabric_primary = true; admin.status = NodeStatus::Ready;
        admin.incarnation_id = Some(IncarnationId::mint());
        let mut node = Node::allocated(rdm_core::PathName::new("mesh1.rpc", "1").unwrap(), "mesh1", rdm_core::NodeKind::RpcNode);
        node.status = NodeStatus::Ready; node.incarnation_id = Some(IncarnationId::mint());
        let birth = crate::build::PinnedBirth::of(&node).unwrap();
        let view = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
            nodes: vec![admin.clone(), node.clone()],
        };
        let storage = MemoryNodesStorage::default();
        let written = Arc::new(crate::node_rows::WrittenRows::default());
        written.put(&storage, &NodeRow { storage_persistence: Some(StorageMeta::Ephemeral), placement: Some(PlacementMeta::default()), ..NodeRow::new(node.name.clone(), "mesh1".to_owned(), rdm_core::NodeKind::RpcNode, node.node_id.clone(), NodeStatus::Ready) }).await.unwrap();
        let records = Arc::new(MemoryNodeRecords::default());
        let application = PropertyApplication::entry(&rdm_core::BuildId::mint(), &node.name);
        let own = NodeProperties {
            birth: RecordBirth { node_id: node.node_id.clone(), incarnation_id: birth.incarnation.clone() },
            config: [("source".into(), "later-node-write".into())].into(), tags: ["node".into()].into(),
            endpoint_id: None, transport_addr: None, listeners: vec![], app_types: None,
            held: Default::default(), applied: [application.clone()].into(),
        };
        records.put_properties(&own).await.unwrap();
        let endpoint = rdm_transport::endpoint::bind(iroh::SecretKey::from_bytes(&[9u8; 32]), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        // No node endpoint is registered: any repeat of the property RPC makes this test fail.
        let client = Arc::new(NodeRpcClient::new(endpoint.clone(), Arc::new(StaticResolver::new())).with_caller_system("rdm"));
        let door = NodeConfigs::new(client, written.clone());
        assert!(door.records.set(records.clone()).is_ok());
        let entry = EntryProperties { application, config: [("source".into(), "entry".into())].into(), tags: ["entry".into()].into() };
        let placed = (StorageMeta::Persistent { disposition: PersistentDisposition::Preserve }, PlacementMeta::default());
        let refusing = crate::workflows::node_config_update::tests::RefusingNodesStorage(MemoryNodesStorage::default());
        let error = door.apply_entry(&admin.name, &admin.node_id, &view, &refusing, &birth, entry.clone(), placed).await.unwrap_err();
        assert!(error.contains("simulated disk refusal"), "{error}");
        door.apply_entry(&admin.name, &admin.node_id, &view, &storage, &birth, entry, placed).await.unwrap();
        assert_eq!(written.row_of(&node.node_id).unwrap().storage_persistence, Some(placed.0));
        assert_eq!(records.properties(&node.node_id).await.unwrap(), Some(own));
        endpoint.close().await;
    }

    // @feature: node-records
    /// i150: both local and downward commands commit at the node, including a stopped process.
    /// Build retries recognize their durable effect without replacing a later local map.
    #[tokio::test]
    async fn local_and_downward_config_use_the_same_durable_writer_and_build_identity() {
        use rdm_node::node_records::{MemoryNodeRecords, NodeRecordsStorage};
        use rdm_core::properties::{EntryProperties, PropertyApplication};
        let mut admin = Node::allocated(rdm_core::PathName::new("unrelated.path", "admin").unwrap(), "mesh1", rdm_core::NodeKind::NodeAdmin);
        admin.status = NodeStatus::Ready; admin.stopped = true;
        admin.is_primary = true; admin.is_fabric_primary = true;
        admin.incarnation_id = Some(IncarnationId::mint());
        let mesh_id = MeshId::mint();
        let view = Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(mesh_id.clone()), name: "mesh1".into(), status: ScopeStatus::Ready }],
            nodes: vec![admin.clone()],
        };
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
        let channels = rdm_transport::topology::TopologyView::join(&gossip, &ep, &view.fabric.id, "mesh1", &mesh_id, admin.node_id.as_str(), rdm_transport::clock::os_clock(), vec![], &[]).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(ep.clone(), Arc::new(StaticResolver::new())).with_caller_system("rdm"));
        let door = NodeConfigs::new(client, Arc::new(crate::node_rows::WrittenRows::default()));
        let records = Arc::new(MemoryNodeRecords::default());
        let storage = MemoryNodesStorage::default();
        let req = |value: &str| StatusRequest::UpdateNodeConfig {
            node_id: admin.node_id.clone(), incarnation: admin.incarnation_id.clone().unwrap(), config: [("source".into(), value.into())].into(),
        };
        assert!(matches!(door.update(&admin.name, &admin.node_id, &view, &storage, &req("unbound")).await, StatusReply::NotReady { .. }));
        assert!(storage.record_at(&admin.name).await.unwrap().is_none());
        door.uses_records(records.clone(), channels.clone());
        door.bound_own(&admin, rdm_core::launch::InitialProperties { config: [("old".into(), "value".into())].into(), tags: ["kept".into()].into(), applied: Default::default() }).await.unwrap();
        let topology = Arc::new(tokio::sync::RwLock::new(view.clone()));
        assert!(matches!(door.serve(&admin.name, &admin.node_id, &view, &topology, &storage, None, SentBy::Caller, &req("caller")).await, StatusReply::ConfigUpdated { .. }));
        let caller = records.properties(&admin.node_id).await.unwrap().unwrap();
        assert_eq!(caller.config, [("source".into(), "caller".into())].into());
        assert_eq!(caller.tags, ["kept".into()].into());
        assert!(storage.record_at(&admin.name).await.unwrap().is_none(), "primaries copy no properties into combined rows");
        let entry = EntryProperties { application: PropertyApplication::entry(&rdm_core::BuildId::mint(), &admin.name), config: [("source".into(), "build".into())].into(), tags: ["build".into()].into() };
        assert!(matches!(door.own_entry(&admin, entry.clone()).await.unwrap().unwrap(), StatusReply::Applied));
        door.own_config(&admin, [("source".into(), "later".into())].into()).await.unwrap().unwrap();
        assert!(matches!(door.own_entry(&admin, entry.clone()).await.unwrap().unwrap(), StatusReply::AlreadyApplied));
        assert_eq!(records.properties(&admin.node_id).await.unwrap().unwrap().config, [("source".into(), "later".into())].into());
        let next = EntryProperties { application: PropertyApplication::entry(&rdm_core::BuildId::mint(), &admin.name), ..entry };
        door.own_entry(&admin, next).await.unwrap().unwrap();
        assert_eq!(records.properties(&admin.node_id).await.unwrap().unwrap().config, [("source".into(), "build".into())].into());
        let absent = door.update_with(&admin.name, &admin.node_id, &view, &storage, &req("metadata"), Some((rdm_core::meta::StorageMeta::Ephemeral, Default::default()))).await;
        assert!(matches!(absent, StatusReply::NodeRowRefused { .. }), "missing lifecycle metadata cannot report success: {absent:?}");
        let mut stale = req("stale");
        if let StatusRequest::UpdateNodeConfig { incarnation, .. } = &mut stale { *incarnation = IncarnationId::mint(); }
        assert!(matches!(door.update(&admin.name, &admin.node_id, &view, &storage, &stale).await, StatusReply::RejectedStaleIncarnation { .. }));
        let mut other = view.clone(); other.nodes[0].is_primary = false;
        assert!(matches!(door.update(&admin.name, &admin.node_id, &other, &storage, &req("wrong-primary")).await, StatusReply::RejectedNotPrimary { .. }));
        assert_eq!(SentBy::of_writer_call(None), SentBy::Caller);
        assert_eq!(SentBy::of_writer_call(Some(&admin)), SentBy::Other);
        ep.close().await;
    }
}
