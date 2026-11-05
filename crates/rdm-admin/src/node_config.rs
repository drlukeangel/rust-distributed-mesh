//! Property commands use the subject node's durable property writer.
//! Caller commands go through the fabric-primary and mesh-primary to the exact birth.
//! Local changes commit at the node without an upward property copy.

use crate::topology::Topology;
use rdm_client::{CallEnd, ExactBirth, NodeRpc};
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget};
use rdm_core::status::{NotPrimary, Status, StatusReply, StatusRequest};
use std::sync::Arc;

/// A mesh-primary's `NodeConfigs`: the client it tells a birth of its mesh through, what holds this
/// admin's own config (the mesh-primary is a member of its mesh and the writer of its own row too),
/// and the lock that makes every config change one at a time.
pub struct NodeConfigs {
    records: std::sync::OnceLock<Arc<dyn crate::storage::NodesStorage>>,
    own_properties: std::sync::OnceLock<rdm_node::node_records::NodePropertyWriter>,
    client: Arc<NodeRpcClient>,
    one_at_a_time: tokio::sync::Mutex<()>,
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
    /// The call to the mesh-primary ended with no answer, its attempts spent (node-rpc.md §43): why,
    /// by name. The owner never resends it.
    Unanswered(String),
}

/// How a caller's property command an admin carries up ended ([`NodeConfigs::forward_up`]).
pub enum Up {
    /// The fabric-primary answered it: the answer the caller is handed.
    Answered(StatusReply),
    /// This admin holds the fabric-primary in this view: the command is served in process.
    InProcess(Topology),
    /// The call to the fabric-primary ended with no answer, its attempts spent (node-rpc.md §43): why,
    /// by name. The owner never resends it.
    Unanswered(String),
}

impl Up {
    /// What the caller is handed for a command whose call up ended unanswered: `NotReady` naming the
    /// call's end, as a read carried to a primary that ended unanswered is (`NodeConfigs::carry_read`).
    pub fn unanswered(why: String) -> StatusReply {
        rdm_core::reply::status::not_ready(why)
    }
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
            Some(n) if rdm_core::is_node_admin(&n.r#type) => Self::Other,
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
    /// The call ended with its attempts spent and no answer: why, by name (node-rpc.md §43).
    Unanswered(String),
}

/// The slot an admin fills once its node-RPC client exists.
pub type NodeConfigsSlot = Arc<std::sync::OnceLock<Arc<NodeConfigs>>>;

impl NodeConfigs {
    /// The `NodeConfigs` over `client`.
    pub fn new(client: Arc<NodeRpcClient>) -> Self {
        Self { records: Default::default(), own_properties: Default::default(), client, one_at_a_time: tokio::sync::Mutex::new(()), views: Default::default() }
    }

    /// Bind the configured provider's nodes store, where each node's record is, both its parts (Q333);
    /// `provider` answers the provider it is written to now. Every access follows provider migration.
    /// `named` answers the admin's current path and name, what each write's span names it by.
    pub fn uses_records(&self, records: Arc<dyn crate::storage::NodesStorage>, provider: impl Fn() -> String + Send + Sync + 'static, named: impl Fn() -> Option<rdm_core::PathName> + Send + Sync + 'static) {
        let writer = rdm_node::node_records::NodePropertyWriter::new(records.clone(), provider);
        writer.named_by(named);
        let _ = self.own_properties.set(writer);
        let _ = self.records.set(records);
    }

    /// The nodes store bound by [`Self::uses_records`]: where each node's record is read.
    pub fn records(&self) -> Option<&Arc<dyn crate::storage::NodesStorage>> {
        self.records.get()
    }

    /// This admin's own serialized property writer, shared by local and downward updates.
    pub fn own_properties(&self) -> Option<&rdm_node::node_records::NodePropertyWriter> {
        self.own_properties.get()
    }

    /// The record this admin's own commands hand its writer: the record its start wrote, with the config
    /// it holds.
    fn own_snapshot(&self) -> Option<rdm_node::nodes_storage::NodeRecord> {
        self.own_properties.get()?.start_record()
    }

    /// Write this admin's own record at its start (Q356), before it may become Ready: `subject` its
    /// identity, bound addresses and data dir, `runtime` the runtime it runs in, `cert` the dates of the
    /// member cert and signing cert it holds, state `starting`. Its config and tags are its own row's
    /// from its hydration sync (Q234) when it read one, else `properties`, the launch's; its storage
    /// persistence and executable `properties`'. Nothing is read from the store at this start (Q227).
    pub async fn bound_own(&self, subject: &crate::model::Node, properties: rdm_core::launch::InitialProperties, runtime: rdm_core::RuntimeFact, cert: (Option<rdm_core::certs::CertDates>, Option<rdm_core::certs::SigningCertDates>)) -> Result<(), String> {
        let writer = self.own_properties.get().ok_or_else(|| format!("{}: own property writer is not bound", subject.node_id))?;
        let (config, tags) = rdm_node::node::retained_properties(writer, &subject.node_id, &properties);
        let (Some(incarnation), Some(endpoint_id), Some(transport_addr)) = (subject.incarnation_id.clone(), subject.endpoint_id.clone(), subject.transport_addr) else {
            return Err(format!("{}: its own record needs its incarnation, endpoint and transport address", subject.node_id));
        };
        let initial = rdm_node::nodes_storage::NodeRecord {
            node_id: subject.node_id.clone(), path: subject.path.clone(), name: subject.name.clone(), r#type: subject.r#type.clone(),
            incarnation, state: crate::model::NodeStatus::Starting, endpoint_id, transport_addr, listeners: subject.listeners.clone(), runtime,
            is_primary: None, is_fabric_primary: None, cert: cert.0, signing_cert: cert.1,
            storage: rdm_core::meta::Storage { persistence: properties.persistence.or_else(|| Some(rdm_core::meta::StoragePersistence::of_type(&subject.r#type))), data_dir: subject.data_dir.clone() },
            executable: properties.executable.clone(), config, tags,
        };
        writer.bound(initial, |_| {}).await.map(|_| ()).map_err(|e| e.to_string())
    }

    /// Write `change` on this admin's own record (Q356), through its own property writer. Nothing is
    /// written before its start's record.
    pub async fn own_update(&self, change: impl FnOnce(&mut rdm_node::nodes_storage::NodeRecord) + Send + 'static) -> Result<(), String> {
        let Some(writer) = self.own_properties.get() else {
            return Err("its own property writer is not bound".into());
        };
        let initial = writer.start_record().ok_or("its start has not written its record yet")?;
        writer.update(initial, change, |_| {}).await.map(|_| ()).map_err(|e| e.to_string())
    }

    /// Write `to` as this admin's own state on its own record (Q356; [`rdm_node::nodes_storage::NodeRecord::set_state`]),
    /// before it publishes or declares it. A state its record holds already, or one its order refuses,
    /// writes nothing.
    pub async fn own_state(&self, to: crate::model::NodeStatus) -> Result<(), String> {
        let Some(writer) = self.own_properties.get() else {
            return Err("its own property writer is not bound".into());
        };
        let initial = writer.start_record().ok_or("its start has not written its record yet")?;
        writer.state(initial, to).await.map(|_| ()).map_err(|e| e.to_string())
    }

    /// This admin's own property writer, once its start bound it: what its `NodeSelf` writes each state
    /// of a stop and a start through (Q356).
    pub fn own_writer(&self) -> Option<rdm_node::node_records::NodePropertyWriter> {
        self.own_properties.get().cloned()
    }

    /// Write `to` as this admin's own path and name on its own record (Q356), before it publishes the
    /// name (record first, mesh second).
    pub async fn own_name(&self, to: &crate::model::PathName) -> Result<(), String> {
        let to = to.clone();
        self.own_update(move |record| { record.path = to.path; record.name = to.name; }).await
    }

    /// Record this admin's own roles on its own record (Q356; node-primary-update.md step 5): the
    /// roles `primaries` names its birth `birth` in `mesh` ([`rdm_node::node_records::write_own_roles`]),
    /// written at each change of the view, from its start's record on, until the view is dropped.
    pub async fn record_own_roles(&self, primaries: rdm_gossip::primary::PrimaryView, birth: crate::storage::BirthRef, mesh: String) {
        let Some(writer) = self.own_properties.get().cloned() else { return };
        let mut changed = primaries.subscribe();
        let mut written = (None, None);
        loop {
            changed.borrow_and_update();
            rdm_node::node_records::write_own_roles(&writer, &primaries, &birth, &mesh, "node-admin", &mut written).await;
            if changed.changed().await.is_err() {
                return;
            }
        }
    }

    /// An admin writes its own map locally through the same writer as downward commands.
    pub async fn own_config(&self, _subject: &crate::model::Node, config: std::collections::BTreeMap<String, String>) -> Option<Result<StatusReply, crate::storage_error::StorageError>> {
        let initial = self.own_snapshot()?;
        let writer = self.own_properties.get()?;
        Some(writer.update(initial, move |record| record.config = config, |_| {}).await.map(|written| StatusReply::ConfigUpdated {
                node_id: written.record.node_id, incarnation: written.record.incarnation,
                config: written.record.config,
            }))
    }

    /// A Build's complete entry uses this admin's ordinary property writer.
    pub async fn own_entry(&self, _subject: &crate::model::Node, entry: rdm_core::properties::EntryProperties) -> Option<Result<StatusReply, crate::storage_error::StorageError>> {
        let initial = self.own_snapshot()?;
        Some(self.own_properties.get()?.entry(initial, entry, |_| {}).await.map(|_| rdm_core::reply::applied()))
    }

    /// A downward tag command to this admin is a local write, with no command to its primary.
    pub async fn own_tags(&self, _subject: &crate::model::Node, change: rdm_node::node_records::TagChange) -> Option<Result<StatusReply, crate::storage_error::StorageError>> {
        let initial = self.own_snapshot()?;
        Some(self.own_properties.get()?.tags(initial, change, |_| {}).await)
    }

    /// Carry a Build's entry from its mesh-primary to the pinned node, which writes it on its own record
    /// (Q356); its storage persistence is on that record from its launch.
    pub async fn apply_entry(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, view: &Topology, birth: &crate::build::PinnedBirth, entry: rdm_core::properties::EntryProperties) -> Result<(), String> {
        let _ = me;
        let subject = view.own(&birth.node_id).ok_or_else(|| format!("{me_id}: property target {} is not held", birth.node_id))?;
        if !birth.is(subject) {
            return Err(format!("{me_id}: property target {} has a different incarnation", birth.node_id));
        }
        if !view.is_mesh_primary(&subject.mesh, me_id) {
            return Err(format!("{me_id}: property target {} is not in its mesh-primary's view", birth.node_id));
        }
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
        Ok(())
    }

    /// The client these `NodeConfigs` tell births through: the mesh-primary's node.name.update tells a birth its
    /// name through it as well (`crate::node_name_update`).
    pub fn client(&self) -> &NodeRpcClient {
        &self.client
    }

    /// [`Self::client`], shared: for a call made after the caller's turn ends.
    pub fn client_arc(&self) -> Arc<NodeRpcClient> {
        self.client.clone()
    }

    /// The turn of one change to a node record at this mesh-primary (node-config-update.md:
    /// "every change to a node record goes through its mesh-primary's one held record, one at a
    /// time"): these `NodeConfigs`' puts and node.name.update's take it.
    pub async fn one_at_a_time(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.one_at_a_time.lock().await
    }

    /// Hold `views`, the tick of this admin's installed view: what a command sent down and refused
    /// because the mesh's primary is changing waits on before it is sent again ([`Self::serve`]).
    pub fn watch_views(&self, views: tokio::sync::watch::Receiver<u64>) {
        let _ = self.views.set(views);
    }

    /// The tick of this admin's installed view, once held ([`Self::watch_views`]).
    pub(crate) fn views(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        self.views.get().cloned()
    }



    /// A caller's config command goes through its primaries to the subject's writer.
    /// The command sent down is owed until the mesh-primary answers it (node-rpc.md §43; D12): one
    /// `Send::default()` call to the mesh-primary this admin's view names, direct first and then through each
    /// other live node-admin of the mesh on Forward `0x1A`. A mesh-primary that answers `RejectedNotPrimary`
    /// (the primary changing: a newly elected admin that does not hold it yet) or is proven gone does
    /// not fail the caller: the owning workflow retargets it to the mesh-primary the view names when the
    /// view next changes, or after the resend schedule's next delay, whichever is first.
    #[allow(clippy::too_many_arguments)]
    pub async fn serve(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, view: &Topology, topology: &Arc<tokio::sync::RwLock<Topology>>, storage: &dyn crate::storage::NodesStorage, sent_by: SentBy, req: &StatusRequest) -> StatusReply {
        let StatusRequest::UpdateNodeConfig { node_id, config, .. } = req else { unreachable!("serve is called for UpdateNodeConfig only") };
        let mut held = None;
        if sent_by == SentBy::Caller && !view.is_fabric_primary(me_id) {
            match self.forward_up(me, me_id, view, topology, req).await {
                Up::Answered(reply) => return reply,
                Up::InProcess(now) => held = Some(now),
                Up::Unanswered(why) => return Up::unanswered(why),
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
            tracing::info_span!("rdm.node_admin.node.reject.via-not-primary", node_path = %me.path, node_name = %me.name, subject_path = %subject.path, subject_name = %subject.name, op = req.op(), detail = %needed, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("node.config.update refused: this admin holds neither the fabric-primary nor the node's mesh-primary"));
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed });
        }
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-fabric-primary",
            receiver_node_id = %me_id,
            subject_path = %subject.path, subject_name = %subject.name,
            node_id = %node_id,
            keys = config.len(),
            mesh_primary = tracing::field::Empty,
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
                    Down::Unanswered(why) => Up::unanswered(why),
                }
            },
            span.clone(),
        )
        .await;
        span.record("outcome", reply.name());
        span.in_scope(|| tracing::info!("node.config.update at the fabric-primary decided"));
        reply
    }





    /// The fabric-primary's command `req` about `subject` down to `subject`'s mesh-primary ([`Self::serve`]):
    /// the mesh-primary's answer, the view in which this admin holds that primary itself (served in
    /// process) or no longer holds the birth, or the call's end when its attempts are spent unanswered.
    /// Each send is one span event naming the mesh-primary, the route and the outcome.
    pub async fn send_down(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, first: &Topology, topology: &tokio::sync::RwLock<Topology>, subject: &crate::model::Node, req: &StatusRequest) -> Down {
        match self.owed(me, me_id, first, topology, req, Hop::Down(subject)).await {
            Owed::Answered(reply) => Down::Answered(reply),
            Owed::InProcess(view) => Down::InProcess(view),
            Owed::Gone(view) => Down::Gone(view),
            Owed::Unanswered(why) => Down::Unanswered(why),
        }
    }

    /// A caller's property command `req` at this admin, which `first` does not name the fabric-primary,
    /// carried up to the fabric-primary (node-config-update.md step 1; commands go down from it, so a
    /// caller's goes up to it first), owed until it answers (node-rpc.md §43): the fabric-primary's
    /// answer, handed back to the caller as it is, or the view in which this admin holds the
    /// fabric-primary itself (served in process). The fabric-primary is the one this admin's view names
    /// from its records, never a digest; one `Send::default()` call addressed to the
    /// fabric-primary role (`NodeTarget::Primary`), direct first and then through each other live
    /// node-admin of its mesh on Forward `0x1A`. A fabric-primary that answers `ReceiverNotPrimary`
    /// (it no longer holds it, or its successor does not yet) does not fail the caller: the command is
    /// sent again to the fabric-primary the view names when it next changes, or after the resend
    /// schedule's next delay. A call whose attempts are spent unanswered ends the command
    /// ([`Up::Unanswered`]): the owner never resends (node-rpc.md §43). One span,
    /// `rdm.node_admin.node.update.via-forward`, names the fabric-primary, the sends and the answer.
    pub async fn forward_up(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, first: &Topology, topology: &tokio::sync::RwLock<Topology>, req: &StatusRequest) -> Up {
        let (node_id, subject) = match req {
            StatusRequest::UpdateNodeConfig { node_id, .. } | StatusRequest::CreateTags { node_id, .. } | StatusRequest::DeleteTags { node_id, .. } | StatusRequest::UpdateName { node_id, .. } => (node_id, first.own(node_id).map(|n| n.location())),
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
            Owed::Unanswered(why) => {
                span.record("outcome", "unanswered");
                Up::Unanswered(why)
            }
        };
        span.in_scope(|| tracing::info!("a caller's property command carried up to the fabric-primary ended"));
        up
    }

    /// `req` to the primary `hop` names in this admin's view: down to the subject's mesh-primary, or up to
    /// the fabric-primary. One `Send::default()` call per send, addressed to the primary role
    /// (`NodeTarget::Primary`), direct first and then through each other live node-admin of that
    /// primary's mesh on Forward `0x1A`; a refusal that it is not the primary sends it again to the
    /// primary the view names when it next changes, or after the resend schedule's next delay
    /// (retargeting on a domain answer is the owning workflow's). A call whose attempts are spent ends
    /// it unanswered: the owner never resends (node-rpc.md §43).
    #[allow(clippy::too_many_arguments)]
    async fn owed(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, first: &Topology, topology: &tokio::sync::RwLock<Topology>, req: &StatusRequest, hop: Hop<'_>) -> Owed {
        owed(&self.client, self.views.get().cloned(), me, me_id, first, topology, req, hop).await
    }
}

/// The route of a call to `primary`, a primary of `mesh` in `view`: direct first; once an attempt sends
/// nothing, through each other live node-admin of `mesh` in turn (Forward `0x1A`), never `me_id`.
fn primary_route(view: &Topology, mesh: &str, primary: &rdm_core::NodeId, me_id: &rdm_core::NodeId) -> rdm_transport::RoutePolicy {
    let carriers: Vec<(rdm_core::NodeId, crate::model::PathName)> = view.births().filter(|n| n.mesh == mesh && rdm_core::is_node_admin(&n.r#type) && n.node_id != *primary && n.node_id != *me_id && n.status.is_live()).map(|n| (n.node_id.clone(), n.location())).collect();
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
                ReadTo::MeshPrimary => view.own(me_id).and_then(|own| view.node_type_primary(&own.mesh, "node-admin")),
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
                        // The birth left the view while its command was owed: the answer a service gives a birth it does not hold.
                        return Owed::Gone(view);
                    }
                    if view.is_mesh_primary(&subject.mesh, me_id) {
                        tracing::Span::current().record("route", "in-process");
                        tracing::Span::current().record(field, me_id.as_str());
                        return Owed::InProcess(view);
                    }
                    view.node_type_primary(&subject.mesh, "node-admin").map(|p| (p.node_id.clone(), p.mesh.clone(), NodeTarget::Primary { mesh: Some(subject.mesh.clone()) })).ok_or_else(|| format!("{} has no mesh-primary in {me_id}'s view", subject.mesh))
                }
                Hop::Up | Hop::Root => {
                    if view.is_fabric_primary(me_id) {
                        tracing::Span::current().record("route", "in-process");
                        tracing::Span::current().record(field, me_id.as_str());
                        return Owed::InProcess(view);
                    }
                    view.fabric_primary().map(|p| (p.node_id.clone(), p.mesh.clone(), NodeTarget::Primary { mesh: None })).ok_or_else(|| format!("no single live node-admin holds the fabric-primary in {me_id}'s view"))
                }
            };
            let ended = match primary {
                Err(none) => none,
                Ok((primary, mesh, role)) => {
                    tracing::Span::current().record(field, primary.as_str());
                    let opts = CallOptions { send: rdm_transport::Send::default(), route: Some(primary_route(&view, &mesh, &primary, me_id)), ..CallOptions::default() };
                    sends += 1;
                    let (out, _) = client.call::<Status>(&role, req, &opts).await;
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
                        // No answer: the call's attempts are spent (or the role names no birth). The owner
                        // never resends it (node-rpc.md §43): the command ends unanswered, by name.
                        other => {
                            let why = format!("{} to {to_whom} ({primary}) ended {} with no answer ({})", req.op(), other.name(), other.detail_name());
                            tracing::Span::current().record("detail", why.as_str());
                            return Owed::Unanswered(why);
                        }
                    }
                }
            };
            waits += 1;
            let delay = client.delivery().pacing().after(waits);
            tracing::Span::current().record("detail", ended.as_str());
            tracing::info!(detail = %ended, wait_ms = delay.as_millis() as u64, "{} is sent again to {to_whom} the view names next", req.op());
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
        // The subject is the exact birth the call names, wherever its row stands: a node whose row its
        // mesh-primary moved to a name node.name.update gave it is still that birth (never found by path and name, its shape
        // included: `Topology::own`). A subject this receiver does not hold is the receiver's own fact
        // (node-rpc-envelope.md: "`SubjectUnknown` when the receiver does not hold the subject at all"),
        // never a peer that did not resolve.
        let Some(subject) = view.own(node_id) else {
            span.record("detail", format!("{me_id}: node {node_id} holds no birth in this admin's view; its config cannot be put").as_str());
            return rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown);
        };
        span.record("subject_path", subject.path.as_str()).record("subject_name", subject.name.as_str());
        span.record("row_status", format!("{:?}", subject.status).as_str());
        // The writer of the row is the mesh-primary of the node's mesh, and no other admin
        // (fabric-node-lifecycle §4.3); a caller that sent it elsewhere is told whose role it needs.
        if !view.is_mesh_primary(&subject.mesh, me_id) {
            let held_by = view.node_type_primary(&subject.mesh, "node-admin").map_or_else(|| "held by no single live node-admin in this view".to_string(), |p| format!("held by {}", p.node_id));
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
                NodeRpc::new(&self.client).config_apply(&birth, config.clone(), &CallOptions::default()).await
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
            return StatusReply::ConfigUpdated { node_id: node_id.clone(), incarnation: incarnation.clone(), config: config.clone() };
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
    use crate::model::{Fabric, FabricId, IncarnationId, Mesh, MeshId, Node, NodeStatus, ProviderKind, ScopeStatus};
    use crate::storage::MemoryNodesStorage;
    use rdm_transport::StaticResolver;

    // @feature: node-records
    /// i150: both local and downward commands commit at the node, including a stopped process; a
    /// Build's entry and a later local map go through the same writer, the last write standing.
    #[tokio::test]
    async fn local_and_downward_config_use_the_same_durable_writer() {
        use crate::storage::NodesStorage as _;
        use rdm_core::properties::EntryProperties;
        let mut admin = Node::allocated(rdm_core::PathName::new("unrelated.path", "admin").unwrap(), "mesh1", "node-admin");
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
        let topology_view = rdm_gossip::topology::TopologyView::new(&view.fabric.id, "mesh1", &mesh_id, admin.node_id.as_str(), rdm_transport::clock::os_clock());
        let channels = rdm_gossip::channel::Channels::new(&gossip, &ep, &topology_view).unwrap();
        channels.join(rdm_gossip::channel::ChannelId::mesh(topology_view.fabric_id(), topology_view.mesh_id()), vec![]).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(ep.clone(), Arc::new(StaticResolver::new())).with_caller_system("rdm"));
        let configs = NodeConfigs::new(client);
        let records = Arc::new(MemoryNodesStorage::default());
        let storage = MemoryNodesStorage::default();
        let req = |value: &str| StatusRequest::UpdateNodeConfig {
            node_id: admin.node_id.clone(), incarnation: admin.incarnation_id.clone().unwrap(), config: [("source".into(), value.into())].into(),
        };
        assert!(matches!(configs.update(&admin.location(), &admin.node_id, &view, &storage, &req("unbound")).await, StatusReply::NotReady { .. }));
        assert!(storage.record_at(&admin.location()).await.unwrap().is_none());
        configs.uses_records(records.clone(), || "memory".into(), || None);
        configs.bound_own(&crate::model::Node { endpoint_id: Some(crate::model::EndpointId("0".repeat(64))), transport_addr: Some("127.0.0.1:1".parse().unwrap()), ..admin.clone() }, rdm_core::launch::InitialProperties { config: [("old".into(), "value".into())].into(), tags: ["kept".into()].into(), executable: None, persistence: None }, rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("config-own")).unwrap(), (None, None)).await.unwrap();
        let topology = Arc::new(tokio::sync::RwLock::new(view.clone()));
        assert!(matches!(configs.serve(&admin.location(), &admin.node_id, &view, &topology, &storage, SentBy::Caller, &req("caller")).await, StatusReply::ConfigUpdated { .. }));
        let own = || async { records.node(&admin.node_id).await.unwrap().unwrap().record };
        let caller = own().await;
        assert_eq!(caller.config, [("source".into(), "caller".into())].into());
        assert_eq!(caller.tags, ["kept".into()].into());
        assert!(storage.record_at(&admin.location()).await.unwrap().is_none(), "the node writes its own record: nothing is written on the store the mesh-primary was handed");
        let entry = EntryProperties { config: [("source".into(), "build".into())].into(), tags: ["build".into()].into() };
        assert!(matches!(configs.own_entry(&admin, entry.clone()).await.unwrap().unwrap(), StatusReply::Applied));
        assert_eq!(own().await.tags, ["build".into()].into());
        configs.own_config(&admin, [("source".into(), "later".into())].into()).await.unwrap().unwrap();
        assert_eq!(own().await.config, [("source".into(), "later".into())].into(), "the later local map stands");
        configs.own_entry(&admin, entry).await.unwrap().unwrap();
        assert_eq!(own().await.config, [("source".into(), "build".into())].into(), "a Build's entry written after it replaces it");
        let mut stale = req("stale");
        if let StatusRequest::UpdateNodeConfig { incarnation, .. } = &mut stale { *incarnation = IncarnationId::mint(); }
        assert!(matches!(configs.update(&admin.location(), &admin.node_id, &view, &storage, &stale).await, StatusReply::RejectedStaleIncarnation { .. }));
        let mut other = view.clone(); other.nodes[0].is_primary = false;
        assert!(matches!(configs.update(&admin.location(), &admin.node_id, &other, &storage, &req("wrong-primary")).await, StatusReply::RejectedNotPrimary { .. }));
        assert_eq!(SentBy::of_writer_call(None), SentBy::Caller);
        assert_eq!(SentBy::of_writer_call(Some(&admin)), SentBy::Other);
        ep.close().await;
    }
}
