//! `node.config.update` (node-config-update.md; ops-naming "Node data: meta, config and exe").
//!
//! The update runs one of two ways (Q235). A caller's goes down: the caller sends it to the
//! fabric-primary, which sends the command to the node's mesh-primary (in process when it is that
//! mesh-primary too; Forward `0x1A` through another node-admin of the mesh when the mesh-primary is not
//! directly reachable). A caller that is not a node-admin and sends it to another admin has it carried up
//! to the fabric-primary its records name ([`NodeConfigs::forward_up`]). A node's own goes up: the node
//! sends it to the mesh-primary its topology view names, which writes the row and its atlas and answers
//! the node, and owes the row up to the fabric-primary ([`NodeConfigs::owe_push`]); a node-admin's own
//! goes to its mesh-primary the same way, in process when it is that mesh-primary.
//!
//! Config is a field of the node record. The mesh-primary of the node's mesh is the only writer of
//! that mesh's rows (fabric-node-lifecycle §4.3; Q235 P5), and steps the row's revision on every write
//! (P1). It writes the node's whole row from what it holds (a blind put of the `path.name` key; it never
//! reads the store on the write path). On a caller's update it tells the exact birth directly, within
//! its mesh, with `ApplyNodeConfig` carrying the map and the row's revision, and answers
//! `node.config.updated` only after that apply answered; a stopped node is told nothing and the command
//! completes on the row. On a node's own update the node already holds the map and is told nothing: the
//! answer carries the row as written. Every install of a map, at the node and in each atlas, is made
//! over an older revision only. A single call, never a Build, and never refused because a Build runs:
//! updates go through this one door one at a time.

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
    /// This admin's view, read again by each owed push up ([`Self::owe_push`]): set at the boot, or
    /// by the first command served.
    topology: std::sync::OnceLock<Arc<tokio::sync::RwLock<Topology>>>,
    /// The push up owed for each node, by the revision it carries (Q235 P3): a newer revision replaces
    /// an older one's send.
    pushes: std::sync::Mutex<std::collections::HashMap<rdm_core::NodeId, (u64, tokio::task::JoinHandle<()>)>>,
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
    /// The node the command names, about itself (a node's own update, Q235): over Node RPC, the call's
    /// endpoint is the subject's; in process, a node-admin's own (`Running::config_update`). Served by
    /// the subject's mesh-primary, never carried up.
    Own,
    /// A node of the fabric that is not a node-admin, about another node: a caller's command, carried up
    /// to the fabric-primary by an admin that is not it (node-config-update.md, a caller's update).
    Caller,
    /// A node-admin over Node RPC (the fabric-primary's command sent down, or a carrier of it), or an
    /// endpoint the view names no birth for: served by the node's mesh-primary, refused by name at an
    /// admin that is neither primary.
    Other,
}

impl SentBy {
    /// The sender the view names for a call's endpoint (`StatusAuthority::sender_of`) of a command naming
    /// `subject`: the subject itself, a caller that is not a node-admin, or a node-admin.
    pub fn of(sender: Option<&crate::model::Node>, subject: &rdm_core::NodeId) -> Self {
        match sender {
            Some(n) if n.node_id == *subject => Self::Own,
            Some(n) if n.kind != rdm_core::NodeKind::NodeAdmin => Self::Caller,
            _ => Self::Other,
        }
    }

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
    /// Up to the fabric-primary, owed by this admin while it is `mesh`'s primary (the push of a node's
    /// row, Q235): withdrawn once it is not.
    Push(&'a str),
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
    /// The door over `client`; `own` holds this admin's config when it is the subject; `written` is
    /// the record the writer of this admin's mesh rows holds (`NodeRows::with_written`).
    pub fn new(client: Arc<NodeRpcClient>, own: crate::status_rpc::Configure, written: Arc<crate::node_rows::WrittenRows>) -> Self {
        Self { client, own, one_at_a_time: tokio::sync::Mutex::new(()), written, views: Default::default(), topology: Default::default(), pushes: Default::default() }
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

    /// Hold `topology`, this admin's view: what an owed push up reads again on each send.
    pub fn reads(&self, topology: Arc<tokio::sync::RwLock<Topology>>) {
        let _ = self.topology.set(topology);
    }

    /// A `UpdateNodeConfig` at the admin `me` (born as `me_id`) sent by `sent_by`, deciding over `view`,
    /// the view `topology` holds (node-config-update.md): a node's own ([`SentBy::Own`]) served by the
    /// node's mesh-primary (`Self::own_update`); a caller's at an admin that is not the fabric-primary,
    /// carried up to the fabric-primary and its answer handed back ([`Self::forward_up`]); at the
    /// fabric-primary, the command sent to the node's mesh-primary ([`Self::update`] in process when it
    /// is that mesh-primary too) and the row as written, from its completion, written into `atlas` over
    /// an older revision (P1: never ahead of the row); a node-admin's at the node's mesh-primary, the
    /// command served ([`Self::update`]); at any other admin refused `ReceiverNotPrimary`, naming the
    /// fabric-primary.
    ///
    /// The command sent down is owed until the mesh-primary answers it (node-rpc.md §43; D12): one
    /// `Budget::Owed` call to the mesh-primary this admin's view names, direct first and then through each
    /// other live node-admin of the mesh on Forward `0x1A`. A mesh-primary that answers `RejectedNotPrimary`
    /// (the primary changing: a newly elected admin that does not hold it yet) or is proven gone does
    /// not fail the caller: the owning workflow retargets it to the mesh-primary the view names when the
    /// view next changes, or after the resend schedule's next delay, whichever is first.
    #[allow(clippy::too_many_arguments)]
    pub async fn serve(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, view: &Topology, topology: &Arc<tokio::sync::RwLock<Topology>>, storage: &dyn crate::storage::NodesStorage, atlas: Option<&crate::atlas::Atlas>, sent_by: SentBy, req: &StatusRequest) -> StatusReply {
        let StatusRequest::UpdateNodeConfig { node_id, config, .. } = req else { unreachable!("serve is called for UpdateNodeConfig only") };
        self.reads(topology.clone());
        if sent_by == SentBy::Own {
            return self.own_update(me, me_id, view, topology, storage, req).await;
        }
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
        // The op's result is the atlas's (Q204), taken from the completion as the row was written, at its
        // revision (P1): the atlas never holds a map the row has not committed.
        if let StatusReply::ConfigUpdated { config, revision, .. } = &reply {
            span.record("revision", *revision);
            span.record("atlas", atlas.map_or_else(|| "no-atlas".to_string(), |a| a.install_config(node_id, config, *revision).name()).as_str());
            // The fabric-primary as the subject of a caller's update its mesh-primary wrote: the command
            // reached its mesh-primary from the subject itself, so it was told nothing there; it installs
            // the row as written over an older revision here.
            if subject.node_id == *me_id && !view.is_mesh_primary(&subject.mesh, me_id) {
                if let Some(configure) = self.own.get() {
                    configure(crate::status_rpc::ConfigHeld::Told { config: config.clone(), revision: *revision }).await;
                }
            }
        }
        span.record("outcome", reply.name());
        span.in_scope(|| tracing::info!("node.config.update at the fabric-primary decided"));
        reply
    }

    /// A node's own `UpdateNodeConfig` (node-config-update.md, a node's own update; Q235), at the admin
    /// `me` deciding over `view`: at the node's mesh-primary, the row and its atlas written and the push
    /// up owed ([`Self::own_write`]); a node-admin's own at an admin that is not its mesh-primary, owed to
    /// the mesh-primary its view names until it answers ([`Self::owed`], in process once this admin holds
    /// it), the answer handed back; any other sender's at an admin that is not the node's mesh-primary is
    /// refused `ReceiverNotPrimary` naming it, and the node sends it again to the mesh-primary its view
    /// names next. One span, `rdm.node_admin.node.update.via-own-config`.
    async fn own_update(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, view: &Topology, topology: &tokio::sync::RwLock<Topology>, storage: &dyn crate::storage::NodesStorage, req: &StatusRequest) -> StatusReply {
        let StatusRequest::UpdateNodeConfig { node_id, config, .. } = req else { unreachable!("own_update is called for UpdateNodeConfig only") };
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-own-config",
            node = %me,
            subject = tracing::field::Empty,
            node_id = %node_id,
            keys = config.len(),
            mesh_primary = tracing::field::Empty,
            route = tracing::field::Empty,
            sends = tracing::field::Empty,
            revision = tracing::field::Empty,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let reply = tracing::Instrument::instrument(
            async {
                let Some(subject) = view.own(node_id).cloned() else {
                    tracing::Span::current().record("detail", format!("{me}: node {node_id} holds no birth in this admin's view; its own config cannot be put").as_str());
                    return rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown);
                };
                tracing::Span::current().record("subject", subject.name.to_string().as_str());
                if view.is_mesh_primary(&subject.mesh, me_id) {
                    tracing::Span::current().record("mesh_primary", me.to_string().as_str());
                    tracing::Span::current().record("route", "in-process");
                    return self.own_write(me, me_id, view, storage, req).await;
                }
                if subject.node_id == *me_id {
                    // This admin's own, in process: owed to its mesh-primary until it answers (P4).
                    return match self.owed(me, me_id, view, topology, req, Hop::Down(&subject)).await {
                        Owed::Answered(reply) => reply,
                        Owed::InProcess(now) => self.own_write(me, me_id, &now, storage, req).await,
                        Owed::Gone(_) => rdm_core::reply::status::rejected_not_primary(NotPrimary::SubjectUnknown),
                    };
                }
                let held_by = view.node_type_primary(&subject.mesh, rdm_core::NodeKind::NodeAdmin).map_or_else(|| "held by no single live node-admin in this view".to_string(), |p| format!("held by {}", p.name));
                let needed = format!("{}'s mesh-primary, where {}'s own node.config.update goes ({held_by}; {me} does not hold it)", subject.mesh, subject.name);
                tracing::Span::current().record("detail", needed.as_str());
                rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed })
            },
            span.clone(),
        )
        .await;
        if let StatusReply::ConfigUpdated { revision, .. } | StatusReply::ConfigRowRefused { revision, .. } = &reply {
            span.record("revision", *revision);
        }
        span.record("outcome", reply.name());
        span.in_scope(|| tracing::info!("a node's own node.config.update decided"));
        reply
    }

    /// A node's own update at its mesh-primary `me`: the row written, stepping its revision, and its atlas
    /// (the writer feeds it), the node told nothing (it holds the map it sent, P2), the push up owed when
    /// `me` is not the fabric-primary; answered with the row as written.
    async fn own_write(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, view: &Topology, storage: &dyn crate::storage::NodesStorage, req: &StatusRequest) -> StatusReply {
        let StatusRequest::UpdateNodeConfig { node_id, incarnation, config } = req else { unreachable!("own_write is called for UpdateNodeConfig only") };
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-config",
            node = %me,
            subject = tracing::field::Empty,
            node_id = %node_id,
            incarnation_id = %incarnation.as_str(),
            keys = config.len(),
            tags = tracing::field::Empty,
            row_status = tracing::field::Empty,
            revision = tracing::field::Empty,
            apply = tracing::field::Empty,
            push = tracing::field::Empty,
            outcome = tracing::field::Empty,
            answered = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let reply = tracing::Instrument::instrument(self.put(me, me_id, view, storage, node_id, incarnation, config, None, Tell::Nobody, &span), span.clone()).await;
        span.record("outcome", reply.name());
        span.in_scope(|| tracing::info!("node.config.update decided"));
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
            StatusRequest::UpdateNodeConfig { node_id, .. } | StatusRequest::CreateTags { node_id, .. } | StatusRequest::DeleteTags { node_id, .. } | StatusRequest::UpdateName { node_id, .. } => (node_id, first.own(node_id).map(|n| n.name.to_string()).unwrap_or_default()),
            other => unreachable!("forward_up carries a node's property command only, never {}", other.op()),
        };
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-forward",
            node = %me,
            op = req.op(),
            subject = %subject,
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
        Some((carrier, path)) if attempt > 0 => rdm_transport::route::RouteChoice::ViaPeer { carrier: carrier.clone(), path: path.clone() },
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
            Entity::Nodes => tracing::info_span!("rdm.node_admin.node.serve.via-forward", node = %me, read = op, pairs = %pairs, fabric_primary = empty, mesh_primary = empty, route = empty, outcome = empty, detail = empty, "otel.kind" = "internal"),
            Entity::Meshes => tracing::info_span!("rdm.node_admin.mesh.serve.via-forward", node = %me, read = op, pairs = %pairs, fabric_primary = empty, mesh_primary = empty, route = empty, outcome = empty, detail = empty, "otel.kind" = "internal"),
            _ => tracing::info_span!("rdm.node_admin.fabric.serve.via-forward", node = %me, read = op, pairs = %pairs, fabric_primary = empty, mesh_primary = empty, route = empty, outcome = empty, detail = empty, "otel.kind" = "internal"),
        };
        let whom = to.whom();
        let carried = async {
            let primary = match to {
                ReadTo::FabricPrimary => view.fabric_primary(),
                ReadTo::MeshPrimary => view.own(me_id).and_then(|own| view.node_type_primary(&own.mesh, rdm_core::NodeKind::NodeAdmin)),
            };
            let Some(primary) = primary else {
                return Err(ReadReply::NotReady { reason: format!("{me} answers no {op} from an atlas, and its view names none to send this one on to ({whom})") });
            };
            span.record(to.field(), primary.name.to_string().as_str());
            span.record("route", "direct");
            match rdm_client::NodeRpc::new(&self.client).read(&NodeTarget::ExactNode(primary.node_id.clone()), read.entity, &read.filter, &CallOptions::default()).await {
                Ok(rows) => Ok(rows),
                Err(rdm_client::CallEnd::Rejected { reason, detail }) if reason == "rejected-filter" => Err(ReadReply::RejectedFilter { reason: detail }),
                Err(e) => Err(ReadReply::NotReady { reason: format!("{me} read {op} from {whom} {}: the read ended {} ({e})", primary.name, e.outcome()) }),
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

/// A read's rows by the reply that carries them: `nodes`, `meshes` or `fabrics`.
pub(crate) fn rows_name(rows: &rdm_core::read::Rows) -> &'static str {
    match rows {
        rdm_core::read::Rows::Nodes(_) => "nodes",
        rdm_core::read::Rows::Meshes(_) => "meshes",
        rdm_core::read::Rows::Fabrics(_) => "fabrics",
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
            Hop::Up | Hop::Push(_) | Hop::Root => ("fabric_primary", "the fabric-primary"),
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
                        tracing::Span::current().record(field, me.to_string().as_str());
                        return Owed::InProcess(view);
                    }
                    view.node_type_primary(&subject.mesh, rdm_core::NodeKind::NodeAdmin).map(|p| (p.name.clone(), p.node_id.clone(), p.mesh.clone())).ok_or_else(|| format!("{} has no mesh-primary in {me}'s view", subject.mesh))
                }
                Hop::Push(mesh) if !view.is_mesh_primary(mesh, me_id) => {
                    // Withdrawn: the mesh's next primary owes it again from the row (P3).
                    tracing::Span::current().record("route", "withdrawn");
                    return Owed::Gone(view);
                }
                Hop::Up | Hop::Push(_) | Hop::Root => {
                    if view.is_fabric_primary(me_id) {
                        tracing::Span::current().record("route", "in-process");
                        tracing::Span::current().record(field, me.to_string().as_str());
                        return Owed::InProcess(view);
                    }
                    view.fabric_primary().map(|p| (p.name.clone(), p.node_id.clone(), p.mesh.clone())).ok_or_else(|| format!("no single live node-admin holds the fabric-primary in {me}'s view"))
                }
            };
            let ended = match primary {
                Err(none) => none,
                Ok((name, primary, mesh)) => {
                    tracing::Span::current().record(field, name.to_string().as_str());
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
                        Hop::Down(_) => tracing::info!(to = %name, send = sends, outcome = %outcome, "{} sent down to the mesh-primary", req.op()),
                        Hop::Up | Hop::Push(_) | Hop::Root => tracing::info!(to = %name, send = sends, outcome = %outcome, "{} carried up to the fabric-primary", req.op()),
                    }
                    match out {
                        rdm_core::outcome::RpcOutcome::Reply(r) => match (r.into_value(), &hop) {
                            // Down: any refusal that the receiver is not the primary is the primary changing. Up: only
                            // `ReceiverNotPrimary` is; any other answer (a subject the fabric-primary does not hold) is the caller's.
                            (StatusReply::RejectedNotPrimary { why }, Hop::Down(_)) => format!("{name} answered rejected-not-primary ({})", why.as_str()),
                            (StatusReply::RejectedNotPrimary { why: why @ NotPrimary::ReceiverNotPrimary { .. } }, Hop::Up | Hop::Push(_) | Hop::Root) => format!("{name} answered rejected-not-primary ({})", why.as_str()),
                            // A fabric-primary holding no atlas yet takes the push, and answers the read, once it holds one.
                            (StatusReply::NotReady { reason }, Hop::Push(_) | Hop::Root) => format!("{name} answered not-ready ({reason})"),
                            (answered, _) => return Owed::Answered(answered),
                        },
                        // An Owed call ends without a reply only when its receiver is proven gone, or
                        // before anything was owed: the command is owed to the primary the view names next.
                        other => format!("the call to {name} ended {} with no reply", other.name()),
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
            node = %me,
            subject = tracing::field::Empty,
            node_id = %node_id,
            incarnation_id = %incarnation.as_str(),
            keys = config.len(),
            tags = tracing::field::Empty,
            row_status = tracing::field::Empty,
            revision = tracing::field::Empty,
            apply = tracing::field::Empty,
            push = tracing::field::Empty,
            outcome = tracing::field::Empty,
            answered = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let reply = tracing::Instrument::instrument(self.put(me, me_id, view, storage, node_id, incarnation, config, onto_row, Tell::Birth, &span), span.clone()).await;
        span.record("outcome", reply.name());
        span.in_scope(|| tracing::info!("node.config.update decided"));
        reply
    }

    #[allow(clippy::too_many_arguments)]
    async fn put(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, view: &Topology, storage: &dyn crate::storage::NodesStorage, node_id: &rdm_core::NodeId, incarnation: &rdm_core::IncarnationId, config: &std::collections::BTreeMap<String, String>, onto_row: Option<(rdm_core::meta::StorageMeta, rdm_core::meta::PlacementMeta)>, tell: Tell, span: &tracing::Span) -> StatusReply {
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
        let mut standing = (std::collections::BTreeMap::new(), 0);
        match self.written.known_row(storage, node_id).await {
            // A writer that meets the record finishes the release a rename left pending
            // (node-name-update.md); one that fails again stays pending, named on its span.
            Ok(Some(r)) => {
                let _ = crate::node_rows::finish_release(storage, &self.written, me, &r, "config-put").await;
                span.record("tags", crate::node_rows::labels(&r.tags).as_str());
                standing = (r.config.clone(), r.revision);
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
            // The refusal carries the row as it stands (P2): a node's own update installs it.
            let (config, revision) = self.written.row_of(node_id).map_or(standing, |r| (r.config, r.revision));
            span.record("revision", revision);
            return rdm_core::reply::status::bounded_config_row_refused_at(&subject.name.to_string(), &detail, config, revision);
        }
        // The row as written, at the revision this write stepped it to (P1): what every answer carries.
        let (written, revision) = self.written.row_of(node_id).map_or_else(|| (config.clone(), 0), |r| (r.config, r.revision));
        span.record("revision", revision);
        // The row up to the fabric-primary's atlas (P3): owed by a mesh-primary that is not it.
        if view.is_fabric_primary(me_id) {
            span.record("push", "fabric-primary");
        } else {
            span.record("push", "owed");
            self.owe_push(me, me_id, view, &subject.mesh, node_id, &written, revision);
        }
        let updated = StatusReply::ConfigUpdated { node_id: node_id.clone(), incarnation: incarnation.clone(), config: written.clone(), revision };
        // A node's own update is told nothing: it holds the map it sent and installs the answer (P2).
        if tell == Tell::Nobody {
            span.record("apply", "own");
            return updated;
        }
        // A stopped node is told nothing: its config is a field of its row, and it gets it back in its
        // hydration sync at its next start (node-rpc-envelope.md "Node properties"), as node.name.update
        // completes a stopped node on the row.
        if subject.stopped {
            span.record("apply", "stopped");
            return updated;
        }
        // The node applies it. This admin as the subject applies it in-process, through what holds
        // its config (the same hand-off its `ApplyNodeConfig` door uses); any other member of its
        // mesh is called directly.
        let told = if subject.node_id == *me_id {
            span.record("apply", "in-process");
            match self.own.get() {
                Some(configure) => {
                    configure(crate::status_rpc::ConfigHeld::Told { config: written.clone(), revision }).await;
                    Ok(rdm_core::reply::applied())
                }
                None => Ok(rdm_core::reply::status::not_ready(format!("{me}: its digest does not exist yet; the config is not held"))),
            }
        } else {
            span.record("apply", "direct");
            let birth = ExactBirth { target: NodeTarget::ExactNode(node_id.clone()), node_id: node_id.clone(), incarnation: incarnation.clone() };
            NodeRpc::new(&self.client).config_apply(&birth, written.clone(), revision, &CallOptions::default()).await
        };
        let (answered, detail) = match told {
            Ok(StatusReply::Applied | StatusReply::AlreadyApplied) => return updated,
            Ok(refused) => (refused.name(), format!("{me} wrote the row; the birth answered {refused:?}")),
            Err(end) => (end.outcome(), format!("{me} wrote the row; apply-node-config ended {}", describe(&end))),
        };
        span.record("answered", answered);
        span.record("detail", detail.as_str());
        rdm_core::reply::status::bounded_config_not_applied(&subject.name.to_string(), answered, &detail)
    }
}

impl NodeConfigs {
    /// Owe the row of `node_id` as written, `config` at `revision`, up to the fabric-primary, while this
    /// admin `me` is `mesh`'s primary (node-config-update.md, a node's own update, step 3; Q235 P3): keyed
    /// by the node and its revision, a newer revision replacing an older one's send; one
    /// `NodeConfigUpdated` owed until the fabric-primary answers by name (`Self::owed`: resent to the
    /// fabric-primary the view names next on `ReceiverNotPrimary` or an exited verdict, with backoff), and
    /// withdrawn once this admin is not `mesh`'s primary: the next one owes it again from the row
    /// ([`Self::owe_pushes`]). Once this admin holds the fabric-primary, its own atlas takes it. One span,
    /// `rdm.node_admin.node.update.via-config-push`.
    #[allow(clippy::too_many_arguments)]
    pub fn owe_push(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, view: &Topology, mesh: &str, node_id: &rdm_core::NodeId, config: &std::collections::BTreeMap<String, String>, revision: u64) {
        let mut pushes = self.pushes.lock().unwrap();
        if let Some((owed, task)) = pushes.get(node_id) {
            if *owed >= revision && !task.is_finished() {
                return;
            }
            task.abort();
        }
        let topology = self.topology.get().cloned().unwrap_or_else(|| Arc::new(tokio::sync::RwLock::new(view.clone())));
        let (client, views, written) = (self.client.clone(), self.views.get().cloned(), self.written.clone());
        let (me, me_id, first, mesh) = (me.clone(), me_id.clone(), view.clone(), mesh.to_string());
        let req = StatusRequest::NodeConfigUpdated { node_id: node_id.clone(), config: config.clone(), revision };
        let subject = view.own(node_id).map(|n| n.name.to_string()).unwrap_or_default();
        let span = tracing::info_span!(
            "rdm.node_admin.node.update.via-config-push",
            node = %me,
            subject = %subject,
            node_id = %node_id,
            revision,
            fabric_primary = tracing::field::Empty,
            route = tracing::field::Empty,
            sends = tracing::field::Empty,
            atlas = tracing::field::Empty,
            outcome = tracing::field::Empty,
            detail = tracing::field::Empty,
            "otel.kind" = "internal",
        );
        let (pushed, config) = (node_id.clone(), config.clone());
        let task = tokio::spawn(tracing::Instrument::instrument(
            async move {
                let outcome = match owed(&client, views, &me, &me_id, &first, &topology, &req, Hop::Push(&mesh)).await {
                    Owed::Answered(reply) => reply.name().to_string(),
                    Owed::InProcess(_) => {
                        let installed = written.atlas().map_or_else(|| "no-atlas".to_string(), |a| a.install_config(&pushed, &config, revision).name());
                        tracing::Span::current().record("atlas", installed.as_str());
                        "in-process".to_string()
                    }
                    Owed::Gone(_) => "withdrawn".to_string(),
                };
                tracing::Span::current().record("outcome", outcome.as_str());
                tracing::info!("a node's row pushed up to the fabric-primary ended");
            },
            span,
        ));
        pushes.insert(node_id.clone(), (revision, task));
    }

    /// The pushes up this admin still owes, by node and revision: sent and not yet answered by name.
    pub fn pushes_owed(&self) -> Vec<(rdm_core::NodeId, u64)> {
        self.pushes.lock().unwrap().iter().filter(|(_, (_, task))| !task.is_finished()).map(|(n, (r, _))| (n.clone(), *r)).collect()
    }

    /// The push of every row of `mesh` this admin's writer holds, owed again by its new primary `me`
    /// at the moment it takes the mesh's writing (Q235 P3: a successor owes it again from the row; the
    /// fabric-primary's atlas answers `AlreadyApplied` for a revision it holds). Nothing is owed while
    /// `me` is the fabric-primary.
    pub fn owe_pushes(&self, me: &crate::model::PathName, me_id: &rdm_core::NodeId, view: &Topology, mesh: &str) {
        if view.is_fabric_primary(me_id) || !view.is_mesh_primary(mesh, me_id) {
            return;
        }
        // A row whose name was released (`Deleted`, `Withdrawn`) names no node to hydrate or diff a Build against.
        for r in self.written.rows_of(mesh) {
            if !matches!(r.status, crate::model::NodeStatus::Withdrawn | crate::model::NodeStatus::Deleted) {
                self.owe_push(me, me_id, view, mesh, &r.node_id, &r.config, r.revision);
            }
        }
    }
}

/// Who a config put tells ([`NodeConfigs::put`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tell {
    /// The live birth, `ApplyNodeConfig` (a caller's update, a Build's).
    Birth,
    /// Nobody: a node's own update, whose node holds the map it sent (Q235 P2).
    Nobody,
}

fn describe(end: &CallEnd) -> String {
    format!("{end:?}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Fabric, FabricId, IncarnationId, Mesh, MeshId, Node, NodeId, NodeStatus, ProviderKind, ScopeStatus};
    use crate::storage::MemoryNodesStorage;
    use rdm_transport::StaticResolver;
    use std::collections::{BTreeMap, BTreeSet};

    // CONTRACT (`docs/architecture/lifecycles/node-config-update.md:5` — "`node.config.update` replaces a
    // node's application config, the whole map: a PUT, never a patch.",
    // `docs/architecture/lifecycles/node-config-update.md:22` — "2. The mesh-primary writes the record,
    // stepping its revision, and its atlas.", and
    // `docs/architecture/lifecycles/node-config-update.md:25` — "A stopped node is sent nothing: the
    // command completes after the durable row write, and its next start receives the config in its
    // hydration sync."): `SentBy::of` classifies the subject itself as `Own`, non-admin peers as `Caller`,
    // and other admins as `Other`; `NodeConfigs::update` replaces the whole map, steps the row revision on
    // each put, preserves existing tags, completes on the row for a stopped node, and refuses stale
    // incarnations and non-mesh-primary receivers.
    // @feature: node-config
    #[tokio::test]
    async fn config_update_replaces_whole_map_steps_revision_preserves_tags_and_classifies_senders() {
        let mut admin1 = Node::allocated("mesh1.admin.1".parse().unwrap());
        admin1.status = NodeStatus::Ready;
        admin1.is_primary = true;
        admin1.is_fabric_primary = true;
        admin1.incarnation_id = Some(IncarnationId::mint());

        let mut admin2 = Node::allocated("mesh1.admin.2".parse().unwrap());
        admin2.status = NodeStatus::Ready;
        admin2.incarnation_id = Some(IncarnationId::mint());

        let mut rpc1 = Node::allocated("mesh1.rpc.1".parse().unwrap());
        rpc1.status = NodeStatus::Leaving;
        rpc1.stopped = true;
        let rpc1_inc = IncarnationId::mint();
        rpc1.incarnation_id = Some(rpc1_inc.clone());

        assert_eq!(SentBy::of(Some(&rpc1), &rpc1.node_id), SentBy::Own);
        assert_eq!(SentBy::of(Some(&rpc1), &admin1.node_id), SentBy::Caller);
        assert_eq!(SentBy::of(Some(&admin2), &rpc1.node_id), SentBy::Other);
        assert_eq!(SentBy::of(None, &rpc1.node_id), SentBy::Other);
        assert_eq!(SentBy::of_writer_call(Some(&admin2)), SentBy::Other);
        assert_eq!(SentBy::of_writer_call(Some(&rpc1)), SentBy::Caller);
        assert_eq!(SentBy::of_writer_call(None), SentBy::Caller);

        let view = Topology {
            fabric: Fabric {
                id: FabricId::mint(),
                name: "fabric1".into(),
                status: ScopeStatus::Ready,
                provider: ProviderKind::Process,
            },
            meshes: vec![Mesh {
                id: Some(MeshId::mint()),
                name: "mesh1".into(),
                status: ScopeStatus::Ready,
            }],
            nodes: vec![admin1.clone(), admin2.clone(), rpc1.clone()],
        };

        let storage = MemoryNodesStorage::default();
        let written = Arc::new(crate::node_rows::WrittenRows::default());
        let atlas = Arc::new(crate::atlas::Atlas::default());
        written.feeds(atlas.clone());

        let mut seeded = NodeRow::new(rpc1.name.clone(), rpc1.node_id.clone(), NodeStatus::Leaving);
        seeded.tags = BTreeSet::from(["gold".to_string()]);
        seeded.config = BTreeMap::from([("old_key".into(), "old_val".into())]);
        written.put(&storage, &seeded).await.unwrap();

        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::from_bytes(&[7u8; 32]), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(ep.clone(), Arc::new(StaticResolver::new())).with_caller_system("rdm"));
        let door = NodeConfigs::new(client, Default::default(), written.clone());

        let map1 = BTreeMap::from([("zone".to_string(), "eu-1".to_string())]);
        let req1 = StatusRequest::UpdateNodeConfig {
            node_id: rpc1.node_id.clone(),
            incarnation: rpc1_inc.clone(),
            config: map1.clone(),
        };

        // Non-mesh-primary refuses ReceiverNotPrimary.
        assert!(matches!(
            door.update(&admin2.name, &admin2.node_id, &view, &storage, &req1).await,
            StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { .. } }
        ));

        // Stale incarnation refuses RejectedStaleIncarnation.
        let stale_req = StatusRequest::UpdateNodeConfig {
            node_id: rpc1.node_id.clone(),
            incarnation: IncarnationId::mint(),
            config: map1.clone(),
        };
        assert_eq!(
            door.update(&admin1.name, &admin1.node_id, &view, &storage, &stale_req).await,
            StatusReply::RejectedStaleIncarnation { held: rpc1_inc.clone() }
        );

        // Mesh-primary replaces the whole map (old_key is removed), steps revision from 1 -> 2, and keeps tags.
        let reply1 = door.update(&admin1.name, &admin1.node_id, &view, &storage, &req1).await;
        assert_eq!(
            reply1,
            StatusReply::ConfigUpdated {
                node_id: rpc1.node_id.clone(),
                incarnation: rpc1_inc.clone(),
                config: map1.clone(),
                revision: 2,
            }
        );
        let row1 = written.row_of(&rpc1.node_id).unwrap();
        assert_eq!(row1.config, map1);
        assert!(!row1.config.contains_key("old_key"), "PUT replaces the whole map, never patches");
        assert_eq!(row1.tags, BTreeSet::from(["gold".to_string()]), "config put preserves tags on the record");
        assert_eq!(row1.revision, 2);

        // Second put steps revision to 3.
        let map2 = BTreeMap::from([("rack".to_string(), "r9".to_string())]);
        let req2 = StatusRequest::UpdateNodeConfig {
            node_id: rpc1.node_id.clone(),
            incarnation: rpc1_inc.clone(),
            config: map2.clone(),
        };
        let reply2 = door.own_write(&admin1.name, &admin1.node_id, &view, &storage, &req2).await;
        assert_eq!(
            reply2,
            StatusReply::ConfigUpdated {
                node_id: rpc1.node_id,
                incarnation: rpc1_inc,
                config: map2,
                revision: 3,
            }
        );
        ep.close().await;
    }
}
