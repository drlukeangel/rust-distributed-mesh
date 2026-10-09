//! The lifecycle/status declarations node-admin applies as an authority (i143.e6.s7;
//! node-rpc.md §38; fabric-mesh-ops.md §3).
//!
//! A declaration reaches this admin over Node RPC (`Status`, op `0x1B`). The sender is never
//! read from the request: it is the authenticated peer, resolved to a birth in this admin's own
//! view. The admin applies a declaration only when it holds the seat the declaration needs and
//! the sender is the subject (or the subject's primary), idempotently on the natural key, forward
//! along the legal order, and answers what happened. `Applied` means the declared state is held
//! in this admin's entity state and, for a node, written to `nodes.storage` as the subject's row.
//!
//! Gossip is untouched: the view's `status` stays what membership says; what the authority
//! applied is the view's `declared`.

use crate::model::{FabricId, IncarnationId, MeshId, NodeId, NodeKind, EndpointId};
use crate::topology::Topology;
use crate::storage::NodeRecord;
use rafka_node_rpc::{PeerContext, ServerBuilder};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::status::{transition, FabricEvent, MeshState, NodeState, NotAuthority, Status, StatusReply, StatusRequest, Transition};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock, RwLock};

/// What this admin applied, by natural key.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Declared {
    /// Per node id: the birth whose state was declared, and the state.
    pub nodes: BTreeMap<NodeId, (IncarnationId, NodeState)>,
    /// Per mesh id.
    pub meshes: std::collections::HashMap<MeshId, MeshState>,
    /// Per fabric id: the events applied, in order, each once.
    pub fabric: std::collections::HashMap<FabricId, Vec<String>>,
    /// Per mesh name: the latest `ReadyForTraffic` report this admin received as the fabric-primary,
    /// from the mesh's primary and the exact birth that sent it. Memory of this process's tenure; it is
    /// never stored or rehydrated, because a report is evidence only while it is fresh.
    pub reports: BTreeMap<String, MeshReport>,
}

/// A mesh primary's round-complete report, as the fabric-primary received it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshReport {
    /// The birth of the node-admin that reported.
    pub incarnation: IncarnationId,
    /// When this admin received it.
    pub at: std::time::Instant,
}

impl Declared {
    pub fn node(&self, node_id: &NodeId) -> Option<(IncarnationId, NodeState)> {
        self.nodes.get(node_id).cloned()
    }
}

impl Declared {
    /// What an admin applied before it last stopped, folded from the rows it put: the greatest
    /// state per Mesh, each Fabric event once, and the state on each node row it wrote. A reader
    /// folds; nothing is written back.
    pub async fn rehydrate(status: &dyn crate::status_storage::StatusStorage, nodes: &dyn crate::storage::NodesStorage) -> Result<Self, String> {
        let mut declared = Self { meshes: status.mesh_statuses().await.map_err(|e| format!("status.storage: {e}"))?, ..Self::default() };
        for e in status.fabric_events().await.map_err(|e| format!("status.storage: {e}"))? {
            declared.fabric.entry(e.fabric_id).or_default().push(e.event);
        }
        for c in nodes.contacts().await.map_err(|e| format!("nodes.storage: {e}"))? {
            if let Some(state) = c.declared.as_deref().and_then(parse_node_state) {
                declared.nodes.insert(c.node_id, (c.incarnation_id, state));
            }
        }
        Ok(declared)
    }
}

/// What the service reads and writes: this admin's identity, its current view, what it applied,
/// and the rows it writes.
pub struct StatusAuthority {
    pub me: crate::model::PathName,
    pub fabric_id: FabricId,
    pub topology: Arc<tokio::sync::RwLock<Topology>>,
    pub declared: Arc<Mutex<Declared>>,
    pub nodes_storage: Arc<dyn crate::storage::NodesStorage>,
    /// Where an applied Mesh status or Fabric event is a keyed row, put before it is answered.
    pub status_storage: Arc<dyn crate::status_storage::StatusStorage>,
    /// The mesh ids this admin holds, by mesh name.
    pub mesh_ids: Arc<dyn Fn() -> BTreeMap<String, MeshId> + Send + Sync>,
    /// This node's re-publish of its presence (its peers handed to its mesh channel again, its
    /// digest published), filled once it has joined: what a node-admin's status kick asks of it.
    pub republish: Republish,
    /// This admin's own drain (the server refuses new calls, in-flight work finishes), answering
    /// the work still in flight; filled by the role binary. What an `ApplyNodeState(Draining)`
    /// naming this admin runs.
    pub drain: Drain,
    /// Testkit knob: hold the next reply past the caller's bound after applying (acceptance 2:
    /// apply + reply loss is `Indeterminate`, and the retry is `AlreadyApplied`).
    pub hold_next_reply: Arc<std::sync::atomic::AtomicBool>,
}

/// What an `Applied` decision changes once its row is acknowledged. A decision never mutates
/// `Declared`: the effect is committed only after the put returned `Ok`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    Node(NodeRecord, NodeState),
    Mesh(MeshId, MeshState),
    Fabric(FabricId, String),
}

/// The decision, pure over a view and what was applied: what the admin does with a declaration
/// from `sender`. It reads `declared` and returns the effect to commit; it writes nothing.
pub fn decide(auth: &StatusAuthority, view: &Topology, declared: &Declared, sender: Option<&crate::model::Node>, req: &StatusRequest) -> (StatusReply, Option<Effect>) {
    let me = match view.nodes.iter().find(|n| n.name == auth.me) {
        Some(n) => n,
        None => return (StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "a view holding this admin".into() } }, None),
    };
    let Some(sender) = sender else {
        return (StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: "unknown peer".into() } }, None);
    };
    match req {
        StatusRequest::DeclareNodeState { node_id, incarnation, state } => {
            if sender.node_id != *node_id {
                return (StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: sender.name.to_string() } }, None);
            }
            // The receiver's seat: a node-admin declares to the fabric-primary, every other kind
            // to its mesh's primary.
            let needed = if sender.kind == NodeKind::NodeAdmin { me.is_fabric_primary } else { me.is_primary && me.mesh == sender.mesh };
            if !needed {
                let seat = if sender.kind == NodeKind::NodeAdmin { "fabric-primary" } else { "mesh-primary" };
                return (StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: format!("{seat} of {}", sender.mesh) } }, None);
            }
            // The exact birth: a declaration for an incarnation the view no longer holds is stale.
            match sender.incarnation_id.as_ref() {
                Some(held) if held == incarnation => {}
                Some(held) => return (StatusReply::RejectedStaleIncarnation { held: held.clone() }, None),
                None => return (StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown }, None),
            }
            let current = declared.node(&sender.node_id).and_then(|(inc, s)| (inc == *incarnation).then_some(s));
            match transition(current, *state) {
                Transition::AlreadyApplied => (StatusReply::AlreadyApplied, None),
                Transition::Backward { current } => (StatusReply::RejectedInvalidNodeTransition { current }, None),
                Transition::Apply => {
                    let row = NodeRecord {
                        node_id: sender.node_id.clone(),
                        name: sender.name.clone(),
                        incarnation_id: incarnation.clone(),
                        endpoint_id: sender.endpoint_id.clone().unwrap_or_else(|| EndpointId(String::new())),
                        transport_addr: sender.transport_addr.unwrap_or_else(|| std::net::SocketAddr::from(([0, 0, 0, 0], 0))),
                        listeners: sender.listeners.clone(),
                        declared: Some(format!("{state:?}")),
                        status: None,
                    };
                    (StatusReply::Applied, Some(Effect::Node(row, *state)))
                }
            }
        }
        StatusRequest::DeclareMeshState { mesh_id, state } => {
            if !me.is_fabric_primary {
                return (StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "fabric-primary".into() } }, None);
            }
            // The sender's Mesh id, as this holder knows it: the ids its own Build created, else the
            // id the Mesh's members carry in the view. The fabric seat moves to whichever admin has
            // the lowest node id, which may hold a Mesh it never created.
            let ids = (auth.mesh_ids)();
            let senders_mesh = ids.get(&sender.mesh).or_else(|| view.meshes.iter().find(|m| m.name == sender.mesh).and_then(|m| m.id.as_ref()));
            if !(sender.kind == NodeKind::NodeAdmin && sender.is_primary && senders_mesh == Some(mesh_id)) {
                return (StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: sender.name.to_string() } }, None);
            }
            apply_mesh(declared, mesh_id, *state)
        }
        // Downward exact-node operations reach the subject itself (`self_subject`); an authority
        // that is not the subject is not the receiver they name.
        StatusRequest::ApplyNodeState { .. } | StatusRequest::ProbeNodeState { .. } => {
            (StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "the subject node itself".into() } }, None)
        }
        StatusRequest::ApplyMeshState { mesh_id, mesh_name, state } => {
            if !sender.is_fabric_primary {
                return (StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: sender.name.to_string() } }, None);
            }
            if me.mesh != *mesh_name {
                return (StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: format!("an admin of {mesh_name}") } }, None);
            }
            // Never a replacement mesh id to satisfy the operation.
            if let Some(held) = (auth.mesh_ids)().get(mesh_name) {
                if held != mesh_id {
                    return (StatusReply::RejectedStaleMesh { held: held.clone() }, None);
                }
            }
            apply_mesh(declared, mesh_id, *state)
        }
        StatusRequest::ApplyFabricEvent { fabric_id, event } => {
            if !sender.is_fabric_primary {
                return (StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: sender.name.to_string() } }, None);
            }
            if !me.is_primary {
                return (StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "mesh-primary".into() } }, None);
            }
            if *fabric_id != auth.fabric_id {
                return (StatusReply::RejectedStaleFabric { held: auth.fabric_id.clone() }, None);
            }
            let key = event_key(event);
            if declared.fabric.get(fabric_id).is_some_and(|events| events.contains(&key)) {
                return (StatusReply::AlreadyApplied, None);
            }
            (StatusReply::Applied, Some(Effect::Fabric(fabric_id.clone(), key)))
        }
    }
}

fn event_key(e: &FabricEvent) -> String {
    match e {
        FabricEvent::ReadyForTraffic => "ready-for-traffic".into(),
        FabricEvent::ShutdownInitiated { initiated_by } => format!("shutdown-initiated:{initiated_by}"),
    }
}

fn apply_mesh(declared: &Declared, mesh_id: &MeshId, state: MeshState) -> (StatusReply, Option<Effect>) {
    match transition(declared.meshes.get(mesh_id).copied(), state) {
        Transition::AlreadyApplied => (StatusReply::AlreadyApplied, None),
        Transition::Backward { current } => (StatusReply::RejectedInvalidMeshTransition { current }, None),
        Transition::Apply => (StatusReply::Applied, Some(Effect::Mesh(mesh_id.clone(), state))),
    }
}

impl StatusAuthority {
    /// The one door every declaration goes through, whoever the sender is: a peer over Node RPC,
    /// or this admin itself when its view names it as the authority. Decide over the view; an
    /// `Applied` node state is durable (the subject's row) before it is answered, and the live
    /// view shows it at once; one span per decision names op, sender, outcome and whether the
    /// receiver holds its mesh's seat (accepting a Pending hand-off is not an election).
    pub async fn apply(&self, sender: Option<crate::model::Node>, req: &StatusRequest) -> StatusReply {
        let view = self.topology.read().await.clone();
        // A downward exact-node operation naming this admin itself (a probe, an apply) is
        // answered by the subject, through the same door.
        if let Some(me) = view.nodes.iter().find(|n| n.name == self.me) {
            if let Some(reply) = self_subject(me, sender.as_ref(), req, &self.republish, &self.drain).await {
                return reply;
            }
        }
        let receiver_is_primary = view.nodes.iter().find(|n| n.name == self.me).is_some_and(|n| n.is_primary);
        let (reply, effect) = {
            let declared = self.declared.lock().unwrap();
            decide(self, &view, &declared, sender.as_ref(), req)
        };
        let reply = match effect {
            Some(effect) => self.persist_then_commit(effect).await,
            None => reply,
        };
        // A `ReadyForTraffic` Mesh declaration the authority took (applied now or already applied) is
        // the mesh primary's round-complete report: the primary sends it only when its checklist is
        // complete. Whichever it was, the report is what this admin received now from that exact birth.
        if let (StatusRequest::DeclareMeshState { state: MeshState::ReadyForTraffic, .. }, StatusReply::Applied | StatusReply::AlreadyApplied, Some(from)) = (req, &reply, sender.as_ref()) {
            if let Some(incarnation) = from.incarnation_id.clone() {
                self.declared.lock().unwrap().reports.insert(from.mesh.clone(), MeshReport { incarnation, at: std::time::Instant::now() });
            }
        }
        tracing::info_span!(
            "rdm.node_admin.status.update.via-declaration",
            node = %self.me,
            op = req.op(),
            sender = %sender.as_ref().map(|n| n.name.to_string()).unwrap_or_default(),
            outcome = reply.name(),
            detail = %detail(&reply),
            receiver_is_primary,
        )
        .in_scope(|| tracing::info!("declaration decided by the authority"));
        reply
    }
}

impl StatusAuthority {
    /// An `Applied` decision: the fact's row is put and acknowledged, and only then is it held as
    /// applied and answered. A refused put answers `NotReady` naming the store and holds nothing,
    /// so the same declaration again is decided afresh. The commit decides again over what is
    /// held now, so two declarations of one natural key racing each other answer `Applied` once.
    async fn persist_then_commit(&self, effect: Effect) -> StatusReply {
        let put = match &effect {
            Effect::Node(row, _) => self.nodes_storage.put_contact(row).await.map_err(|e| format!("nodes.storage refused the row: {e}")),
            Effect::Mesh(mesh_id, state) => {
                self.status_storage.put_mesh_status(&crate::status_storage::MeshStatusRow { mesh_id: mesh_id.clone(), state: *state }).await.map_err(|e| format!("status.storage refused the Mesh status row: {e}"))
            }
            Effect::Fabric(fabric_id, event) => {
                self.status_storage.put_fabric_event(&crate::status_storage::FabricEventRow { fabric_id: fabric_id.clone(), event: event.clone() }).await.map_err(|e| format!("status.storage refused the Fabric event row: {e}"))
            }
        };
        if let Err(reason) = put {
            return StatusReply::NotReady { reason };
        }
        let reply = {
            let mut declared = self.declared.lock().unwrap();
            match &effect {
                Effect::Node(row, state) => match transition(declared.node(&row.node_id).and_then(|(inc, s)| (inc == row.incarnation_id).then_some(s)), *state) {
                    Transition::Apply => {
                        declared.nodes.insert(row.node_id.clone(), (row.incarnation_id.clone(), *state));
                        StatusReply::Applied
                    }
                    Transition::AlreadyApplied => StatusReply::AlreadyApplied,
                    Transition::Backward { current } => StatusReply::RejectedInvalidNodeTransition { current },
                },
                Effect::Mesh(mesh_id, state) => match apply_mesh(&declared, mesh_id, *state) {
                    (StatusReply::Applied, _) => {
                        declared.meshes.insert(mesh_id.clone(), *state);
                        StatusReply::Applied
                    }
                    (other, _) => other,
                },
                Effect::Fabric(fabric_id, event) => {
                    let events = declared.fabric.entry(fabric_id.clone()).or_default();
                    if events.contains(event) {
                        StatusReply::AlreadyApplied
                    } else {
                        events.push(event.clone());
                        StatusReply::Applied
                    }
                }
            }
        };
        if let (StatusReply::Applied, Effect::Node(row, _)) = (&reply, &effect) {
            let mut t = self.topology.write().await;
            if let Some(n) = t.nodes.iter_mut().find(|n| n.node_id == row.node_id && n.incarnation_id.as_ref() == Some(&row.incarnation_id)) {
                n.declared = row.declared.clone();
            }
        }
        reply
    }

    /// Initial fabric bootstrap: the Day-0 root has no upstream authority to hand it its mesh's
    /// Pending, so it applies it to itself (e4.s11 "single-admin recovery root"). Only Pending, only
    /// for its own mesh; the seat check does not apply because no seat exists yet. The same span
    /// as every decision, with the sender named as itself.
    pub async fn self_apply_mesh_pending(&self, mesh_id: &MeshId) -> StatusReply {
        let receiver_is_primary = self.topology.read().await.nodes.iter().find(|n| n.name == self.me).is_some_and(|n| n.is_primary);
        let (decided, effect) = apply_mesh(&self.declared.lock().unwrap(), mesh_id, MeshState::Pending);
        let reply = match effect {
            Some(effect) => self.persist_then_commit(effect).await,
            None => decided,
        };
        tracing::info_span!(
            "rdm.node_admin.status.update.via-declaration",
            node = %self.me,
            op = "apply-mesh-state",
            sender = %self.me,
            outcome = reply.name(),
            detail = "self-applied: initial fabric bootstrap, no upstream authority",
            receiver_is_primary,
        )
        .in_scope(|| tracing::info!("the Day-0 root applies its own mesh's Pending"));
        reply
    }
}

/// Serve `Status` on this admin. `authority` is filled once the admin holds a view; until then a
/// declaration is `NotReady` by name.
/// A node's re-publish of its presence, filled once it has joined its mesh.
pub type Republish = Arc<OnceLock<Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>>>;

/// A node row's stored `declared` text (the state's `Debug` name), read back as the state.
pub fn parse_node_state(s: &str) -> Option<NodeState> {
    [NodeState::Pending, NodeState::ReadyForTraffic, NodeState::Draining, NodeState::Leaving].into_iter().find(|n| format!("{n:?}") == s)
}

/// What a node is now, in the declaration vocabulary.
pub fn node_state_of(s: crate::model::NodeStatus) -> NodeState {
    use crate::model::NodeStatus as S;
    match s {
        S::Pending => NodeState::Pending,
        S::ReadyForTraffic => NodeState::ReadyForTraffic,
        S::Draining => NodeState::Draining,
        // A birth under restart is leaving; the frozen status vocabulary has no Restarting.
        S::Leaving | S::Restarting | S::PendingReconnect | S::Dead => NodeState::Leaving,
    }
}

/// A node's own drain, filled by the role binary: what `ApplyNodeState(Draining)` runs on the
/// subject; answers the work still in flight.
pub type Drain = Arc<OnceLock<Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = u64> + Send>> + Send + Sync>>>;

/// The downward exact-node operations, answered by the subject itself. `Some(reply)` when `req`
/// names this node and comes from a fabric member other than itself (a probe) or from a node-admin
/// other than itself (an apply); `None` for every other request.
///
/// The tickle: ask the exact birth to reassert itself. Protocol name: `ProbeNodeState`. The node
/// re-publishes its presence and answers its current state; no transition.
/// `ApplyNodeState(Draining)`: the node enters `Draining` (or already is) and answers the work
/// still in flight, the current count on every repeat. Any other state applied to a node-admin
/// is not served in this build, by name.
async fn self_subject(me: &crate::model::Node, sender: Option<&crate::model::Node>, req: &StatusRequest, republish: &Republish, drain: &Drain) -> Option<StatusReply> {
    let (node_id, incarnation) = match req {
        StatusRequest::ProbeNodeState { node_id, incarnation } | StatusRequest::ApplyNodeState { node_id, incarnation, .. } => (node_id, incarnation),
        _ => return None,
    };
    // A probe changes nothing: it asks this node to say it is here, so any fabric member other than
    // the subject may ask, and a node-admin's probe from another mesh arrives through a carrier (a
    // forward carries no origin identity: the target authenticates the carrier, node-rpc.md 36.1).
    // An apply changes the node: only a node-admin may ask.
    let sender_may = |s: &crate::model::Node| s.name != me.name && (matches!(req, StatusRequest::ProbeNodeState { .. }) || s.kind == NodeKind::NodeAdmin);
    if me.node_id != *node_id || !sender.is_some_and(sender_may) {
        return None;
    }
    let Some(held) = me.incarnation_id.as_ref() else {
        return Some(StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown });
    };
    if held != incarnation {
        return Some(StatusReply::RejectedStaleIncarnation { held: held.clone() });
    }
    let sender_name = sender.map(|n| n.name.to_string()).unwrap_or_default();
    match req {
        StatusRequest::ProbeNodeState { .. } => {
            if let Some(republish) = republish.get() {
                republish().await;
            }
            let state = node_state_of(me.status);
            tracing::info_span!("rdm.node_admin.status.update.via-probe", node = %me.name, sender = %sender_name, state = ?state, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("probed by a node-admin: presence re-published, current state answered"));
            Some(StatusReply::Current { node_id: me.node_id.clone(), incarnation: held.clone(), state })
        }
        StatusRequest::ApplyNodeState { state: NodeState::Draining, .. } => {
            let Some(drain) = drain.get() else {
                return Some(StatusReply::NotReady { reason: format!("{} has no drain door in this build", me.name) });
            };
            let in_flight = drain().await;
            tracing::info_span!("rdm.node_admin.status.update.via-apply-draining", node = %me.name, sender = %sender_name, in_flight, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("draining applied by a node-admin"));
            Some(StatusReply::NodeDrainingApplied { in_flight })
        }
        StatusRequest::ApplyNodeState { state, .. } => {
            Some(StatusReply::NotReady { reason: format!("{} serves apply-node-state only for Draining in this build; {state:?} is not served", me.name) })
        }
        _ => None,
    }
}

/// Serve `Status` on this admin. `authority` is filled once the admin holds a view; until then a
/// declaration is `NotReady` by name.
pub fn serve(b: ServerBuilder, authority: Arc<OnceLock<Arc<StatusAuthority>>>) -> ServerBuilder {
    b.serve::<Status, _, _>(OpOwner::Product("rdm".into()), move |peer: PeerContext, req: StatusRequest| {
        let authority = authority.clone();
        async move {
            let Some(auth) = authority.get().cloned() else {
                return Ok(StatusReply::NotReady { reason: "this admin holds no view yet".into() });
            };
            let peer_id = EndpointId(peer.endpoint_id.to_string());
            let sender = auth.topology.read().await.nodes.iter().find(|n| n.endpoint_id.as_ref() == Some(&peer_id)).cloned();
            let reply = auth.apply(sender, &req).await;
            if auth.hold_next_reply.swap(false, std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
            Ok(reply)
        }
    })
}

fn detail(r: &StatusReply) -> String {
    match r {
        StatusReply::RejectedNotAuthority { why } => why.as_str().to_string(),
        StatusReply::RejectedStaleIncarnation { held } => held.to_string(),
        StatusReply::RejectedStaleMesh { held } => held.to_string(),
        StatusReply::RejectedStaleFabric { held } => held.to_string(),
        StatusReply::RejectedInvalidNodeTransition { current } => format!("{current:?}"),
        StatusReply::RejectedInvalidMeshTransition { current } => format!("{current:?}"),
        _ => String::new(),
    }
}

/// Shared handles, for the admin's wiring.
pub type AuthoritySlot = Arc<OnceLock<Arc<StatusAuthority>>>;

#[allow(dead_code)]
fn _assert_send_sync() {
    fn f<T: Send + Sync>() {}
    f::<StatusAuthority>();
    let _ = RwLock::new(());
}

#[cfg(test)]
mod tests {
    use super::*;
    use rafka_node_rpc_contract::status::StatusReply as R;

    fn node(name: &str) -> crate::model::Node {
        let mut n = crate::model::Node::allocated(name.parse().unwrap());
        n.incarnation_id = Some(IncarnationId::mint());
        n
    }

    /// CONTRACT (#2803 detection): a fabric primary probes ANOTHER mesh's node-admin through a
    /// member of that mesh (Forward carries no origin identity, so the node-admin authenticates the
    /// member). The probe is answered with the node-admin's current state; the same sender cannot
    /// apply a state to it, and the node-admin itself is never its own sender.
    #[tokio::test]
    async fn a_probe_carried_by_a_member_is_answered_and_an_apply_from_it_is_not() {
        let admin = node("mesh2.admin.1");
        let carrier = node("mesh2.rpc.1");
        let (node_id, incarnation) = (admin.node_id.clone(), admin.incarnation_id.clone().unwrap());
        let (republish, drain) = (Republish::default(), Drain::default());
        let probe = StatusRequest::ProbeNodeState { node_id: node_id.clone(), incarnation: incarnation.clone() };
        let answered = self_subject(&admin, Some(&carrier), &probe, &republish, &drain).await;
        assert!(matches!(answered, Some(R::Current { .. })), "{answered:?}");
        let apply = StatusRequest::ApplyNodeState { node_id, incarnation, state: NodeState::Draining };
        assert!(self_subject(&admin, Some(&carrier), &apply, &republish, &drain).await.is_none(), "an apply from a member is not the subject's to answer");
        assert!(self_subject(&admin, Some(&admin), &probe, &republish, &drain).await.is_none(), "a node-admin is not its own sender");
        assert!(self_subject(&admin, None, &probe, &republish, &drain).await.is_none(), "an unresolved peer is answered by no one");
    }
}
