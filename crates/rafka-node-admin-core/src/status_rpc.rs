//! The lifecycle/status declarations node-admin applies as an authority (i143.e6.s7;
//! node-rpc.md §38; fabric-mesh-ops.md §3).
//!
//! A declaration reaches this admin over Node RPC (`Status`, tag `0x1B`). The sender is never
//! read from the request: it is the authenticated peer, resolved to a birth in this admin's own
//! view. The admin applies a declaration only when it holds the seat the declaration needs and
//! the sender is the subject (or the subject's primary), idempotently on the natural key, forward
//! along the legal order, and answers what happened. `Applied` means the declared state is held
//! in this admin's entity state and, for a node, written to `nodes.storage` as the subject's row.
//!
//! Gossip is untouched: the view's `status` stays what membership says; what the authority
//! applied is the view's `declared`.

use crate::model::{IncarnationId, NodeId, NodeKind, EndpointId};
use crate::topology::Topology;
use crate::storage::NodeRecord;
use rafka_node_rpc::{PeerContext, ServerBuilder};
use rafka_node_rpc_contract::catalog::TagOwner;
use rafka_node_rpc_contract::status::{transition, FabricEvent, MeshState, NodeState, NotAuthority, Status, StatusReply, StatusRequest, Transition};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock, RwLock};

/// What this admin applied, by natural key.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Declared {
    /// Per node id: the birth whose state was declared, and the state.
    pub nodes: BTreeMap<NodeId, (IncarnationId, NodeState)>,
    /// Per mesh id.
    pub meshes: BTreeMap<String, MeshState>,
    /// Per fabric id: the events applied, in order, each once.
    pub fabric: BTreeMap<String, Vec<String>>,
}

impl Declared {
    pub fn node(&self, node_id: &NodeId) -> Option<(IncarnationId, NodeState)> {
        self.nodes.get(node_id).cloned()
    }
}

/// What the service reads and writes: this admin's identity, its current view, what it applied,
/// and the rows it writes.
pub struct StatusAuthority {
    pub me: crate::model::PathName,
    pub fabric_id: String,
    pub topology: Arc<tokio::sync::RwLock<Topology>>,
    pub declared: Arc<Mutex<Declared>>,
    pub nodes_storage: Arc<dyn crate::storage::NodesStorage>,
    /// The mesh ids this admin holds, by mesh name.
    pub mesh_ids: Arc<dyn Fn() -> BTreeMap<String, String> + Send + Sync>,
    /// Testkit knob: hold the next reply past the caller's bound after applying (acceptance 2:
    /// apply + reply loss is `Indeterminate`, and the retry is `AlreadyApplied`).
    pub hold_next_reply: Arc<std::sync::atomic::AtomicBool>,
}

/// The decision, pure over a view: what the admin does with a declaration from `sender`.
pub fn decide(auth: &StatusAuthority, view: &Topology, declared: &mut Declared, sender: Option<&crate::model::Node>, req: &StatusRequest) -> (StatusReply, Option<NodeRecord>) {
    let me = match view.nodes.iter().find(|n| n.name == auth.me) {
        Some(n) => n,
        None => return (StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "a view holding this admin".into() } }, None),
    };
    let Some(sender) = sender else {
        return (StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: "unknown peer".into() } }, None);
    };
    match req {
        StatusRequest::DeclareNodeState { node_id, incarnation, state } => {
            if sender.node_id.as_str() != node_id {
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
                Some(held) if held.0 == *incarnation => {}
                Some(held) => return (StatusReply::RejectedStaleBirth { held: held.0.clone() }, None),
                None => return (StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown }, None),
            }
            let current = declared.node(&sender.node_id).and_then(|(inc, s)| (inc.0 == *incarnation).then_some(s));
            match transition(current, *state) {
                Transition::AlreadyApplied => (StatusReply::AlreadyApplied, None),
                Transition::Backward { current } => (StatusReply::RejectedInvalidTransition { current }, None),
                Transition::Apply => {
                    declared.nodes.insert(sender.node_id.clone(), (IncarnationId(incarnation.clone()), *state));
                    let row = NodeRecord {
                        node_id: sender.node_id.clone(),
                        name: sender.name.clone(),
                        incarnation_id: IncarnationId(incarnation.clone()),
                        endpoint_id: sender.endpoint_id.clone().unwrap_or_else(|| EndpointId(String::new())),
                        transport_addr: sender.transport_addr.unwrap_or_else(|| std::net::SocketAddr::from(([0, 0, 0, 0], 0))),
                        listeners: sender.listeners.clone(),
                        declared: Some(format!("{state:?}")),
                    };
                    (StatusReply::Applied, Some(row))
                }
            }
        }
        StatusRequest::DeclareMeshState { mesh_id, state } => {
            if !me.is_fabric_primary {
                return (StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "fabric-primary".into() } }, None);
            }
            let ids = (auth.mesh_ids)();
            let senders_mesh = ids.get(&sender.mesh);
            if !(sender.kind == NodeKind::NodeAdmin && sender.is_primary && senders_mesh.map(String::as_str) == Some(mesh_id.as_str())) {
                return (StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: sender.name.to_string() } }, None);
            }
            apply_mesh(declared, mesh_id, *state)
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
                    return (StatusReply::RejectedStaleBirth { held: held.clone() }, None);
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
                return (StatusReply::RejectedStaleBirth { held: auth.fabric_id.clone() }, None);
            }
            let events = declared.fabric.entry(fabric_id.clone()).or_default();
            let key = event_key(event);
            if events.contains(&key) {
                return (StatusReply::AlreadyApplied, None);
            }
            events.push(key);
            (StatusReply::Applied, None)
        }
    }
}

fn event_key(e: &FabricEvent) -> String {
    match e {
        FabricEvent::ReadyForTraffic => "ready-for-traffic".into(),
        FabricEvent::ShutdownInitiated { initiated_by } => format!("shutdown-initiated:{initiated_by}"),
    }
}

fn apply_mesh(declared: &mut Declared, mesh_id: &str, state: MeshState) -> (StatusReply, Option<NodeRecord>) {
    match transition(declared.meshes.get(mesh_id).copied(), state) {
        Transition::AlreadyApplied => (StatusReply::AlreadyApplied, None),
        Transition::Backward { current } => (StatusReply::RejectedInvalidTransition { current }, None),
        Transition::Apply => {
            declared.meshes.insert(mesh_id.to_string(), state);
            (StatusReply::Applied, None)
        }
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
        let receiver_is_primary = view.nodes.iter().find(|n| n.name == self.me).is_some_and(|n| n.is_primary);
        let (reply, row) = {
            let mut declared = self.declared.lock().unwrap();
            decide(self, &view, &mut declared, sender.as_ref(), req)
        };
        let reply = match (&reply, row) {
            (StatusReply::Applied, Some(row)) => match self.nodes_storage.put_contact(&row) {
                Ok(()) => {
                    let mut t = self.topology.write().await;
                    if let Some(n) = t.nodes.iter_mut().find(|n| n.node_id == row.node_id && n.incarnation_id.as_ref() == Some(&row.incarnation_id)) {
                        n.declared = row.declared.clone();
                    }
                    reply
                }
                Err(e) => StatusReply::NotReady { reason: format!("nodes.storage refused the row: {e}") },
            },
            _ => reply,
        };
        tracing::info_span!(
            "rafka.node_admin.status.update.via-declaration",
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
    /// Initial fabric bootstrap: the Day-0 root has no upstream authority to hand it its mesh's
    /// Pending, so it applies it to itself (e4.s11 "single-admin recovery root"). Only Pending, only
    /// for its own mesh; the seat check does not apply because no seat exists yet. The same span
    /// as every decision, with the sender named as itself.
    pub async fn self_apply_mesh_pending(&self, mesh_id: &str) -> StatusReply {
        let receiver_is_primary = self.topology.read().await.nodes.iter().find(|n| n.name == self.me).is_some_and(|n| n.is_primary);
        let (reply, _) = apply_mesh(&mut self.declared.lock().unwrap(), mesh_id, MeshState::Pending);
        tracing::info_span!(
            "rafka.node_admin.status.update.via-declaration",
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
pub fn serve(b: ServerBuilder, authority: Arc<OnceLock<Arc<StatusAuthority>>>) -> ServerBuilder {
    b.serve::<Status, _, _>(TagOwner::Product("rdm".into()), move |peer: PeerContext, req: StatusRequest| {
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
        StatusReply::RejectedStaleBirth { held } => held.clone(),
        StatusReply::RejectedInvalidTransition { current } => current.clone(),
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
