//! What node-admin declares upward over `Status` (i143.e4.s11), and how it keeps declaring until
//! the authority answers by name.
//!
//! A declaration is a fact about this admin or its Mesh with a natural key; the declarer holds
//! the ones not yet answered and re-sends each on its own cadence, resolving the authority from
//! the current view at every attempt (the fabric-primary moves; the key does not). The outcomes:
//!
//! ```text
//! Applied | AlreadyApplied          done
//! RejectedNotAuthority             the seat moved or the view is early: resolve again next round
//! RejectedStaleBirth               this birth is superseded in the authority's view: done, named
//! RejectedInvalidTransition        the authority holds a later state: done, named
//! NotSent | Indeterminate | other  retry the same declaration next round (never a negative state)
//! ```
//!
//! Nothing here gates gossip or readiness: the admin's own `ReadyForTraffic` is published by
//! membership as before; the declaration is certainty on top of it.

use crate::model::{NodeId, NodeKind, PathName};
use crate::topology::Topology;
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::status::{FabricEvent, MeshState, NodeState, Status, StatusReply, StatusRequest};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// One declaration this admin owes, by natural key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Key {
    /// This admin's own birth: `(incarnation, state)`.
    OwnState(String, NodeState),
    /// This admin's Mesh: `(mesh_id, state)`.
    Mesh(String, MeshState),
    /// A fabric event at one mesh primary: `(receiver mesh, event)`.
    FabricEventAt(String, String),
}

/// Who receives a declaration: resolved from the view at each attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authority {
    FabricPrimary,
    /// The current primary of this mesh.
    MeshPrimaryOf(String),
}

#[derive(Debug, Clone)]
pub struct Pending {
    pub key: Key,
    pub to: Authority,
    pub request: StatusRequest,
    pub attempts: u32,
    pub last: Option<String>,
}

/// The declarer's state: what is owed, and what was answered by name.
#[derive(Debug, Default)]
pub struct Declarer {
    pending: Mutex<BTreeMap<Key, Pending>>,
    done: Mutex<BTreeMap<Key, String>>,
}

impl Declarer {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Owe `request` to `to` under `key`; a key already owed or answered is left as it is.
    pub fn owe(&self, key: Key, to: Authority, request: StatusRequest) {
        if self.done.lock().unwrap().contains_key(&key) {
            return;
        }
        self.pending.lock().unwrap().entry(key.clone()).or_insert(Pending { key, to, request, attempts: 0, last: None });
    }

    pub fn answered(&self, key: &Key) -> Option<String> {
        self.done.lock().unwrap().get(key).cloned()
    }

    pub fn owed(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    /// One round: send every owed declaration to its authority as the view names it now.
    pub async fn round(&self, me: &PathName, me_id: &NodeId, view: &Topology, client: &NodeRpcClient) {
        let owed: Vec<Pending> = self.pending.lock().unwrap().values().cloned().collect();
        for p in owed {
            let target = match &p.to {
                Authority::FabricPrimary => view.fabric_primary(),
                Authority::MeshPrimaryOf(mesh) => view.cohort_primary(mesh, NodeKind::NodeAdmin),
            };
            let Some(target) = target else {
                self.note(&p.key, 0, "no authority in view");
                continue;
            };
            if &target.node_id == me_id {
                // The authority is this admin: apply locally through the same door is e4.s11's
                // next step; until then the view's `declared` is what the authority holds.
                self.note(&p.key, 0, "authority is self");
                continue;
            }
            let (out, _) = client.call::<Status>(&NodeTarget::ExactNode(target.node_id.clone()), &p.request, &CallOptions::default()).await;
            let (outcome, finished) = match &out {
                RpcOutcome::Reply(r) => match r.value() {
                    StatusReply::Applied | StatusReply::AlreadyApplied => (r.value().name().to_string(), true),
                    StatusReply::RejectedStaleBirth { .. } | StatusReply::RejectedInvalidTransition { .. } => (format!("{:?}", r.value()), true),
                    other => (format!("{other:?}"), false),
                },
                other => (format!("{}: {other:?}", other.name()), false),
            };
            tracing::info_span!(
                "rafka.node_admin.status.update.via-declare",
                node = %me,
                key = ?p.key,
                to = %target.name,
                attempt = p.attempts + 1,
                outcome = %outcome,
                finished,
            )
            .in_scope(|| tracing::info!("declared to the authority"));
            if finished {
                self.pending.lock().unwrap().remove(&p.key);
                self.done.lock().unwrap().insert(p.key.clone(), outcome);
            } else {
                self.note(&p.key, 1, &outcome);
            }
        }
    }

    fn note(&self, key: &Key, add: u32, last: &str) {
        if let Some(p) = self.pending.lock().unwrap().get_mut(key) {
            p.attempts += add;
            p.last = Some(last.to_string());
        }
    }
}

/// The declarations an admin owes from its view and its own state, derived each round so a
/// successor or a restarted admin owes the same ones from the same facts.
pub fn owed_from_view(d: &Declarer, me: &PathName, me_incarnation: &str, own_ready: bool, view: &Topology, mesh_ids: &BTreeMap<String, String>) {
    // This admin's own birth, once it is ready: to the fabric-primary.
    if own_ready {
        if let Some(my) = view.nodes.iter().find(|n| &n.name == me) {
            d.owe(
                Key::OwnState(me_incarnation.to_string(), NodeState::ReadyForTraffic),
                Authority::FabricPrimary,
                StatusRequest::DeclareNodeState { node_id: my.node_id.to_string(), incarnation: me_incarnation.to_string(), state: NodeState::ReadyForTraffic },
            );
        }
    }
    let i_am_mesh_primary = view.cohort_primary(&me.mesh, NodeKind::NodeAdmin).is_some_and(|n| &n.name == me);
    let i_am_fabric_primary = view.fabric_primary().is_some_and(|n| &n.name == me);
    // My Mesh's status, as its primary, to the fabric-primary.
    if i_am_mesh_primary {
        if let (Some(mesh), Some(id)) = (view.meshes.iter().find(|m| m.name == me.mesh), mesh_ids.get(&me.mesh)) {
            if mesh.status == crate::model::ScopeStatus::ReadyForTraffic {
                d.owe(Key::Mesh(id.clone(), MeshState::ReadyForTraffic), Authority::FabricPrimary, StatusRequest::DeclareMeshState { mesh_id: id.clone(), state: MeshState::ReadyForTraffic });
            }
        }
    }
    // The fabric's readiness, as the fabric-primary, to every other mesh primary.
    if i_am_fabric_primary && view.fabric.status == crate::model::ScopeStatus::ReadyForTraffic {
        for m in &view.meshes {
            if m.name == me.mesh {
                continue;
            }
            d.owe(
                Key::FabricEventAt(m.name.clone(), "ready-for-traffic".into()),
                Authority::MeshPrimaryOf(m.name.clone()),
                StatusRequest::ApplyFabricEvent { fabric_id: view.fabric.id.to_string(), event: FabricEvent::ReadyForTraffic },
            );
        }
    }
}
