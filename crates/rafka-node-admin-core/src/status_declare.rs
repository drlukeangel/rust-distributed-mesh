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
//!
//! A key is owed again when the authority is another node or a new incarnation of the same node.
//! A Mesh declaration is owed by the Mesh's primary and a fabric event by the fabric-primary.
//! When this admin no longer holds the seat that owes a key, the key is withdrawn: the seat's
//! new holder owes it from its own view.
//! RejectedStaleBirth               this birth is superseded in the authority's view: done, named
//! RejectedInvalidTransition        the authority holds a later state: done, named
//! NotSent | Indeterminate | other  retry the same declaration next round (never a negative state)
//! ```
//!
//! Nothing here gates gossip or readiness: the admin's own `ReadyForTraffic` is published by
//! membership as before; the declaration is certainty on top of it.

use crate::model::{IncarnationId, NodeId, NodeKind, PathName};
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

/// The declarer's state: what is owed, and what was answered by name, and by which authority. A
/// key answered by one authority is owed again when the view names another: the seat moved, and
/// the new authority answers `AlreadyApplied` or applies it, never assumes it.
#[derive(Debug, Default)]
pub struct Declarer {
    pending: Mutex<BTreeMap<Key, Pending>>,
    done: Mutex<BTreeMap<Key, (String, NodeId, Option<IncarnationId>)>>,
    /// Every declaration ever owed, by key: what is re-owed when the authority moves.
    requests: Mutex<BTreeMap<Key, (Authority, StatusRequest)>>,
}

impl Declarer {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Owe `request` to `to` under `key`; a key already owed or answered is left as it is.
    pub fn owe(&self, key: Key, to: Authority, request: StatusRequest) {
        self.requests.lock().unwrap().entry(key.clone()).or_insert((to.clone(), request.clone()));
        if self.done.lock().unwrap().contains_key(&key) {
            return;
        }
        self.pending.lock().unwrap().entry(key.clone()).or_insert(Pending { key, to, request, attempts: 0, last: None });
    }

    pub fn answered(&self, key: &Key) -> Option<String> {
        self.done.lock().unwrap().get(key).map(|(o, _, _)| o.clone())
    }

    pub fn owed(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    /// A key answered by an authority the view no longer names is owed to the current one: another
    /// node holds the seat, or the same node holds it in a new birth. What an authority applied for
    /// a Mesh report is memory of its birth, so a restart (same NodeId, new incarnation) must be
    /// told again; an authority whose birth the view does not yet name changes nothing.
    pub fn reowe_moved(&self, me: &PathName, view: &Topology) {
        let mut done = self.done.lock().unwrap();
        let mut pending = self.pending.lock().unwrap();
        let moved: Vec<Key> = done
            .iter()
            .filter(|(key, (_, by, by_birth))| {
                let now = match key {
                    Key::OwnState(..) | Key::Mesh(..) => view.fabric_primary(),
                    Key::FabricEventAt(mesh, _) => view.cohort_primary(mesh, NodeKind::NodeAdmin),
                };
                now.is_some_and(|n| &n.node_id != by || matches!((&n.incarnation_id, by_birth), (Some(now), Some(then)) if now != then))
            })
            .map(|(k, _)| k.clone())
            .collect();
        for key in moved {
            if let Some((_, by, _)) = done.remove(&key) {
                let (to, request) = match self.requests.lock().unwrap().get(&key) {
                    Some(r) => r.clone(),
                    None => continue,
                };
                tracing::info!(node = %me, key = ?key, was = %by, "the authority moved: the declaration is owed again to the current one");
                pending.insert(key.clone(), Pending { key, to, request, attempts: 0, last: None });
            }
        }
    }

    /// One round: send every owed declaration to its authority as the view names it now. When the
    /// view names this admin, the declaration goes through this admin's own door (`authority`),
    /// decided exactly as a peer's would be; no seat is assumed.
    pub async fn round(&self, me: &PathName, me_id: &NodeId, view: &Topology, client: &NodeRpcClient, authority: Option<&crate::status_rpc::StatusAuthority>) {
        self.withdraw_unowned(me, view);
        self.reowe_moved(me, view);
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
            let classify = |reply: &StatusReply| match reply {
                StatusReply::Applied | StatusReply::AlreadyApplied => (reply.name().to_string(), true),
                StatusReply::RejectedStaleIncarnation { .. }
                | StatusReply::RejectedStaleMesh { .. }
                | StatusReply::RejectedStaleFabric { .. }
                | StatusReply::RejectedInvalidNodeTransition { .. }
                | StatusReply::RejectedInvalidMeshTransition { .. } => (format!("{reply:?}"), true),
                other => (format!("{other:?}"), false),
            };
            let (outcome, finished, via) = if &target.node_id == me_id {
                match authority {
                    Some(auth) => {
                        let reply = auth.apply(Some(target.clone()), &p.request).await;
                        let (o, f) = classify(&reply);
                        (o, f, "self")
                    }
                    None => {
                        self.note(&p.key, 0, "authority is self, but this admin holds no view yet");
                        continue;
                    }
                }
            } else {
                let (out, _) = client.call::<Status>(&NodeTarget::ExactNode(target.node_id.clone()), &p.request, &CallOptions::default()).await;
                let (o, f) = match &out {
                    RpcOutcome::Reply(r) => classify(r.value()),
                    other => (format!("{}: {other:?}", other.name()), false),
                };
                (o, f, "node-rpc")
            };
            tracing::info_span!(
                "rdm.node_admin.status.update.via-declare",
                node = %me,
                key = ?p.key,
                to = %target.name,
                via,
                attempt = p.attempts + 1,
                outcome = %outcome,
                finished,
            )
            .in_scope(|| tracing::info!("declared to the authority"));
            if finished {
                self.pending.lock().unwrap().remove(&p.key);
                self.done.lock().unwrap().insert(p.key.clone(), (outcome, target.node_id.clone(), target.incarnation_id.clone()));
            } else {
                self.note(&p.key, 1, &outcome);
            }
        }
    }

    /// Withdraw every key whose owing seat the view no longer gives this admin: a Mesh key once
    /// it is not its Mesh's admin primary, a fabric event once it is not the fabric-primary. A
    /// key withdrawn is forgotten whole, so holding the seat again owes it afresh. A view that
    /// names no holder of the seat withdraws nothing: the seat is unknown, not moved.
    pub fn withdraw_unowned(&self, me: &PathName, view: &Topology) {
        let holder = |key: &Key| match key {
            Key::OwnState(..) => None,
            Key::Mesh(..) => Some(view.cohort_primary(&me.mesh, NodeKind::NodeAdmin)),
            Key::FabricEventAt(..) => Some(view.fabric_primary()),
        };
        let lost: Vec<(Key, String)> = self
            .requests
            .lock()
            .unwrap()
            .keys()
            .filter_map(|k| match holder(k) {
                Some(Some(n)) if &n.name != me => Some((k.clone(), n.name.to_string())),
                _ => None,
            })
            .collect();
        for (key, held_by) in lost {
            self.requests.lock().unwrap().remove(&key);
            self.done.lock().unwrap().remove(&key);
            self.pending.lock().unwrap().remove(&key);
            tracing::info_span!("rdm.node_admin.status.remove.via-seat-moved", node = %me, key = ?key, held_by = %held_by)
                .in_scope(|| tracing::info!("the seat that owes this declaration is another admin's: withdrawn"));
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
pub fn owed_from_view(d: &Declarer, me: &PathName, me_incarnation: &str, own_ready: bool, mesh_round_done: bool, view: &Topology, mesh_ids: &BTreeMap<String, String>) {
    // This admin's own birth, once it is ready: to the fabric-primary.
    if own_ready {
        if let Some(my) = view.nodes.iter().find(|n| &n.name == me) {
            d.owe(
                Key::OwnState(me_incarnation.to_string(), NodeState::ReadyForTraffic),
                Authority::FabricPrimary,
                StatusRequest::DeclareNodeState { node_id: my.node_id.clone(), incarnation: crate::model::IncarnationId(me_incarnation.to_string()), state: NodeState::ReadyForTraffic },
            );
        }
    }
    let i_am_mesh_primary = view.cohort_primary(&me.mesh, NodeKind::NodeAdmin).is_some_and(|n| &n.name == me);
    let i_am_fabric_primary = view.fabric_primary().is_some_and(|n| &n.name == me);
    // My Mesh's status, as its primary, to the fabric-primary: the report that its round is complete,
    // owed only once every planned birth of the accepted Build checked in (`crate::round`).
    if i_am_mesh_primary && mesh_round_done {
        if let (Some(mesh), Some(id)) = (view.meshes.iter().find(|m| m.name == me.mesh), mesh_ids.get(&me.mesh)) {
            if mesh.status == crate::model::ScopeStatus::ReadyForTraffic {
                d.owe(Key::Mesh(id.clone(), MeshState::ReadyForTraffic), Authority::FabricPrimary, StatusRequest::DeclareMeshState { mesh_id: match crate::model::MeshId::parse(id) { Ok(m) => m, Err(_) => return }, state: MeshState::ReadyForTraffic });
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
                StatusRequest::ApplyFabricEvent { fabric_id: view.fabric.id.clone(), event: FabricEvent::ReadyForTraffic },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;

    fn node(name: &str, primary: bool, fabric_primary: bool) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.status = NodeStatus::ReadyForTraffic;
        n.is_primary = primary;
        n.is_fabric_primary = fabric_primary;
        n
    }

    fn view(mesh_primary: &str, fabric_primary: &str) -> Topology {
        let names = ["mesh1.admin.1", "mesh1.admin.2", "mesh2.admin.1"];
        Topology {
            fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
            meshes: vec![
                Mesh { id: Some(MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::ReadyForTraffic },
                Mesh { id: Some(MeshId::mint()), name: "mesh2".into(), status: ScopeStatus::ReadyForTraffic },
            ],
            nodes: names.iter().map(|n| node(n, *n == mesh_primary || *n == "mesh2.admin.1", *n == fabric_primary)).collect(),
        }
    }

    fn owe_all(d: &Declarer) {
        d.owe(Key::OwnState("inc".into(), NodeState::ReadyForTraffic), Authority::FabricPrimary, StatusRequest::DeclareNodeState { node_id: crate::model::NodeId::mint(), incarnation: crate::model::IncarnationId("inc".into()), state: NodeState::ReadyForTraffic });
        d.owe(Key::Mesh("m1".into(), MeshState::ReadyForTraffic), Authority::FabricPrimary, StatusRequest::DeclareMeshState { mesh_id: crate::model::MeshId::mint(), state: MeshState::ReadyForTraffic });
        d.owe(Key::FabricEventAt("mesh2".into(), "ready-for-traffic".into()), Authority::MeshPrimaryOf("mesh2".into()), StatusRequest::ApplyFabricEvent { fabric_id: crate::model::FabricId::mint(), event: FabricEvent::ReadyForTraffic });
    }

    #[test]
    fn a_former_mesh_primary_withdraws_its_mesh_declaration() {
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let d = Declarer::new();
        owe_all(&d);
        d.withdraw_unowned(&me, &view("mesh1.admin.1", "mesh1.admin.1"));
        assert_eq!(d.owed(), 3, "every seat is still this admin's");

        d.withdraw_unowned(&me, &view("mesh1.admin.2", "mesh1.admin.1"));
        let left: Vec<Key> = d.pending.lock().unwrap().keys().cloned().collect();
        assert_eq!(left, vec![Key::OwnState("inc".into(), NodeState::ReadyForTraffic), Key::FabricEventAt("mesh2".into(), "ready-for-traffic".into())], "the Mesh key is the new mesh primary's");

        d.withdraw_unowned(&me, &view("mesh1.admin.2", "mesh2.admin.1"));
        let left: Vec<Key> = d.pending.lock().unwrap().keys().cloned().collect();
        assert_eq!(left, vec![Key::OwnState("inc".into(), NodeState::ReadyForTraffic)], "the fabric event is the new fabric-primary's; its own birth stays owed");
    }

    #[test]
    fn a_withdrawn_key_is_owed_afresh_when_the_seat_returns() {
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let d = Declarer::new();
        owe_all(&d);
        d.done.lock().unwrap().insert(Key::Mesh("m1".into(), MeshState::ReadyForTraffic), ("applied".into(), NodeId::mint(), None));
        d.pending.lock().unwrap().remove(&Key::Mesh("m1".into(), MeshState::ReadyForTraffic));
        d.withdraw_unowned(&me, &view("mesh1.admin.2", "mesh1.admin.1"));
        assert_eq!(d.answered(&Key::Mesh("m1".into(), MeshState::ReadyForTraffic)), None, "forgotten whole");
        owe_all(&d);
        assert!(d.pending.lock().unwrap().contains_key(&Key::Mesh("m1".into(), MeshState::ReadyForTraffic)), "owed again once the seat is back");
    }

    #[test]
    fn a_view_naming_no_holder_withdraws_nothing() {
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let d = Declarer::new();
        owe_all(&d);
        let mut v = view("mesh1.admin.1", "mesh1.admin.1");
        for n in &mut v.nodes {
            n.is_primary = false;
            n.is_fabric_primary = false;
        }
        d.withdraw_unowned(&me, &v);
        assert_eq!(d.owed(), 3);
    }

    fn answered_by(d: &Declarer, key: &Key, who: &Node) {
        d.pending.lock().unwrap().remove(key);
        d.done.lock().unwrap().insert(key.clone(), ("applied".into(), who.node_id.clone(), who.incarnation_id.clone()));
    }

    #[test]
    fn a_restarted_authority_is_owed_every_declaration_again() {
        let me: PathName = "mesh2.admin.1".parse().unwrap();
        let d = Declarer::new();
        owe_all(&d);
        let mut v = view("mesh2.admin.1", "mesh1.admin.1");
        for n in &mut v.nodes {
            n.incarnation_id = Some(IncarnationId::mint());
        }
        let fp = v.nodes.iter().find(|n| n.is_fabric_primary).unwrap().clone();
        let own = Key::OwnState("inc".into(), NodeState::ReadyForTraffic);
        let mesh = Key::Mesh("m1".into(), MeshState::ReadyForTraffic);
        answered_by(&d, &own, &fp);
        answered_by(&d, &mesh, &fp);

        d.reowe_moved(&me, &v);
        assert!(!d.pending.lock().unwrap().contains_key(&mesh), "the same birth of the same authority answered it: nothing is re-owed");

        for n in &mut v.nodes {
            if n.is_fabric_primary {
                n.incarnation_id = Some(IncarnationId::mint());
            }
        }
        d.reowe_moved(&me, &v);
        let owed: Vec<Key> = d.pending.lock().unwrap().keys().cloned().collect();
        assert!(owed.contains(&own) && owed.contains(&mesh), "a new birth of the same authority rebuilt its applied state: {owed:?}");
        assert_eq!(d.answered(&mesh), None);
    }

    #[test]
    fn a_restart_of_another_admin_re_owes_nothing() {
        let me: PathName = "mesh2.admin.1".parse().unwrap();
        let d = Declarer::new();
        owe_all(&d);
        let mut v = view("mesh2.admin.1", "mesh1.admin.1");
        for n in &mut v.nodes {
            n.incarnation_id = Some(IncarnationId::mint());
        }
        let fp = v.nodes.iter().find(|n| n.is_fabric_primary).unwrap().clone();
        let mesh = Key::Mesh("m1".into(), MeshState::ReadyForTraffic);
        answered_by(&d, &mesh, &fp);
        for n in &mut v.nodes {
            if !n.is_fabric_primary {
                n.incarnation_id = Some(IncarnationId::mint());
            }
        }
        d.reowe_moved(&me, &v);
        assert!(!d.pending.lock().unwrap().contains_key(&mesh));
        assert!(d.answered(&mesh).is_some());
    }

    #[test]
    fn a_seat_moved_to_another_node_is_owed_again() {
        let me: PathName = "mesh2.admin.1".parse().unwrap();
        let d = Declarer::new();
        owe_all(&d);
        let v = view("mesh2.admin.1", "mesh1.admin.1");
        let old = v.nodes.iter().find(|n| n.is_fabric_primary).unwrap().clone();
        let mesh = Key::Mesh("m1".into(), MeshState::ReadyForTraffic);
        answered_by(&d, &mesh, &old);
        let mut moved = v.clone();
        for n in &mut moved.nodes {
            n.is_fabric_primary = n.name.to_string() == "mesh1.admin.2";
        }
        d.reowe_moved(&me, &moved);
        assert!(d.pending.lock().unwrap().contains_key(&mesh));
    }
}
