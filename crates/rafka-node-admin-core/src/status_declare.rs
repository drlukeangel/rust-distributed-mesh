//! What node-admin declares upward over `Status` (i143.e4.s11), and how it keeps declaring until
//! the authority answers by name.
//!
//! A declaration is a fact about this admin or its Mesh with a natural key; the declarer holds
//! the ones not yet answered and resolves the authority from the current view at every round (the
//! fabric-primary moves; the key does not). A declaration is sent when it becomes owed, and again
//! only on an eligible event (R-S2): the authoritative destination changes to a deliverable birth,
//! a failed delivery gains a viable route, or the authority addresses this admin with its down op
//! (`DeclareWake::addressed_by`). A typed reply is terminal, `RejectedNotAuthority` included, and a
//! target going unreachable sends nothing. One `DeclareWake` wakes the declarer; no timer does.
//! The outcomes:
//!
//! ```text
//! Applied | AlreadyApplied          done
//! RejectedNotAuthority             terminal until an eligible event: the authority's down op, or
//!                                  a destination change, sends it again
//!
//! A key is owed again when the authority is another node or a new incarnation of the same node.
//! A Mesh declaration is owed by the Mesh's primary and a fabric event by the fabric-primary.
//! When this admin no longer holds the seat that owes a key, the key is withdrawn: the seat's
//! new holder owes it from its own view. A round completion is withdrawn too once the accepted
//! Build has moved past its round (`FabricRounds::withdraw_superseded`, before each round of
//! sends): the completion of a superseded round is never sent.
//! RejectedStaleBirth               this birth is superseded in the authority's view: done, named
//! RejectedInvalidTransition        the authority holds a later state: done, named
//! NotSent | Indeterminate          failed delivery: sent again when the route is viable again
//!                                  (never a negative state)
//! ```
//!
//! Nothing here gates gossip or readiness: the admin's own `ReadyForTraffic` is published by
//! membership as before; the declaration is certainty on top of it.

use crate::model::{IncarnationId, NodeId, NodeKind, PathName};
use crate::topology::Topology;
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::status::{FabricEvent, MeshState, NodeState, Status, StatusReply, StatusRequest};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use tracing::Instrument;

/// What wakes the declarer: ONE notify, poked by its inputs (the topology, this admin's own
/// digest status, the declare gate, the meshes it holds and a down op's receipt), and the
/// authorities that addressed this admin since the last round. `Notify` stores one permit, so a
/// poke during a round is not lost and a burst of pokes runs one round.
#[derive(Debug, Default)]
pub struct DeclareWake {
    notify: tokio::sync::Notify,
    addressed: Mutex<BTreeSet<NodeId>>,
    /// The same down ops, counted for a `hydrate_before_ready` hook blocked on its authority's word.
    pub(crate) hydration: Arc<crate::app_hydration::Addressed>,
}

impl DeclareWake {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn poke(&self) {
        self.notify.notify_one();
    }

    /// A down op reached this admin on a call from the endpoint `peer` (as the transport proved
    /// it), from the authority `from` where a record names it: a hook blocked on its authority's
    /// word counts the endpoint, and every declaration owed to `from` is sent again, whatever its
    /// last outcome.
    pub(crate) fn addressed_by(&self, from: Option<&NodeId>, peer: Option<iroh::PublicKey>) {
        if let Some(peer) = peer {
            self.hydration.note(&peer);
        }
        if let Some(from) = from {
            self.addressed.lock().unwrap().insert(from.clone());
        }
        self.poke();
    }

    /// Wait for a poke.
    pub(crate) async fn woken(&self) {
        self.notify.notified().await;
    }

    /// The authorities that addressed this admin since the last call; a down op arriving after
    /// the take is kept for the next round.
    pub(crate) fn take_addressed(&self) -> BTreeSet<NodeId> {
        std::mem::take(&mut *self.addressed.lock().unwrap())
    }
}

/// Where a declaration went: the exact birth, and the address facts the view held for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Destination {
    pub node_id: NodeId,
    pub incarnation: Option<IncarnationId>,
    pub endpoint: Option<crate::model::EndpointId>,
    pub addr: Option<std::net::SocketAddr>,
}

impl Destination {
    pub(crate) fn of(n: &crate::model::Node) -> Self {
        Self { node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone(), endpoint: n.endpoint_id.clone(), addr: n.transport_addr }
    }
}

/// The last send of one declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Sent {
    pub to: Destination,
    /// A typed reply came back: terminal until an eligible event.
    pub typed: bool,
    /// The view showed the target unreachable after an untyped failure: a viable route is a gain.
    pub route_lost: bool,
}

/// Is a declaration due (R-S2)? Sent when it becomes owed, and again only when the authority
/// addressed this admin, or the destination changed to a deliverable birth, or a failed delivery
/// gained a viable route. A target that is not deliverable is sent nothing.
pub fn eligible(sent: Option<&Sent>, now: &Destination, deliverable: bool, addressed: bool) -> bool {
    if addressed {
        return true;
    }
    if !deliverable {
        return false;
    }
    match sent {
        None => true,
        Some(s) => &s.to != now || (!s.typed && s.route_lost),
    }
}

/// One declaration this admin owes, by natural key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Key {
    /// This admin's own birth: `(incarnation, state)`.
    OwnState(String, NodeState),
    /// This admin's Mesh: `(mesh_id, state)`.
    Mesh(String, MeshState),
    /// A fabric event at one mesh primary: `(receiver mesh, event)`.
    FabricEventAt(String, String),
    /// A mesh primary's completion of a fabric round (`state-committed`, `traffic-opened`) to the
    /// fabric-primary: `(completion/fabric/build/attempt)`.
    Round(String),
}

impl Key {
    /// The completion of `round`: one key per round, so a round's completion is owed once.
    pub(crate) fn round(round: &crate::fabric_state::RoundKey) -> Self {
        Self::Round(format!("{}/{}/{}/{}", round.kind.completion(), round.fabric_id, round.build_id, round.attempt))
    }
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
    /// The last send; `None` until the first.
    pub sent: Option<Sent>,
    /// The `traceparent` the declaration continues (a round's completion keeps the round's trace). A
    /// string, never a span: a stored span handle would hold its parents open and unexported.
    pub trace: Option<String>,
}

/// The declarer's state: what is owed, and what was answered by name, and by which authority. A
/// key answered by one authority is owed again when the view names another: the seat moved, and
/// the new authority answers `AlreadyApplied` or applies it, never assumes it.
#[derive(Debug, Default)]
pub(crate) struct Declarer {
    pending: Mutex<BTreeMap<Key, Pending>>,
    done: Mutex<BTreeMap<Key, (String, NodeId, Option<IncarnationId>)>>,
    /// Every declaration ever owed, by key: what is re-owed when the authority moves.
    requests: Mutex<BTreeMap<Key, (Authority, StatusRequest, Option<String>)>>,
}

impl Declarer {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Owe `request` to `to` under `key`; a key already owed or answered is left as it is.
    pub(crate) fn owe(&self, key: Key, to: Authority, request: StatusRequest) {
        self.owe_in(key, to, request, None)
    }

    /// [`Declarer::owe`], sent under `trace`: the declaration continues that `traceparent`.
    pub(crate) fn owe_in(&self, key: Key, to: Authority, request: StatusRequest, trace: Option<String>) {
        self.requests.lock().unwrap().entry(key.clone()).or_insert((to.clone(), request.clone(), trace.clone()));
        if self.done.lock().unwrap().contains_key(&key) {
            return;
        }
        self.pending.lock().unwrap().entry(key.clone()).or_insert(Pending { key, to, request, attempts: 0, last: None, sent: None, trace });
    }

    /// A start of this admin's birth `incarnation` begins its next cycle: the own-state declarations
    /// its last cycle owed or had answered are forgotten whole, so the node.started this start reports
    /// is owed afresh to its mesh's primary (states.md: node ReadyForTraffic is set from "the node's
    /// node.started report, after every start gate"), never stood for by the last cycle's answer.
    pub(crate) fn begin_own_cycle(&self, me: &PathName, incarnation: &str) {
        let own = |k: &Key| matches!(k, Key::OwnState(inc, _) if inc == incarnation);
        let forgotten: Vec<Key> = self.requests.lock().unwrap().keys().filter(|k| own(k)).cloned().collect();
        self.requests.lock().unwrap().retain(|k, _| !own(k));
        self.done.lock().unwrap().retain(|k, _| !own(k));
        self.pending.lock().unwrap().retain(|k, _| !own(k));
        tracing::info_span!("rdm.node_admin.status.remove.via-cycle-begun", node = %me, incarnation_id = incarnation, forgotten = ?forgotten)
            .in_scope(|| tracing::info!("a start begins this birth's next cycle: its own state is owed afresh at node.started"));
    }

    #[cfg(test)]
    pub fn answered(&self, key: &Key) -> Option<String> {
        self.done.lock().unwrap().get(key).map(|(o, _, _)| o.clone())
    }

    #[cfg(test)]
    pub fn owed(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    #[cfg(test)]
    pub(crate) fn owed_keys(&self) -> Vec<Key> {
        self.pending.lock().unwrap().keys().cloned().collect()
    }

    /// A key answered by an authority the view no longer names is owed to the current one: another
    /// node holds the seat, or the same node holds it in a new birth. What an authority applied for
    /// a Mesh report is memory of its birth, so a restart (same NodeId, new incarnation) must be
    /// told again; an authority whose birth the view does not yet name changes nothing.
    pub(crate) fn reowe_moved(&self, me: &PathName, view: &Topology) {
        let mut done = self.done.lock().unwrap();
        let mut pending = self.pending.lock().unwrap();
        let moved: Vec<Key> = done
            .iter()
            .filter(|(key, (_, by, by_birth))| {
                let now = match key {
                    Key::OwnState(..) => view.cohort_primary(&me.mesh, NodeKind::NodeAdmin),
                    Key::Mesh(..) | Key::Round(..) => view.fabric_primary(),
                    Key::FabricEventAt(mesh, _) => view.cohort_primary(mesh, NodeKind::NodeAdmin),
                };
                now.is_some_and(|n| &n.node_id != by || matches!((&n.incarnation_id, by_birth), (Some(now), Some(then)) if now != then))
            })
            .map(|(k, _)| k.clone())
            .collect();
        for key in moved {
            if let Some((_, by, _)) = done.remove(&key) {
                let (to, request, trace) = match self.requests.lock().unwrap().get(&key) {
                    Some(r) => r.clone(),
                    None => continue,
                };
                tracing::info!(node = %me, key = ?key, was = %by, "the authority moved: the declaration is owed again to the current one");
                pending.insert(key.clone(), Pending { key, to, request, attempts: 0, last: None, sent: None, trace });
            }
        }
    }

    /// The declarations due now, each with the authority the view names for it. Pure over the
    /// declarer's state and the view: a send is due only on an eligible event ([`eligible`]).
    /// The reachability a failed delivery waits on is observed here, every round, for every key.
    pub fn due(&self, me_id: &NodeId, view: &Topology, addressed: &BTreeSet<NodeId>) -> Vec<(Pending, crate::model::Node)> {
        let mut pending = self.pending.lock().unwrap();
        let mut due = Vec::new();
        for p in pending.values_mut() {
            let target = match &p.to {
                Authority::FabricPrimary => view.fabric_primary(),
                Authority::MeshPrimaryOf(mesh) => view.cohort_primary(mesh, NodeKind::NodeAdmin),
            };
            let Some(target) = target else {
                p.last = Some("no authority in view".into());
                // The seat names no deliverable holder: a failed delivery has lost its route.
                if let Some(s) = p.sent.as_mut().filter(|s| !s.typed) {
                    s.route_lost = true;
                }
                continue;
            };
            let now = Destination::of(target);
            let deliverable = &target.node_id == me_id || (target.status.is_live() && target.incarnation_id.is_some());
            if let Some(s) = p.sent.as_mut() {
                if !s.typed && !deliverable {
                    s.route_lost = true;
                }
            }
            if eligible(p.sent.as_ref(), &now, deliverable, addressed.contains(&target.node_id)) {
                due.push((p.clone(), target.clone()));
            }
        }
        due
    }

    /// One round: send every declaration that is due to its authority as the view names it now.
    /// When the view names this admin, the declaration goes through this admin's own door
    /// (`authority`), decided exactly as a peer's would be; no seat is assumed. `addressed` is
    /// the authorities that sent this admin their down op since the last round.
    pub async fn round(&self, me: &PathName, me_id: &NodeId, view: &Topology, client: &NodeRpcClient, authority: Option<&crate::status_rpc::StatusAuthority>, addressed: &BTreeSet<NodeId>) {
        self.withdraw_unowned(me, me_id, view);
        self.reowe_moved(me, view);
        for (p, target) in self.due(me_id, view, addressed) {
            let classify = |reply: &StatusReply| match reply {
                StatusReply::Applied | StatusReply::AlreadyApplied => (reply.name().to_string(), true),
                StatusReply::RejectedStaleIncarnation { .. }
                | StatusReply::RejectedStaleMesh { .. }
                | StatusReply::RejectedStaleFabric { .. }
                | StatusReply::RejectedInvalidNodeTransition { .. }
                | StatusReply::RejectedInvalidMeshTransition { .. } => (format!("{reply:?}"), true),
                other => (format!("{other:?}"), false),
            };
            let (outcome, finished, typed, via) = if &target.node_id == me_id {
                match authority {
                    Some(auth) => {
                        let reply = auth.apply(Some(target.clone()), &p.request).await;
                        let (o, f) = classify(&reply);
                        (o, f, true, "self")
                    }
                    None => {
                        // Unreachable: the declarer starts after the authority is filled.
                        tracing::error!(node = %me, key = ?p.key, "authority is self, but the Status authority is not filled: unreachable, the declarer starts after it is filled");
                        debug_assert!(false, "authority is self, but the Status authority is not filled");
                        self.note(&p.key, 0, "authority is self, but this admin holds no view yet");
                        continue;
                    }
                }
            } else {
                let (out, _) = client.call::<Status>(&NodeTarget::ExactNode(target.node_id.clone()), &p.request, &CallOptions::default()).instrument(completion_span(&p, &target)).await;
                match &out {
                    RpcOutcome::Reply(r) => {
                        let (o, f) = classify(r.value());
                        (o, f, true, "node-rpc")
                    }
                    other => (format!("{}: {other:?}", other.name()), false, false, "node-rpc"),
                }
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
                addressed = addressed.contains(&target.node_id),
            )
            .in_scope(|| tracing::info!("declared to the authority"));
            if finished {
                self.pending.lock().unwrap().remove(&p.key);
                self.done.lock().unwrap().insert(p.key.clone(), (outcome, target.node_id.clone(), target.incarnation_id.clone()));
            } else {
                self.note(&p.key, 1, &outcome);
                self.record_sent(&p.key, &target, typed);
            }
        }
    }

    /// Withdraw every key whose owing seat the view no longer gives this admin: a Mesh key once
    /// it is not its Mesh's admin primary, a fabric event once it is not the fabric-primary. A
    /// key withdrawn is forgotten whole, so holding the seat again owes it afresh. A view that
    /// names no holder of the seat withdraws nothing: the seat is unknown, not moved.
    pub(crate) fn withdraw_unowned(&self, me: &PathName, me_id: &NodeId, view: &Topology) {
        let holder = |key: &Key| match key {
            Key::OwnState(..) => None,
            Key::Mesh(..) | Key::Round(..) => Some(view.cohort_primary(&me.mesh, NodeKind::NodeAdmin)),
            Key::FabricEventAt(..) => Some(view.fabric_primary()),
        };
        let lost: Vec<(Key, String)> = self
            .requests
            .lock()
            .unwrap()
            .keys()
            .filter_map(|k| match holder(k) {
                Some(Some(n)) if &n.node_id != me_id => Some((k.clone(), n.name.to_string())),
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

    /// Withdraw the round completion `key`, owed or answered, because the accepted Build moved past the
    /// round it completes (`superseded_by` says how). A completion withdrawn is forgotten whole, so a
    /// successor fabric-primary is not owed it either.
    pub(crate) fn withdraw_round(&self, me: &PathName, key: &Key, superseded_by: &str) {
        self.requests.lock().unwrap().remove(key);
        self.done.lock().unwrap().remove(key);
        self.pending.lock().unwrap().remove(key);
        tracing::info_span!("rdm.node_admin.status.remove.via-round-superseded", node = %me, key = ?key, superseded_by = %superseded_by)
            .in_scope(|| tracing::info!("the accepted Build moved past the round this completion belongs to: withdrawn"));
    }

    /// Every round completion this admin holds, owed or answered, with the round it completes.
    pub(crate) fn round_completions(&self) -> Vec<(Key, crate::fabric_state::RoundKey)> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| matches!(k, Key::Round(_)))
            .filter_map(|(k, (_, request, _))| crate::fabric_state::RoundOp::of(request).map(|op| (k.clone(), op.key())))
            .collect()
    }

    /// The send of `key` to `target` ended without finishing it; `typed` says a typed reply came back.
    pub(crate) fn record_sent(&self, key: &Key, target: &crate::model::Node, typed: bool) {
        if let Some(held) = self.pending.lock().unwrap().get_mut(key) {
            held.sent = Some(Sent { to: Destination::of(target), typed, route_lost: false });
        }
    }

    fn note(&self, key: &Key, add: u32, last: &str) {
        if let Some(p) = self.pending.lock().unwrap().get_mut(key) {
            p.attempts += add;
            p.last = Some(last.to_string());
        }
    }
}

/// The span a declaration is sent under: a round's completion is a new caller span in the round's
/// trace; any other declaration is sent under no span.
fn completion_span(p: &Pending, target: &crate::model::Node) -> tracing::Span {
    match &p.trace {
        Some(traceparent) => {
            let span = tracing::info_span!("rdm.node_admin.status.update.via-round-completion", key = ?p.key, to = %target.name, "otel.kind" = "internal");
            rafka_mesh_telemetry::set_parent(&span, traceparent);
            span
        }
        None => tracing::Span::none(),
    }
}

/// The declarations an admin owes from its view and its own state, derived each round so a
/// successor or a restarted admin owes the same ones from the same facts. `own` is the state this
/// admin holds of its own birth. `own_mesh_held` is the state this admin's own committed Mesh status
/// row holds for its mesh, when it holds one.
#[allow(clippy::too_many_arguments)]
pub(crate) fn owed_from_view(d: &Declarer, me: &PathName, me_id: &NodeId, me_incarnation: &str, own: rafka_mesh_entity::MemberStatus, mesh_round_done: bool, own_mesh_held: Option<MeshState>, view: &Topology, mesh_ids: &BTreeMap<String, String>) {
    // This admin's own birth, to its own mesh's primary, which sets its status from the report
    // (node-status.md "Status report"): its node.started once it is ready, and its Draining once it
    // drains (a drain command, or the fabric shutdown freezing it): its heartbeat says neither.
    use rafka_mesh_entity::MemberStatus as M;
    let reports: &[NodeState] = match own {
        M::Pending => &[],
        M::ReadyForTraffic | M::Leaving => &[NodeState::ReadyForTraffic],
        M::Draining => &[NodeState::ReadyForTraffic, NodeState::Draining],
    };
    if let Some(my) = view.own(me_id) {
        for state in reports {
            d.owe(
                Key::OwnState(me_incarnation.to_string(), *state),
                Authority::MeshPrimaryOf(me.mesh.clone()),
                StatusRequest::DeclareNodeState { node_id: my.node_id.clone(), incarnation: crate::model::IncarnationId(me_incarnation.to_string()), state: *state },
            );
        }
    }
    let i_am_mesh_primary = view.holds_mesh_seat(&me.mesh, me_id);
    let i_am_fabric_primary = view.holds_fabric_seat(me_id);
    // My Mesh's status, as its primary, to the fabric-primary: the report that its round is complete,
    // owed only once every planned birth of the accepted Build checked in (`crate::round`), and only
    // while the state this admin holds for its mesh is not past ready-for-traffic: a primary holding
    // Leaving reports the Leaving it holds (below), never a backward move.
    let past_ready = matches!(own_mesh_held, Some(MeshState::Leaving | MeshState::Dead));
    if i_am_mesh_primary && mesh_round_done && !past_ready {
        if let (Some(mesh), Some(id)) = (view.meshes.iter().find(|m| m.name == me.mesh), mesh_ids.get(&me.mesh)) {
            if mesh.status == crate::model::ScopeStatus::ReadyForTraffic {
                d.owe(Key::Mesh(id.clone(), MeshState::ReadyForTraffic), Authority::FabricPrimary, StatusRequest::DeclareMeshState { mesh_id: match crate::model::MeshId::parse(id) { Ok(m) => m, Err(_) => return }, state: MeshState::ReadyForTraffic });
            }
        }
    }
    // My Mesh's Leaving, as its primary, to the fabric-primary: its own mesh row committed Leaving
    // when it admitted leave-mesh ("one commit stores the row, applies it, gossips it and answers
    // the op", states.md), reported upward as committed state like its ready-for-traffic, and not
    // gated on a round: the fabric-primary's map holds the mesh's state it hands an admin it creates.
    if i_am_mesh_primary && own_mesh_held == Some(MeshState::Leaving) {
        if let Some(Ok(mesh_id)) = mesh_ids.get(&me.mesh).map(|id| crate::model::MeshId::parse(id).map(|m| (id.clone(), m))) {
            let (key, mesh_id) = mesh_id;
            d.owe(Key::Mesh(key, MeshState::Leaving), Authority::FabricPrimary, StatusRequest::DeclareMeshState { mesh_id, state: MeshState::Leaving });
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

    /// The one node id `name` is born with across every view of a cell: a node finds itself by it.
    fn id_of(name: &str) -> NodeId {
        let p: PathName = name.parse().unwrap();
        let mesh: u32 = p.mesh.trim_start_matches("mesh").parse().unwrap();
        let kind = if p.kind == NodeKind::NodeAdmin { 1 } else { 2 };
        NodeId::parse(&format!("{mesh}{kind}{:010}", p.ordinal)).unwrap()
    }

    fn node(name: &str, primary: bool, fabric_primary: bool) -> Node {
        let mut n = Node::allocated(name.parse().unwrap());
        n.node_id = id_of(name);
        n.status = NodeStatus::ReadyForTraffic;
        n.incarnation_id = Some(IncarnationId::mint());
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
        d.owe(Key::OwnState("inc".into(), NodeState::ReadyForTraffic), Authority::MeshPrimaryOf("mesh1".into()), StatusRequest::DeclareNodeState { node_id: crate::model::NodeId::mint(), incarnation: crate::model::IncarnationId("inc".into()), state: NodeState::ReadyForTraffic });
        d.owe(Key::Mesh("m1".into(), MeshState::ReadyForTraffic), Authority::FabricPrimary, StatusRequest::DeclareMeshState { mesh_id: crate::model::MeshId::mint(), state: MeshState::ReadyForTraffic });
        d.owe(Key::FabricEventAt("mesh2".into(), "ready-for-traffic".into()), Authority::MeshPrimaryOf("mesh2".into()), StatusRequest::ApplyFabricEvent { fabric_id: crate::model::FabricId::mint(), event: FabricEvent::ReadyForTraffic });
    }

    #[test]
    fn a_former_mesh_primary_withdraws_its_mesh_declaration() {
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let d = Declarer::new();
        owe_all(&d);
        d.withdraw_unowned(&me, &id_of(&me.to_string()), &view("mesh1.admin.1", "mesh1.admin.1"));
        assert_eq!(d.owed(), 3, "every seat is still this admin's");

        d.withdraw_unowned(&me, &id_of(&me.to_string()), &view("mesh1.admin.2", "mesh1.admin.1"));
        let left: Vec<Key> = d.pending.lock().unwrap().keys().cloned().collect();
        assert_eq!(left, vec![Key::OwnState("inc".into(), NodeState::ReadyForTraffic), Key::FabricEventAt("mesh2".into(), "ready-for-traffic".into())], "the Mesh key is the new mesh primary's");

        d.withdraw_unowned(&me, &id_of(&me.to_string()), &view("mesh1.admin.2", "mesh2.admin.1"));
        let left: Vec<Key> = d.pending.lock().unwrap().keys().cloned().collect();
        assert_eq!(left, vec![Key::OwnState("inc".into(), NodeState::ReadyForTraffic)], "the fabric event is the new fabric-primary's; its own birth stays owed");
    }

    /// CONTRACT: a mesh primary's completion of a fabric round is owed to the fabric-primary the view
    /// names. Once the fabric-primary took it, a successor fabric-primary is owed it again (the
    /// checklist a new primary opens rebuilds from fresh check-ins), and a former mesh primary
    /// withdraws it whole.
    #[test]
    fn a_round_completion_is_owed_again_to_a_successor_fabric_primary_and_withdrawn_with_the_mesh_seat() {
        let me: PathName = "mesh2.admin.1".parse().unwrap();
        let d = Declarer::new();
        let key = Key::Round("state-committed/fabric/bld/1".into());
        d.owe_in(key.clone(), Authority::FabricPrimary, StatusRequest::StateCommitted { fabric_id: FabricId::mint(), node_id: NodeId::mint(), incarnation: IncarnationId("inc".into()), build_id: "bld".into(), attempt: 1, operation: "commit-state:f".into() }, Some("00-f16a8b75a1b2c3d4e5f60718293a4b5c-1111111111111111-01".into()));
        let first = view("mesh1.admin.1", "mesh1.admin.1");
        let holder = first.fabric_primary().unwrap().clone();
        // The fabric-primary took the completion.
        d.pending.lock().unwrap().remove(&key);
        d.done.lock().unwrap().insert(key.clone(), ("applied".into(), holder.node_id.clone(), holder.incarnation_id.clone()));
        d.reowe_moved(&me, &first);
        assert_eq!(d.owed(), 0, "the same holder, the same birth: nothing is owed again");
        d.reowe_moved(&me, &view("mesh1.admin.2", "mesh1.admin.2"));
        assert_eq!(d.owed(), 1, "a successor fabric-primary is owed the completion");
        assert!(d.pending.lock().unwrap().get(&key).unwrap().trace.is_some(), "the completion keeps the round's trace");
        d.withdraw_unowned(&me, &id_of(&me.to_string()), &view("mesh2.admin.1", "mesh1.admin.2"));
        assert_eq!(d.owed(), 1, "this admin is still its mesh's primary");
        let mut v = view("mesh2.admin.1", "mesh1.admin.2");
        for n in v.nodes.iter_mut().filter(|n| n.mesh == "mesh2") {
            n.is_primary = false;
        }
        v.nodes.push({ let mut n = node("mesh2.admin.2", true, false); n.mesh = "mesh2".into(); n });
        d.withdraw_unowned(&me, &id_of(&me.to_string()), &v);
        assert_eq!(d.owed(), 0, "a former mesh primary withdraws the completion whole");
    }

    /// CONTRACT (node-rpc-envelope.md, "Fabric status rounds": "The round is scoped to the accepted Build,
    /// attempt and planned exact births"): a round completion the accepted Build moved past is withdrawn
    /// whole, owed or answered, and a successor fabric-primary is not owed it again. Another round's
    /// completion is untouched. Must NOT happen: the completion of a superseded round kept owed and sent
    /// on every eligible event (gate red b24a4303, mesh_elections__sticky_seat: mesh2.admin.1 declared
    /// the attempt-2 completion and was answered NotReady "round holds 3, reported 2").
    #[test]
    fn a_round_completion_the_accepted_build_moved_past_is_withdrawn_whole_owed_or_answered() {
        let me: PathName = "mesh2.admin.1".parse().unwrap();
        let d = Declarer::new();
        let fabric = FabricId::mint();
        let round = |attempt: u32| crate::fabric_state::RoundKey { kind: crate::fabric_state::RoundKind::StateCommit, fabric_id: fabric.clone(), build_id: "bld".into(), attempt };
        let owe = |attempt: u32| {
            let r = round(attempt);
            d.owe_in(Key::round(&r), Authority::FabricPrimary, StatusRequest::StateCommitted { fabric_id: fabric.clone(), node_id: NodeId::mint(), incarnation: IncarnationId("inc".into()), build_id: "bld".into(), attempt, operation: "commit-state:f".into() }, None);
            Key::round(&r)
        };
        let (owed_two, answered_three, current_four) = (owe(2), owe(3), owe(4));
        let first = view("mesh1.admin.1", "mesh1.admin.1");
        let holder = first.fabric_primary().unwrap().clone();
        answered_by(&d, &answered_three, &holder);
        assert_eq!(d.owed(), 2, "attempts 2 and 4 are owed; attempt 3 was answered");

        d.withdraw_round(&me, &owed_two, "the accepted Build bld is at attempt 4, past this round's attempt 2");
        d.withdraw_round(&me, &answered_three, "the accepted Build bld is at attempt 4, past this round's attempt 3");
        assert_eq!(d.owed_keys(), vec![current_four.clone()], "the superseded completions are no longer owed; the current round's is");
        assert_eq!(d.answered(&answered_three), None, "an answered completion of a superseded round is forgotten whole");
        d.reowe_moved(&me, &view("mesh1.admin.2", "mesh1.admin.2"));
        assert_eq!(d.owed_keys(), vec![current_four], "a successor fabric-primary is not owed a superseded round's completion");
    }

    #[test]
    fn a_withdrawn_key_is_owed_afresh_when_the_seat_returns() {
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let d = Declarer::new();
        owe_all(&d);
        d.done.lock().unwrap().insert(Key::Mesh("m1".into(), MeshState::ReadyForTraffic), ("applied".into(), NodeId::mint(), None));
        d.pending.lock().unwrap().remove(&Key::Mesh("m1".into(), MeshState::ReadyForTraffic));
        d.withdraw_unowned(&me, &id_of(&me.to_string()), &view("mesh1.admin.2", "mesh1.admin.1"));
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
        d.withdraw_unowned(&me, &id_of(&me.to_string()), &v);
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

    // ---- R-S2: sent when owed, and again only on an eligible event ----

    fn mesh_key() -> Key {
        Key::Mesh("m1".into(), MeshState::ReadyForTraffic)
    }

    fn fabric_primary_of(v: &Topology) -> Node {
        v.fabric_primary().unwrap().clone()
    }

    /// CONTRACT: the receiver's view catches up while the sender's target tuple stays identical:
    /// the declaration answered `RejectedNotAuthority` (typed, terminal) is sent again exactly
    /// once, when the authority's down op addresses this admin, and not before.
    #[test]
    fn a_typed_refusal_is_sent_again_once_when_the_authority_addresses_this_admin() {
        let me_id = NodeId::mint();
        let v = view("mesh2.admin.1", "mesh1.admin.1");
        let d = Declarer::new();
        owe_all(&d);
        let none = BTreeSet::new();
        let first: Vec<Key> = d.due(&me_id, &v, &none).into_iter().map(|(p, _)| p.key).collect();
        assert!(first.contains(&mesh_key()), "owed and never sent: due {first:?}");
        let fp = fabric_primary_of(&v);
        d.record_sent(&mesh_key(), &fp, true);

        assert!(!d.due(&me_id, &v, &none).iter().any(|(p, _)| p.key == mesh_key()), "a typed reply is terminal: nothing before the down op");
        assert!(!d.due(&me_id, &v, &none).iter().any(|(p, _)| p.key == mesh_key()), "and a second look still sends nothing");

        let from_fp: BTreeSet<NodeId> = [fp.node_id.clone()].into();
        let after: Vec<Key> = d.due(&me_id, &v, &from_fp).into_iter().map(|(p, _)| p.key).collect();
        assert_eq!(after.iter().filter(|k| **k == mesh_key()).count(), 1, "the down op re-sends it exactly once: {after:?}");
        d.record_sent(&mesh_key(), &fp, true);
        assert!(!d.due(&me_id, &v, &none).iter().any(|(p, _)| p.key == mesh_key()), "and then nothing again");

        let other: BTreeSet<NodeId> = [NodeId::mint()].into();
        assert!(!d.due(&me_id, &v, &other).iter().any(|(p, _)| p.key == mesh_key()), "a down op from a node that is not the key's authority re-sends nothing");
    }

    /// CONTRACT: a target that goes live -> unreachable is sent nothing; an untyped failure is
    /// sent again when the route is viable again, a typed reply is not.
    #[test]
    fn a_target_going_unreachable_sends_nothing_and_a_failed_delivery_resends_when_it_returns() {
        let me_id = NodeId::mint();
        let mut v = view("mesh2.admin.1", "mesh1.admin.1");
        let none = BTreeSet::new();
        let d = Declarer::new();
        owe_all(&d);
        let fp = fabric_primary_of(&v);
        let typed_key = Key::OwnState("inc".into(), NodeState::ReadyForTraffic);
        d.record_sent(&mesh_key(), &fp, false);
        d.record_sent(&typed_key, &fp, true);

        for n in &mut v.nodes {
            if n.is_fabric_primary {
                n.status = NodeStatus::PendingReconnect;
            }
        }
        let gone: Vec<Key> = d.due(&me_id, &v, &none).into_iter().map(|(p, _)| p.key).collect();
        assert!(!gone.contains(&mesh_key()) && !gone.contains(&typed_key), "an unreachable target is sent nothing: {gone:?}");

        for n in &mut v.nodes {
            if n.is_fabric_primary {
                n.status = NodeStatus::ReadyForTraffic;
            }
        }
        let back: Vec<Key> = d.due(&me_id, &v, &none).into_iter().map(|(p, _)| p.key).collect();
        assert!(back.contains(&mesh_key()), "the failed delivery gained a viable route: {back:?}");
        assert!(!back.contains(&typed_key), "a typed reply stays terminal when the route returns: {back:?}");
    }

    /// CONTRACT: a destination that changes to a deliverable birth is sent to once.
    #[test]
    fn a_new_destination_birth_is_sent_to_once() {
        let me_id = NodeId::mint();
        let mut v = view("mesh2.admin.1", "mesh1.admin.1");
        let none = BTreeSet::new();
        let d = Declarer::new();
        owe_all(&d);
        let fp = fabric_primary_of(&v);
        d.record_sent(&mesh_key(), &fp, true);
        for n in &mut v.nodes {
            if n.is_fabric_primary {
                n.incarnation_id = Some(IncarnationId::mint());
            }
        }
        let moved = d.due(&me_id, &v, &none);
        let (_, to) = moved.iter().find(|(p, _)| p.key == mesh_key()).expect("a new birth of the authority is a new destination");
        d.record_sent(&mesh_key(), to, true);
        assert!(!d.due(&me_id, &v, &none).iter().any(|(p, _)| p.key == mesh_key()));
    }

    /// CONTRACT (node-status.md "Status report": "A node reports to its mesh's primary, which sets
    /// its status"): a node-admin that is not its mesh's primary owes its own node.started
    /// (DeclareNodeState(ReadyForTraffic)) to its own mesh's primary, not to the fabric-primary, and
    /// once that primary answered, a move of the fabric seat owes it to nobody new. Must NOT happen:
    /// the declaration sent to the fabric-primary of another mesh.
    #[test]
    fn a_node_admin_owes_its_own_node_started_to_its_own_mesh_primary() {
        let me: PathName = "mesh1.admin.2".parse().unwrap();
        let v = view("mesh1.admin.1", "mesh2.admin.1");
        let ids: BTreeMap<String, String> = v.meshes.iter().map(|m| (m.name.clone(), m.id.clone().unwrap().to_string())).collect();
        let none = BTreeSet::new();
        let d = Declarer::new();
        owed_from_view(&d, &me, &id_of(&me.to_string()), "inc", rafka_mesh_entity::MemberStatus::ReadyForTraffic, false, None, &v, &ids);
        let due: Vec<(Key, String)> = d.due(&id_of(&me.to_string()), &v, &none).into_iter().map(|(p, to)| (p.key, to.name.to_string())).collect();
        assert_eq!(due, vec![(Key::OwnState("inc".into(), NodeState::ReadyForTraffic), "mesh1.admin.1".to_string())], "owed to mesh1's primary");
        let mesh1_primary = v.nodes.iter().find(|n| n.name.to_string() == "mesh1.admin.1").unwrap().clone();
        d.pending.lock().unwrap().remove(&Key::OwnState("inc".into(), NodeState::ReadyForTraffic));
        d.done.lock().unwrap().insert(Key::OwnState("inc".into(), NodeState::ReadyForTraffic), ("applied".into(), mesh1_primary.node_id.clone(), mesh1_primary.incarnation_id.clone()));
        let moved = view("mesh1.admin.1", "mesh1.admin.1");
        let moved = Topology { nodes: moved.nodes.into_iter().map(|mut n| { if n.name == mesh1_primary.name { n.incarnation_id = mesh1_primary.incarnation_id.clone(); } n }).collect(), ..moved };
        d.reowe_moved(&me, &moved);
        assert!(d.pending.lock().unwrap().is_empty(), "the fabric seat moved, its own mesh's primary did not: nothing is owed again");
    }

    /// CONTRACT (node-status.md "Status report": "A node reports to its mesh's primary, which sets
    /// its status"; gossip.md §3.1: a Digest "carries no status"): a node-admin that drains (a drain
    /// command, or the fabric shutdown freezing it) owes its Draining to its own mesh's primary beside
    /// its node.started, so the other admins hold it Draining from that primary's projection. Must NOT
    /// happen: a drained admin whose Draining no other admin can learn.
    #[test]
    fn a_draining_node_admin_reports_its_draining_to_its_own_mesh_primary() {
        let me: PathName = "mesh1.admin.2".parse().unwrap();
        let v = view("mesh1.admin.1", "mesh2.admin.1");
        let ids: BTreeMap<String, String> = v.meshes.iter().map(|m| (m.name.clone(), m.id.clone().unwrap().to_string())).collect();
        let d = Declarer::new();
        owed_from_view(&d, &me, &id_of(&me.to_string()), "inc", rafka_mesh_entity::MemberStatus::Draining, false, None, &v, &ids);
        let due: Vec<(Key, String)> = d.due(&id_of(&me.to_string()), &v, &BTreeSet::new()).into_iter().map(|(p, to)| (p.key, to.name.to_string())).collect();
        assert_eq!(
            due,
            vec![(Key::OwnState("inc".into(), NodeState::ReadyForTraffic), "mesh1.admin.1".to_string()), (Key::OwnState("inc".into(), NodeState::Draining), "mesh1.admin.1".to_string())],
            "its node.started and its Draining, each to mesh1's primary"
        );
    }

    /// CONTRACT (states.md "Who sets each state": node ReadyForTraffic is set by "the node's
    /// mesh-primary" from "the node's node.started report, after every start gate"; node-update.md:
    /// "a stopped predecessor to or from `<name>.old`", the stopped node reads that name at its next
    /// start): a node-admin renamed to `mesh1.admin.2.old` while stopped and started again under the
    /// row's name is the same birth, so once it is ready it owes its node.started to mesh1's primary,
    /// found by the node id it was born with. Must NOT happen: the admin missing from its own view
    /// because its row's name ends `.old`, so it owes nothing and its mesh-primary holds it Pending.
    #[test]
    fn a_node_admin_started_under_a_dot_old_row_name_owes_its_node_started_to_its_mesh_primary() {
        let me: PathName = "mesh1.admin.2".parse().unwrap();
        let me_id = id_of(&me.to_string());
        let mut v = view("mesh1.admin.1", "mesh2.admin.1");
        let own = v.nodes.iter_mut().find(|n| n.node_id == me_id).unwrap();
        own.name = me.renamed();
        assert!(own.name.old, "the row's name is {}", own.name);
        let ids: BTreeMap<String, String> = v.meshes.iter().map(|m| (m.name.clone(), m.id.clone().unwrap().to_string())).collect();
        let d = Declarer::new();
        owed_from_view(&d, &me.renamed(), &me_id, "inc", rafka_mesh_entity::MemberStatus::ReadyForTraffic, false, None, &v, &ids);
        let due: Vec<(Key, String)> = d.due(&me_id, &v, &BTreeSet::new()).into_iter().map(|(p, to)| (p.key, to.name.to_string())).collect();
        assert_eq!(due, vec![(Key::OwnState("inc".into(), NodeState::ReadyForTraffic), "mesh1.admin.1".to_string())], "its node.started, owed to mesh1's primary by its exact birth");
    }

    /// CONTRACT: an obligation newly created by this admin's own readiness or the declare gate,
    /// with no topology change, is due at once (the wake that finds it is the declarer's input).
    #[test]
    fn an_obligation_newly_owed_without_a_topology_change_is_due() {
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let v = view("mesh1.admin.1", "mesh2.admin.1");
        let d = Declarer::new();
        let ids: BTreeMap<String, String> = [("mesh1".to_string(), MeshId::mint().to_string())].into();
        let none = BTreeSet::new();
        owed_from_view(&d, &me, &id_of(&me.to_string()), "inc", rafka_mesh_entity::MemberStatus::Pending, false, None, &v, &ids);
        assert!(d.due(&NodeId::mint(), &v, &none).is_empty(), "not ready, gate closed: nothing is owed");
        owed_from_view(&d, &me, &id_of(&me.to_string()), "inc", rafka_mesh_entity::MemberStatus::ReadyForTraffic, false, None, &v, &ids);
        let own: Vec<Key> = d.due(&NodeId::mint(), &v, &none).into_iter().map(|(p, _)| p.key).collect();
        assert_eq!(own, vec![Key::OwnState("inc".into(), NodeState::ReadyForTraffic)], "own readiness owes its birth");
        owed_from_view(&d, &me, &id_of(&me.to_string()), "inc", rafka_mesh_entity::MemberStatus::ReadyForTraffic, true, None, &v, &ids);
        let both = d.due(&NodeId::mint(), &v, &none).len();
        assert_eq!(both, 2, "the gate opening owes the Mesh declaration");
    }

    /// CONTRACT (states.md: "One commit stores the row, applies it, gossips it and answers the op";
    /// fabric-mesh-ops.md: "`DeclareNodeState` and `DeclareMeshState` report committed state
    /// upward"): a mesh primary whose own Mesh status row is committed Leaving (leave-mesh admitted)
    /// owes `DeclareMeshState(Leaving)` to the fabric-primary, whether or not its round is done, so
    /// the fabric-primary's map holds the mesh's Leaving and hands it to an admin it creates for the
    /// mesh. An admin that is not its mesh's primary owes none: the Mesh declaration is the seat's.
    #[test]
    fn a_mesh_primary_whose_own_mesh_row_is_leaving_owes_the_fabric_primary_its_leaving() {
        let me: PathName = "mesh2.admin.1".parse().unwrap();
        let v = view("mesh1.admin.1", "mesh1.admin.1");
        let mesh2 = v.meshes.iter().find(|m| m.name == "mesh2").unwrap().id.clone().unwrap();
        let ids: BTreeMap<String, String> = v.meshes.iter().map(|m| (m.name.clone(), m.id.clone().unwrap().to_string())).collect();
        let none = BTreeSet::new();
        let d = Declarer::new();
        owed_from_view(&d, &me, &id_of(&me.to_string()), "inc", rafka_mesh_entity::MemberStatus::Pending, false, Some(MeshState::Leaving), &v, &ids);
        let due: Vec<(Key, StatusRequest)> = d.due(&id_of(&me.to_string()), &v, &none).into_iter().map(|(p, _)| (p.key, p.request)).collect();
        assert_eq!(
            due,
            vec![(Key::Mesh(mesh2.to_string(), MeshState::Leaving), StatusRequest::DeclareMeshState { mesh_id: mesh2.clone(), state: MeshState::Leaving })],
            "the leaving mesh's primary owes its committed Leaving to the fabric-primary"
        );
        // A primary holding its mesh's Leaving whose round is done (the admin created for a leaving
        // mesh, handed Leaving) reports the state it holds, never ready-for-traffic: a backward
        // declaration the fabric-primary can only refuse.
        let d = Declarer::new();
        owed_from_view(&d, &me, &id_of(&me.to_string()), "inc", rafka_mesh_entity::MemberStatus::Pending, true, Some(MeshState::Leaving), &v, &ids);
        let keys: Vec<Key> = d.due(&id_of(&me.to_string()), &v, &none).into_iter().map(|(p, _)| p.key).collect();
        assert_eq!(keys, vec![Key::Mesh(mesh2.to_string(), MeshState::Leaving)], "a primary holding Leaving owes no ready-for-traffic declaration");
        let other: PathName = "mesh1.admin.2".parse().unwrap();
        let d = Declarer::new();
        owed_from_view(&d, &other, &id_of(&other.to_string()), "inc", rafka_mesh_entity::MemberStatus::Pending, false, Some(MeshState::Leaving), &v, &ids);
        assert_eq!(d.owed(), 0, "an admin that is not its mesh's primary owes no Mesh declaration");
    }

    /// CONTRACT (R-RC1, node-replace.md "Successor | New node.id"): the view holds one row per
    /// path.name (`admin::project_at` keys it by `PathName`), so after a replace of the fabric-primary
    /// the row at mesh1.admin.1 can be the retired birth's, live and holding both seats, while the
    /// reborn admin's own row is absent. The reborn admin decides its seats by the node id it was born
    /// with: it owes no fabric event and no Mesh report, and withdraws the Mesh key it held, because
    /// the seat is another birth's. What must NOT happen: the retired birth's seats read as this
    /// admin's, so it owes the fabric's readiness to every mesh primary as a fabric-primary it is not.
    #[test]
    fn a_reborn_admin_whose_view_seats_its_retired_birth_owes_nothing_of_the_seat() {
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let mut v = view("mesh1.admin.1", "mesh1.admin.1");
        let retired = v.nodes.iter_mut().find(|n| n.name == me).unwrap();
        retired.status = NodeStatus::Draining;
        let retired = retired.clone();
        let mut reborn = node("mesh1.admin.1", false, false);
        reborn.node_id = NodeId::mint();
        assert_ne!(retired.node_id, reborn.node_id, "a replace mints a new node id");
        assert_eq!(v.fabric_primary().map(|n| n.node_id.clone()), Some(retired.node_id.clone()), "the retired birth holds the fabric seat in this view");
        let ids: BTreeMap<String, String> = v.meshes.iter().map(|m| (m.name.clone(), m.id.clone().unwrap().to_string())).collect();
        let d = Declarer::new();
        owed_from_view(&d, &me, &reborn.node_id, "inc", rafka_mesh_entity::MemberStatus::Pending, true, None, &v, &ids);
        assert_eq!(d.owed(), 0, "the retired birth's seats owe this admin nothing: {:?}", d.pending.lock().unwrap().keys().collect::<Vec<_>>());
        d.owe(mesh_key(), Authority::FabricPrimary, StatusRequest::DeclareMeshState { mesh_id: MeshId::mint(), state: MeshState::ReadyForTraffic });
        d.withdraw_unowned(&me, &reborn.node_id, &v);
        assert_eq!(d.owed(), 0, "the mesh seat is the retired birth's, not this admin's: the Mesh key is withdrawn");
    }

    /// CONTRACT: a burst of 5 pokes wakes the declarer once; a poke during a round is kept as the
    /// one stored permit; and with nothing poked the declarer sleeps.
    #[tokio::test]
    async fn a_burst_of_pokes_runs_one_round() {
        let w = DeclareWake::new();
        for _ in 0..5 {
            w.poke();
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), w.woken()).await.expect("the burst woke the declarer");
        assert!(tokio::time::timeout(std::time::Duration::from_millis(200), w.woken()).await.is_err(), "5 pokes ran one round, not five");
        w.poke();
        tokio::time::timeout(std::time::Duration::from_secs(5), w.woken()).await.expect("a poke after the round wakes it again");
    }

    /// CONTRACT: a down op taken in a round is gone from the next; one arriving after the take is kept.
    #[test]
    fn the_down_ops_taken_by_a_round_are_not_taken_twice() {
        let w = DeclareWake::new();
        let a = NodeId::mint();
        w.addressed_by(Some(&a), None);
        w.addressed_by(Some(&a), None);
        assert_eq!(w.take_addressed(), [a.clone()].into());
        assert!(w.take_addressed().is_empty());
        let b = NodeId::mint();
        w.addressed_by(Some(&b), None);
        assert_eq!(w.take_addressed(), [b].into());
    }
}
