//! What node-admin declares upward over `Status` (i143.e4.s11), and how it keeps declaring until
//! the authority answers by name.
//!
//! A declaration is a fact about this admin or its Mesh with a natural key; the declarer holds
//! the ones not yet answered and resolves the authority from the current view at every round (the
//! fabric-primary moves; the key does not). A declaration is sent when it becomes owed, and again
//! only on an eligible event (R-S2): the authoritative destination changes to a deliverable birth,
//! a failed delivery gains a viable route, or the authority addresses this admin with its down op
//! (`DeclareWake::addressed_by`). A typed reply is terminal, `RejectedNotPrimary` included, and a
//! target going unreachable sends nothing. One `DeclareWake` wakes the declarer; no timer does.
//! The outcomes:
//!
//! ```text
//! Applied | AlreadyApplied          done
//! RejectedNotPrimary             terminal until an eligible event: the authority's down op, or
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
//! no answer                        the receiver's exit or removal is proven: owed to the birth the
//!                                  view names next (never a negative state)
//! ```
//!
//! Delivery is Node RPC's (node-rpc.md §43, i146 Q44): each declaration sent is one `Budget::Owed`
//! call held by the declarer, resent by the layer until the authority answers. The declarer never
//! sends a key again because a delivery failed: a call in flight to the destination the view names
//! is left to run, and is dropped and owed to the new destination when the view names another birth
//! or the key is withdrawn.
//!
//! Nothing here gates gossip or readiness: the admin's own `ReadyForTraffic` is published by
//! membership as before; the declaration is certainty on top of it.

use crate::model::{IncarnationId, NodeId, NodeKind, PathName};
use crate::topology::Topology;
use rdm_transport::{Budget, CallOptions, NodeRpcClient, NodeTarget};
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{DeclaredOperation, FabricEvent, MeshState, NodeState, Status, StatusReply, StatusRequest};
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
    /// Wakes the hierarchy and peer-mesh investigation loops when a Mesh's round-complete report arrives.
    pub(crate) report_notify: tokio::sync::Notify,
    addressed: Mutex<BTreeSet<NodeId>>,
    /// The same down ops, counted for a `before_ready` hook blocked on its authority's word.
    pub(crate) before_ready: Arc<crate::before_ready::Addressed>,
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
            self.before_ready.note(&peer);
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

pub use rdm_node::rounds::{eligible, Destination, Sent};

/// How a reply ends a declaration: its name, and whether it finishes the key (applied, or refused for
/// a fact the authority holds that this declaration can never move past).
fn classify(reply: &StatusReply) -> (String, bool) {
    match reply {
        StatusReply::Applied | StatusReply::AlreadyApplied => (reply.name().to_string(), true),
        StatusReply::RejectedStaleIncarnation { .. }
        | StatusReply::RejectedStaleMesh { .. }
        | StatusReply::RejectedStaleFabric { .. }
        | StatusReply::RejectedInvalidNodeTransition { .. }
        | StatusReply::RejectedInvalidMeshTransition { .. } => (format!("{reply:?}"), true),
        other => (format!("{other:?}"), false),
    }
}

/// A declaration's Owed call in flight to `to`: dropped (its task aborted, the request owed no
/// longer) when the key is sent to another destination or withdrawn.
#[derive(Debug)]
struct Inflight {
    to: Destination,
    task: tokio::task::JoinHandle<()>,
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
    /// Whether the fabric has first reached ready-for-traffic: the fabric-primary owes the ready-for-traffic
    /// event to every mesh primary from then on, a resize that puts the fabric back to pending included.
    first_ready: crate::fabric_state::FirstReady,
    pending: Mutex<BTreeMap<Key, Pending>>,
    done: Mutex<BTreeMap<Key, (String, NodeId, Option<IncarnationId>)>>,
    /// Every declaration ever owed, by key: what is re-owed when the authority moves.
    requests: Mutex<BTreeMap<Key, (Authority, StatusRequest, Option<String>)>>,
    /// The operation that produced each own state of this birth's current cycle (node-status.md
    /// "Status report", i146 Q37): a start's ReadyForTraffic names the start, a stop's Stopped the
    /// stop; a state this admin measured itself names none.
    own_operations: Mutex<BTreeMap<NodeState, DeclaredOperation>>,
    /// The Owed call in flight for each key sent and not yet answered.
    inflight: Mutex<BTreeMap<Key, Inflight>>,
    /// Fabric recovery in a view (Q122): the node it re-creates at the target's path.name reports to its
    /// executor while its mesh has no mesh-primary (build.md §5.3).
    pub(crate) fabric_recovery: crate::fabric_recovery::ReaderSlot,
    /// The admin that made this node (its maker, node-mesh-join.md "Report"), when one did.
    pub(crate) maker: std::sync::OnceLock<NodeId>,
    /// How this admin, as its mesh's primary, records a Mesh state the fabric-primary answered on its
    /// own Mesh record (Q144): a declaration of it stays owed until that write lands.
    pub(crate) record_mesh: std::sync::OnceLock<RecordMesh>,
}

/// The write of a Mesh state on the mesh-primary's own Mesh record, by mesh id: `Err` names why it
/// did not land.
#[derive(Clone)]
pub(crate) struct RecordMesh(pub(crate) Arc<dyn Fn(crate::model::MeshId, MeshState) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>> + Send + Sync>);

impl std::fmt::Debug for RecordMesh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecordMesh")
    }
}

impl Declarer {
    #[cfg(test)]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// A declarer that reads whether the fabric first reached ready-for-traffic from `first_ready`, the handle
    /// its admin's fabric state sets.
    pub fn with_first_ready(first_ready: crate::fabric_state::FirstReady) -> Arc<Self> {
        Arc::new(Self { first_ready, ..Self::default() })
    }

    /// Whether the initial fabric of `view` has first reached ready-for-traffic, read before
    /// [`owed_from_view`] derives what the fabric-primary owes (states.md, "The initial fabric before
    /// ready-for-traffic"), `rows` being the status rows of this admin's mesh on the store root: decided
    /// on the durable fact ([`crate::fabric_state::FirstReady::reached_on_root`]), what this admin already
    /// knows, else a point read of the fabric's ready-for-traffic row among `rows`, which the fabric state
    /// it hears by gossip can lag (an outside successor of the fabric seat). A row found latches the
    /// handle the derivation reads. A read that fails is the caller's to name.
    pub(crate) async fn read_first_ready(&self, fabric: &dyn crate::fabric_storage::FabricStorage, view: &Topology) -> Result<bool, crate::storage_error::StorageError> {
        self.first_ready.reached_on_root(fabric, &view.fabric.id, view.fabric.status).await
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

    /// The authority asked again for `key`, a round completion it already answered: the round it
    /// holds is another tenure's (i146 Q45: a fabric-primary healed from a cut-off re-opens its
    /// rounds), so the completion is owed again from its record ("Members already done check in
    /// again"). Whether it was owed again; a key not yet answered is left as it is.
    pub(crate) fn owe_again(&self, me: &PathName, key: &Key) -> bool {
        let Some((by, _, _)) = self.done.lock().unwrap().remove(key).map(|(o, by, inc)| (by, o, inc)) else { return false };
        let Some((to, request, trace)) = self.requests.lock().unwrap().get(key).cloned() else { return false };
        tracing::info_span!("rdm.node_admin.status.update.via-asked-again", node = %me, key = ?key, was = %by, "otel.kind" = "internal")
            .in_scope(|| tracing::info!("the authority asked again for a completion it answered: owed again from its record"));
        self.pending.lock().unwrap().insert(key.clone(), Pending { key: key.clone(), to, request, attempts: 0, last: None, sent: None, trace });
        true
    }

    /// A start of this admin's birth `incarnation` begins its next cycle: the own-state declarations
    /// its last cycle owed or had answered are forgotten whole, so the node.started this start reports
    /// is owed afresh to its mesh's primary (states.md: node ReadyForTraffic is set from "the node's
    /// node.started report, after every start gate"), never stood for by the last cycle's answer. The
    /// cycle's ReadyForTraffic names `start`, the operation that began it (node-status.md "Status
    /// report").
    pub(crate) fn begin_own_cycle(&self, me: &PathName, incarnation: &str, start: &DeclaredOperation) {
        let own = |k: &Key| matches!(k, Key::OwnState(inc, _) if inc == incarnation);
        let forgotten: Vec<Key> = self.requests.lock().unwrap().keys().filter(|k| own(k)).cloned().collect();
        self.requests.lock().unwrap().retain(|k, _| !own(k));
        self.done.lock().unwrap().retain(|k, _| !own(k));
        self.pending.lock().unwrap().retain(|k, _| !own(k));
        {
            let mut ops = self.own_operations.lock().unwrap();
            ops.clear();
            ops.insert(NodeState::ReadyForTraffic, start.clone());
        }
        tracing::info_span!("rdm.node_admin.status.remove.via-cycle-begun", node = %me, incarnation_id = incarnation, operation = %start.operation, forgotten = ?forgotten)
            .in_scope(|| tracing::info!("a start begins this birth's next cycle: its own state is owed afresh at node.started"));
    }

    /// The open start stream reports Ready to this authority; a later seat move still owes it.
    pub(crate) fn own_on_stream(&self, me: &PathName, node_id: &NodeId, incarnation: &IncarnationId, operation: &DeclaredOperation, authority: &rdm_core::MeshDigest) {
        let key = Key::OwnState(incarnation.to_string(), NodeState::ReadyForTraffic);
        let request = StatusRequest::DeclareNodeState { node_id: node_id.clone(), incarnation: incarnation.clone(), state: NodeState::ReadyForTraffic, operation: Some(operation.clone()) };
        self.requests.lock().unwrap().insert(key.clone(), (Authority::MeshPrimaryOf(me.mesh.clone()), request, None));
        self.done.lock().unwrap().insert(key.clone(), ("command-stream".into(), authority.node.node_id.clone(), Some(authority.node.incarnation.clone())));
        self.pending.lock().unwrap().remove(&key);
    }

    /// The operation that produced this birth's own `state` in its current cycle, if one did.
    pub(crate) fn own_operation(&self, state: NodeState) -> Option<DeclaredOperation> {
        self.own_operations.lock().unwrap().get(&state).cloned()
    }

    /// Owe this admin's own `state` to its mesh's primary, naming `operation` (a stop's Stopped names
    /// the stop, node-stop.md step 4): the declaration [`Declarer::round`] sends.
    pub(crate) fn owe_own(&self, me: &PathName, node_id: &NodeId, incarnation: &IncarnationId, state: NodeState, operation: Option<DeclaredOperation>) {
        if let Some(op) = &operation {
            self.own_operations.lock().unwrap().insert(state, op.clone());
        }
        self.owe(
            Key::OwnState(incarnation.to_string(), state),
            Authority::MeshPrimaryOf(me.mesh.clone()),
            StatusRequest::DeclareNodeState { node_id: node_id.clone(), incarnation: incarnation.clone(), state, operation },
        );
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
                    Key::OwnState(..) => view.node_type_primary(&me.mesh, NodeKind::NodeAdmin),
                    Key::Mesh(..) | Key::Round(..) => view.fabric_primary(),
                    Key::FabricEventAt(mesh, _) => view.node_type_primary(mesh, NodeKind::NodeAdmin),
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

    /// While fabric recovery runs for `mesh` and the mesh has no mesh-primary, the node it re-created at the
    /// target's path.name reports to its maker, the executor that made it (node-mesh-join.md "Report"; build.md
    /// §5.3, Q122): a node just born may not yet hold the executor ReadyForTraffic in its own view.
    fn recovery_executor<'a>(&self, me_id: &NodeId, view: &'a Topology, mesh: &str) -> Option<&'a crate::model::Node> {
        let fr = self.fabric_recovery.get().and_then(|read| read(view)).filter(|fr| fr.target.mesh == mesh)?;
        if fr.at.as_ref().and_then(|at| view.node(at)).is_some_and(|n| &n.node_id != me_id) {
            return None;
        }
        let maker = self.maker.get()?;
        view.members().find(|n| &n.node_id == maker)
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
                Authority::MeshPrimaryOf(mesh) => view.node_type_primary(mesh, NodeKind::NodeAdmin).or_else(|| self.recovery_executor(me_id, view, mesh)),
            };
            let Some(target) = target else {
                p.last = Some("no authority in view".into());
                continue;
            };
            let now = Destination::of(target);
            let deliverable = &target.node_id == me_id || (target.status.is_live() && target.incarnation_id.is_some());
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
    pub async fn round(self: &Arc<Self>, me: &PathName, me_id: &NodeId, view: &Topology, client: &NodeRpcClient, authority: Option<&crate::status_rpc::StatusAuthority>, addressed: &BTreeSet<NodeId>, wake: &Arc<DeclareWake>) {
        self.withdraw_unowned(me, me_id, view);
        self.reowe_moved(me, view);
        for (p, target) in self.due(me_id, view, addressed) {
            let addressed_now = addressed.contains(&target.node_id);
            if &target.node_id == me_id {
                match authority {
                    Some(auth) => {
                        let reply = auth.apply(Some(target.clone()), &p.request).await;
                        let (o, f) = self.recorded(&p, &reply, classify(&reply)).await;
                        self.declared(me, &p, &target, "self", addressed_now, o, f, true);
                    }
                    None => {
                        // Unreachable: the declarer starts after the authority is filled.
                        tracing::error!(node = %me, key = ?p.key, "authority is self, but the Status authority is not filled: unreachable, the declarer starts after it is filled");
                        debug_assert!(false, "authority is self, but the Status authority is not filled");
                        self.note(&p.key, 0, "authority is self, but this admin holds no view yet");
                    }
                }
                continue;
            }
            // One Owed call per key (node-rpc.md §43, i146 Q44): the layer resends it until the
            // authority answers; the declarer only retargets it.
            let to = Destination::of(&target);
            let mut inflight = self.inflight.lock().unwrap();
            if !self.still_due(&inflight, &p.key, &to) {
                continue;
            }
            if let Some(old) = inflight.remove(&p.key) {
                old.task.abort();
                tracing::info_span!("rdm.node_admin.status.update.via-declare-retargeted", node = %me, key = ?p.key, to = %target.name, "otel.kind" = "internal")
                    .in_scope(|| tracing::info!("the view names another birth for this declaration's authority: the call in flight is dropped and the declaration is owed to it"));
            }
            let (this, client, wake, me, key) = (self.clone(), client.clone(), wake.clone(), me.clone(), p.key.clone());
            let sent_to = to.clone();
            // Spawned under the lock: the call's own end, which takes the lock, never runs ahead of its record.
            let task = tokio::spawn(async move {
                let owed = CallOptions { budget: Budget::Owed, ..CallOptions::default() };
                let (out, _) = client.call::<Status>(&NodeTarget::ExactNode(target.node_id.clone()), &p.request, &owed).instrument(completion_span(&p, &target)).await;
                let (outcome, finished, typed) = match &out {
                    RpcOutcome::Reply(r) => {
                        let (o, f) = this.recorded(&p, r.value(), classify(r.value())).await;
                        (o, f, true)
                    }
                    other => (format!("{}: {}", other.name(), other.code().name()), false, false),
                };
                // The end is recorded before the in-flight claim is released: a round that finds the
                // claim gone finds the key finished too ([`Declarer::still_due`]).
                this.declared(&me, &p, &target, "node-rpc", addressed_now, outcome, finished, typed);
                {
                    let mut inflight = this.inflight.lock().unwrap();
                    if inflight.get(&p.key).is_some_and(|f| f.to == sent_to) {
                        inflight.remove(&p.key);
                    }
                }
                wake.poke();
            });
            inflight.insert(key, Inflight { to, task });
        }
    }

    /// A Mesh ReadyForTraffic the fabric-primary answered (`Applied` or `AlreadyApplied`) is recorded on
    /// this admin's own Mesh record before its declaration is finished (Q144): a write that does not
    /// land leaves the declaration owed, and the next answer writes it again.
    async fn recorded(&self, p: &Pending, reply: &StatusReply, (outcome, finished): (String, bool)) -> (String, bool) {
        let (StatusRequest::DeclareMeshState { mesh_id, state: state @ MeshState::ReadyForTraffic }, StatusReply::Applied | StatusReply::AlreadyApplied) = (&p.request, reply) else {
            return (outcome, finished);
        };
        let Some(record) = self.record_mesh.get() else { return (outcome, finished) };
        match (record.0)(mesh_id.clone(), *state).await {
            Ok(()) => (outcome, finished),
            Err(why) => (format!("{outcome}; the Mesh record write did not land: {why}"), false),
        }
    }

    /// Whether a send of `key` to `to`, decided from a [`Self::due`] read, still goes out, read under the
    /// in-flight lock (`inflight`): the key is still owed and no call of it is in flight to `to`. A
    /// `due` read can predate the end of the key's last call; that end is recorded before its in-flight
    /// claim is released, so a key it finished is owed no longer here.
    fn still_due(&self, inflight: &BTreeMap<Key, Inflight>, key: &Key, to: &Destination) -> bool {
        self.pending.lock().unwrap().contains_key(key) && !inflight.get(key).is_some_and(|f| &f.to == to)
    }

    /// The end of one send of `p` to `target`, spanned: an answer that finishes it records the key done
    /// by that authority; any other end is the key's last send.
    #[allow(clippy::too_many_arguments)]
    fn declared(&self, me: &PathName, p: &Pending, target: &crate::model::Node, via: &'static str, addressed: bool, outcome: String, finished: bool, typed: bool) {
        let operation = match &p.request {
            StatusRequest::DeclareNodeState { operation: Some(op), .. } => op.operation.as_str(),
            _ => "",
        };
        tracing::info_span!(
            "rdm.node_admin.status.update.via-declare",
            node = %me,
            key = ?p.key,
            operation,
            to = %target.name,
            via,
            attempt = p.attempts + 1,
            outcome = %outcome,
            finished,
            addressed,
        )
        .in_scope(|| tracing::info!("declared to the authority"));
        if finished {
            self.pending.lock().unwrap().remove(&p.key);
            self.done.lock().unwrap().insert(p.key.clone(), (outcome, target.node_id.clone(), target.incarnation_id.clone()));
        } else {
            self.note(&p.key, 1, &outcome);
            self.record_sent(&p.key, target, typed);
        }
    }

    /// Withdraw every key whose owing seat the view no longer gives this admin: a Mesh key once
    /// it is not its Mesh's admin primary, a fabric event once it is not the fabric-primary. A
    /// key withdrawn is forgotten whole, so holding the seat again owes it afresh. A view that
    /// names no holder of the seat withdraws nothing: the seat is unknown, not moved.
    pub(crate) fn withdraw_unowned(&self, me: &PathName, me_id: &NodeId, view: &Topology) {
        let holder = |key: &Key| match key {
            Key::OwnState(..) => None,
            Key::Mesh(..) | Key::Round(..) => Some(view.node_type_primary(&me.mesh, NodeKind::NodeAdmin)),
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
            if let Some(f) = self.inflight.lock().unwrap().remove(&key) {
                f.task.abort();
            }
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
        if let Some(f) = self.inflight.lock().unwrap().remove(key) {
            f.task.abort();
        }
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
            held.sent = Some(Sent { to: Destination::of(target), typed });
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
            rdm_transport::set_parent(&span, traceparent);
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
pub(crate) fn owed_from_view(d: &Declarer, me: &PathName, me_id: &NodeId, me_incarnation: &crate::model::IncarnationId, own: rdm_core::MemberStatus, mesh_round_done: bool, own_mesh_held: Option<MeshState>, view: &Topology, mesh_ids: &BTreeMap<String, String>) {
    // This admin's own birth, to its own mesh's primary, which sets its status from the report
    // (node-status.md "Status report"): its node.started once it is ready, and its ConnectionsDeleting once it
    // drains (a drain command, or the fabric shutdown freezing it): its heartbeat says neither.
    use rdm_core::MemberStatus as M;
    let reports: &[NodeState] = match own {
        M::Pending => &[],
        M::ReadyForTraffic | M::Leaving => &[NodeState::ReadyForTraffic],
        M::ConnectionsDeleting => &[NodeState::ReadyForTraffic, NodeState::ConnectionsDeleting],
    };
    if let Some(my) = view.own(me_id) {
        for state in reports {
            d.owe(
                Key::OwnState(me_incarnation.to_string(), *state),
                Authority::MeshPrimaryOf(me.mesh.clone()),
                StatusRequest::DeclareNodeState { node_id: my.node_id.clone(), incarnation: me_incarnation.clone(), state: *state, operation: d.own_operation(*state) },
            );
        }
    }
    let i_am_mesh_primary = view.is_mesh_primary(&me.mesh, me_id);
    let i_am_fabric_primary = view.is_fabric_primary(me_id);
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
    // when it admitted delete-mesh ("one commit stores the row, applies it, gossips it and answers
    // the op", states.md), reported upward as committed state like its ready-for-traffic, and not
    // gated on a round: the fabric-primary's map holds the mesh's state it hands an admin it creates.
    if i_am_mesh_primary && own_mesh_held == Some(MeshState::Leaving) {
        if let Some(Ok(mesh_id)) = mesh_ids.get(&me.mesh).map(|id| crate::model::MeshId::parse(id).map(|m| (id.clone(), m))) {
            let (key, mesh_id) = mesh_id;
            d.owe(Key::Mesh(key, MeshState::Leaving), Authority::FabricPrimary, StatusRequest::DeclareMeshState { mesh_id, state: MeshState::Leaving });
        }
    }
    // The fabric's readiness, as the fabric-primary, to every mesh primary, its own mesh's included: the row the
    // event leaves in a mesh's status rows is the one durable fact that the fabric first reached
    // ready-for-traffic, so each mesh holds it from the first time the fabric holds it, whatever status it
    // holds later (a resize puts the fabric back to pending).
    if i_am_fabric_primary && d.first_ready.reached(view.fabric.status) {
        for m in &view.meshes {
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
    use crate::model::IncarnationId;

    /// The incarnation the declarer fixtures name.
    const INC: &str = "n4w3mjy921q8";
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
        d.owe(Key::OwnState(INC.into(), NodeState::ReadyForTraffic), Authority::MeshPrimaryOf("mesh1".into()), StatusRequest::DeclareNodeState { node_id: crate::model::NodeId::mint(), incarnation: IncarnationId::parse(INC).unwrap(), state: NodeState::ReadyForTraffic, operation: None });
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
        assert_eq!(left, vec![Key::OwnState(INC.into(), NodeState::ReadyForTraffic), Key::FabricEventAt("mesh2".into(), "ready-for-traffic".into())], "the Mesh key is the new mesh primary's");

        d.withdraw_unowned(&me, &id_of(&me.to_string()), &view("mesh1.admin.2", "mesh2.admin.1"));
        let left: Vec<Key> = d.pending.lock().unwrap().keys().cloned().collect();
        assert_eq!(left, vec![Key::OwnState(INC.into(), NodeState::ReadyForTraffic)], "the fabric event is the new fabric-primary's; its own birth stays owed");
    }

    /// CONTRACT: a mesh primary's completion of a fabric round is owed to the fabric-primary the view
    /// names. Once the fabric-primary took it, a successor fabric-primary is owed it again (the
    /// checklist a new primary opens rebuilds from fresh check-ins), and a former mesh primary
    /// withdraws it whole.
    #[test]
    fn a_round_completion_is_owed_again_to_a_successor_fabric_primary_and_withdrawn_with_the_mesh_primary() {
        let me: PathName = "mesh2.admin.1".parse().unwrap();
        let d = Declarer::new();
        let key = Key::Round("state-committed/fabric/bld/1".into());
        d.owe_in(key.clone(), Authority::FabricPrimary, StatusRequest::StateCommitted { fabric_id: FabricId::mint(), node_id: NodeId::mint(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("inc")).unwrap(), build_id: "bld".into(), attempt: 1, operation: "commit-state:f".into() }, Some("00-f16a8b75a1b2c3d4e5f60718293a4b5c-1111111111111111-01".into()));
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
            d.owe_in(Key::round(&r), Authority::FabricPrimary, StatusRequest::StateCommitted { fabric_id: fabric.clone(), node_id: NodeId::mint(), incarnation: IncarnationId::parse(&rdm_core::fixture_crockford60("inc")).unwrap(), build_id: "bld".into(), attempt, operation: "commit-state:f".into() }, None);
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
    fn a_withdrawn_key_is_owed_afresh_when_the_primary_returns() {
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
    fn a_view_naming_no_primary_withdraws_nothing() {
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
    fn a_restarted_primary_is_owed_every_declaration_again() {
        let me: PathName = "mesh2.admin.1".parse().unwrap();
        let d = Declarer::new();
        owe_all(&d);
        let mut v = view("mesh2.admin.1", "mesh1.admin.1");
        for n in &mut v.nodes {
            n.incarnation_id = Some(IncarnationId::mint());
        }
        let fp = v.nodes.iter().find(|n| n.is_fabric_primary).unwrap().clone();
        let own = Key::OwnState(INC.into(), NodeState::ReadyForTraffic);
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
    fn a_primary_moved_to_another_node_is_owed_again() {
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
    /// the declaration answered `RejectedNotPrimary` (typed, terminal) is sent again exactly
    /// once, when the authority's down op addresses this admin, and not before.
    #[test]
    fn a_typed_refusal_is_sent_again_once_when_the_primary_addresses_this_admin() {
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

    /// CONTRACT (i146 e3 owner 3, the control; node-rpc.md §43): a failed delivery is the delivery
    /// layer's. A target that goes unreachable is sent nothing new, and when it is reachable again the
    /// declarer sends nothing again either: a key is sent again only when its authority addresses
    /// this admin or the view names another birth. Must NOT happen: the declarer resending a key
    /// because its route returned (the retry loop the Owed call replaced).
    #[test]
    fn a_failed_delivery_is_the_layers_and_the_declarer_sends_it_again_only_to_a_new_destination() {
        let me_id = NodeId::mint();
        let mut v = view("mesh2.admin.1", "mesh1.admin.1");
        let none = BTreeSet::new();
        let d = Declarer::new();
        owe_all(&d);
        let fp = fabric_primary_of(&v);
        let typed_key = Key::OwnState(INC.into(), NodeState::ReadyForTraffic);
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
        assert!(!back.contains(&mesh_key()) && !back.contains(&typed_key), "a returned route sends nothing again: the layer owns the failed delivery: {back:?}");
        let addressed: BTreeSet<NodeId> = [fp.node_id.clone()].into();
        let asked: Vec<Key> = d.due(&me_id, &v, &addressed).into_iter().map(|(p, _)| p.key).collect();
        assert!(asked.contains(&mesh_key()), "the fabric-primary addressing this admin sends the key owed to it again: {asked:?}");
    }

    /// CONTRACT (node-rpc.md §43, i146 Q44: one Owed call per key, the layer resending it until the
    /// authority answers): a send decided from a `due` read taken before the key's call ended is not made
    /// once that end finished the key. The gate's `admin_start_declares` red: mesh1.admin.2's
    /// ReadyForTraffic declared twice 16 ms apart, `applied` then `already-applied`. Must NOT happen: the
    /// stale read sending the answered key again.
    #[test]
    fn a_send_decided_before_the_keys_call_ended_is_not_made_once_that_end_finished_it() {
        let me_id = NodeId::mint();
        let v = view("mesh2.admin.1", "mesh1.admin.1");
        let none = BTreeSet::new();
        let d = Declarer::new();
        owe_all(&d);
        let (p, target) = d.due(&me_id, &v, &none).into_iter().find(|(p, _)| p.key == mesh_key()).expect("the Mesh key is owed");
        // The key's call ends answered: its end is recorded, and its in-flight claim released.
        d.declared(&"mesh2.admin.1".parse().unwrap(), &p, &target, "node-rpc", false, "applied".into(), true, true);
        assert!(!d.still_due(&BTreeMap::new(), &p.key, &Destination::of(&target)), "the read taken before the end sends the answered key again");
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
        owed_from_view(&d, &me, &id_of(&me.to_string()), &IncarnationId::parse(INC).unwrap(), rdm_core::MemberStatus::ReadyForTraffic, false, None, &v, &ids);
        let due: Vec<(Key, String)> = d.due(&id_of(&me.to_string()), &v, &none).into_iter().map(|(p, to)| (p.key, to.name.to_string())).collect();
        assert_eq!(due, vec![(Key::OwnState(INC.into(), NodeState::ReadyForTraffic), "mesh1.admin.1".to_string())], "owed to mesh1's primary");
        let mesh1_primary = v.nodes.iter().find(|n| n.name.to_string() == "mesh1.admin.1").unwrap().clone();
        d.pending.lock().unwrap().remove(&Key::OwnState(INC.into(), NodeState::ReadyForTraffic));
        d.done.lock().unwrap().insert(Key::OwnState(INC.into(), NodeState::ReadyForTraffic), ("applied".into(), mesh1_primary.node_id.clone(), mesh1_primary.incarnation_id.clone()));
        let moved = view("mesh1.admin.1", "mesh1.admin.1");
        let moved = Topology { nodes: moved.nodes.into_iter().map(|mut n| { if n.name == mesh1_primary.name { n.incarnation_id = mesh1_primary.incarnation_id.clone(); } n }).collect(), ..moved };
        d.reowe_moved(&me, &moved);
        assert!(d.pending.lock().unwrap().is_empty(), "the fabric seat moved, its own mesh's primary did not: nothing is owed again");
    }

    /// CONTRACT (node-status.md "Status report": a report "names the operation that produced the
    /// state when one did": a stop's, a start's; i146 Q37, one rule for every node): a node-admin's
    /// stop owes its Leaving naming the stop, the next start's node.started names that start and
    /// nothing of the stopped cycle stays owed, and a state the admin measured itself (its birth's
    /// node.started, a ConnectionsDeleting) names none. Must NOT happen: an admin's start or stop declared as a
    /// state it measured, or a cycle's declaration naming the last cycle's operation.
    #[test]
    fn a_node_admins_own_declarations_name_the_operation_that_produced_them() {
        use rdm_core::MemberStatus as M;
        let me: PathName = "mesh1.admin.2".parse().unwrap();
        let me_id = id_of(&me.to_string());
        let inc = IncarnationId::parse(INC).unwrap();
        let v = view("mesh1.admin.1", "mesh2.admin.1");
        let ids: BTreeMap<String, String> = v.meshes.iter().map(|m| (m.name.clone(), m.id.clone().unwrap().to_string())).collect();
        let op = |operation: &str, attempt: u32| DeclaredOperation { build_id: "b".into(), attempt, operation: operation.into() };
        let operation_of = |d: &Declarer, state: NodeState| match d.requests.lock().unwrap().get(&Key::OwnState(INC.into(), state)) {
            Some((_, StatusRequest::DeclareNodeState { operation, .. }, _)) => operation.clone(),
            other => panic!("no own {state:?} declaration: {other:?}"),
        };
        let d = Declarer::new();
        owed_from_view(&d, &me, &me_id, &inc, M::ReadyForTraffic, false, None, &v, &ids);
        assert_eq!(operation_of(&d, NodeState::ReadyForTraffic), None, "its birth's node.started is a state it measured");

        let stop = op("node.stop:mesh1.admin.2", 1);
        d.owe_own(&me, &me_id, &inc, NodeState::Stopped, Some(stop.clone()));
        assert_eq!(operation_of(&d, NodeState::Stopped), Some(stop), "its stop's Stopped names the stop");
        let due: Vec<(Key, String)> = d.due(&me_id, &v, &BTreeSet::new()).into_iter().map(|(p, to)| (p.key, to.name.to_string())).collect();
        assert!(due.contains(&(Key::OwnState(INC.into(), NodeState::Stopped), "mesh1.admin.1".to_string())), "owed to its mesh's primary: {due:?}");

        let start = op("node.start:mesh1.admin.2", 2);
        d.begin_own_cycle(&me, INC, &start);
        owed_from_view(&d, &me, &me_id, &inc, M::ReadyForTraffic, false, None, &v, &ids);
        assert_eq!(operation_of(&d, NodeState::ReadyForTraffic), Some(start), "the start's node.started names the start");
        assert!(!d.requests.lock().unwrap().contains_key(&Key::OwnState(INC.into(), NodeState::Stopped)), "the stop's Stopped belongs to the stopped cycle");

        owed_from_view(&d, &me, &me_id, &inc, M::ConnectionsDeleting, false, None, &v, &ids);
        assert_eq!(operation_of(&d, NodeState::ConnectionsDeleting), None, "a ConnectionsDeleting it measured names none");
    }

    /// CONTRACT (node-status.md "Status report": "A node reports to its mesh's primary, which sets
    /// its status"; gossip.md §3.1: a Digest "carries no status"): a node-admin that drains (a drain
    /// command, or the fabric shutdown freezing it) owes its ConnectionsDeleting to its own mesh's primary beside
    /// its node.started, so the other admins hold it ConnectionsDeleting from that primary's projection. Must NOT
    /// happen: an admin deleting its connections whose ConnectionsDeleting no other admin can learn.
    #[test]
    fn a_connections_deleting_node_admin_reports_it_to_its_own_mesh_primary() {
        let me: PathName = "mesh1.admin.2".parse().unwrap();
        let v = view("mesh1.admin.1", "mesh2.admin.1");
        let ids: BTreeMap<String, String> = v.meshes.iter().map(|m| (m.name.clone(), m.id.clone().unwrap().to_string())).collect();
        let d = Declarer::new();
        owed_from_view(&d, &me, &id_of(&me.to_string()), &IncarnationId::parse(INC).unwrap(), rdm_core::MemberStatus::ConnectionsDeleting, false, None, &v, &ids);
        let due: Vec<(Key, String)> = d.due(&id_of(&me.to_string()), &v, &BTreeSet::new()).into_iter().map(|(p, to)| (p.key, to.name.to_string())).collect();
        assert_eq!(
            due,
            vec![(Key::OwnState(INC.into(), NodeState::ReadyForTraffic), "mesh1.admin.1".to_string()), (Key::OwnState(INC.into(), NodeState::ConnectionsDeleting), "mesh1.admin.1".to_string())],
            "its node.started and its ConnectionsDeleting, each to mesh1's primary"
        );
    }

    /// CONTRACT (states.md "Who sets each state": node ReadyForTraffic is set by "the node's
    /// mesh-primary" from "the node's node.started report, after every start gate"; lifecycles/node-name-update.md:
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
        owed_from_view(&d, &me.renamed(), &me_id, &IncarnationId::parse(INC).unwrap(), rdm_core::MemberStatus::ReadyForTraffic, false, None, &v, &ids);
        let due: Vec<(Key, String)> = d.due(&me_id, &v, &BTreeSet::new()).into_iter().map(|(p, to)| (p.key, to.name.to_string())).collect();
        assert_eq!(due, vec![(Key::OwnState(INC.into(), NodeState::ReadyForTraffic), "mesh1.admin.1".to_string())], "its node.started, owed to mesh1's primary by its exact birth");
    }

    /// CONTRACT (wire.md `ApplyFabricEvent`: "the fabric-primary applies a Fabric event at a mesh primary";
    /// fabric-ready-for-traffic.md: "the fabric is ready-for-traffic"): the fabric-primary owes the
    /// ready-for-traffic fabric event to every mesh's primary once its view holds the fabric
    /// ready-for-traffic, its own mesh's primary (itself) included: the row that event leaves is the one
    /// durable fact that the fabric first reached ready-for-traffic, and the fabric-primary's mesh holds
    /// it as every other mesh does. Must NOT happen: the event owed to every mesh but the fabric-primary's own.
    #[test]
    fn the_fabric_primary_owes_the_ready_for_traffic_event_to_every_mesh_primary_its_own_included() {
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let v = view("mesh1.admin.1", "mesh1.admin.1");
        let ids: BTreeMap<String, String> = v.meshes.iter().map(|m| (m.name.clone(), m.id.clone().unwrap().to_string())).collect();
        let none = BTreeSet::new();
        let d = Declarer::new();
        owed_from_view(&d, &me, &id_of(&me.to_string()), &IncarnationId::parse(INC).unwrap(), rdm_core::MemberStatus::ReadyForTraffic, false, None, &v, &ids);
        let mut events: Vec<(Key, String)> = d.due(&id_of(&me.to_string()), &v, &none).into_iter().filter(|(p, _)| matches!(p.key, Key::FabricEventAt(..))).map(|(p, to)| (p.key, to.name.to_string())).collect();
        events.sort();
        assert_eq!(
            events,
            vec![
                (Key::FabricEventAt("mesh1".into(), "ready-for-traffic".into()), "mesh1.admin.1".to_string()),
                (Key::FabricEventAt("mesh2".into(), "ready-for-traffic".into()), "mesh2.admin.1".to_string()),
            ],
            "the event is owed at each mesh's primary, the fabric-primary's own mesh included"
        );
    }

    /// CONTRACT (states.md "A resize or roll puts the fabric back to pending"): once the fabric has first
    /// reached ready-for-traffic, the fabric-primary owes the ready-for-traffic event to every mesh primary
    /// the view names, whatever status the fabric holds now: a mesh a resize adds while the fabric is back at
    /// pending is told too. Before the fabric first reached ready-for-traffic, nothing is owed.
    /// Must NOT happen: a mesh created in a resize left with no row of the fact until the fabric climbs again.
    #[test]
    fn the_fabric_primary_owes_the_event_to_a_mesh_a_resize_adds_while_the_fabric_is_back_at_pending() {
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let mut v = view("mesh1.admin.1", "mesh1.admin.1");
        v.fabric.status = ScopeStatus::Pending;
        let ids: BTreeMap<String, String> = v.meshes.iter().map(|m| (m.name.clone(), m.id.clone().unwrap().to_string())).collect();
        let none = BTreeSet::new();
        let events = |d: &Declarer| -> Vec<Key> { d.due(&id_of(&me.to_string()), &v, &none).into_iter().map(|(p, _)| p.key).filter(|k| matches!(k, Key::FabricEventAt(..))).collect() };

        let before = Declarer::new();
        owed_from_view(&before, &me, &id_of(&me.to_string()), &IncarnationId::parse(INC).unwrap(), rdm_core::MemberStatus::ReadyForTraffic, false, None, &v, &ids);
        assert!(events(&before).is_empty(), "the fabric has not first reached ready-for-traffic: no event is owed");

        let first_ready = crate::fabric_state::FirstReady::new();
        first_ready.reach("the test fabric reached ready-for-traffic");
        let after = Declarer::with_first_ready(first_ready);
        owed_from_view(&after, &me, &id_of(&me.to_string()), &IncarnationId::parse(INC).unwrap(), rdm_core::MemberStatus::ReadyForTraffic, false, None, &v, &ids);
        let mut owed = events(&after);
        owed.sort();
        assert_eq!(owed, vec![Key::FabricEventAt("mesh1".into(), "ready-for-traffic".into()), Key::FabricEventAt("mesh2".into(), "ready-for-traffic".into())], "owed at each mesh primary though the fabric is back at pending");
    }

    /// CONTRACT (states.md "The initial fabric before ready-for-traffic": the ready-for-traffic event's row
    /// among the mesh's status rows is the durable fact; "A resize or roll puts the fabric back to
    /// pending"): a fabric-primary whose latch was never set and whose view holds the fabric at pending
    /// (an outside successor of the fabric seat that has not heard ready) reads its mesh's status rows on
    /// the store root before it derives what it owes. Over rows with no ready-for-traffic row of its fabric
    /// nothing is owed. Once the rows hold it, the read answers that the fabric first reached
    /// ready-for-traffic and the fabric-primary owes the event to every mesh primary its view names,
    /// though it still hears pending. Must NOT happen: a fabric-primary owing nothing while the root holds
    /// the fabric's ready-for-traffic row.
    #[tokio::test]
    async fn a_fabric_primary_that_never_heard_ready_owes_the_event_on_the_ready_row_on_the_store_root() {
        use crate::fabric_storage::{FabricRecord, FabricStorage, MemoryFabricStorage};
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let mut v = view("mesh1.admin.1", "mesh1.admin.1");
        v.fabric.status = ScopeStatus::Pending;
        let ids: BTreeMap<String, String> = v.meshes.iter().map(|m| (m.name.clone(), m.id.clone().unwrap().to_string())).collect();
        let none = BTreeSet::new();
        let events = |d: &Declarer| -> Vec<Key> {
            let mut k: Vec<Key> = d.due(&id_of(&me.to_string()), &v, &none).into_iter().map(|(p, _)| p.key).filter(|k| matches!(k, Key::FabricEventAt(..))).collect();
            k.sort();
            k
        };
        let rows = MemoryFabricStorage::new();
        let d = Declarer::new();
        assert!(!d.read_first_ready(&rows, &v).await.unwrap(), "no ready-for-traffic row of the fabric on the root");
        owed_from_view(&d, &me, &id_of(&me.to_string()), &IncarnationId::parse(INC).unwrap(), rdm_core::MemberStatus::ReadyForTraffic, false, None, &v, &ids);
        assert!(events(&d).is_empty(), "nothing owed before the fabric first reached ready-for-traffic");
        rows.put_fabric(&FabricRecord { fabric_id: v.fabric.id.clone(), name: v.fabric.name.clone(), events: vec!["ready-for-traffic".into()] }).await.unwrap();
        assert!(d.read_first_ready(&rows, &v).await.unwrap(), "the root's row says the fabric first reached ready-for-traffic");
        owed_from_view(&d, &me, &id_of(&me.to_string()), &IncarnationId::parse(INC).unwrap(), rdm_core::MemberStatus::ReadyForTraffic, false, None, &v, &ids);
        assert_eq!(events(&d), vec![Key::FabricEventAt("mesh1".into(), "ready-for-traffic".into()), Key::FabricEventAt("mesh2".into(), "ready-for-traffic".into())], "owed at each mesh primary, decided on the root's row");
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
        owed_from_view(&d, &me, &id_of(&me.to_string()), &IncarnationId::parse(INC).unwrap(), rdm_core::MemberStatus::Pending, false, None, &v, &ids);
        assert!(d.due(&NodeId::mint(), &v, &none).is_empty(), "not ready, gate closed: nothing is owed");
        owed_from_view(&d, &me, &id_of(&me.to_string()), &IncarnationId::parse(INC).unwrap(), rdm_core::MemberStatus::ReadyForTraffic, false, None, &v, &ids);
        let own: Vec<Key> = d.due(&NodeId::mint(), &v, &none).into_iter().map(|(p, _)| p.key).collect();
        assert_eq!(own, vec![Key::OwnState(INC.into(), NodeState::ReadyForTraffic)], "own readiness owes its birth");
        owed_from_view(&d, &me, &id_of(&me.to_string()), &IncarnationId::parse(INC).unwrap(), rdm_core::MemberStatus::ReadyForTraffic, true, None, &v, &ids);
        let both = d.due(&NodeId::mint(), &v, &none).len();
        assert_eq!(both, 2, "the gate opening owes the Mesh declaration");
    }

    /// CONTRACT (Q144; lifecycles/states.md "Who sets each state", mesh ReadyForTraffic: "the
    /// fabric-primary decides and answers `Applied`; the mesh-primary records it on its mesh's record, and
    /// the state is durable at that write; the declaration stays owed until then and is answered
    /// `Applied` again if repeated"): a mesh-primary answered `Applied` whose Mesh record write does not
    /// land (killed between the answer and its write) keeps its declaration owed; the next answer,
    /// `Applied` again, is written on the record and finishes it. Must NOT happen: the declaration
    /// finished on the answer alone, so a lost write leaves the state answered and never durable.
    #[tokio::test]
    async fn a_mesh_ready_answered_stays_owed_until_its_mesh_record_write_lands() {
        let d = Declarer::new();
        let landed: Arc<std::sync::Mutex<Vec<(crate::model::MeshId, MeshState)>>> = Default::default();
        let refuse = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let (l, r) = (landed.clone(), refuse.clone());
        let _ = d.record_mesh.set(RecordMesh(Arc::new(move |id, state| {
            let (l, r) = (l.clone(), r.clone());
            Box::pin(async move {
                if r.load(std::sync::atomic::Ordering::SeqCst) {
                    return Err("the writer was lost before its write".to_string());
                }
                l.lock().unwrap().push((id, state));
                Ok(())
            })
        })));
        let mesh_id = crate::model::MeshId::mint();
        let request = StatusRequest::DeclareMeshState { mesh_id: mesh_id.clone(), state: MeshState::ReadyForTraffic };
        let p = Pending { key: Key::Mesh(mesh_id.to_string(), MeshState::ReadyForTraffic), to: Authority::FabricPrimary, request, attempts: 0, last: None, sent: None, trace: None };
        let applied = StatusReply::Applied;
        let (outcome, finished) = d.recorded(&p, &applied, classify(&applied)).await;
        assert!(!finished && outcome.contains("did not land"), "answered, not written: still owed: {outcome}");
        assert!(landed.lock().unwrap().is_empty());
        refuse.store(false, std::sync::atomic::Ordering::SeqCst);
        let (outcome, finished) = d.recorded(&p, &applied, classify(&applied)).await;
        assert!(finished, "the next Applied is written and finishes the declaration: {outcome}");
        assert_eq!(*landed.lock().unwrap(), vec![(mesh_id, MeshState::ReadyForTraffic)], "the state is on the mesh's own record");
    }

    /// CONTRACT (states.md: "One commit stores the row, applies it, gossips it and answers the op";
    /// node-rpc-envelope.md: "`DeclareNodeState` and `DeclareMeshState` report committed state
    /// upward"): a mesh primary whose own Mesh status row is committed Leaving (delete-mesh admitted)
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
        owed_from_view(&d, &me, &id_of(&me.to_string()), &IncarnationId::parse(INC).unwrap(), rdm_core::MemberStatus::Pending, false, Some(MeshState::Leaving), &v, &ids);
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
        owed_from_view(&d, &me, &id_of(&me.to_string()), &IncarnationId::parse(INC).unwrap(), rdm_core::MemberStatus::Pending, true, Some(MeshState::Leaving), &v, &ids);
        let keys: Vec<Key> = d.due(&id_of(&me.to_string()), &v, &none).into_iter().map(|(p, _)| p.key).collect();
        assert_eq!(keys, vec![Key::Mesh(mesh2.to_string(), MeshState::Leaving)], "a primary holding Leaving owes no ready-for-traffic declaration");
        let other: PathName = "mesh1.admin.2".parse().unwrap();
        let d = Declarer::new();
        owed_from_view(&d, &other, &id_of(&other.to_string()), &IncarnationId::parse(INC).unwrap(), rdm_core::MemberStatus::Pending, false, Some(MeshState::Leaving), &v, &ids);
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
    fn a_re_born_admin_whose_view_names_its_superseded_birth_primary_owes_nothing_as_primary() {
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let mut v = view("mesh1.admin.1", "mesh1.admin.1");
        let retired = v.nodes.iter_mut().find(|n| n.name == me).unwrap();
        retired.status = NodeStatus::ConnectionsDeleting;
        let retired = retired.clone();
        let mut reborn = node("mesh1.admin.1", false, false);
        reborn.node_id = NodeId::mint();
        assert_ne!(retired.node_id, reborn.node_id, "a replace mints a new node id");
        assert_eq!(v.fabric_primary().map(|n| n.node_id.clone()), Some(retired.node_id.clone()), "the retired birth holds the fabric seat in this view");
        let ids: BTreeMap<String, String> = v.meshes.iter().map(|m| (m.name.clone(), m.id.clone().unwrap().to_string())).collect();
        let d = Declarer::new();
        owed_from_view(&d, &me, &reborn.node_id, &IncarnationId::parse(INC).unwrap(), rdm_core::MemberStatus::Pending, true, None, &v, &ids);
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
