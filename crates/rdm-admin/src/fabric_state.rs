//! The fabric states and the round checklist (states.md; fabric-state-sync.md,
//! fabric-state-commit.md, lifecycles/fabric-ready-for-traffic.md).
//!
//! A fabric is `pending`, then `state-sync` once every mesh is ready-for-traffic, then
//! `state-commit` once the application answers state-synced, then `ready-for-traffic` once the
//! commit-state and open-traffic rounds have both completed. Only the fabric-primary moves it.
//!
//! A round is a command pair, fabric-primary → mesh-primary → member, and its completion pair back
//! up. Each primary keeps a checklist of the planned births of the accepted Build; a member counts
//! only when it checks in as its planned birth, after the primary received this round's down op.
//! Nothing leaves a checklist by a timer.

use crate::model::{IncarnationId, Node, NodeId, NodeStatus};
use crate::status_declare::Destination;

/// The last send of one round command. A round command still rides a bounded call, so a failed
/// delivery waits here for a viable route (the declarer's declarations ride Owed calls and keep no
/// route state, i146 e3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Sent {
    pub to: Destination,
    /// A typed reply came back: terminal until an eligible event.
    pub typed: bool,
    /// The view showed the target unreachable after an untyped failure: a viable route is a gain.
    pub route_lost: bool,
}

/// Is a round command due again? When the birth addressed this admin, the destination changed to a
/// deliverable birth, or a failed delivery gained a viable route. A target that is not deliverable is
/// sent nothing.
fn eligible(sent: Option<&Sent>, now: &Destination, deliverable: bool, addressed: bool) -> bool {
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
use crate::topology::Topology;
use crate::round::Missing;
use rdm_core::status::{NotAuthority, StatusReply, StatusRequest};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;
use tokio::sync::watch;

/// The state of the fabric: the value of a FabricStatus frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum FabricState {
    /// Created; a mesh is not yet ready-for-traffic.
    #[default]
    Pending,
    /// Every mesh is ready-for-traffic; the application does its work.
    StateSync,
    /// The application answered state-synced; the scratchpads are published and traffic is opened.
    StateCommit,
    /// Both rounds completed.
    ReadyForTraffic,
}

impl FabricState {
    /// The name the FabricStatus frame, the control API and the spans carry.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::StateSync => "state-sync",
            Self::StateCommit => "state-commit",
            Self::ReadyForTraffic => "ready-for-traffic",
        }
    }

    /// The state a FabricStatus frame's value names, when it names one.
    pub fn parse(s: &str) -> Option<Self> {
        [Self::Pending, Self::StateSync, Self::StateCommit, Self::ReadyForTraffic].into_iter().find(|f| f.as_str() == s)
    }

    /// The view's scope status for this state.
    pub fn scope(self) -> crate::model::ScopeStatus {
        use crate::model::ScopeStatus as S;
        match self {
            Self::Pending => S::Pending,
            Self::StateSync => S::StateSync,
            Self::StateCommit => S::StateCommit,
            Self::ReadyForTraffic => S::ReadyForTraffic,
        }
    }
}

/// Whether the initial fabric has first reached ready-for-traffic (states.md, "The initial fabric
/// before ready-for-traffic"). Once true it stays true: a resize or roll puts the fabric back to
/// pending, and the rows it already published to the store root stay there. One handle is shared by
/// everything on an admin that refuses a workflow until it is true.
///
/// The durable fact is the ready-for-traffic Fabric event's row (`crate::status_storage::FabricEventRow`).
/// The initial fabric first reaching ready-for-traffic is the fabric-primary's one fact (states.md: "The
/// fabric-primary declares fabric state through the completed rounds"): it puts the row among its own
/// mesh's status rows as the first act of entering ready-for-traffic, before it latches or admits
/// anything, then applies the event at every other mesh primary. An admin of any mesh reads the row among
/// its own mesh's rows, else among the fabric-primary's mesh's rows on the store root, so a command the
/// fabric-primary admitted once the fabric was ready finds the fact wherever it lands. The Fabric row on
/// the store root is published at the first state-commit, before that, and is not the signal.
#[derive(Debug, Clone, Default)]
pub struct FirstReady(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl FirstReady {
    /// A fabric that has not reached ready-for-traffic.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that the fabric is ready-for-traffic, and why this admin knows: `because` is told once, the
    /// first time (`rdm.node_admin.fabric.update.via-first-ready`).
    pub fn reach(&self, because: &'static str) {
        if !self.0.swap(true, std::sync::atomic::Ordering::SeqCst) {
            tracing::info_span!("rdm.node_admin.fabric.update.via-first-ready", reason = because, "otel.kind" = "internal")
                .in_scope(|| tracing::info!("this admin knows the fabric has first reached ready-for-traffic: workflows a caller submits are admitted as usual"));
        }
    }

    /// Read the durable fact once: the status rows of this admin's mesh, else those of the
    /// fabric-primary's mesh `fabric_primary_mesh` (the mesh the seat book names the fabric seat's holder
    /// in; `None` when it names none), hold the ready-for-traffic event of `fabric_id`. A read that fails
    /// is the caller's to refuse by name.
    pub async fn read_from(&self, status: &dyn crate::status_storage::StatusStorage, fabric_id: &crate::model::FabricId, fabric_primary_mesh: Option<&str>) -> Result<(), crate::record_store::StorageError> {
        let ready = crate::status_rpc::event_key(&rdm_core::status::FabricEvent::ReadyForTraffic);
        let holds = |rows: &[crate::status_storage::FabricEventRow]| rows.iter().any(|row| row.fabric_id == *fabric_id && row.event == ready);
        if holds(&status.fabric_events().await?) {
            self.reach(READY_ROW_ON_ROOT);
            return Ok(());
        }
        if let Some(mesh) = fabric_primary_mesh {
            if holds(&status.fabric_events_of(mesh).await?) {
                self.reach(READY_ROW_OF_FABRIC_PRIMARY);
            }
        }
        Ok(())
    }

    /// Whether the fabric has reached ready-for-traffic at some time, or holds it `now`.
    pub fn reached(&self, now: crate::model::ScopeStatus) -> bool {
        self.0.load(std::sync::atomic::Ordering::SeqCst) || matches!(now, crate::model::ScopeStatus::ReadyForTraffic | crate::model::ScopeStatus::Degraded)
    }

    /// Whether the initial fabric `fabric_id` has first reached ready-for-traffic, decided on the durable
    /// fact (states.md, "The initial fabric before ready-for-traffic"): what this admin already knows (or
    /// `now`, its view's fabric status), else a point read of the ready-for-traffic row among `status`, its
    /// mesh's status rows on the store root, else among the rows of the fabric-primary's mesh
    /// `fabric_primary_mesh`, which hold the fact from the moment the fabric-primary entered
    /// ready-for-traffic, while the fabric state this admin hears by gossip, and its own mesh's row, can
    /// still lag it. A read that fails is the caller's to refuse by name.
    pub async fn reached_on_root(&self, status: &dyn crate::status_storage::StatusStorage, fabric_id: &crate::model::FabricId, now: crate::model::ScopeStatus, fabric_primary_mesh: Option<&str>) -> Result<bool, crate::record_store::StorageError> {
        if self.reached(now) {
            return Ok(true);
        }
        self.read_from(status, fabric_id, fabric_primary_mesh).await?;
        Ok(self.reached(now))
    }
}

/// Why an admin knows the fabric first reached ready-for-traffic when its root holds the fact.
pub const READY_ROW_ON_ROOT: &str = "ready-for-traffic-row-on-store-root";

/// Why an admin knows the fabric first reached ready-for-traffic when the fabric-primary's mesh's status
/// rows on the root hold the row the fabric-primary put as it entered ready-for-traffic, before the
/// admin's own mesh's row is put.
pub const READY_ROW_OF_FABRIC_PRIMARY: &str = "ready-for-traffic-row-of-the-fabric-primary-on-store-root";

pub use rdm_node::rounds::{hook_frame, RoundKey, RoundKind, RoundOp};

/// A planned birth a checklist names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Expected {
    /// The planned birth's incarnation as the primary's view holds it now.
    pub incarnation: IncarnationId,
    /// Its `path.name`.
    pub name: String,
}

/// Where one planned birth's down op stands.
#[derive(Debug, Clone)]
enum Command {
    /// The call is out: nothing is sent while it is. `addressed`: the target addressed this primary
    /// while it was out, an eligible event the call does not consume: if the call ends undelivered,
    /// the command is due again on it. `route_lost`: the target became unreachable in the view while
    /// the call was out, so if the call ends without a reply it is due again once the target is
    /// reachable again.
    Out { to: Destination, status: NodeStatus, addressed: bool, route_lost: bool },
    /// The birth answered (it admitted the op or refused it by name): the op is never sent again.
    Answered,
    /// The call was not delivered, or the birth said it is not ready. Sent again only on an eligible
    /// event (R-S2): the target is reachable again after its route was lost, the destination moved
    /// (a new endpoint, address or incarnation), the target's state changed, or the target addressed
    /// this admin.
    Undelivered { sent: Sent, status: NodeStatus, addressed: bool },
}

#[derive(Debug, Default)]
struct Round {
    expected: BTreeMap<NodeId, Expected>,
    /// Planned births the primary's view holds no birth of yet: each blocks completion by name.
    unresolved: Vec<Missing>,
    checked: HashSet<(NodeId, IncarnationId)>,
    commands: HashMap<(NodeId, IncarnationId), Command>,
}

/// The rounds one primary has received the down op of, with their checklists. A change to any
/// checklist, or a birth with an undelivered command addressing this primary, moves `tick`: the
/// waiter arms on it before it reads, so a change that happened during the read is not lost.
#[derive(Debug, Default)]
pub struct RoundBook {
    rounds: Mutex<HashMap<RoundKey, Round>>,
    tick: watch::Sender<u64>,
}

fn deliverable(n: &Node) -> bool {
    matches!(n.status, NodeStatus::ReadyForTraffic | NodeStatus::Draining) && n.incarnation_id.is_some()
}

impl RoundBook {
    /// Open the round `key`: the primary received (or issued) its down op. Whether it is new.
    pub fn open(&self, key: &RoundKey) -> bool {
        let new = !self.rounds.lock().unwrap().contains_key(key);
        if new {
            self.rounds.lock().unwrap().insert(key.clone(), Round::default());
        }
        new
    }

    /// The rounds this book holds.
    pub fn keys(&self) -> Vec<RoundKey> {
        self.rounds.lock().unwrap().keys().cloned().collect()
    }

    /// Forget every round: the tenure that opened them ended.
    pub fn clear(&self) {
        self.rounds.lock().unwrap().clear();
    }

    /// Whether the round is open.
    pub fn is_open(&self, key: &RoundKey) -> bool {
        self.rounds.lock().unwrap().contains_key(key)
    }

    /// Carry an ordinary node's round check-in only while this mesh-admin holds the round open.
    /// Receipt never drives this checklist. The caller supplies the existing backbone publication;
    /// its named outcome is returned to the hook's receive span (Q21: x).
    pub fn carry_check_in(&self, frame: &rdm_transport::membership::Frame, carry: impl FnOnce(&rdm_transport::membership::Frame)) -> rdm_transport::membership::CompletionCarry {
        use rdm_transport::membership::{CompletionCarry, Frame};
        let (hook, kind) = match frame.data() {
            Frame::StateCommitted { hook, .. } => (hook, RoundKind::StateCommit),
            Frame::TrafficOpened { hook, .. } => (hook, RoundKind::OpenTraffic),
            _ => return CompletionCarry::NotHeld,
        };
        match hook.publisher.parse::<crate::model::PathName>() {
            Ok(author) if author.kind == crate::model::NodeKind::NodeAdmin => return CompletionCarry::NodeAdmin,
            Err(_) => return CompletionCarry::NotOrdinary,
            _ => {}
        }
        let key = RoundKey { kind, fabric_id: hook.fabric_id.clone(), build_id: hook.build_id.clone(), attempt: hook.attempt };
        if !self.is_open(&key) {
            return CompletionCarry::ClosedRound;
        }
        carry(frame);
        CompletionCarry::Carried
    }

    /// Forget the round: its completion went up.
    pub fn close(&self, key: &RoundKey) {
        self.rounds.lock().unwrap().remove(key);
    }

    /// Replace the planned births the round's checklist names. A check-in already held for a birth
    /// that is no longer planned stays held but counts for nothing.
    /// `unresolved` are planned births with no birth in the view: they block completion.
    pub fn expect(&self, key: &RoundKey, planned: BTreeMap<NodeId, Expected>, unresolved: Vec<Missing>) {
        let moved = match self.rounds.lock().unwrap().get_mut(key) {
            Some(r) if r.expected != planned || r.unresolved != unresolved => {
                r.expected = planned;
                r.unresolved = unresolved;
                true
            }
            _ => false,
        };
        if moved {
            self.bump();
        }
    }

    /// The planned births whose down op is due now, marked out. A birth never commanded is due once it
    /// is deliverable in `view`. A command that was not delivered is due again only on an eligible
    /// event ([`eligible`], R-S2, and a change of the target's state): reading the checklist again
    /// is not one.
    pub fn take_due(&self, key: &RoundKey, view: &Topology) -> Vec<(NodeId, Expected, &'static str)> {
        let mut rounds = self.rounds.lock().unwrap();
        let Some(r) = rounds.get_mut(key) else { return Vec::new() };
        let mut due = Vec::new();
        for (n, e) in &r.expected {
            let target = view.members().find(|t| &t.node_id == n);
            let slot = (n.clone(), e.incarnation.clone());
            // The event that makes the op due, named: `first` for a birth never commanded.
            let event: Option<&'static str> = match (r.commands.get_mut(&slot), target) {
                (None, Some(t)) => deliverable(t).then_some("first"),
                (None, None) => None,
                (Some(Command::Answered), _) => None,
                (Some(Command::Out { route_lost, .. }), target) => {
                    if target.is_none_or(|t| !deliverable(t)) {
                        *route_lost = true;
                    }
                    None
                }
                (Some(Command::Undelivered { sent, .. }), None) => {
                    if !sent.typed {
                        sent.route_lost = true;
                    }
                    None
                }
                (Some(Command::Undelivered { sent, status, addressed }), Some(t)) => {
                    let ok = deliverable(t);
                    if !sent.typed && !ok {
                        sent.route_lost = true;
                    }
                    let now = Destination::of(t);
                    if !(eligible(Some(sent), &now, ok, *addressed) || (ok && t.status != *status)) {
                        None
                    } else if *addressed {
                        Some("addressed")
                    } else if sent.to != now {
                        Some("destination-changed")
                    } else if !sent.typed && sent.route_lost {
                        Some("reachable-again")
                    } else {
                        Some("state-changed")
                    }
                }
            };
            if let Some(event) = event {
                let t = target.expect("a due birth is in the view");
                r.commands.insert(slot, Command::Out { to: Destination::of(t), status: t.status, addressed: false, route_lost: false });
                due.push((n.clone(), e.clone(), event));
            }
        }
        due
    }

    /// The down op to this birth was answered, by name: it is not sent again.
    pub fn answered(&self, key: &RoundKey, node_id: &NodeId, incarnation: &IncarnationId) {
        if let Some(r) = self.rounds.lock().unwrap().get_mut(key) {
            r.commands.insert((node_id.clone(), incarnation.clone()), Command::Answered);
        }
    }

    /// The down op to this birth was not delivered (`typed`: the birth replied that it is not ready;
    /// otherwise no reply came): it waits for an eligible event. The waiter is woken so the current
    /// view is checked against what was sent (an address, route loss, or state/destination change
    /// while the call was out).
    pub fn undelivered(&self, key: &RoundKey, node_id: &NodeId, incarnation: &IncarnationId, typed: bool) -> bool {
        let recorded = match self.rounds.lock().unwrap().get_mut(key) {
            Some(r) => {
                let slot = (node_id.clone(), incarnation.clone());
                match r.commands.get(&slot).cloned() {
                    Some(Command::Out { to, status, addressed, route_lost }) => {
                        r.commands.insert(
                            slot,
                            Command::Undelivered {
                                sent: Sent { to, typed, route_lost: !typed && route_lost },
                                status,
                                addressed,
                            },
                        );
                        true
                    }
                    _ => false,
                }
            }
            None => false,
        };
        if recorded {
            self.bump();
        }
        recorded
    }

    /// `node_id` addressed this primary (any Status call from it): an undelivered command to it is
    /// eligible to be sent again, and so is one out now, should that call end undelivered.
    pub fn heard_from(&self, node_id: &NodeId) {
        let mut any = false;
        for r in self.rounds.lock().unwrap().values_mut() {
            for ((n, _), c) in r.commands.iter_mut() {
                if let (true, Command::Undelivered { addressed, .. } | Command::Out { addressed, .. }) = (n == node_id, c) {
                    *addressed = true;
                    any = true;
                }
            }
        }
        if any {
            self.bump();
        }
    }

    /// The tick a waiter arms on: it moves when a checklist or an undelivered command's eligibility may have.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.tick.subscribe()
    }

    fn bump(&self) {
        self.tick.send_modify(|v| *v += 1);
    }

    /// Record the check-in of the planned birth `node_id`/`incarnation` without a call (a primary's
    /// own mesh, which it completes itself).
    pub fn check_in_local(&self, key: &RoundKey, node_id: &NodeId, incarnation: &IncarnationId) {
        if let Some(r) = self.rounds.lock().unwrap().get_mut(key) {
            r.checked.insert((node_id.clone(), incarnation.clone()));
        }
        self.bump();
    }

    /// Whether every planned birth has checked in as its planned birth. An empty checklist is complete.
    pub fn complete(&self, key: &RoundKey) -> bool {
        self.missing(key).is_empty()
    }

    /// The planned births that have not checked in, and why.
    pub fn missing(&self, key: &RoundKey) -> Vec<Missing> {
        let rounds = self.rounds.lock().unwrap();
        let Some(r) = rounds.get(key) else { return vec![Missing { who: key.kind.command().into(), why: "round-not-open" }] };
        r.unresolved
            .iter()
            .cloned()
            .chain(
                r.expected
                    .iter()
                    .filter(|(n, e)| !r.checked.contains(&((*n).clone(), e.incarnation.clone())))
                    .map(|(n, e)| Missing { who: e.name.clone(), why: if r.checked.iter().any(|(c, _)| c == n) { "checked-in-as-another-birth" } else { "no-check-in" } }),
            )
            .collect()
    }

    /// The check-in `req` makes, decided by this primary for the subject it names, whoever calls
    /// (node-rpc.md §14). `subject` is that birth as this primary's view holds it: `(incarnation
    /// held for it, path.name)`, `None` when the view holds no such node. A refusal names the one
    /// fact that is wrong, `NotReady` for a round this primary does not hold open (an unmatched round
    /// is never a check-in).
    pub fn check_in(&self, receiver: &str, subject: Option<(Option<&IncarnationId>, &str)>, req: &StatusRequest) -> StatusReply {
        let Some(op) = RoundOp::of(req).filter(|o| !o.is_down) else { return rdm_core::reply::status::not_ready(format!("{receiver}: {} is not a completion call", req.op())) };
        let completion = op.kind.completion();
        let Some((held, from_name)) = subject else {
            return rdm_core::reply::status::rejected_not_authority(NotAuthority::SubjectUnknown);
        };
        match held {
            Some(held) if held != op.incarnation => return rdm_core::reply::stale(held.clone()),
            None => return rdm_core::reply::status::rejected_not_authority(NotAuthority::SubjectUnknown),
            Some(_) => {}
        }
        let key = op.key();
        let mut rounds = self.rounds.lock().unwrap();
        let reply = match rounds.get_mut(&key) {
            None => {
                let near = rounds.iter().filter(|(k, _)| k.kind == op.kind).min_by_key(|(k, _)| (k.fabric_id != *op.fabric_id, k.build_id != op.build_id, k.attempt != op.attempt));
                let why = match near {
                    None => format!("{receiver} holds no open {} round; {completion} from {from_name} counts for nothing", op.kind.command()),
                    Some((k, _)) if k.fabric_id != *op.fabric_id => format!("{receiver}: {completion} from {from_name}: field fabric_id: round holds {}, reported {}", k.fabric_id, op.fabric_id),
                    Some((k, _)) if k.build_id != op.build_id => format!("{receiver}: {completion} from {from_name}: field build_id: round holds {}, reported {}", k.build_id, op.build_id),
                    Some((k, _)) => format!("{receiver}: {completion} from {from_name}: field attempt: round holds {}, reported {}", k.attempt, op.attempt),
                };
                rdm_core::reply::status::not_ready(why)
            }
            Some(r) => {
                let want = op.kind.operation(&key.fabric_id);
                if op.operation != want {
                    rdm_core::reply::status::not_ready(format!("{receiver}: {completion} from {from_name}: field operation: round holds {want}, reported {}", op.operation))
                } else {
                    match r.expected.get(op.node_id) {
                        None => rdm_core::reply::status::not_ready(format!("{receiver}: {completion} from {from_name}: not on this round's checklist")),
                        Some(e) if e.incarnation != *op.incarnation => {
                            rdm_core::reply::status::not_ready(format!("{receiver}: {completion} from {from_name}: field incarnation: the checklist plans birth {}, reported {}", e.incarnation, op.incarnation))
                        }
                        Some(_) => {
                            if r.checked.insert((op.node_id.clone(), op.incarnation.clone())) {
                                rdm_core::reply::applied()
                            } else {
                                rdm_core::reply::already_applied()
                            }
                        }
                    }
                }
            }
        };
        drop(rounds);
        if matches!(reply, StatusReply::Applied) {
            self.bump();
        }
        reply
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::FabricId;

    fn key(build: &str, attempt: u32, f: &FabricId) -> RoundKey {
        RoundKey { kind: RoundKind::StateCommit, fabric_id: f.clone(), build_id: build.into(), attempt }
    }

    fn planned(n: &NodeId, i: &IncarnationId, name: &str) -> BTreeMap<NodeId, Expected> {
        BTreeMap::from([(n.clone(), Expected { incarnation: i.clone(), name: name.into() })])
    }

    /// CONTRACT: a check-in counts only for a planned birth, in an open round of the same Build and
    /// attempt, for the birth the receiver holds; every other case is refused naming the one fact
    /// that is wrong, and a repeat is already applied.
    #[test]
    fn a_check_in_counts_only_as_the_planned_birth_of_the_open_round() {
        let (fabric, node, inc) = (FabricId::mint(), NodeId::mint(), IncarnationId::mint());
        let book = RoundBook::default();
        let k = key("66h8qrqvaajk", 1, &fabric);
        let up = |build: &str, attempt: u32, inc: &IncarnationId| RoundKind::StateCommit.up(&key(build, attempt, &fabric), node.clone(), inc.clone());
        fn sender<'a>(_node: &'a NodeId, held: &'a IncarnationId) -> Option<(Option<&'a IncarnationId>, &'static str)> {
            Some((Some(held), "mesh2.admin.1"))
        }

        let early = book.check_in("mesh1.admin.1", sender(&node, &inc), &up("66h8qrqvaajk", 1, &inc));
        assert!(matches!(&early, StatusReply::NotReady { reason } if reason.contains("no open commit-state round")), "a check-in before the down op: {early:?}");

        book.open(&k);
        book.expect(&k, planned(&node, &inc, "mesh2.admin.1"), Vec::new());
        assert!(!book.complete(&k));
        assert_eq!(book.missing(&k), vec![Missing { who: "mesh2.admin.1".into(), why: "no-check-in" }]);

        let foreign = book.check_in("mesh1.admin.1", sender(&node, &inc), &up("t9ktgq6zvxtt", 1, &inc));
        assert!(matches!(&foreign, StatusReply::NotReady { reason } if reason.contains("field build_id: round holds 66h8qrqvaajk, reported t9ktgq6zvxtt")), "{foreign:?}");
        let old = book.check_in("mesh1.admin.1", sender(&node, &inc), &up("66h8qrqvaajk", 2, &inc));
        assert!(matches!(&old, StatusReply::NotReady { reason } if reason.contains("field attempt: round holds 1, reported 2")), "{old:?}");
        let other = IncarnationId::mint();
        assert_eq!(book.check_in("mesh1.admin.1", sender(&node, &other), &up("66h8qrqvaajk", 1, &inc)), StatusReply::RejectedStaleIncarnation { held: other.clone() });
        assert_eq!(book.check_in("mesh1.admin.1", None, &up("66h8qrqvaajk", 1, &inc)), StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown }, "a subject the view does not hold");

        assert_eq!(book.check_in("mesh1.admin.1", sender(&node, &inc), &up("66h8qrqvaajk", 1, &inc)), StatusReply::Applied);
        assert_eq!(book.check_in("mesh1.admin.1", sender(&node, &inc), &up("66h8qrqvaajk", 1, &inc)), StatusReply::AlreadyApplied);
        assert!(book.complete(&k));

        let reborn = IncarnationId::mint();
        book.expect(&k, planned(&node, &reborn, "mesh2.admin.1"), Vec::new());
        assert!(!book.complete(&k), "a rolled birth does not match its planned birth by the old birth's check-in");
        assert_eq!(book.missing(&k), vec![Missing { who: "mesh2.admin.1".into(), why: "checked-in-as-another-birth" }]);
    }

    /// CONTRACT (node-rpc.md §14: "Nothing inside the mesh checks who is calling"): a check-in is
    /// decided for the subject it names, as this primary's view holds it, never for its caller. A
    /// subject the view does not hold is the direction refusal `SubjectUnknown`, and nothing counts.
    /// What must NOT happen: `SenderNotSubject` "unknown peer".
    #[test]
    fn a_check_in_for_a_subject_the_view_does_not_hold_is_subject_unknown() {
        let (fabric, node, inc) = (FabricId::mint(), NodeId::mint(), IncarnationId::mint());
        let book = RoundBook::default();
        let k = key("66h8qrqvaajk", 1, &fabric);
        book.open(&k);
        book.expect(&k, planned(&node, &inc, "mesh2.admin.1"), Vec::new());
        let up = RoundKind::StateCommit.up(&k, node.clone(), inc.clone());
        assert_eq!(book.check_in("mesh1.admin.1", None, &up), StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown });
        assert!(!book.complete(&k), "nothing counted");
    }

    fn target(inc: &IncarnationId, status: NodeStatus) -> Node {
        let mut n = Node::allocated("mesh2.admin.1".parse().unwrap());
        n.status = status;
        n.incarnation_id = Some(inc.clone());
        n
    }

    fn view_of(nodes: Vec<Node>) -> Topology {
        Topology { fabric: crate::model::Fabric { id: FabricId::mint(), name: "fabric1".into(), status: crate::model::ScopeStatus::Pending, provider: crate::model::ProviderKind::Process }, meshes: Vec::new(), nodes }
    }

    /// A book holding an open round whose checklist names `n` (as `inc`), the command to it out and then not delivered.
    fn undelivered(typed: bool, status: NodeStatus) -> (RoundBook, RoundKey, Node, IncarnationId) {
        let (fabric, inc) = (FabricId::mint(), IncarnationId::mint());
        let (book, k, n) = (RoundBook::default(), key("66h8qrqvaajk", 1, &fabric), target(&inc, status));
        book.open(&k);
        book.expect(&k, planned(&n.node_id, &inc, "mesh2.admin.1"), Vec::new());
        assert_eq!(book.take_due(&k, &view_of(vec![n.clone()])).len(), 1);
        book.undelivered(&k, &n.node_id, &inc, typed);
        (book, k, n, inc)
    }

    /// CONTRACT: a planned birth is commanded once as its exact birth, once it is deliverable; a birth
    /// that replaced it is commanded as a new one; one that answered is never commanded again.
    #[test]
    fn a_planned_birth_is_commanded_once_per_exact_birth() {
        let (fabric, a, b) = (FabricId::mint(), IncarnationId::mint(), IncarnationId::mint());
        let book = RoundBook::default();
        let k = key("66h8qrqvaajk", 1, &fabric);
        let n = target(&a, NodeStatus::ReadyForTraffic);
        book.open(&k);
        book.expect(&k, planned(&n.node_id, &a, "mesh2.admin.1"), Vec::new());
        assert!(book.take_due(&k, &view_of(vec![])).is_empty(), "a birth the view does not hold is not sent to");
        let dead = Node { status: NodeStatus::Dead, ..n.clone() };
        assert!(book.take_due(&k, &view_of(vec![dead])).is_empty(), "a birth that is not live is not sent to");
        let view = view_of(vec![n.clone()]);
        assert_eq!(book.take_due(&k, &view).iter().map(|d| d.2).collect::<Vec<_>>(), ["first"]);
        assert!(book.take_due(&k, &view).is_empty(), "the call is out");
        book.answered(&k, &n.node_id, &a);
        assert!(book.take_due(&k, &view).is_empty(), "an answered op is not sent again");
        let reborn = Node { incarnation_id: Some(b.clone()), ..n.clone() };
        book.expect(&k, planned(&n.node_id, &b, "mesh2.admin.1"), Vec::new());
        assert_eq!(book.take_due(&k, &view_of(vec![reborn])).len(), 1, "the new birth is commanded as itself");
    }

    /// CONTRACT (R-S2): a command that was not delivered is sent again only on an eligible event, never
    /// because the checklist was read again. Must NOT happen: an undelivered command back in the
    /// next read of the checklist.
    #[test]
    fn an_undelivered_command_is_not_sent_again_by_reading_the_checklist_again() {
        for typed in [false, true] {
            let (book, k, n, _) = undelivered(typed, NodeStatus::ReadyForTraffic);
            let view = view_of(vec![n]);
            for _ in 0..3 {
                assert!(book.take_due(&k, &view).is_empty(), "typed={typed}: nothing happened to the target: the command is not sent again");
            }
        }
    }

    /// CONTRACT (R-S1, R-S2): a birth in `Pending` (still inside `node.start`) is not commanded by a
    /// round until it reaches `ReadyForTraffic`.
    #[test]
    fn a_pending_birth_is_not_commanded_until_it_reaches_ready_for_traffic() {
        let (fabric, inc) = (FabricId::mint(), IncarnationId::mint());
        let book = RoundBook::default();
        let k = key("66h8qrqvaajk", 1, &fabric);
        let n = target(&inc, NodeStatus::Pending);
        book.open(&k);
        book.expect(&k, planned(&n.node_id, &inc, "mesh2.admin.1"), Vec::new());
        assert!(book.take_due(&k, &view_of(vec![n.clone()])).is_empty(), "a Pending birth is not commanded before node.start completes");
        let ready = Node { status: NodeStatus::ReadyForTraffic, ..n };
        assert_eq!(book.take_due(&k, &view_of(vec![ready])).iter().map(|d| d.2).collect::<Vec<_>>(), ["first"], "commanded once ReadyForTraffic");
    }

    /// CONTRACT (R-S2): a command that never got a reply is sent again when the target becomes reachable
    /// again after its route was lost, and not while it stays unreachable.
    #[test]
    fn an_undelivered_command_is_sent_again_when_the_target_becomes_reachable() {
        let (book, k, n, _) = undelivered(false, NodeStatus::ReadyForTraffic);
        let gone = Node { status: NodeStatus::PendingReconnect, ..n.clone() };
        for _ in 0..2 {
            assert!(book.take_due(&k, &view_of(vec![gone.clone()])).is_empty(), "an unreachable target is sent nothing");
        }
        let back = view_of(vec![n.clone()]);
        assert_eq!(book.take_due(&k, &back).iter().map(|d| d.2).collect::<Vec<_>>(), ["reachable-again"], "reachable again: sent again");
        assert!(book.take_due(&k, &back).is_empty(), "and once");
    }

    /// CONTRACT (R-S2): a command that the birth refused as not ready is sent again when the birth's
    /// state changed, and when its destination moved, and not otherwise.
    #[test]
    fn an_undelivered_command_is_sent_again_when_the_targets_state_or_destination_changed() {
        let (book, k, n, inc) = undelivered(true, NodeStatus::Draining);
        assert!(book.take_due(&k, &view_of(vec![n.clone()])).is_empty());
        let ready = Node { status: NodeStatus::ReadyForTraffic, ..n.clone() };
        assert_eq!(book.take_due(&k, &view_of(vec![ready])).iter().map(|d| d.2).collect::<Vec<_>>(), ["state-changed"], "its state changed");
        book.undelivered(&k, &n.node_id, &inc, true);
        let moved = Node { transport_addr: Some("127.0.0.1:9".parse().unwrap()), ..n.clone() };
        assert_eq!(book.take_due(&k, &view_of(vec![moved])).iter().map(|d| d.2).collect::<Vec<_>>(), ["destination-changed"], "its destination moved");
    }

    /// CONTRACT (R-S2): a command not delivered is sent again when the target addressed this primary,
    /// and the address counts once.
    #[test]
    fn an_undelivered_command_is_sent_again_when_the_target_addresses_the_primary() {
        let (book, k, n, _) = undelivered(false, NodeStatus::ReadyForTraffic);
        let view = view_of(vec![n.clone()]);
        assert!(book.take_due(&k, &view).is_empty());
        book.heard_from(&NodeId::mint());
        assert!(book.take_due(&k, &view).is_empty(), "a stranger addressing this primary is not the target");
        book.heard_from(&n.node_id);
        assert_eq!(book.take_due(&k, &view).iter().map(|d| d.2).collect::<Vec<_>>(), ["addressed"], "the target addressed this primary");
    }

    /// CONTRACT (R-S2): the target addressing this primary while the command to it is out is an
    /// eligible event the call does not consume: when that call ends undelivered (the target was not
    /// ready when it took it), the command is sent again on the address. Must NOT happen: the address
    /// dropped because no undelivered command stood when it arrived, so a target whose own
    /// declaration (its last call to this primary) crossed a refused command is never commanded again
    /// (gate red, tree b24a4303, fabric_states__rounds trace 95adb727: mesh1.admin.2's declaration
    /// reached mesh1.admin.1 at 788604 while the commit-state sent at 788599 was out; it came back
    /// NotReady and was next sent only 28 s later).
    #[test]
    fn an_address_that_arrives_while_the_command_is_out_sends_it_again_once_the_call_ends_undelivered() {
        let (fabric, inc) = (FabricId::mint(), IncarnationId::mint());
        let (book, k, n) = (RoundBook::default(), key("66h8qrqvaajk", 1, &fabric), target(&inc, NodeStatus::ReadyForTraffic));
        book.open(&k);
        book.expect(&k, planned(&n.node_id, &inc, "mesh2.admin.1"), Vec::new());
        let view = view_of(vec![n.clone()]);
        assert_eq!(book.take_due(&k, &view).iter().map(|d| d.2).collect::<Vec<_>>(), ["first"]);
        book.heard_from(&n.node_id);
        assert!(book.take_due(&k, &view).is_empty(), "the call is out: nothing is sent beside it");
        book.undelivered(&k, &n.node_id, &inc, true);
        assert_eq!(book.take_due(&k, &view).iter().map(|d| d.2).collect::<Vec<_>>(), ["addressed"], "the address made while the call was out sends the command again");
        book.undelivered(&k, &n.node_id, &inc, true);
        assert!(book.take_due(&k, &view).is_empty(), "the address counts once");
    }

    /// CONTRACT (R-S2): the target becoming unreachable in the view while the command to it is out
    /// is an eligible route loss the call does not consume: when that call ends without a reply
    /// (`typed == false`), the command is sent again once the target is reachable again, and the book
    /// wakes the waiter on `undelivered` so a view already holding the unreachable state is observed
    /// before the target returns. Must NOT happen: `PendingReconnect` observed while `Command::Out`
    /// is in flight discarded by `take_due` and `undelivered` not waking the waiter, so when the
    /// target returns to `ReadyForTraffic` (matching the `status` recorded when the call went out)
    /// `route_lost` stays `false` and `status == t.status` suppresses re-send forever (gate red, tree
    /// a269d7e5, accepted_topology trace 6d1b67fa: mesh1.admin.1 sent commit-state to mesh1.admin.2
    /// at 14.968 s while ReadyForTraffic; mesh1.admin.2 went PendingReconnect at 17.268 s while the
    /// call was out; the call failed ReplyLost at 17.327 s; mesh1.admin.2 returned ReadyForTraffic
    /// at 17.875 s and was never commanded again for the remaining 142 s).
    #[test]
    fn a_target_going_unreachable_while_the_command_is_out_sends_it_again_when_it_is_reachable_again() {
        let (fabric, inc) = (FabricId::mint(), IncarnationId::mint());
        let (book, k, n) = (RoundBook::default(), key("7k63hjhgtbsw", 1, &fabric), target(&inc, NodeStatus::ReadyForTraffic));
        book.open(&k);
        book.expect(&k, planned(&n.node_id, &inc, "mesh1.admin.2"), Vec::new());
        let ready = view_of(vec![n.clone()]);
        let stale = view_of(vec![Node { status: NodeStatus::PendingReconnect, ..n.clone() }]);
        assert_eq!(book.take_due(&k, &ready).iter().map(|d| d.2).collect::<Vec<_>>(), ["first"]);
        // Case 1: view-rebuild sees PendingReconnect while the call is Out, and the call fails after.
        assert!(book.take_due(&k, &stale).is_empty(), "the call is out while the target goes unreachable");
        let mut rx = book.subscribe();
        let before = *rx.borrow_and_update();
        assert!(book.undelivered(&k, &n.node_id, &inc, false));
        assert!(*rx.borrow_and_update() > before, "undelivered wakes the round waiter");
        assert!(book.take_due(&k, &stale).is_empty(), "still unreachable: not sent yet");
        assert_eq!(book.take_due(&k, &ready).iter().map(|d| d.2).collect::<Vec<_>>(), ["reachable-again"], "reachable again after route loss while Out: sent again");
        // Case 2: if a typed refusal arrives instead of a lost reply, route_lost is not set.
        assert!(book.take_due(&k, &stale).is_empty());
        book.undelivered(&k, &n.node_id, &inc, true);
        assert!(book.take_due(&k, &ready).is_empty(), "typed refusal is not a lost route");
    }

    #[test]
    fn the_states_are_ordered_and_named_as_the_frame_carries_them() {
        assert!(FabricState::Pending < FabricState::StateSync && FabricState::StateSync < FabricState::StateCommit && FabricState::StateCommit < FabricState::ReadyForTraffic);
        for s in [FabricState::Pending, FabricState::StateSync, FabricState::StateCommit, FabricState::ReadyForTraffic] {
            assert_eq!(FabricState::parse(s.as_str()), Some(s));
            assert_eq!(serde_json::to_value(s.scope()).unwrap(), serde_json::json!(s.as_str()), "the view's scope status names the state the same way");
        }
        assert_eq!(FabricState::parse("degraded"), None);
    }
}
