//! The fabric states and the round checklist (states.md; fabric-state-sync.md,
//! fabric-state-commit.md, fabric-open-traffic.md).
//!
//! A fabric is `pending`, then `state-sync` once every mesh is ready-for-traffic, then
//! `state-commit` once the application answers state-synced, then `ready-for-traffic` once the
//! commit-state and open-traffic rounds have both completed. Only the fabric-primary moves it.
//!
//! A round is a command pair, fabric-primary → mesh-primary → member, and its completion pair back
//! up. Each primary keeps a checklist of the planned births of the accepted Build; a member counts
//! only when it checks in as its planned birth, after the primary received this round's down op.
//! Nothing leaves a checklist by a timer.

use crate::model::{FabricId, IncarnationId, Node, NodeId, NodeStatus};
use crate::status_declare::{eligible, Destination, Sent};
use crate::topology::Topology;
use crate::round::Missing;
use rafka_node_rpc_contract::status::{NotAuthority, StatusReply, StatusRequest};
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

/// Which of the two rounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RoundKind {
    /// commit-state / state-committed.
    StateCommit,
    /// open-traffic / traffic-opened.
    OpenTraffic,
}

impl RoundKind {
    /// The down op's name.
    pub fn command(self) -> &'static str {
        match self {
            Self::StateCommit => "commit-state",
            Self::OpenTraffic => "open-traffic",
        }
    }

    /// The up op's name.
    pub fn completion(self) -> &'static str {
        match self {
            Self::StateCommit => "state-committed",
            Self::OpenTraffic => "traffic-opened",
        }
    }

    /// The round's operation: `commit-state:<fabric_id>` or `open-traffic:<fabric_id>`.
    pub fn operation(self, fabric_id: &FabricId) -> String {
        format!("{}:{}", self.command(), fabric_id)
    }

    /// The down op naming `node_id`/`incarnation` as its subject.
    pub fn down(self, key: &RoundKey, node_id: NodeId, incarnation: IncarnationId) -> StatusRequest {
        let (fabric_id, build_id, attempt, operation) = (key.fabric_id.clone(), key.build_id.clone(), key.attempt, self.operation(&key.fabric_id));
        match self {
            Self::StateCommit => StatusRequest::CommitState { fabric_id, node_id, incarnation, build_id, attempt, operation },
            Self::OpenTraffic => StatusRequest::OpenTraffic { fabric_id, node_id, incarnation, build_id, attempt, operation },
        }
    }

    /// The up op reporting `node_id`/`incarnation`.
    pub fn up(self, key: &RoundKey, node_id: NodeId, incarnation: IncarnationId) -> StatusRequest {
        let (fabric_id, build_id, attempt, operation) = (key.fabric_id.clone(), key.build_id.clone(), key.attempt, self.operation(&key.fabric_id));
        match self {
            Self::StateCommit => StatusRequest::StateCommitted { fabric_id, node_id, incarnation, build_id, attempt, operation },
            Self::OpenTraffic => StatusRequest::TrafficOpened { fabric_id, node_id, incarnation, build_id, attempt, operation },
        }
    }
}

/// The fields every round op carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoundOp<'a> {
    /// Which round.
    pub kind: RoundKind,
    /// Down (command) or up (completion).
    pub is_down: bool,
    /// The fabric.
    pub fabric_id: &'a FabricId,
    /// The subject.
    pub node_id: &'a NodeId,
    /// The subject's birth.
    pub incarnation: &'a IncarnationId,
    /// The Build.
    pub build_id: &'a str,
    /// The attempt.
    pub attempt: u32,
    /// The operation.
    pub operation: &'a str,
}

impl<'a> RoundOp<'a> {
    /// The round op `req` is, when it is one.
    pub fn of(req: &'a StatusRequest) -> Option<Self> {
        let (kind, is_down, fabric_id, node_id, incarnation, build_id, attempt, operation) = match req {
            StatusRequest::CommitState { fabric_id, node_id, incarnation, build_id, attempt, operation } => (RoundKind::StateCommit, true, fabric_id, node_id, incarnation, build_id, *attempt, operation),
            StatusRequest::StateCommitted { fabric_id, node_id, incarnation, build_id, attempt, operation } => (RoundKind::StateCommit, false, fabric_id, node_id, incarnation, build_id, *attempt, operation),
            StatusRequest::OpenTraffic { fabric_id, node_id, incarnation, build_id, attempt, operation } => (RoundKind::OpenTraffic, true, fabric_id, node_id, incarnation, build_id, *attempt, operation),
            StatusRequest::TrafficOpened { fabric_id, node_id, incarnation, build_id, attempt, operation } => (RoundKind::OpenTraffic, false, fabric_id, node_id, incarnation, build_id, *attempt, operation),
            _ => return None,
        };
        Some(Self { kind, is_down, fabric_id, node_id, incarnation, build_id, attempt, operation })
    }

    /// The round this op belongs to.
    pub fn key(&self) -> RoundKey {
        RoundKey { kind: self.kind, fabric_id: self.fabric_id.clone(), build_id: self.build_id.to_string(), attempt: self.attempt }
    }
}

/// A round: the Build and attempt it belongs to, in one fabric.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoundKey {
    /// Which round.
    pub kind: RoundKind,
    /// The fabric.
    pub fabric_id: FabricId,
    /// The accepted Build.
    pub build_id: String,
    /// Its attempt.
    pub attempt: u32,
}

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
    /// The call is out: nothing is sent while it is.
    Out { to: Destination, status: NodeStatus },
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
    n.status.is_live() && n.incarnation_id.is_some()
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

    /// Forget every round: the tenure that opened them ended.
    pub fn clear(&self) {
        self.rounds.lock().unwrap().clear();
    }

    /// Whether the round is open.
    pub fn is_open(&self, key: &RoundKey) -> bool {
        self.rounds.lock().unwrap().contains_key(key)
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
    pub fn take_due(&self, key: &RoundKey, view: &Topology) -> Vec<(NodeId, Expected)> {
        let mut rounds = self.rounds.lock().unwrap();
        let Some(r) = rounds.get_mut(key) else { return Vec::new() };
        let mut due = Vec::new();
        for (n, e) in &r.expected {
            let target = view.members().find(|t| &t.node_id == n);
            let slot = (n.clone(), e.incarnation.clone());
            let go = match (r.commands.get_mut(&slot), target) {
                (None, Some(t)) => deliverable(t),
                (None, None) => false,
                (Some(Command::Out { .. } | Command::Answered), _) => false,
                (Some(Command::Undelivered { sent, .. }), None) => {
                    if !sent.typed {
                        sent.route_lost = true;
                    }
                    false
                }
                (Some(Command::Undelivered { sent, status, addressed }), Some(t)) => {
                    let ok = deliverable(t);
                    if !sent.typed && !ok {
                        sent.route_lost = true;
                    }
                    eligible(Some(sent), &Destination::of(t), ok, *addressed) || (ok && t.status != *status)
                }
            };
            if go {
                let t = target.expect("a due birth is in the view");
                r.commands.insert(slot, Command::Out { to: Destination::of(t), status: t.status });
                due.push((n.clone(), e.clone()));
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
    /// otherwise no reply came): it waits for an eligible event.
    pub fn undelivered(&self, key: &RoundKey, node_id: &NodeId, incarnation: &IncarnationId, typed: bool) {
        if let Some(r) = self.rounds.lock().unwrap().get_mut(key) {
            let slot = (node_id.clone(), incarnation.clone());
            if let Some(Command::Out { to, status }) = r.commands.get(&slot).cloned() {
                r.commands.insert(slot, Command::Undelivered { sent: Sent { to, typed, route_lost: false }, status, addressed: false });
            }
        }
    }

    /// `node_id` addressed this primary (any Status call from it): an undelivered command to it is
    /// eligible to be sent again.
    pub fn heard_from(&self, node_id: &NodeId) {
        let mut any = false;
        for r in self.rounds.lock().unwrap().values_mut() {
            for ((n, _), c) in r.commands.iter_mut() {
                if let (true, Command::Undelivered { addressed, .. }) = (n == node_id, c) {
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

    /// The check-in `req` makes, decided by this primary. `sender` is the authenticated caller:
    /// `(node id, incarnation held for it, path.name)`. A refusal names the one fact that is wrong,
    /// `NotReady` for a round this primary does not hold open (an unmatched round is never a check-in).
    pub fn check_in(&self, receiver: &str, sender: Option<(&NodeId, Option<&IncarnationId>, &str)>, req: &StatusRequest) -> StatusReply {
        let Some(op) = RoundOp::of(req).filter(|o| !o.is_down) else { return StatusReply::NotReady { reason: format!("{receiver}: {} is not a completion call", req.op()) } };
        let completion = op.kind.completion();
        let Some((from, held, from_name)) = sender else {
            return StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: "unknown peer".into() } };
        };
        if from != op.node_id {
            return StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: from_name.to_string() } };
        }
        match held {
            Some(held) if held != op.incarnation => return StatusReply::RejectedStaleIncarnation { held: held.clone() },
            None => return StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown },
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
                StatusReply::NotReady { reason: why }
            }
            Some(r) => {
                let want = op.kind.operation(&key.fabric_id);
                if op.operation != want {
                    StatusReply::NotReady { reason: format!("{receiver}: {completion} from {from_name}: field operation: round holds {want}, reported {}", op.operation) }
                } else {
                    match r.expected.get(op.node_id) {
                        None => StatusReply::NotReady { reason: format!("{receiver}: {completion} from {from_name}: not on this round's checklist") },
                        Some(e) if e.incarnation != *op.incarnation => {
                            StatusReply::NotReady { reason: format!("{receiver}: {completion} from {from_name}: field incarnation: the checklist plans birth {}, reported {}", e.incarnation, op.incarnation) }
                        }
                        Some(_) => {
                            if r.checked.insert((op.node_id.clone(), op.incarnation.clone())) {
                                StatusReply::Applied
                            } else {
                                StatusReply::AlreadyApplied
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

    fn key(build: &str, attempt: u32, f: &FabricId) -> RoundKey {
        RoundKey { kind: RoundKind::StateCommit, fabric_id: f.clone(), build_id: build.into(), attempt }
    }

    fn planned(n: &NodeId, i: &IncarnationId, name: &str) -> BTreeMap<NodeId, Expected> {
        BTreeMap::from([(n.clone(), Expected { incarnation: i.clone(), name: name.into() })])
    }

    /// CONTRACT: a check-in counts only for a planned birth, in an open round of the same Build and
    /// attempt, from the birth itself; every other case is refused naming the one fact that is wrong,
    /// and a repeat is already applied.
    #[test]
    fn a_check_in_counts_only_as_the_planned_birth_of_the_open_round() {
        let (fabric, node, inc) = (FabricId::mint(), NodeId::mint(), IncarnationId::mint());
        let book = RoundBook::default();
        let k = key("bld_1", 1, &fabric);
        let up = |build: &str, attempt: u32, inc: &IncarnationId| RoundKind::StateCommit.up(&key(build, attempt, &fabric), node.clone(), inc.clone());
        fn sender<'a>(node: &'a NodeId, held: &'a IncarnationId) -> Option<(&'a NodeId, Option<&'a IncarnationId>, &'static str)> {
            Some((node, Some(held), "mesh2.admin.1"))
        }

        let early = book.check_in("mesh1.admin.1", sender(&node, &inc), &up("bld_1", 1, &inc));
        assert!(matches!(&early, StatusReply::NotReady { reason } if reason.contains("no open commit-state round")), "a check-in before the down op: {early:?}");

        book.open(&k);
        book.expect(&k, planned(&node, &inc, "mesh2.admin.1"), Vec::new());
        assert!(!book.complete(&k));
        assert_eq!(book.missing(&k), vec![Missing { who: "mesh2.admin.1".into(), why: "no-check-in" }]);

        let foreign = book.check_in("mesh1.admin.1", sender(&node, &inc), &up("bld_9", 1, &inc));
        assert!(matches!(&foreign, StatusReply::NotReady { reason } if reason.contains("field build_id: round holds bld_1, reported bld_9")), "{foreign:?}");
        let old = book.check_in("mesh1.admin.1", sender(&node, &inc), &up("bld_1", 2, &inc));
        assert!(matches!(&old, StatusReply::NotReady { reason } if reason.contains("field attempt: round holds 1, reported 2")), "{old:?}");
        let other = IncarnationId::mint();
        assert_eq!(book.check_in("mesh1.admin.1", sender(&node, &other), &up("bld_1", 1, &inc)), StatusReply::RejectedStaleIncarnation { held: other.clone() });
        let stranger = NodeId::mint();
        assert!(matches!(book.check_in("mesh1.admin.1", Some((&stranger, Some(&inc), "mesh3.admin.1")), &up("bld_1", 1, &inc)), StatusReply::RejectedNotAuthority { .. }));

        assert_eq!(book.check_in("mesh1.admin.1", sender(&node, &inc), &up("bld_1", 1, &inc)), StatusReply::Applied);
        assert_eq!(book.check_in("mesh1.admin.1", sender(&node, &inc), &up("bld_1", 1, &inc)), StatusReply::AlreadyApplied);
        assert!(book.complete(&k));

        let reborn = IncarnationId::mint();
        book.expect(&k, planned(&node, &reborn, "mesh2.admin.1"), Vec::new());
        assert!(!book.complete(&k), "a rolled birth does not match its planned birth by the old birth's check-in");
        assert_eq!(book.missing(&k), vec![Missing { who: "mesh2.admin.1".into(), why: "checked-in-as-another-birth" }]);
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
        let (book, k, n) = (RoundBook::default(), key("bld_1", 1, &fabric), target(&inc, status));
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
        let k = key("bld_1", 1, &fabric);
        let n = target(&a, NodeStatus::ReadyForTraffic);
        book.open(&k);
        book.expect(&k, planned(&n.node_id, &a, "mesh2.admin.1"), Vec::new());
        assert!(book.take_due(&k, &view_of(vec![])).is_empty(), "a birth the view does not hold is not sent to");
        let dead = Node { status: NodeStatus::Dead, ..n.clone() };
        assert!(book.take_due(&k, &view_of(vec![dead])).is_empty(), "a birth that is not live is not sent to");
        let view = view_of(vec![n.clone()]);
        assert_eq!(book.take_due(&k, &view).len(), 1);
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
            let (book, k, n, _) = undelivered(typed, NodeStatus::Pending);
            let view = view_of(vec![n]);
            for _ in 0..3 {
                assert!(book.take_due(&k, &view).is_empty(), "typed={typed}: nothing happened to the target: the command is not sent again");
            }
        }
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
        assert_eq!(book.take_due(&k, &back).len(), 1, "reachable again: sent again");
        assert!(book.take_due(&k, &back).is_empty(), "and once");
    }

    /// CONTRACT (R-S2): a command that the birth refused as not ready is sent again when the birth's
    /// state changed, and when its destination moved, and not otherwise.
    #[test]
    fn an_undelivered_command_is_sent_again_when_the_targets_state_or_destination_changed() {
        let (book, k, n, inc) = undelivered(true, NodeStatus::Pending);
        assert!(book.take_due(&k, &view_of(vec![n.clone()])).is_empty());
        let ready = Node { status: NodeStatus::ReadyForTraffic, ..n.clone() };
        assert_eq!(book.take_due(&k, &view_of(vec![ready])).len(), 1, "its state changed");
        book.undelivered(&k, &n.node_id, &inc, true);
        let moved = Node { transport_addr: Some("127.0.0.1:9".parse().unwrap()), ..n.clone() };
        assert_eq!(book.take_due(&k, &view_of(vec![moved])).len(), 1, "its destination moved");
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
        assert_eq!(book.take_due(&k, &view).len(), 1, "the target addressed this primary");
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
