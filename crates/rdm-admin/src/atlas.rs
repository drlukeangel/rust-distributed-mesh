//! A node-admin's atlas, `Atlas` (docs/plans/proposal/fabric-atlas.md, Q194, Q197, Q207, Q224): the
//! union of the ops currently executing and the current topology state over the meshes the node-admin is
//! primary for (every mesh at the fabric-primary), read by Build acceptance and answered by
//! `fabric.atlas.get` (the fabric's read, the fabric-primary's atlas).
//!
//! Its sources are ops, never a gossip frame (Q197: "nothing starts or stops or does any action through
//! gossip"):
//! - the birth at a path is the one the current-pair lookup names (query.md §1: its name claim, then its
//!   lifecycle record at its node id, read through the reader, never a copy held here;
//!   a recovery's or a re-create's new birth included);
//! - an operation accepted on a node and not yet complete is written at its acceptance;
//! - a completed lifecycle operation is written from the receipts the fabric-primary's own attempts
//!   returned (directly, or streamed up `build.attempt.run` for a peer mesh), before the caller is
//!   answered `Complete`;
//! - the effects a node row records of any admin's op (a mesh-primary's self-healing included), each
//!   ordered against the fabric-primary's own by its Build's acceptance and its attempt.
//!
//! Nothing moves backward: an effect is taken only over an older one (the Build's acceptance stamp,
//! then its attempt). Gossip is context (`observed`) and decides nothing.
//!
//! It holds no node record (query.md §1: "Node rows and property maps are not copied into an atlas to
//! serve `node.get` or acceptance"): every record it decides on is read through the reader
//! (`crate::current_record::CurrentRecords`).

use std::collections::{BTreeMap, HashMap};
use rdm_core::framing::Birth;
use std::sync::{Arc, Mutex};

use crate::accepted::AttemptAction;
use crate::build_state::{BuildProjection, BuildStateAdapter, StepOutcome};
use crate::model::{IncarnationId, NodeId, NodeStatus, PathName};
use crate::topology::Topology;
use crate::storage::NodeRow;

/// Effect order: Build admission, original step position, then attempt within that step.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Stamp {
    time: u64,
    build_id: String,
    step: Option<u32>,
    attempt: u32,
}

/// A birth's lifecycle as a completed operation left it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    /// node.start completed: the birth is ready again (node-start.md "Completion").
    Started,
    /// node.stop completed: stopped with a live process (node-stop.md "Completion").
    Stopped,
    /// node.delete completed: identity and name released.
    Deleted,
}

#[derive(Debug, Clone)]
struct Effect {
    lifecycle: Lifecycle,
    stamp: Stamp,
}

/// An operation accepted on a node whose attempt has not ended.
#[derive(Debug, Clone)]
struct Accepted {
    operation: rdm_core::OperationId,
    build_id: String,
    attempt: u32,
}

#[derive(Debug, Default)]
struct Held {
    /// The births a create this admin took the receipt of made (an op's result, Q197): in the view
    /// acceptance decides from before the gossip view hears them, their records read through the reader
    /// ([`Atlas::apply`]). Each goes once the view holds its node.
    created: std::collections::HashSet<Birth>,
    /// The completed lifecycle effect of each exact node birth.
    effects: HashMap<Birth, Effect>,
    /// The operation accepted on each explicit subject, using node IDs when admitted.
    accepted: BTreeMap<rdm_core::OperationSubject, Accepted>,
    /// The meshes this admin is primary for (Q207): its own mesh as a mesh-primary, every mesh as the
    /// fabric-primary; empty for an admin that holds no primary.
    meshes: std::collections::BTreeSet<String>,
}

/// Q207's decision on a caller birth a primary refused as one it does not hold (a status declaration
/// refused `SubjectUnknown`, a deleted birth's Digest heard again), from this admin's atlas alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CallerDecision {
    /// Another birth is at the caller's `(path, name)` now, or an op that fills the path is executing:
    /// the caller is deleted.
    DeleteHeld {
        /// The caller's path.
        path: String,
        /// The name, beside its path.
        name: String,
        /// What is at the path: `<node id>@<incarnation>`, or the accepted operation's key.
        by: String,
    },
    /// Nothing is at the path and the topology does not need it: the caller is deleted.
    DeleteUnneeded {
        /// The caller's path.
        path: String,
        /// The name, beside its path.
        name: String,
    },
    /// The caller's record is deleted and nothing else is at the path: the caller is deleted, its
    /// runtime terminated, never re-admitted (Q217). A path the topology still `needed` is recovered by
    /// a re-create at that path and name, a new birth, through node recovery (its owed resubmit,
    /// `RecoverySubmitter::owed_submits`).
    DeleteDeletedBirth {
        /// The caller's path.
        path: String,
        /// The name, beside its path.
        name: String,
        /// Whether the current Build's topology names the path.
        needed: bool,
    },
    /// The caller's record is live, nothing else is at the path and the topology needs it: the caller
    /// is re-admitted.
    ReAdmit {
        /// The caller's path.
        path: String,
        /// The name, beside its path.
        name: String,
    },
    /// The atlas holds no record of the caller: it has no `(path, name)` to decide on.
    NoRecord,
    /// The caller's mesh is not one this admin is primary for: another primary decides.
    NotMine {
        /// The caller's path.
        path: String,
        /// The name, beside its path.
        name: String,
    },
}

impl CallerDecision {
    /// The name its span carries.
    pub(crate) fn name(&self) -> &'static str {
        match self {
            CallerDecision::DeleteHeld { .. } => "delete-path-held",
            CallerDecision::DeleteUnneeded { .. } => "delete-path-unneeded",
            CallerDecision::DeleteDeletedBirth { .. } => "delete-deleted-birth",
            CallerDecision::ReAdmit { .. } => "re-admit",
            CallerDecision::NoRecord => "no-record",
            CallerDecision::NotMine { .. } => "not-mine",
        }
    }

    /// The caller's `(path, name)`, when the atlas holds its record.
    pub(crate) fn path(&self) -> Option<PathName> {
        match self {
            CallerDecision::DeleteHeld { path, name, .. } | CallerDecision::DeleteUnneeded { path, name } | CallerDecision::DeleteDeletedBirth { path, name, .. } | CallerDecision::ReAdmit { path, name } | CallerDecision::NotMine { path, name } => Some(PathName { path: path.clone(), name: name.clone() }),
            CallerDecision::NoRecord => None,
        }
    }

    /// The decision once the fabric-primary answered `fabric.atlas.get` from its atlas (Q207: a
    /// node-admin that is not the fabric-primary verifies at the root, and the root's answer wins: it
    /// may have missed an op). `root` lists the current birth of every path the current Build's
    /// topology names, a deleted one omitted: another birth at the caller's path there is held; the
    /// caller's own birth there is the root's current birth, live and needed: re-admitted; no row at a
    /// path the topology needs is the caller's record deleted at the root: deleted, never re-admitted
    /// (Q217); no row at a path it does not need is unneeded.
    pub(crate) fn verified(self, caller: (&NodeId, &IncarnationId), root: &rdm_core::views::AtlasView, needed: bool) -> CallerDecision {
        let Some(path) = self.path() else { return self };
        if matches!(self, CallerDecision::NotMine { .. }) {
            return self;
        }
        let (node_id, incarnation) = caller;
        match root.node(node_id).or_else(|| root.nodes.iter().find(|r| r.location() == path)) {
            // A caller whose record this admin holds deleted is deleted, never re-admitted (Q217:
            // "Re-admission is for a caller whose record is live"): a root that still names it at its
            // path has not heard the delete its mesh-primary wrote, and cannot undo it.
            Some(r) if r.node_id == *node_id && matches!(self, CallerDecision::DeleteDeletedBirth { .. }) => self,
            Some(r) if r.node_id == *node_id && r.incarnation_id.as_ref().is_none_or(|i| i == incarnation) => CallerDecision::ReAdmit { path: path.path.clone(), name: path.name.clone()},
            Some(r) => CallerDecision::DeleteHeld { path: path.path.clone(), name: path.name.clone(), by: format!("{}@{}", r.node_id, r.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or("")) },
            None if needed => CallerDecision::DeleteDeletedBirth { path: path.path.clone(), name: path.name.clone(), needed },
            None => CallerDecision::DeleteUnneeded { path: path.path.clone(), name: path.name.clone()},
        }
    }
}

/// One node of the atlas: the birth its node row names current and its lifecycle as the ops left it.
#[derive(Debug, Clone)]
pub(crate) struct AtlasRow {
    pub(crate) mesh: String,
    pub(crate) r#type: String,
    /// The name the record's last move came from, at its unchanged path (node-name-update.md "Check
    /// and move": the record's `renamed_from` names it); `None` when no rename moved it.
    pub(crate) moved_from: Option<String>,
    pub(crate) path: String,
    /// The name, beside its path.
    pub(crate) name: String,
    pub(crate) node_id: NodeId,
    pub(crate) incarnation: Option<IncarnationId>,
    /// The status the ops left the birth in.
    pub(crate) status: NodeStatus,
    /// A completed node.stop of this birth, not followed by a completed node.start.
    pub(crate) stopped: bool,
    /// Live for acceptance: created, and not stopped or deleted by an op since.
    pub(crate) live: bool,
    /// The operation accepted on the node whose attempt has not ended.
    pub(crate) accepted: Option<rdm_core::OperationId>,
    /// Context only: the gossip view's status of the same birth.
    pub(crate) observed: Option<NodeStatus>,
    /// Where the birth was read from: `node-row`.
    pub(crate) birth_source: &'static str,
}

impl AtlasRow {
    /// The `(path, name)` lookup index key of path and name.
    pub(crate) fn location(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.path.clone(), name: self.name.clone() } }
}

impl AtlasRow {
    /// The accepted entry a rename moved this node off, when `accepted` does not name the pair its
    /// record stands at now: the name its record's last move came from, at its unchanged path. `None` while the accepted topology names the pair, or when no rename moved the record.
    /// A pair is resolved through its name claim to this record (query.md §1), never by its spelling.
    pub(crate) fn entry_left(&self, accepted: &crate::accepted::FabricTopology) -> Option<PathName> {
        if accepted.contains(&self.location()) {
            return None;
        }
        self.moved_from.clone().map(|name| PathName { path: self.path.clone(), name })
    }
}

/// A node-admin's atlas (Q224): one per node-admin, over the meshes it is primary for.
#[derive(Debug, Default)]
pub struct Atlas {
    held: Mutex<Held>,
    /// What a refused caller's decision acts through (Q207), installed once this admin's recovery
    /// submitter and heard-again door exist.
    callers: std::sync::OnceLock<Arc<crate::refused_caller::RefusedCallers>>,
}

/// The births and operation keys an action is accepted on.
fn accepted_on(action: &AttemptAction) -> Vec<rdm_core::OperationId> {
    use rdm_core::OperationId;
    match action {
        AttemptAction::Delete { node_id, .. } => vec![OperationId::node("node.delete", node_id)],
    }
}

/// Recover the exact admitted birth from explicit metadata and the original step position.
/// A receipt payload must belong to this Build, attempt and operation before it can supply a birth.
fn birth_of_step(rows: &[NodeRow], p: &BuildProjection, receipt: &crate::build_state::BuildStepReceipt) -> Option<Birth> {
    use rdm_core::OperationSubject;
    let (attempt, operation) = (receipt.attempt, &receipt.operation);
    let expected = match p.attempt_actions.get(&attempt) {
        Some(AttemptAction::Delete { node_id, from_incarnation, .. }) => {
            match &operation.subject {
                Some(OperationSubject::Node(id)) if id == node_id => Some((node_id.clone(), from_incarnation.clone())),
                Some(OperationSubject::Birth { node_id: id, incarnation }) if id == node_id && incarnation == from_incarnation => Some((id.clone(), incarnation.clone())),
                _ => return None,
            }
        }
        None => match &operation.subject {
            Some(OperationSubject::Birth { node_id, incarnation }) => Some((node_id.clone(), incarnation.clone())),
            Some(OperationSubject::Node(id)) => p.topology.pinned.iter().find(|(_, birth)| birth.node_id == *id).map(|(_, birth)| (birth.node_id.clone(), birth.incarnation.clone())),
            _ => None,
        },
    };
    // With no admission pin, require the node's exact recorded acknowledgement.
    // Cross-attempt acknowledgements pass the same reuse boundary as the pipeline.
    let expected = expected.or_else(|| {
        let subject_matches = |id: &NodeId| match &operation.subject {
            Some(OperationSubject::Node(expected) | OperationSubject::Birth { node_id: expected, .. }) => id == expected,
            _ => false,
        };
        let mut targets = rows.iter().filter(|row| subject_matches(&row.node_id)).flat_map(|row| row.births.iter().map(move |birth| (row, birth))).filter(|(_, birth)| {
            birth.operations.iter().any(|op| crate::build_state::declaration_names_receipt(p, receipt, &op.operation))
        }).map(|(row, birth)| (row.node_id.clone(), birth.incarnation_id.clone()));
        let target = targets.next()?;
        targets.all(|other| other == target).then_some(target)
    })?;
    if let Some(value) = receipt.output.as_ref().filter(|value| !value.is_null()) {
        let op = serde_json::from_value::<rdm_core::LifecycleOp>(value.clone()).ok()?;
        crate::build_state::validate_carried_lifecycle(p, receipt, &op, (&expected.0, &expected.1))?;
    }
    Some(Birth { node_id: expected.0, incarnation: expected.1 })
}

/// The current records of the subjects of `operations` (query.md §1, read through `records`): a node or
/// birth subject by its node ID. What names a receipt's birth when
/// its attempt pinned none ([`birth_of_step`]). `Err` names a read that failed.
pub(crate) async fn subject_rows<'a>(records: &crate::current_record::CurrentRecords<'_>, operations: impl IntoIterator<Item = &'a rdm_core::OperationId>) -> Result<Vec<NodeRow>, String> {
    let mut out: Vec<NodeRow> = Vec::new();
    for operation in operations {
        let row = match &operation.subject {
            Some(rdm_core::OperationSubject::Node(id) | rdm_core::OperationSubject::Birth { node_id: id, .. }) => records.row(id).await?,
            _ => None,
        };
        if let Some(r) = row.filter(|r| !out.iter().any(|o| o.node_id == r.node_id)) {
            out.push(r);
        }
    }
    Ok(out)
}

/// What a streamed receipt wrote in the atlas ([`Atlas::take_receipt`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Taken {
    /// The Build's journal holds no completed receipt of the operation in the attempt.
    NoReceipt,
    /// The operation changes nothing the atlas holds (a connections delete, a mesh leave).
    Nothing,
    /// The lifecycle effect of its birth only (no current record names the birth).
    Effect(Option<&'static str>),
    /// The lifecycle effect, its birth named by its current record.
    Status(NodeStatus),
    /// The record's new name.
    Renamed(PathName),
    /// The record's config or tags.
    Property,
    /// A create completed at the path: its birth is held until the view holds it ([`Atlas::apply`]).
    Created(Birth),
    /// The admitted existing node whose record the operation changed: its current record is what any
    /// later read reads (query.md §1).
    Refresh(NodeId),
}

impl Taken {
    /// The name its span carries.
    pub(crate) fn name(&self) -> String {
        match self {
            Taken::NoReceipt => "no-receipt".into(),
            Taken::Nothing => "nothing".into(),
            Taken::Effect(l) => format!("effect:{}", l.unwrap_or("none")),
            Taken::Status(s) => format!("status:{s:?}"),
            Taken::Renamed(_) => "renamed".into(),
            Taken::Property => "property".into(),
            Taken::Created(_) => "created".into(),
            Taken::Refresh(_) => "refreshed".into(),
        }
    }
}

/// The completed lifecycle receipt `s` of `p` taken over an older effect of its birth; the lifecycle's
/// name when it is one.
fn take_effect(held: &mut Held, rows: &[NodeRow], p: &BuildProjection, s: &crate::build_state::BuildStepReceipt) -> Option<&'static str> {
    let lifecycle = lifecycle_of(&s.operation)?;
    let birth = birth_of_step(rows, p, s)?;
    let stamp = Stamp { time: p.time, build_id: p.build_id.to_string(), step: s.operation.step, attempt: s.attempt };
    if held.effects.get(&birth).is_none_or(|e| e.stamp < stamp) {
        held.effects.insert(birth, Effect { lifecycle, stamp });
    }
    Some(match lifecycle {
        Lifecycle::Started => "started",
        Lifecycle::Stopped => "stopped",
        Lifecycle::Deleted => "deleted",
    })
}

/// The lifecycle indicated by an explicit action token; no subject string is parsed.
fn lifecycle_of(operation: &rdm_core::OperationId) -> Option<Lifecycle> {
    match operation.action.as_str() {
        "node.stop" => Some(Lifecycle::Stopped),
        "node.start" => Some(Lifecycle::Started),
        "node.delete" => Some(Lifecycle::Deleted),
        _ => None,
    }
}

impl Atlas {
    /// Install what a refused caller's decision acts through (Q207); the first install stands.
    pub(crate) fn refused_callers(&self, callers: Arc<crate::refused_caller::RefusedCallers>) {
        let _ = self.callers.set(callers);
    }

    pub(crate) fn callers(&self) -> Option<&Arc<crate::refused_caller::RefusedCallers>> {
        self.callers.get()
    }

    /// Whether `row` (its node's current record) is `Deleted` with the birth `(node_id, incarnation)`, or
    /// a node.delete of that birth completed. A withdrawn claim is not a delete: its birth may still
    /// live, surplus, until a removal deletes it (node-investigation.md: "a caller whose record is
    /// deleted is deleted"; Q217).
    pub(crate) fn holds_deleted_by_delete(&self, node_id: &NodeId, incarnation: &IncarnationId, row: Option<&NodeRow>) -> bool {
        let recorded = row.is_some_and(|r| r.node_id == *node_id && r.status == NodeStatus::Deleted && r.birth(incarnation).is_some());
        recorded || self.deleted_by_op(node_id, incarnation)
    }

    /// A node.delete of the birth completed.
    fn deleted_by_op(&self, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
        self.held.lock().unwrap().effects.get(&Birth { node_id: node_id.clone(), incarnation: incarnation.clone() }).is_some_and(|e| e.lifecycle == Lifecycle::Deleted)
    }

    /// The atlas's meshes set to `meshes`, the meshes this admin is primary for now (Q207): a mesh taken
    /// as it takes that primary, a mesh handed away narrowed off. The meshes taken and handed away.
    pub fn scope(&self, meshes: std::collections::BTreeSet<String>) -> (Vec<String>, Vec<String>) {
        let mut held = self.held.lock().unwrap();
        let taken: Vec<String> = meshes.difference(&held.meshes).cloned().collect();
        let handed: Vec<String> = held.meshes.difference(&meshes).cloned().collect();
        held.meshes = meshes;
        (taken, handed)
    }

    /// The meshes this admin is primary for, as the atlas holds them.
    pub(crate) fn meshes(&self) -> std::collections::BTreeSet<String> {
        self.held.lock().unwrap().meshes.clone()
    }

    /// Q207: the caller birth of `caller`'s node, `incarnation`, a primary refused as one it does not hold,
    /// decided from `caller` (its record at its node id, query.md §1), `at_path` (the
    /// record the current-pair lookup names at `caller`'s `(path, name)`) and the ops executing (never the
    /// gossip view): another birth at the caller's `(path, name)`, or an op executing that fills it,
    /// deletes the caller; nothing there and the caller's record deleted deletes it, never re-admitted
    /// (Q217); nothing there, its record live and `planned` (the current Build's topology, by mesh) naming
    /// the path re-admits it; nothing there and the topology not naming it deletes it.
    pub(crate) fn refused_caller(&self, caller: Option<&NodeRow>, at_path: Option<&NodeRow>, incarnation: &IncarnationId, planned: &BTreeMap<String, Vec<PathName>>) -> CallerDecision {
        let held = self.held.lock().unwrap();
        let Some(caller) = caller else { return CallerDecision::NoRecord };
        let node_id = &caller.node_id;
        let path = caller.location();
        if !held.meshes.contains(&caller.mesh) {
            return CallerDecision::NotMine { path: path.path.clone(), name: path.name.clone()};
        }
        let gone = |r: &NodeRow| matches!(r.status, NodeStatus::Deleted | NodeStatus::Withdrawn);
        // Another birth at the path now: another node's live record there, or a newer birth of the
        // caller's own node.
        let other = at_path.filter(|r| r.location() == path && r.node_id != *node_id && !gone(r)).and_then(|r| r.newest_birth().map(|b| format!("{}@{}", r.node_id, b.incarnation_id.as_str())));
        let newer = (!gone(caller)).then(|| caller.newest_birth()).flatten().filter(|b| b.incarnation_id != *incarnation).map(|b| format!("{}@{}", node_id, b.incarnation_id.as_str()));
        if let Some(by) = other.or(newer) {
            return CallerDecision::DeleteHeld { path: path.path.clone(), name: path.name.clone(), by };
        }
        let needed = planned.get(&caller.mesh).is_some_and(|paths| paths.contains(&path));
        // A caller whose record is deleted is deleted for real (Q217): its record stays deleted, and a
        // needed path is recovered by a re-create, a new birth.
        if gone(caller) {
            CallerDecision::DeleteDeletedBirth { path: path.path.clone(), name: path.name.clone(), needed }
        } else if needed {
            CallerDecision::ReAdmit { path: path.path.clone(), name: path.name.clone()}
        } else {
            CallerDecision::DeleteUnneeded { path: path.path.clone(), name: path.name.clone()}
        }
    }

    /// The operations `action` acts with, accepted as attempt `attempt` of `build_id`: written at
    /// acceptance, before the attempt can be claimed (R-FA2).
    pub(crate) fn note_accepted(&self, build_id: &crate::build::BuildId, attempt: u32, action: &AttemptAction) {
        let mut held = self.held.lock().unwrap();
        for operation in accepted_on(action) {
            if let Some(subject) = operation.subject.clone() {
                let replace = held.accepted.get(&subject).is_none_or(|previous| previous.build_id != build_id.as_str() || previous.attempt < attempt);
                if replace {
                    held.accepted.insert(subject, Accepted { operation, build_id: build_id.to_string(), attempt });
                }
            }
        }
    }

    /// The paths this atlas holds a create of Build `build` at (fabric-atlas.md R-FA1, R-FA2): accepted and
    /// executing, or completed, its Complete receipt in the Build's own log. A birth the
    /// Build's own op created is not surplus to its accepted topology, which a recipe's `Create` step does
    /// not change (Luke 2026-10-30: drift is decided from the atlas, not the topology), whichever mesh's
    /// section ran it: the fabric-primary's atlas takes every section's receipts.
    /// The completed creates are read from the Build's own receipts (`p`, its log), the source R-FA2
    /// writes this atlas from, so a fabric-primary restarted inside the Build holds them too: its
    /// accepted map is memory only.
    pub(crate) fn created_by(&self, p: &BuildProjection) -> std::collections::BTreeSet<PathName> {
        let build = &p.build_id;
        let held = self.held.lock().unwrap();
        // A create's path is the one the Build record names its node.id at (D58).
        let location = |operation: &rdm_core::OperationId| match &operation.subject {
            Some(rdm_core::OperationSubject::Node(id)) if operation.action == "node.create" => p.topology.created.iter().find(|(_, created)| created == id).map(|(at, _)| at.clone()),
            _ => None,
        };
        let mut out: std::collections::BTreeSet<PathName> = held.accepted.values().filter(|a| a.build_id == build.as_str()).filter_map(|a| location(&a.operation)).collect();
        out.extend(p.steps.iter().filter(|s| s.step == "Complete" && s.outcome == StepOutcome::Complete).filter_map(|s| location(&s.operation)));
        out
    }

    /// Attempt `attempt` of `p` ended without completing: what it accepted is no longer executing.
    pub(crate) fn ended(&self, build_id: &crate::build::BuildId, attempt: u32) {
        self.held.lock().unwrap().accepted.retain(|_, a| !(a.build_id == build_id.as_str() && a.attempt <= attempt));
    }

    /// Attempt `attempt` of `p` completed: every completed lifecycle receipt of the Build is taken over
    /// an older effect of its birth, and what the attempt accepted is no longer executing. Called before
    /// the caller is answered `Complete` (R-FA2). `rows` are the current records of the receipts'
    /// subjects ([`receipt_subject_rows`]), what names an unpinned receipt's birth.
    pub(crate) fn receipts(&self, p: &BuildProjection, attempt: u32, rows: &[NodeRow]) {
        let mut held = self.held.lock().unwrap();
        for s in p.steps.iter().filter(|s| s.step == "Complete" && s.outcome == StepOutcome::Complete) {
            take_effect(&mut held, rows, p, s);
        }
        held.accepted.retain(|_, a| !(a.build_id == p.build_id.as_str() && a.attempt <= attempt));
    }

    /// The `Complete` receipt of `operation` in attempt `attempt` of `p`, taken the moment it streams up
    /// `build.attempt.run` to this fabric-primary, whichever admin executed it (R-FA2: "execution
    /// progress writes it from each attempt's receipts as they stream up `build.attempt.run`, before
    /// the caller is answered `Complete`"): the lifecycle it leaves its birth in (stop, start, delete)
    /// and what it changed on the node's record (a rename's name, a stop's or a delete's status, the
    /// config and tags the Build applied in place, read from its record through the reader). What it
    /// took, for its span; a create's is [`Taken::Created`], its birth held for [`Self::apply`]. `rows`
    /// are the current records of the receipt's subject, what names an unpinned receipt's birth.
    pub(crate) fn take_receipt(&self, p: &BuildProjection, attempt: u32, operation: &rdm_core::OperationId, lane: &Option<String>, rows: &[NodeRow]) -> Taken {
        let Some(s) = p.steps.iter().rev().find(|s| s.mesh == *lane && s.build_id == p.build_id && s.attempt == attempt && &s.operation == operation && s.step == "Complete" && s.outcome == StepOutcome::Complete) else {
            return Taken::NoReceipt;
        };
        let mut held = self.held.lock().unwrap();
        let op = operation.action.as_str();
        let node_id = match &operation.subject {
            Some(rdm_core::OperationSubject::Node(id) | rdm_core::OperationSubject::Birth { node_id: id, .. }) => id.clone(),
            _ => return Taken::Nothing,
        };
        let lifecycle = take_effect(&mut held, rows, p, s);
        let node_of = || -> Option<NodeId> { Some(node_id.clone()) };
        match op {
            "node.stop" | "node.delete" => {
                let to = if op == "node.stop" { NodeStatus::Stopped } else { NodeStatus::Deleted };
                match birth_of_step(rows, p, s) {
                    Some(_) => Taken::Status(to),
                    None => Taken::Effect(lifecycle),
                }
            }
            "node.start" => Taken::Effect(lifecycle),
            "node.update" | "node.config.update" | "node.tag.create" | "node.tag.delete" => {
                node_of().map(Taken::Refresh).unwrap_or(Taken::Nothing)
            }
            "node.create" => {
                let candidates: Vec<_> = p.steps.iter().filter(|r| r.mesh == *lane && r.build_id == p.build_id && r.attempt <= attempt && r.operation == *operation).collect();
                let floor = crate::build_state::operation_reuse_boundary(&candidates, &p.build_id, lane, operation, attempt);
                let allocations: Vec<_> = candidates.into_iter().filter(|r| r.attempt > floor && r.step == "AllocateIdentity" && r.outcome == StepOutcome::Complete).collect();
                let Some(latest) = allocations.iter().map(|r| r.attempt).max() else { return Taken::Nothing };
                let mut identity = None;
                for receipt in allocations.into_iter().filter(|r| r.attempt == latest) {
                    let Some(value) = receipt.output.as_ref() else { return Taken::Nothing };
                    let Ok(found) = serde_json::from_value::<crate::deployment::pipeline::Identity>(value.clone()) else { return Taken::Nothing };
                    if identity.as_ref().is_some_and(|prior| prior != &found) { return Taken::Nothing; }
                    identity = Some(found);
                }
                match identity {
                    Some(identity) => {
                        let birth = identity.birth();
                        held.created.insert(birth.clone());
                        Taken::Created(birth)
                    }
                    None => Taken::Nothing,
                }
            },
            _ => Taken::Nothing,
        }
    }

    /// `t`, a view an acceptance decides from, with each of its births as its record (read through
    /// `records`, query.md §1) and the ops left it ([`Self::answer`]; R-FA1: the fabric-primary accepts or
    /// denies from the atlas, the gossip view context only): a birth an op deleted is not in it. A birth a
    /// create this atlas took the receipt of made is in it before the gossip view hears it, from its
    /// record, in place of a birth at its path and name that is no longer live: a node action on a path
    /// node recovery just re-created is decided on the replacement, never refused `node-not-live` on the
    /// deleted birth (rdm#239). With no `records` (none open yet) only the ops' effects are applied.
    /// `Err` names a record that could not be read.
    pub(crate) async fn apply(&self, t: &mut Topology, records: Option<&crate::current_record::CurrentRecords<'_>>) -> Result<(), String> {
        let observed = t.clone();
        let mut kept = Vec::with_capacity(t.nodes.len());
        for mut n in std::mem::take(&mut t.nodes) {
            let row = match records {
                Some(records) => records.row(&n.node_id).await?,
                None => None,
            };
            if self.answer(&mut n, row.as_ref()) {
                kept.push(n);
            }
        }
        t.nodes = kept;
        let Some(records) = records else { return Ok(()) };
        let created: Vec<Birth> = {
            let mut held = self.held.lock().unwrap();
            held.created.retain(|b| !observed.births().any(|n| n.node_id == b.node_id));
            held.created.iter().cloned().collect()
        };
        for b in created {
            let Some(r) = records.row(&b.node_id).await? else { continue };
            let live = r.status.is_live() || r.status == NodeStatus::Starting;
            let Some(nb) = r.newest_birth().filter(|nb| live && nb.incarnation_id == b.incarnation) else { continue };
            // Its addresses are the node's own part of its record at that birth (Q333).
            let own = records.properties(&b.node_id).await?;
            let own = crate::current_record::of_birth(own.as_ref(), &r.node_id, &nb.incarnation_id);
            if self.deleted_by_op(&b.node_id, &b.incarnation) || t.nodes.iter().any(|n| n.node_id == r.node_id || (n.location() == r.location() && n.status.is_live())) {
                continue;
            }
            t.nodes.retain(|n| n.location() != r.location());
            t.nodes.push(crate::model::Node {
                renamed_from: None,
                path: r.path.clone(), name: r.name.clone(),
                r#type: r.r#type.clone(),
                mesh: r.mesh.clone(),
                node_id: r.node_id.clone(),
                endpoint_id: own.and_then(|o| o.endpoint_id.clone()),
                incarnation_id: Some(nb.incarnation_id.clone()),
                deployment_id: None,
                provider: None,
                data_dir: None,
                status: r.status,
                is_primary: false,
                is_fabric_primary: false,
                admin_api_base: None,
                transport_addr: own.and_then(|o| o.transport_addr),
                listeners: own.map(|o| o.listeners.clone()).unwrap_or_default(),
                routable: false,
                declared: None,
                stopped: false,
            });
        }
        Ok(())
    }

    /// `n`, a birth Build acceptance decides from ([`Self::apply`]; a node read answers [`Self::read`],
    /// the row's status as written), as `row`, its node's record, and the ops left it: the name its
    /// record holds, and the lifecycle the ops left it in (a completed stop: stopped; a completed start:
    /// ready over every state its stop and its start pass through, the gossip view's stopped birth and its
    /// `Starting` and `Pending` before the mesh-primary's Ready, node-start.md "Completion", so a
    /// projection published before the start completed never moves it backward, R-FA3). `false` when an
    /// op deleted the birth: it is not answered. The gossip view's status stands where no op moved the
    /// birth, and over a completed start where it is a state after ready (R-FA3: context).
    pub(crate) fn answer(&self, n: &mut crate::model::Node, row: Option<&NodeRow>) -> bool {
        let held = self.held.lock().unwrap();
        if let Some(r) = row.filter(|r| r.node_id == n.node_id && r.status != NodeStatus::Withdrawn) {
            { let at = r.location(); n.path = at.path; n.name = at.name; }
        }
        let effect = n.incarnation_id.as_ref().and_then(|i| held.effects.get(&Birth { node_id: n.node_id.clone(), incarnation: i.clone() })).map(|e| e.lifecycle);
        match effect {
            Some(Lifecycle::Deleted) => return false,
            Some(Lifecycle::Stopped) => {
                n.stopped = true;
                n.status = NodeStatus::Leaving;
                n.routable = false;
            }
            Some(Lifecycle::Started) if n.stopped || matches!(n.status, NodeStatus::Starting | NodeStatus::Pending) => {
                n.stopped = false;
                n.status = NodeStatus::Ready;
            }
            _ => {}
        }
        true
    }

    /// `n`, a birth `node.get` answers, as `row` carries it and the ops left it (query.md §1; Q233,
    /// states.md "Who sets each state": "A query answers the state the row last took from the node's frame
    /// or the mesh-primary's receipt"): `row` is the node's lifecycle record at its node
    /// id ([`crate::current_record::CurrentRecords::row`]), whose name and replacement provenance the
    /// birth takes, and, where it is of this birth (its newest, or a record with no birth yet), its status,
    /// `stopped` exactly when that is stopped. Never a lifecycle effect laid over the view: a command the
    /// node has not acknowledged moved the row nowhere, so it moves the answer nowhere. Silence is not a
    /// state a command sets: a birth its row holds live (starting through connections-deleted) that the
    /// view holds silent (`PendingReconnect`, `Dead`, observer-inferred, node-identity-and-state.md §7.3)
    /// is answered as the view holds it, cleared the moment it is heard; a birth its row holds leaving,
    /// stopped or deleting is silent as expected (Q58) and answers its row. A birth with no record answers
    /// the view's status. `false` when an op deleted the birth: it is not answered. Build acceptance reads
    /// [`Self::answer`], not this.
    pub(crate) fn read(&self, n: &mut crate::model::Node, row: Option<&NodeRow>) -> bool {
        let held = self.held.lock().unwrap();
        if n.incarnation_id.as_ref().and_then(|i| held.effects.get(&Birth { node_id: n.node_id.clone(), incarnation: i.clone() })).is_some_and(|e| e.lifecycle == Lifecycle::Deleted) {
            return false;
        }
        if let Some(r) = row.filter(|r| r.node_id == n.node_id && r.status != NodeStatus::Withdrawn) {
            { let at = r.location(); n.path = at.path; n.name = at.name; }
            let of_birth = r.newest_birth().is_none_or(|b| Some(&b.incarnation_id) == n.incarnation_id.as_ref());
            let live = matches!(r.status, NodeStatus::Starting | NodeStatus::Pending | NodeStatus::Ready | NodeStatus::ConnectionsDeleting | NodeStatus::ConnectionsDeleted);
            let silent = live && matches!(n.status, NodeStatus::PendingReconnect | NodeStatus::Dead);
            if of_birth && !silent {
                n.status = r.status;
                n.stopped = r.status == NodeStatus::Stopped;
            }
        }
        true
    }

    /// The effect the node row records last of `row`'s birth `incarnation`, with its stamp: a stop's
    /// `Stopped`, a start's `Ready`; any other effect leaves the lifecycle where it is.
    fn row_effect(row: &NodeRow, incarnation: &IncarnationId) -> Option<Effect> {
        let op = row.birth(incarnation)?.operations.iter().rev().find(|o| match o.state {
            rdm_core::status::NodeState::Stopped => o.operation.operation.action == "node.stop",
            rdm_core::status::NodeState::Ready => o.operation.operation.action == "node.start",
            _ => false,
        })?;
        let lifecycle = if op.state == rdm_core::status::NodeState::Stopped { Lifecycle::Stopped } else { Lifecycle::Started };
        // A node command belongs to no Build (Q342): its effect is the record's, read from no Build.
        Some(Effect { lifecycle, stamp: Stamp { time: 0, build_id: rdm_core::LifecycleOp::NO_BUILD.into(), step: op.operation.operation.step, attempt: rdm_core::LifecycleOp::NO_ATTEMPT } })
    }

    /// The atlas row of `path`: the birth the current-pair lookup names (query.md §1: the pair's name
    /// claim, then the lifecycle record at the claimant's node id, `records`), its lifecycle as the
    /// newest of the fabric-primary's own receipts and the record's effects, and the operation accepted on
    /// it. `None` when no claim holds the pair or its claimant's record stands at another: the gossip
    /// view never stands in for one (Q197).
    pub(crate) async fn row(&self, path: &PathName, records: &crate::current_record::CurrentRecords<'_>, observed: &Topology) -> Result<Option<AtlasRow>, String> {
        let row = records.row_at(path).await?;
        self.project_row(row, observed).await
    }

    /// Read an admitted node directly by its globally unique ID, its record at that id (query.md §1),
    /// never through a location index.
    pub(crate) async fn row_id(&self, node_id: &NodeId, records: &crate::current_record::CurrentRecords<'_>, observed: &Topology) -> Result<Option<AtlasRow>, String> {
        let row = records.row(node_id).await?;
        self.project_row(row, observed).await
    }

    async fn project_row(&self, row: Option<NodeRow>, observed: &Topology) -> Result<Option<AtlasRow>, String> {
        let Some(row) = row else { return Ok(None) };
        let birth = row.newest_birth().map(|b| b.incarnation_id.clone());
        let accepted = {
            let held = self.held.lock().unwrap();
            birth.as_ref().and_then(|incarnation| held.accepted.get(&rdm_core::OperationSubject::Birth { node_id: row.node_id.clone(), incarnation: incarnation.clone() }))
                .or_else(|| held.accepted.get(&rdm_core::OperationSubject::Node(row.node_id.clone())))
                .map(|a| a.operation.clone())
        };
        let observed_status = observed.births().find(|n| n.node_id == row.node_id && n.incarnation_id == birth).map(|n| n.status);
        let held = birth.as_ref().and_then(|i| self.held.lock().unwrap().effects.get(&Birth { node_id: row.node_id.clone(), incarnation: i.clone() }).cloned());
        let from_row = match &birth {
            Some(i) => Self::row_effect(&row, i),
            None => None,
        };
        let effect = match (held, from_row) {
            // The row's newest stop or start, a node command's, is the record: the newest effect (Q342).
            (_, Some(r)) => Some(r),
            (h, None) => h,
        };
        let deleted = matches!(row.status, NodeStatus::Deleted | NodeStatus::Withdrawn) || effect.as_ref().is_some_and(|e| e.lifecycle == Lifecycle::Deleted);
        let (status, stopped) = if deleted {
            (NodeStatus::Deleted, false)
        } else {
            match effect.map(|e| e.lifecycle) {
                Some(Lifecycle::Stopped) => (NodeStatus::Leaving, true),
                Some(Lifecycle::Started) => (NodeStatus::Ready, false),
                // No op moved the birth since its create: shown as the gossip view holds the same birth
                // (context), else as far as its node row's declarations reached.
                _ => match observed_status {
                    Some(s) => (s, false),
                    None => {
                        let ready = birth.as_ref().and_then(|i| row.birth(i)).is_some_and(|b| b.declared.contains(&rdm_core::status::NodeState::Ready));
                        (if ready { NodeStatus::Ready } else { NodeStatus::Pending }, false)
                    }
                },
            }
        };
        // Live for acceptance from the ops alone: created, and not stopped or deleted since.
        let live = !deleted && !stopped;
        let moved_from = row.renamed_from.clone().filter(|from| *from != row.name);
        Ok(Some(AtlasRow { mesh: row.mesh.clone(), r#type: row.r#type.clone(), moved_from, path: row.path.clone(), name: row.name.clone(), node_id: row.node_id.clone(), incarnation: birth, status, stopped, live, accepted, observed: observed_status, birth_source: "node-row" }))
    }

    /// `fabric.atlas.get`: one row for every node `paths` names (the current Build's topology), in
    /// `(path, name)` order.
    pub(crate) async fn view(&self, paths: impl IntoIterator<Item = PathName>, records: &crate::current_record::CurrentRecords<'_>, observed: &Topology) -> Result<rdm_core::views::AtlasView, String> {
        let mut paths: Vec<PathName> = paths.into_iter().collect();
        paths.sort();
        paths.dedup();
        let mut nodes = Vec::new();
        for path in paths {
            if let Some(r) = self.row(&path, records, observed).await? {
                if r.status == NodeStatus::Deleted {
                    continue;
                }
                nodes.push(rdm_core::views::AtlasNodeView {
                    path: r.path.clone(), name: r.name.clone(),
                    node_id: r.node_id,
                    incarnation_id: r.incarnation,
                    status: crate::views::node_status(r.status),
                    stopped: r.stopped,
                    accepted: r.accepted,
                    observed: r.observed.map(crate::views::node_status),
                });
            }
        }
        Ok(rdm_core::views::AtlasView { nodes })
    }
}

impl crate::control_plane::ControlPlane {
    /// The `Complete` receipt of `operation` of attempt `attempt` of `build_id`, as it streams up
    /// `build.attempt.run` to this fabric-primary (read from `builds`, the attempt's own Build journal), taken
    /// into its atlas before the frame is passed on (fabric-atlas.md R-FA2), whichever admin
    /// executed it; a create's record read once at its path. One
    /// `rdm.node_admin.fabric.update.via-atlas-receipt` span.
    pub(crate) async fn take_streamed_receipt(&self, builds: &dyn BuildStateAdapter, build_id: &crate::build::BuildId, attempt: u32, operation: &rdm_core::OperationId) {
        let span = tracing::info_span!("rdm.node_admin.fabric.update.via-atlas-receipt", build_id = %build_id.log(), attempt, operation = %operation.action, operation_subject = ?operation.subject, step = ?operation.step, outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal");
        let p = match builds.read_build(build_id).await {
            Ok(p) => p,
            Err(e) => {
                span.record("outcome", "build-unread").record("detail", format!("Build {build_id} could not be read for the receipt: {e}").as_str());
                span.in_scope(|| tracing::info!("a streamed receipt was not taken into the atlas"));
                return;
            }
        };
        // The receipt's subject as its current record holds it (query.md §1): what names its birth when
        // the attempt pinned none.
        let rows = match self.current_records() {
            Some(records) => {
                subject_rows(&records, [operation]).await.unwrap_or_else(|e| {
                    span.record("detail", e.as_str());
                    Vec::new()
                })
            }
            None => {
                span.record("detail", "no node records are open yet");
                Vec::new()
            }
        };
        let taken = self.atlas.take_receipt(&p, attempt, operation, &builds.receipt_mesh(), &rows);
        span.record("outcome", taken.name().as_str());
        span.in_scope(|| tracing::info!("a streamed receipt is the atlas's"));
    }

    /// The current stored record for the exact node admitted by a standalone action.
    pub(crate) async fn atlas_row_id(&self, node_id: &NodeId, observed: &Topology) -> Result<Option<AtlasRow>, crate::control_plane::Refusal> {
        match self.current_records() {
            Some(records) => self.atlas.row_id(node_id, &records, observed).await.map_err(crate::control_plane::Refusal::Unavailable),
            None => Ok(None),
        }
    }


    /// The reader of the current records (query.md §1), once this admin's nodes.storage is open: each
    /// node's record, both its parts, and the name claims.
    pub(crate) fn current_records(&self) -> Option<crate::current_record::CurrentRecords<'_>> {
        Some(crate::current_record::CurrentRecords { nodes: &**self.node_rows.get()? })
    }

    /// `t` as this fabric-primary's atlas holds its births ([`crate::atlas::Atlas::apply`]), their records
    /// read through the reader: a record that cannot be read refuses the acceptance, named.
    pub(crate) async fn apply_atlas(&self, t: &mut Topology) -> Result<(), crate::control_plane::Refusal> {
        let records = self.current_records();
        self.atlas.apply(t, records.as_ref()).await.map_err(crate::control_plane::Refusal::Unavailable)
    }

    /// [`Self::current_records`]' stores, held, once they are open.
    pub(crate) fn record_stores(&self) -> Option<crate::current_record::RecordStores> {
        Some(crate::current_record::RecordStores { nodes: self.node_rows.get()?.clone() })
    }

    /// The atlas row of `path` Build acceptance reads (fabric-atlas.md R-FA1): the current-pair lookup over
    /// this admin's records once they are open (query.md §1), else none.
    pub(crate) async fn atlas_row(&self, path: &PathName, observed: &Topology) -> Result<Option<AtlasRow>, crate::control_plane::Refusal> {
        let span = tracing::info_span!("rdm.node_admin.fabric.resolve.via-atlas-row", node_path = %path.path, node_name = %path.name, birth_source = tracing::field::Empty, status = tracing::field::Empty, stopped = tracing::field::Empty, accepted = tracing::field::Empty);
        // Before this admin's nodes.storage and node records are open it holds no record of any path.
        let row = match self.current_records() {
            Some(records) => self.atlas.row(path, &records, observed).await.map_err(crate::control_plane::Refusal::Unavailable)?,
            None => None,
        };
        if let Some(r) = &row {
            span.record("birth_source", r.birth_source).record("status", format!("{:?}", r.status).as_str()).record("stopped", r.stopped).record("accepted", tracing::field::debug(&r.accepted));
        }
        span.in_scope(|| tracing::info!("the atlas row Build acceptance reads"));
        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    //! Q207: a refused caller birth is deleted or re-admitted by its `(path, name)` and the atlas.
    use super::*;
    use crate::storage::{MemoryNodesStorage, NodesStorage};

    struct TestAtlas {
        atlas: Atlas,
    }
    impl TestAtlas {
        fn new() -> Self {
            Self { atlas: Atlas::default() }
        }
    }
    impl std::ops::Deref for TestAtlas {
        type Target = Atlas;
        fn deref(&self) -> &Atlas { &self.atlas }
    }

    #[test]
    fn lifecycle_effect_does_not_cross_nodes_sharing_an_incarnation() {
        let atlas = TestAtlas::new();
        let incarnation = IncarnationId::mint();
        let first = NodeId::mint();
        let second = NodeId::mint();
        atlas.held.lock().unwrap().effects.insert(Birth { node_id: first.clone(), incarnation: incarnation.clone() }, Effect {
            lifecycle: Lifecycle::Deleted,
            stamp: Stamp { time: 1, build_id: "fixture".into(), step: None, attempt: 1 },
        });
        assert!(atlas.holds_deleted_by_delete(&first, &incarnation, None));
        assert!(!atlas.holds_deleted_by_delete(&second, &incarnation, None));
    }


    #[test]
    fn accepted_attempt_refresh_preserves_new_admission_after_old_cleanup() {
        let atlas = TestAtlas::new();
        let build = crate::build::BuildId::parse(&rdm_core::fixture_crockford60("accepted-refresh")).unwrap();
        let node_id = NodeId::mint();
        let action = AttemptAction::Delete {
            path: PathName::new("unrelated.location", "north").unwrap(),
            mesh: "mesh1".into(), r#type: "node".into(),
            node_id: node_id.clone(), from_incarnation: IncarnationId::mint(),
        };
        atlas.note_accepted(&build, 1, &action);
        atlas.note_accepted(&build, 2, &action);
        atlas.note_accepted(&build, 1, &action);
        atlas.ended(&build, 1);
        let held = atlas.held.lock().unwrap();
        let accepted = held.accepted.get(&rdm_core::OperationSubject::Node(node_id.clone())).unwrap();
        assert_eq!(accepted.attempt, 2);
        assert_eq!(accepted.operation, rdm_core::OperationId::node("node.delete", &node_id));
    }

    #[test]
    fn lifecycle_order_keeps_original_step_ahead_of_attempt_retry() {
        let stamp = |step, attempt| Stamp { time: 1, build_id: "same-build".into(), step: Some(step), attempt };
        assert!(stamp(2, 1) > stamp(1, 9));
        assert!(stamp(2, 2) > stamp(2, 1));
    }

    fn path(path: &str, name: &str) -> PathName {
        PathName::new(path, name).unwrap()
    }

    /// A record of a birth at `at`, in `status`.
    fn record(at: PathName, mesh: &str, r#type: &str, status: NodeStatus) -> (NodeRow, IncarnationId) {
        let (name, incarnation) = (at, IncarnationId::mint());
        let mut r = NodeRow::new(name.clone(), mesh, r#type, NodeId::mint(), status);
        r.birth_mut(&incarnation, &name);
        (r, incarnation)
    }

    /// The stores `rows` are read from as current records (query.md §1): each row's name claimed, its
    /// record written at its node id, and a fabric-primary that `observed` gains, acting for every mesh
    /// of `rows` (each given an id in `observed`), which names no mesh-primary.
    struct Current {
        claims: MemoryNodesStorage,
        mesh_ids: BTreeMap<String, crate::model::MeshId>,
    }

    impl Current {
        async fn of(rows: &[&NodeRow], observed: &mut Topology) -> Self {
            let mut fp = crate::model::Node::allocated(path("writer.node-admin", "1"), rows.first().map(|r| r.mesh.as_str()).unwrap_or("mesh1"), "node-admin");
            fp.node_id = NodeId::mint();
            fp.incarnation_id = Some(IncarnationId::mint());
            fp.status = NodeStatus::Ready;
            fp.is_fabric_primary = true;
            observed.nodes.push(fp);
            let mut current = Self { claims: MemoryNodesStorage::default(), mesh_ids: BTreeMap::new() };
            for row in rows {
                if !current.mesh_ids.contains_key(&row.mesh) {
                    let id = crate::model::MeshId::mint();
                    observed.meshes.push(crate::model::Mesh { id: Some(id.clone()), name: row.mesh.clone(), status: crate::model::ScopeStatus::Ready });
                    current.mesh_ids.insert(row.mesh.clone(), id);
                }
                current.put(row).await;
            }
            current
        }

        /// Write `row` at its node id.
        async fn put(&self, row: &NodeRow) {
            self.claims.put_node(row).await.unwrap();
        }

        fn reader(&self) -> crate::current_record::CurrentRecords<'_> {
            crate::current_record::CurrentRecords { nodes: &self.claims }
        }
    }

    /// An atlas primary for `meshes`; the records a decision reads are handed to it.
    async fn atlas_of(_rows: &[&NodeRow], meshes: &[&str]) -> TestAtlas {
        let atlas = TestAtlas::new();
        atlas.scope(meshes.iter().map(|m| m.to_string()).collect());
        atlas
    }

    /// The record the current-pair lookup names at `name` among `rows`: the one whose node holds the
    /// name's claim, the live record there (a released one holds no claim).
    fn claimant<'a>(rows: &[&'a NodeRow], name: &PathName) -> Option<&'a NodeRow> {
        rows.iter().copied().find(|r| r.location() == *name && !matches!(r.status, NodeStatus::Deleted | NodeStatus::Withdrawn))
    }

    fn planned(paths: &[(PathName, &str)]) -> BTreeMap<String, Vec<PathName>> {
        let mut out: BTreeMap<String, Vec<PathName>> = BTreeMap::new();
        for (p, mesh) in paths {
            out.entry((*mesh).into()).or_default().push(p.clone());
        }
        out
    }

    /// CONTRACT (query.md §1: "A current-pair lookup resolves the path and name to a node ID from the node
    /// records. The reader reads the node's record at that node ID: one record per node, append-only, its
    /// latest line current"; Q323; Q109: a version a deleted birth writes after its re-creation is
    /// ignored by every reader): a deleted birth still running (a paused node-admin a recovery deleted,
    /// resumed) writes its own record after the birth that re-created the path, which names it its
    /// predecessor. The atlas row Build acceptance reads at the path is the re-created birth's, live, and
    /// its own record is read at its own id; the deleted birth read by its id is its own latest line.
    /// Must NOT happen: the path read from the deleted birth's own write and a stop of its live successor
    /// refused `node-not-live` (S2 container seed 1431101 round 40, candidate 187fe558a).
    #[tokio::test]
    async fn a_deleted_births_own_write_never_hides_the_birth_that_holds_its_path() {
        use crate::model::{Fabric, FabricId, Mesh, MeshId, ProviderKind, ScopeStatus};
        let at = path("mesh2.node-admin", "1");
        let (old, old_inc) = record(path("mesh2.node-admin", "1"), "mesh2", "node-admin", NodeStatus::Deleted);
        let (mut new, new_inc) = record(path("mesh2.node-admin", "1"), "mesh2", "node-admin", NodeStatus::Ready);
        new.birth_mut(&new_inc, &at).declared.insert(rdm_core::status::NodeState::Ready);
        // The re-creation names the deleted birth its predecessor (node-recovery.md).
        new.predecessor = Some(crate::storage::BirthRef { node_id: old.node_id.clone(), incarnation_id: old_inc.clone() });
        let atlas = TestAtlas::new();
        // mesh2's installed mesh-primary is the re-created birth: it writes mesh2's records.
        let (fabric_id, mesh_id) = (FabricId::mint(), MeshId::mint());
        let mut primary = crate::model::Node::allocated(at.clone(), "mesh2", "node-admin");
        primary.node_id = new.node_id.clone();
        primary.incarnation_id = Some(new_inc.clone());
        primary.status = NodeStatus::Ready;
        primary.is_primary = true;
        let observed = crate::topology::Topology { fabric: Fabric { id: fabric_id.clone(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process }, meshes: vec![Mesh { id: Some(mesh_id.clone()), name: "mesh2".into(), status: ScopeStatus::Ready }], nodes: vec![primary] };
        let claims = MemoryNodesStorage::default();
        claims.put_node(&new).await.unwrap();
        claims.put_node(&old).await.unwrap();
        // The deleted birth, resumed, writes its own record live, after the re-creation.
        let mut late = old.clone();
        late.status = NodeStatus::Ready;
        late.writer = Some(crate::storage::BirthRef { node_id: old.node_id.clone(), incarnation_id: old_inc.clone() });
        claims.put_node(&late).await.unwrap();
        // The re-creation's live record holds the path.
        let reader = crate::current_record::CurrentRecords { nodes: &claims };
        let row = atlas.row(&at, &reader, &observed).await.unwrap().expect("a record is current at the path");
        assert_eq!((row.node_id.clone(), row.status, row.live), (new.node_id.clone(), NodeStatus::Ready, true), "the re-created birth is the path's, live: {row:?}");
        // By its own id, the deleted birth answers its own latest line (Q323: one record per node, latest wins).
        let by_id = atlas.row_id(&old.node_id, &reader, &observed).await.unwrap().expect("the deleted birth's record");
        assert_eq!(by_id.node_id, old.node_id, "the deleted birth's own record: {by_id:?}");
    }

    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:31
    /// "A caller birth a primary does not hold ... is decided by its path and name and the primary's atlas,
    ///  one decision for the fabric-primary and every mesh-primary (Q207): another birth at that path and name
    ///  now, or an op executing that re-creates it there, deletes the caller; nothing there, its record
    ///  live and the topology needing the path re-admits it through [node.mesh.join](node-mesh-join.md);
    ///  nothing there and the topology not needing it deletes it. A caller whose record is deleted is
    ///  deleted, its runtime terminated, never re-admitted; a path still needed is recovered by re-create (Q217)."
    #[tokio::test]
    async fn a_deleted_birth_whose_path_is_re_created_is_deleted() {
        let (old, old_inc) = record(path("mesh2.compute", "2"), "mesh2", "compute", NodeStatus::Deleted);
        let (new, new_inc) = record(path("mesh2.compute", "2"), "mesh2", "compute", NodeStatus::Ready);
        let atlas = atlas_of(&[&old, &new], &["mesh2"]).await;
        let decided = atlas.refused_caller(Some(&old), claimant(&[&old, &new], &old.location()), &old_inc, &planned(&[(path("mesh2.compute", "2"), "mesh2")]));
        assert_eq!(decided, CallerDecision::DeleteHeld { path: "mesh2.compute".into(), name: "2".into(), by: format!("{}@{}", new.node_id, new_inc.as_str()) });
    }

    /// CONTRACT (Q207): nothing else is at the caller's path and name, its record is live (one the gossip
    /// view lost) and the current Build's topology names it: the caller is re-admitted. Must NOT
    /// happen: a needed path's only live birth deleted.
    #[tokio::test]
    async fn nothing_at_a_needed_path_re_admits_a_caller_whose_record_is_live() {
        let (live, live_inc) = record(path("mesh1.node", "1"), "mesh1", "node", NodeStatus::Ready);
        let atlas = atlas_of(&[&live], &["mesh1"]).await;
        assert_eq!(atlas.refused_caller(Some(&live), claimant(&[&live], &live.location()), &live_inc, &planned(&[(path("mesh1.node", "1"), "mesh1")])), CallerDecision::ReAdmit { path: "mesh1.node".into(), name: "1".into() });
    }

    /// CONTRACT (Q217, Luke: "delete a caller deletes the caller, not just from the record but
    /// literally as well: if they come back and they are deleted they get actually deleted"): a caller
    /// whose record is deleted, nothing else at its path and name, is deleted whether or not the topology
    /// needs the path; the decision names the need, the path a re-create recovers. Must NOT happen: a
    /// deleted birth re-admitted.
    #[tokio::test]
    async fn a_caller_whose_record_is_deleted_is_deleted_needed_or_not() {
        let (gone, gone_inc) = record(path("mesh1.node", "2"), "mesh1", "node", NodeStatus::Deleted);
        let atlas = atlas_of(&[&gone], &["mesh1"]).await;
        assert_eq!(atlas.refused_caller(Some(&gone), None, &gone_inc, &planned(&[(path("mesh1.node", "2"), "mesh1")])), CallerDecision::DeleteDeletedBirth { path: "mesh1.node".into(), name: "2".into(), needed: true });
        assert_eq!(atlas.refused_caller(Some(&gone), None, &gone_inc, &planned(&[(path("mesh1.node", "1"), "mesh1")])), CallerDecision::DeleteDeletedBirth { path: "mesh1.node".into(), name: "2".into(), needed: false });
    }

    /// CONTRACT (Q207): nothing is at the caller's path and name, its record is live, and the topology does
    /// not name the path: the caller is deleted.
    #[tokio::test]
    async fn nothing_at_an_unneeded_path_deletes_the_caller() {
        let (live, live_inc) = record(path("mesh1.node", "3"), "mesh1", "node", NodeStatus::Ready);
        let atlas = atlas_of(&[&live], &["mesh1"]).await;
        assert_eq!(atlas.refused_caller(Some(&live), claimant(&[&live], &live.location()), &live_inc, &planned(&[(path("mesh1.node", "1"), "mesh1")])), CallerDecision::DeleteUnneeded { path: "mesh1.node".into(), name: "3".into() });
    }

    /// CONTRACT (Q207): an older birth of a node whose newer birth is current at the path is deleted.
    #[tokio::test]
    async fn an_older_birth_of_the_node_at_the_path_is_deleted() {
        let (mut r, older) = record(path("mesh1.node", "1"), "mesh1", "node", NodeStatus::Ready);
        let newer = IncarnationId::mint();
        r.birth_mut(&newer, &path("mesh1.node", "1"));
        let atlas = atlas_of(&[&r], &["mesh1"]).await;
        assert_eq!(atlas.refused_caller(Some(&r), claimant(&[&r], &r.location()), &older, &planned(&[(path("mesh1.node", "1"), "mesh1")])), CallerDecision::DeleteHeld { path: "mesh1.node".into(), name: "1".into(), by: format!("{}@{}", r.node_id, newer.as_str()) });
    }

    /// CONTRACT (Q207, one atlas for every node-admin): an admin decides only for the meshes it is
    /// primary for; a caller of another mesh, or one it holds no record of, is not its decision.
    #[tokio::test]
    async fn a_caller_outside_the_atlas_meshes_or_unrecorded_is_not_decided() {
        let (other, other_inc) = record(path("mesh2.node", "1"), "mesh2", "node", NodeStatus::Deleted);
        let atlas = atlas_of(&[&other], &["mesh1"]).await;
        assert_eq!(atlas.refused_caller(Some(&other), None, &other_inc, &planned(&[(path("mesh2.node", "1"), "mesh2")])), CallerDecision::NotMine { path: "mesh2.node".into(), name: "1".into() });
        assert_eq!(atlas.refused_caller(None, None, &IncarnationId::mint(), &planned(&[])), CallerDecision::NoRecord);
    }

    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:39
    /// "A node-admin's atlas is its topology and its in-flight commands over the meshes it is primary for,
    ///  its own mesh as a mesh-primary and every mesh as the fabric-primary, taken as it takes that primary
    ///  and narrowed off as it hands it away; the caller's record and the record at its path.name are read
    ///  through the current-pair lookup. A node-admin that is not the
    ///  fabric-primary verifies its decision at the root with `fabric.atlas.get` before it acts, and the
    ///  root's answer wins, except that a caller whose record it holds deleted stays deleted (Q217)."
    /// CONTRACT (Q207, "call home and verify at the root"): the fabric-primary's `fabric.atlas.get`
    /// wins over the mesh-primary's own decision: another birth at the path there deletes the caller;
    /// the caller current there is re-admitted, unless this admin holds its record deleted (Q217: never
    /// re-admitted); no row at a needed path is the caller's record deleted there, deleted and never
    /// re-admitted (Q217); no row at an unneeded path is unneeded.
    #[test]
    fn the_root_answer_wins() {
        let (caller, inc) = (NodeId::mint(), IncarnationId::mint());
        let at = path("mesh2.compute", "2");
        let row = |node_id: &NodeId, i: &IncarnationId| rdm_core::views::AtlasView {
            nodes: vec![rdm_core::views::AtlasNodeView { path: at.path.clone(), name: at.name.clone(), node_id: node_id.clone(), incarnation_id: Some(i.clone()), status: rdm_core::views::NodeStatus::Ready, stopped: false, accepted: None, observed: None }],
        };
        let (other, other_inc) = (NodeId::mint(), IncarnationId::mint());
        let mine = CallerDecision::ReAdmit { path: at.path.clone(), name: at.name.clone()};
        assert_eq!(mine.clone().verified((&caller, &inc), &row(&other, &other_inc), true), CallerDecision::DeleteHeld { path: at.path.clone(), name: at.name.clone(), by: format!("{other}@{}", other_inc.as_str()) });
        let held = CallerDecision::DeleteHeld { path: at.path.clone(), name: at.name.clone(), by: "x".into() };
        assert_eq!(held.clone().verified((&caller, &inc), &row(&caller, &inc), true), CallerDecision::ReAdmit { path: at.path.clone(), name: at.name.clone()});
        let empty = rdm_core::views::AtlasView { nodes: vec![] };
        assert_eq!(mine.verified((&caller, &inc), &empty, true), CallerDecision::DeleteDeletedBirth { path: at.path.clone(), name: at.name.clone(), needed: true }, "a caller this admin holds live is deleted when the root holds no live record of it");
        assert_eq!(held.clone().verified((&caller, &inc), &empty, true), CallerDecision::DeleteDeletedBirth { path: at.path.clone(), name: at.name.clone(), needed: true });
        assert_eq!(held.verified((&caller, &inc), &empty, false), CallerDecision::DeleteUnneeded { path: at.path.clone(), name: at.name.clone()});
        // Q217: a caller whose record this admin holds deleted stays deleted, whatever the root still
        // names at its path (the root has not yet heard the delete).
        let deleted = CallerDecision::DeleteDeletedBirth { path: at.path.clone(), name: at.name.clone(), needed: true };
        assert_eq!(deleted.clone().verified((&caller, &inc), &row(&caller, &inc), true), deleted, "a deleted record is never re-admitted by a root that has not heard its delete");
        assert_eq!(deleted.verified((&caller, &inc), &row(&other, &other_inc), true), CallerDecision::DeleteHeld { path: at.path.clone(), name: at.name.clone(), by: format!("{other}@{}", other_inc.as_str()) });
    }

    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:39
    /// "A node-admin's atlas is its topology and its in-flight commands over the meshes it is primary for,
    ///  its own mesh as a mesh-primary and every mesh as the fabric-primary, taken as it takes that primary
    ///  and narrowed off as it hands it away": the meshes taken and handed away, and no node record held.
    #[test]
    fn a_mesh_taken_is_scoped_and_a_mesh_handed_away_is_narrowed() {
        let atlas = TestAtlas::new();
        assert_eq!(atlas.scope(["mesh2".to_string()].into()), (vec!["mesh2".to_string()], vec![]), "a secondary of mesh2 takes its primary");
        assert_eq!(atlas.scope(["mesh1".to_string(), "mesh2".to_string()].into()), (vec!["mesh1".to_string()], vec![]), "mesh2's primary takes the fabric-primary: every mesh");
        assert_eq!(atlas.scope(["mesh1".to_string(), "mesh2".to_string()].into()), (vec![], vec![]), "nothing taken");
        assert_eq!(atlas.scope(["mesh2".to_string()].into()), (vec![], vec!["mesh1".to_string()]), "the fabric-primary handed away");
        assert_eq!(atlas.meshes(), ["mesh2".to_string()].into());
    }

}
