//! A node-admin's atlas, `Atlas` (docs/plans/proposal/fabric-atlas.md, Q194, Q197, Q207, Q224): the
//! union of the ops currently executing and the current topology state over the meshes the node-admin is
//! primary for (every mesh at the fabric-primary), read by Build acceptance and answered by
//! `fabric.atlas.get` (the fabric's read, the fabric-primary's atlas).
//!
//! Its sources are ops, never a gossip frame (Q197: "nothing starts or stops or does any action through
//! gossip"):
//! - the birth at a path is the one its node row names current (the store record its mesh-primary
//!   writes, a recovery's or a re-create's new birth included);
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
//! It holds every node record too (Q203, Q204): the fabric-primary fills them from the node records
//! once, at its start or hand-over, and each op writes them from then on: a record this admin writes
//! or holds as a writer (`crate::node_rows::WrittenRows`), the config `node.config.update` puts, and
//! the records of the meshes a Build names once its attempt completes (an op's result, Q197).
//! `node.get`, `mesh.nodes.get` and `fabric.nodes.get` answer from them and never read the node store.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::accepted::AttemptAction;
use crate::build_state::{BuildProjection, BuildStateAdapter, StepOutcome};
use crate::model::{IncarnationId, NodeId, NodeStatus, PathName};
use crate::topology::Topology;
use crate::storage::{NodeRow, NodesStorage};

/// Where an effect stands in the order of ops: its Build's acceptance stamp, the Build, its attempt.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Stamp {
    submitted_at_ms: u64,
    build_id: String,
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
    operation: String,
    build_id: String,
    attempt: u32,
}

#[derive(Debug, Default)]
struct Held {
    /// Every node record, by node id (Q203): what the node reads answer from.
    records: BTreeMap<NodeId, NodeRow>,
    /// The births whose record the atlas holds as it stood once the birth was heard: each record's
    /// births, and each birth read once when it was first heard (`read_new_births`).
    births_read: std::collections::BTreeSet<(NodeId, String)>,
    /// The completed lifecycle effect of each birth, by incarnation.
    effects: BTreeMap<String, Effect>,
    /// The operation accepted on each path, by `path.name`.
    accepted: BTreeMap<String, Accepted>,
    /// The meshes this admin is primary for (Q207): its own mesh as a mesh-primary, every mesh as the
    /// fabric-primary; empty for an admin that holds no primary. Each was filled once as it was taken.
    meshes: std::collections::BTreeSet<String>,
    /// The mesh whose rows this admin's writer writes, every write fed here as it is made
    /// ([`crate::node_rows::WrittenRows::feeds`]): its records are the rows as written. Another mesh's
    /// records are read at a fill and moved by this atlas's own receipts only.
    written: std::collections::BTreeSet<String>,
}

/// Q207's decision on a caller birth a primary refused as one it does not hold (a status declaration
/// refused `SubjectUnknown`, a deleted birth's Digest heard again), from this admin's atlas alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CallerDecision {
    /// Another birth is at the caller's `path.name` now, or an op that fills the path is executing:
    /// the caller is deleted.
    DeleteHeld {
        /// The caller's `path.name`.
        path: PathName,
        /// What is at the path: `<node id>@<incarnation>`, or the accepted operation's key.
        by: String,
    },
    /// Nothing is at the path and the topology does not need it: the caller is deleted.
    DeleteUnneeded {
        /// The caller's `path.name`.
        path: PathName,
    },
    /// The caller's record is deleted and nothing else is at the path: the caller is deleted, its
    /// runtime terminated, never re-admitted (Q217). A path the topology still `needed` is recovered by
    /// a re-create at that path.name, a new birth, through node recovery (its owed resubmit,
    /// `RecoverySubmitter::owed_submits`).
    DeleteDeletedBirth {
        /// The caller's `path.name`.
        path: PathName,
        /// Whether the current Build's topology names the path.
        needed: bool,
    },
    /// The caller's record is live, nothing else is at the path and the topology needs it: the caller
    /// is re-admitted.
    ReAdmit {
        /// The caller's `path.name`.
        path: PathName,
    },
    /// The atlas holds no record of the caller: it has no `path.name` to decide on.
    NoRecord,
    /// The caller's mesh is not one this admin is primary for: another primary decides.
    NotMine {
        /// The caller's `path.name`.
        path: PathName,
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

    /// The caller's `path.name`, when the atlas holds its record.
    pub(crate) fn path(&self) -> Option<&PathName> {
        match self {
            CallerDecision::DeleteHeld { path, .. } | CallerDecision::DeleteUnneeded { path } | CallerDecision::DeleteDeletedBirth { path, .. } | CallerDecision::ReAdmit { path } | CallerDecision::NotMine { path } => Some(path),
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
        let Some(path) = self.path().cloned() else { return self };
        if matches!(self, CallerDecision::NotMine { .. }) {
            return self;
        }
        let (node_id, incarnation) = caller;
        match root.node(&path.to_string()) {
            // A caller whose record this admin holds deleted is deleted, never re-admitted (Q217:
            // "Re-admission is for a caller whose record is live"): a root that still names it at its
            // path has not heard the delete its mesh-primary wrote, and cannot undo it.
            Some(r) if r.node_id == *node_id && matches!(self, CallerDecision::DeleteDeletedBirth { .. }) => self,
            Some(r) if r.node_id == *node_id && r.incarnation_id.as_ref().is_none_or(|i| i == incarnation) => CallerDecision::ReAdmit { path },
            Some(r) => CallerDecision::DeleteHeld { path, by: format!("{}@{}", r.node_id, r.incarnation_id.as_ref().map(|i| i.as_str()).unwrap_or("")) },
            None if needed => CallerDecision::DeleteDeletedBirth { path, needed },
            None => CallerDecision::DeleteUnneeded { path },
        }
    }
}

/// One node of the atlas: the birth its node row names current and its lifecycle as the ops left it.
#[derive(Debug, Clone)]
pub(crate) struct AtlasRow {
    pub(crate) name: PathName,
    pub(crate) node_id: NodeId,
    pub(crate) incarnation: Option<IncarnationId>,
    /// The status the ops left the birth in.
    pub(crate) status: NodeStatus,
    /// A completed node.stop of this birth, not followed by a completed node.start.
    pub(crate) stopped: bool,
    /// Live for acceptance: created, and not stopped or deleted by an op since.
    pub(crate) live: bool,
    /// The operation accepted on the node whose attempt has not ended.
    pub(crate) accepted: Option<String>,
    /// Context only: the gossip view's status of the same birth.
    pub(crate) observed: Option<NodeStatus>,
    /// Where the birth was read from: `node-row`.
    pub(crate) birth_source: &'static str,
}

/// How [`Atlas::install_config`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigInstall {
    /// The atlas holds the map at its revision now.
    Installed,
    /// The atlas holds the node's config at this revision, the one given or a newer one: nothing changed.
    Held(u64),
    /// The atlas holds no record of the node: nothing is written; the record it reads later is the row's.
    NoRecord,
}

impl ConfigInstall {
    /// The name a span carries.
    pub fn name(&self) -> String {
        match self {
            Self::Installed => "installed".into(),
            Self::Held(r) => format!("held:{r}"),
            Self::NoRecord => "no-record".into(),
        }
    }
}

/// `row` with the config `held` holds kept where `held`'s revision is newer (Q235 P1): a record read
/// back never takes the atlas's config back past a revision it installed.
fn keep_newer_config(held: Option<&NodeRow>, row: &mut NodeRow) {
    if let Some(h) = held.filter(|h| h.node_id == row.node_id && h.revision > row.revision) {
        row.config = h.config.clone();
        row.revision = h.revision;
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
fn accepted_on(action: &AttemptAction) -> Vec<(PathName, String)> {
    match action {
        AttemptAction::DeleteConnections { path, .. } => vec![(path.clone(), format!("node.connection.delete:{path}"))],
        AttemptAction::Stop { path, .. } => vec![(path.clone(), format!("node.stop:{path}"))],
        AttemptAction::Start { path, .. } => vec![(path.clone(), format!("node.start:{path}"))],
        AttemptAction::Delete { path, .. } => vec![(path.clone(), format!("node.delete:{path}"))],
        AttemptAction::Recover { path, .. } => vec![(path.clone(), format!("node.create:{path}"))],
        AttemptAction::Terminate { path, from_incarnation, .. } => vec![(path.clone(), format!("node.terminate:{path}@{}", from_incarnation.as_str()))],
        AttemptAction::Steps { steps, .. } => {
            let mut out: Vec<(PathName, String)> = Vec::new();
            for step in steps {
                if !out.iter().any(|(p, _)| p == step.node()) {
                    out.push((step.node().clone(), crate::build::step_key(step)));
                }
            }
            out
        }
    }
}

/// The birth a completed step of `p` acted on at `path`: the stop receipt's own birth, else the one the
/// attempt's action is targeted to.
fn birth_of_step(p: &BuildProjection, attempt: u32, path: &PathName, output: Option<&serde_json::Value>) -> Option<IncarnationId> {
    if let Some(op) = output.and_then(|v| serde_json::from_value::<rdm_core::LifecycleOp>(v.clone()).ok()) {
        return Some(op.incarnation);
    }
    match p.attempt_actions.get(&attempt)? {
        AttemptAction::DeleteConnections { path: at, from_incarnation, .. } | AttemptAction::Stop { path: at, from_incarnation, .. } | AttemptAction::Start { path: at, from_incarnation, .. } | AttemptAction::Delete { path: at, from_incarnation, .. } | AttemptAction::Recover { path: at, from_incarnation, .. } => {
            (at == path).then(|| from_incarnation.clone())
        }
        AttemptAction::Steps { pinned, .. } => pinned.iter().find(|(at, _)| at == path).map(|(_, b)| b.incarnation.clone()),
        AttemptAction::Terminate { .. } => None,
    }
}

/// What a streamed receipt wrote in the atlas ([`Atlas::take_receipt`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Taken {
    /// The Build's journal holds no completed receipt of the operation in the attempt.
    NoReceipt,
    /// The operation changes nothing the atlas holds (a connections delete, a mesh leave), or the
    /// atlas holds no record of its node.
    Nothing,
    /// The lifecycle effect of its birth only (the atlas holds no record of the node).
    Effect(Option<&'static str>),
    /// The lifecycle effect and the record's status.
    Status(NodeStatus),
    /// The record's new name.
    Renamed(PathName),
    /// The record's config or tags.
    Property,
    /// A create completed at the path: its record is read once.
    Created(PathName),
}

impl Taken {
    /// The name its span carries.
    pub(crate) fn name(&self) -> String {
        match self {
            Taken::NoReceipt => "no-receipt".into(),
            Taken::Nothing => "nothing".into(),
            Taken::Effect(l) => format!("effect:{}", l.unwrap_or("none")),
            Taken::Status(s) => format!("status:{s:?}"),
            Taken::Renamed(to) => format!("renamed:{to}"),
            Taken::Property => "property".into(),
            Taken::Created(at) => format!("created:{at}"),
        }
    }
}

/// The completed lifecycle receipt `s` of `p` taken over an older effect of its birth; the lifecycle's
/// name when it is one.
fn take_effect(held: &mut Held, p: &BuildProjection, s: &crate::build_state::BuildStepReceipt) -> Option<&'static str> {
    let (lifecycle, path) = lifecycle_of(&s.operation)?;
    let incarnation = birth_of_step(p, s.attempt, &path, s.output.as_ref())?;
    let stamp = Stamp { submitted_at_ms: p.submitted_at_ms, build_id: p.build_id.to_string(), attempt: s.attempt };
    if held.effects.get(incarnation.as_str()).is_none_or(|e| e.stamp < stamp) {
        held.effects.insert(incarnation.as_str().to_string(), Effect { lifecycle, stamp });
    }
    Some(match lifecycle {
        Lifecycle::Started => "started",
        Lifecycle::Stopped => "stopped",
        Lifecycle::Deleted => "deleted",
    })
}

/// The lifecycle a completed operation key leaves its birth in, and its path.
fn lifecycle_of(operation: &str) -> Option<(Lifecycle, PathName)> {
    let (op, path) = operation.split_once(':')?;
    let lifecycle = match op {
        "node.stop" => Lifecycle::Stopped,
        "node.start" => Lifecycle::Started,
        "node.delete" => Lifecycle::Deleted,
        _ => return None,
    };
    Some((lifecycle, path.parse().ok()?))
}

impl Atlas {
    /// Install what a refused caller's decision acts through (Q207); the first install stands.
    pub(crate) fn refused_callers(&self, callers: Arc<crate::refused_caller::RefusedCallers>) {
        let _ = self.callers.set(callers);
    }

    pub(crate) fn callers(&self) -> Option<&Arc<crate::refused_caller::RefusedCallers>> {
        self.callers.get()
    }

    /// Hold `row` as its node's record: a write an op made (a writer's record, an op's result). A config
    /// the atlas holds at a newer revision than `row`'s stays (Q235 P1: every install over an older
    /// revision only); the row's writer steps the revision on every write, so its own records always
    /// replace.
    pub fn write_record(&self, row: &NodeRow) {
        let mut held = self.held.lock().unwrap();
        held.births_read.extend(row.births.iter().map(|b| (row.node_id.clone(), b.incarnation_id.as_str().to_string())));
        let mut row = row.clone();
        keep_newer_config(held.records.get(&row.node_id), &mut row);
        held.records.insert(row.node_id.clone(), row);
    }

    /// The record of each birth of `births` the atlas has not read since the birth was first heard
    /// (a mesh-primary's self-healing, a recovery's or a re-create's new birth: an op the
    /// fabric-primary did not run, whose result reaches it through the view and the store's record,
    /// Q197), read once: one point read per such birth, never per request. The number read.
    pub(crate) async fn read_new_births(&self, rows: &dyn NodesStorage, births: Vec<(NodeId, IncarnationId)>) -> usize {
        let new: Vec<(NodeId, IncarnationId)> = {
            let held = self.held.lock().unwrap();
            births.into_iter().filter(|(n, i)| !held.births_read.contains(&(n.clone(), i.as_str().to_string()))).collect()
        };
        let mut read = 0;
        for (node_id, incarnation) in new {
            if let Ok(Some(r)) = rows.node(&node_id).await {
                let mut held = self.held.lock().unwrap();
                held.births_read.insert((node_id.clone(), incarnation.as_str().to_string()));
                held.births_read.extend(r.births.iter().map(|b| (node_id.clone(), b.incarnation_id.as_str().to_string())));
                let mut r = r;
                keep_newer_config(held.records.get(&node_id), &mut r);
                held.records.insert(node_id, r);
                read += 1;
            }
        }
        read
    }

    /// Apply `change` to the record the atlas holds for `node_id`: an op's result on a record another
    /// admin writes (Q197). `false` when the atlas holds no record of the node.
    pub(crate) fn update_record(&self, node_id: &NodeId, change: impl FnOnce(&mut NodeRow)) -> bool {
        match self.held.lock().unwrap().records.get_mut(node_id) {
            Some(r) => {
                change(r);
                true
            }
            None => false,
        }
    }

    /// The node `node_id` holds no record any more (a withdrawn claim).
    pub(crate) fn forget_record(&self, node_id: &NodeId) {
        self.held.lock().unwrap().records.remove(node_id);
    }

    /// The row of `node_id` as its mesh-primary wrote it, its `config` at `revision`, written into the
    /// atlas over an older revision only (node-config-update.md; Q235 P1): a caller's command's
    /// completion, or a mesh-primary's push up of a node's own update. The atlas never takes a map the
    /// row has not committed, and never ahead of the row.
    pub fn install_config(&self, node_id: &NodeId, config: &BTreeMap<String, String>, revision: u64) -> ConfigInstall {
        let mut held = self.held.lock().unwrap();
        let Some(r) = held.records.get_mut(node_id) else { return ConfigInstall::NoRecord };
        if revision <= r.revision {
            return ConfigInstall::Held(r.revision);
        }
        r.config = config.clone();
        r.revision = revision;
        ConfigInstall::Installed
    }

    /// The record the atlas holds for `node_id`.
    pub fn record(&self, node_id: &NodeId) -> Option<NodeRow> {
        self.held.lock().unwrap().records.get(node_id).cloned()
    }

    /// Whether the atlas holds the birth `(node_id, incarnation)` deleted: its node's record deleted
    /// with that birth, or a node.delete of that birth completed (Q217: such a birth that comes back is
    /// deleted, its runtime terminated).
    pub(crate) fn holds_deleted(&self, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
        let held = self.held.lock().unwrap();
        let recorded = held.records.get(node_id).is_some_and(|r| matches!(r.status, NodeStatus::Deleted | NodeStatus::Withdrawn) && r.birth(incarnation).is_some());
        recorded || held.effects.get(incarnation.as_str()).is_some_and(|e| e.lifecycle == Lifecycle::Deleted)
    }

    /// Whether the atlas holds the birth `(node_id, incarnation)` deleted by a delete: its node's record
    /// `Deleted` with that birth, or a node.delete of that birth completed. A withdrawn claim is not a
    /// delete: its birth may still live, surplus, until a removal deletes it (node-investigation.md: "a
    /// caller whose record is deleted is deleted"; Q217).
    pub(crate) fn holds_deleted_by_delete(&self, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
        let held = self.held.lock().unwrap();
        let recorded = held.records.get(node_id).is_some_and(|r| r.status == NodeStatus::Deleted && r.birth(incarnation).is_some());
        recorded || held.effects.get(incarnation.as_str()).is_some_and(|e| e.lifecycle == Lifecycle::Deleted)
    }

    /// `mesh` is the mesh whose rows this admin's writer writes and feeds to this atlas.
    pub fn writes(&self, mesh: &str) {
        self.held.lock().unwrap().written.insert(mesh.to_string());
    }

    /// `meshes` are the meshes whose rows this admin's writer writes now (its own as its mesh-primary,
    /// a mesh with no primary as the fabric-primary, `crate::node_rows::writes_for`): the writer read each
    /// whole once as it took its writing and feeds this atlas every row of it, so a fill reads none of
    /// them ([`Self::fill`]).
    pub fn writing(&self, meshes: std::collections::BTreeSet<String>) {
        self.held.lock().unwrap().written = meshes;
    }

    /// The records the atlas holds of `mesh`'s nodes, a mesh this admin is primary for (Q224; none
    /// otherwise): what `fabric.get`'s cert summary folds (fabric-certs.md §Tracking), never a store read.
    pub fn records_of(&self, mesh: &str) -> Vec<NodeRow> {
        let held = self.held.lock().unwrap();
        if !held.meshes.contains(mesh) {
            return Vec::new();
        }
        held.records.values().filter(|r| r.name.mesh == mesh).cloned().collect()
    }

    /// The record the atlas holds at `path`: the one whose node holds the name now.
    pub fn record_at(&self, path: &PathName) -> Option<NodeRow> {
        self.held.lock().unwrap().records.values().find(|r| r.name == *path).cloned()
    }

    /// The record a join at `path` of the node `node_id` is reconciled to (Q234; never the store,
    /// Q227): the live record at `path` (the node's own first), else a `Deleted` one there (the node's
    /// own first): a live claim of another node at the name stands over the joiner's deleted record, as
    /// the store's `path.name` index names the last record written there. A `Withdrawn` record names
    /// nothing (a withdrawn claim).
    pub fn record_for_join(&self, path: &PathName, node_id: &NodeId) -> Option<NodeRow> {
        let held = self.held.lock().unwrap();
        let at: Vec<&NodeRow> = held.records.values().filter(|r| r.name == *path && r.status != NodeStatus::Withdrawn).collect();
        let (live, deleted): (Vec<&NodeRow>, Vec<&NodeRow>) = at.into_iter().partition(|r| r.status != NodeStatus::Deleted);
        let own_first = |rows: &[&NodeRow]| rows.iter().find(|r| r.node_id == *node_id).or_else(|| rows.first()).map(|r| (*r).clone());
        own_first(&live).or_else(|| own_first(&deleted))
    }

    /// Every record the atlas holds, in node id order.
    pub fn records(&self) -> Vec<NodeRow> {
        self.held.lock().unwrap().records.values().cloned().collect()
    }

    /// The record of the node whose birth holds the endpoint key `endpoint_id`, its newest birth first.
    pub fn record_of_endpoint(&self, endpoint_id: &str) -> Option<NodeRow> {
        let held = self.held.lock().unwrap();
        let of = |r: &&NodeRow| r.newest_birth().and_then(|b| b.endpoint_id.as_ref()).is_some_and(|e| e.0 == endpoint_id);
        held.records.values().find(of).cloned()
    }

    /// Fill the records of each mesh in `meshes` from the node records, read whole (one listing per
    /// mesh): at the fabric-primary's start or hand-over (Q204), and once a Build's attempt completed
    /// (its op results on every mesh it names, Q197). A mesh this admin writes is not read: its records
    /// are its writer's, fed here as they are filled and written ([`Self::writing`]). The number of
    /// records held after.
    pub async fn fill(&self, rows: &dyn NodesStorage, meshes: impl IntoIterator<Item = String>) -> Result<usize, String> {
        let written = self.held.lock().unwrap().written.clone();
        let mut read = Vec::new();
        for mesh in meshes.into_iter().filter(|m| !written.contains(m)) {
            let listed = crate::node_rows::fold_records("atlas-fill", rows.nodes(&mesh).await.map_err(|e| format!("nodes.storage: the node records of {mesh} could not be read: {e}"))?);
            read.push((mesh, listed));
        }
        let mut held = self.held.lock().unwrap();
        for (mesh, listed) in read {
            let before: BTreeMap<NodeId, NodeRow> = held.records.iter().filter(|(_, r)| r.name.mesh == mesh).map(|(k, r)| (k.clone(), r.clone())).collect();
            held.records.retain(|_, r| r.name.mesh != mesh);
            for mut r in listed {
                // A version written by a birth this atlas holds deleted (its record deleted with that
                // birth, or a node.delete of it completed) is ignored where the atlas held the record
                // (build.md: "a deleted birth whose writes every reader refuses"): the held one stays.
                if let (Some(w), Some(kept)) = (r.writer.clone(), before.get(&r.node_id)) {
                    let deleted = before.get(&w.node_id).or(held.records.get(&w.node_id)).is_some_and(|d| matches!(d.status, NodeStatus::Deleted | NodeStatus::Withdrawn) && d.birth(&w.incarnation_id).is_some())
                        || held.effects.get(w.incarnation_id.as_str()).is_some_and(|e| e.lifecycle == Lifecycle::Deleted);
                    if deleted && kept.writer != r.writer {
                        crate::node_rows::ignored_version("atlas-fill", &r, &w, &format!("{} {:?} at revision {}", kept.node_id, kept.status, kept.revision));
                        held.records.insert(r.node_id.clone(), kept.clone());
                        continue;
                    }
                }
                let births: Vec<(NodeId, String)> = r.births.iter().map(|b| (r.node_id.clone(), b.incarnation_id.as_str().to_string())).collect();
                held.births_read.extend(births);
                keep_newer_config(before.get(&r.node_id), &mut r);
                held.records.insert(r.node_id.clone(), r);
            }
        }
        Ok(held.records.len())
    }

    /// The atlas's meshes set to `meshes`, the meshes this admin is primary for now (Q207): a mesh
    /// taken (a mesh-primary's own at its start or hand-over, every mesh at the fabric-primary's) is
    /// filled once from its node records ([`Self::fill`], Q204), a mesh this admin writes from its
    /// writer's rows, never the store, and a mesh handed away is narrowed off,
    /// its records no longer held. A mesh whose records could not be read is not taken, so the next
    /// call fills it. The meshes taken and handed away.
    pub async fn scope(&self, rows: &dyn NodesStorage, meshes: std::collections::BTreeSet<String>) -> Result<(Vec<String>, Vec<String>), String> {
        let (taken, handed): (Vec<String>, Vec<String>) = {
            let held = self.held.lock().unwrap();
            (meshes.difference(&held.meshes).cloned().collect(), held.meshes.difference(&meshes).cloned().collect())
        };
        if taken.is_empty() && handed.is_empty() {
            return Ok((taken, handed));
        }
        let filled = self.fill(rows, taken.iter().cloned()).await;
        let mut held = self.held.lock().unwrap();
        for mesh in &handed {
            held.records.retain(|_, r| r.name.mesh != *mesh);
            held.meshes.remove(mesh);
        }
        filled?;
        held.meshes.extend(taken.iter().cloned());
        Ok((taken, handed))
    }

    /// The meshes this admin is primary for, as the atlas holds them.
    pub(crate) fn meshes(&self) -> std::collections::BTreeSet<String> {
        self.held.lock().unwrap().meshes.clone()
    }

    /// Q207: the caller birth `(node_id, incarnation)` a primary refused as one it does not hold, decided
    /// from this atlas alone (its records and the ops executing; never the gossip view, never a store
    /// read): another birth at the caller's `path.name`, or an op executing that fills it, deletes the
    /// caller; nothing there and the caller's record deleted deletes it, never re-admitted (Q217);
    /// nothing there, its record live and `planned` (the current Build's topology, by mesh) naming the
    /// path re-admits it; nothing there and the topology not naming it deletes it.
    pub(crate) fn refused_caller(&self, node_id: &NodeId, incarnation: &IncarnationId, planned: &BTreeMap<String, Vec<PathName>>) -> CallerDecision {
        let held = self.held.lock().unwrap();
        let Some(caller) = held.records.get(node_id) else { return CallerDecision::NoRecord };
        let path = caller.name.clone();
        if !held.meshes.contains(&path.mesh) {
            return CallerDecision::NotMine { path };
        }
        let gone = |r: &NodeRow| matches!(r.status, NodeStatus::Deleted | NodeStatus::Withdrawn);
        // Another birth at the path now: another node's live record there, or a newer birth of the
        // caller's own node.
        let other = held.records.values().find(|r| r.name == path && r.node_id != *node_id && !gone(r)).and_then(|r| r.newest_birth().map(|b| format!("{}@{}", r.node_id, b.incarnation_id.as_str())));
        let newer = (!gone(caller)).then(|| caller.newest_birth()).flatten().filter(|b| b.incarnation_id != *incarnation).map(|b| format!("{}@{}", node_id, b.incarnation_id.as_str()));
        if let Some(by) = other.or(newer) {
            return CallerDecision::DeleteHeld { path, by };
        }
        // An op executing that fills the path of a deleted caller (its re-create) is something at it
        // too; a live caller's own record is the birth such an op created.
        if let Some(a) = held.accepted.get(&path.to_string()).filter(|a| gone(caller) && a.operation.starts_with("node.create:")) {
            return CallerDecision::DeleteHeld { path, by: a.operation.clone() };
        }
        let needed = planned.get(&path.mesh).is_some_and(|paths| paths.contains(&path));
        // A caller whose record is deleted is deleted for real (Q217): its record stays deleted, and a
        // needed path is recovered by a re-create, a new birth.
        if gone(caller) {
            CallerDecision::DeleteDeletedBirth { path, needed }
        } else if needed {
            CallerDecision::ReAdmit { path }
        } else {
            CallerDecision::DeleteUnneeded { path }
        }
    }

    /// The operations `action` acts with, accepted as attempt `attempt` of `build_id`: written at
    /// acceptance, before the attempt can be claimed (R-FA2).
    pub(crate) fn note_accepted(&self, build_id: &crate::build::BuildId, attempt: u32, action: &AttemptAction) {
        let mut held = self.held.lock().unwrap();
        for (path, operation) in accepted_on(action) {
            held.accepted.insert(path.to_string(), Accepted { operation, build_id: build_id.to_string(), attempt });
        }
    }

    /// Attempt `attempt` of `p` ended without completing: what it accepted is no longer executing.
    pub(crate) fn ended(&self, build_id: &crate::build::BuildId, attempt: u32) {
        self.held.lock().unwrap().accepted.retain(|_, a| !(a.build_id == build_id.as_str() && a.attempt <= attempt));
    }

    /// Attempt `attempt` of `p` completed: every completed lifecycle receipt of the Build is taken over
    /// an older effect of its birth, and what the attempt accepted is no longer executing. Called before
    /// the caller is answered `Complete` (R-FA2).
    pub(crate) fn receipts(&self, p: &BuildProjection, attempt: u32) {
        let mut held = self.held.lock().unwrap();
        for s in p.steps.iter().filter(|s| s.step == "Complete" && s.outcome == StepOutcome::Complete) {
            take_effect(&mut held, p, s);
        }
        held.accepted.retain(|_, a| !(a.build_id == p.build_id.as_str() && a.attempt <= attempt));
    }

    /// The `Complete` receipt of `operation` in attempt `attempt` of `p`, taken the moment it streams up
    /// `build.attempt.run` to this fabric-primary, whichever admin executed it (R-FA2: "execution
    /// progress writes it from each attempt's receipts as they stream up `build.attempt.run`, before
    /// the caller is answered `Complete`"): the lifecycle it leaves its birth in (stop, start, delete)
    /// and what it changed on the node's record (a rename's name, a stop's or a delete's status, the
    /// config and tags the Build applied in place). What it took, for its span; a create's is
    /// [`Taken::Created`], whose record its mesh-primary wrote and the caller reads once
    /// ([`Self::take_created`]).
    pub(crate) fn take_receipt(&self, p: &BuildProjection, attempt: u32, operation: &str) -> Taken {
        let Some(s) = p.steps.iter().rev().find(|s| s.attempt == attempt && s.operation == operation && s.step == "Complete" && s.outcome == StepOutcome::Complete) else {
            return Taken::NoReceipt;
        };
        let Some((op, path)) = operation.split_once(':').and_then(|(op, path)| Some((op, path.parse::<PathName>().ok()?))) else {
            return Taken::Nothing;
        };
        let mut held = self.held.lock().unwrap();
        let lifecycle = take_effect(&mut held, p, s);
        let node_of = |held: &Held| -> Option<NodeId> {
            let pinned = match p.attempt_actions.get(&attempt) {
                Some(AttemptAction::Steps { pinned, .. }) => pinned.iter().find(|(at, _)| *at == path).map(|(_, b)| b.node_id.clone()),
                // The node id the gates pinned (build.md §4.4).
                Some(AttemptAction::DeleteConnections { path: at, node_id, .. } | AttemptAction::Stop { path: at, node_id, .. } | AttemptAction::Start { path: at, node_id, .. } | AttemptAction::Delete { path: at, node_id, .. }) if *at == path => Some(node_id.clone()),
                _ => None,
            };
            pinned.or_else(|| held.records.values().find(|r| r.name == path && !matches!(r.status, NodeStatus::Deleted | NodeStatus::Withdrawn)).map(|r| r.node_id.clone()))
        };
        match op {
            "node.stop" | "node.delete" => {
                let to = if op == "node.stop" { NodeStatus::Stopped } else { NodeStatus::Deleted };
                let birth = birth_of_step(p, attempt, &path, s.output.as_ref());
                match (node_of(&held), birth) {
                    (Some(node_id), Some(incarnation)) => match held.records.get_mut(&node_id) {
                        Some(r) => {
                            r.set_status(&incarnation, to);
                            Taken::Status(to)
                        }
                        None => Taken::Effect(lifecycle),
                    },
                    _ => Taken::Effect(lifecycle),
                }
            }
            "node.start" => Taken::Effect(lifecycle),
            "node.update" => {
                // The Build's node.name.update: the name the step names (the receipt carries none).
                let to = match p.attempt_actions.get(&attempt) {
                    Some(AttemptAction::Steps { steps, .. }) => steps.iter().find_map(|st| match st {
                        rdm_core::build::Step::Update { node, name } if *node == path => Some(name.clone()),
                        _ => None,
                    }),
                    _ => None,
                };
                match (node_of(&held), to) {
                    (Some(node_id), Some(to)) => match held.records.get_mut(&node_id) {
                        Some(r) => {
                            r.renamed_from = Some(path.clone());
                            r.name = to.clone();
                            Taken::Renamed(to)
                        }
                        None => Taken::Nothing,
                    },
                    _ => Taken::Nothing,
                }
            }
            // A Build's config reaches the atlas as the row its mesh-primary wrote, at the row's revision
            // (Q235 P1: the mesh-primary's push up, or its own writer's record): never from the spec, which
            // carries no revision and would put an older map over a newer one.
            "node.config.update" => Taken::Nothing,
            "node.tag.create" | "node.tag.delete" => {
                // In place, to the create spec the accepted Build names for the path (build.md §1, "The
                // Build applies the diff"): its tags added (create) or the ones it does not name removed
                // (delete).
                let Some(spec) = p.topology.mesh(&path.mesh).and_then(|m| m.spec(&path)).cloned() else { return Taken::Nothing };
                let Some(node_id) = node_of(&held) else { return Taken::Nothing };
                let Some(r) = held.records.get_mut(&node_id) else { return Taken::Nothing };
                match op {
                    "node.tag.create" => r.tags.extend(spec.tags),
                    _ => r.tags = r.tags.intersection(&spec.tags).cloned().collect(),
                }
                Taken::Property
            }
            "node.create" => Taken::Created(path),
            _ => Taken::Nothing,
        }
    }

    /// The record a completed node.create at `path` wrote (its mesh-primary's write, the op's result,
    /// Q197): one point read at the path, held from now on. `false` when no record is written there.
    pub(crate) async fn take_created(&self, rows: &dyn NodesStorage, path: &PathName) -> Result<bool, String> {
        match rows.node_at(path).await.map_err(|e| format!("nodes.storage: the node row at {path} could not be read: {e}"))?.filter(|r| r.name == *path) {
            Some(r) => {
                self.write_record(&r);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// `t`, a view an acceptance decides from, with each of its births as this atlas holds it
    /// ([`Self::answer`]; R-FA1: the fabric-primary accepts or denies from the atlas, the gossip view
    /// context only): a birth an op deleted is not in it.
    pub(crate) fn apply(&self, t: &mut Topology) {
        t.nodes.retain_mut(|n| self.answer(n));
        // A birth an op created whose record this atlas holds (a create's completion, `take_created`)
        // is in the view acceptance decides from before the gossip view hears it (R-FA1: accepted or
        // denied from the atlas, the gossip view context only), in place of a birth at its path.name
        // that is no longer live: a node action on a path node recovery just re-created is decided on
        // the replacement, never refused `node-not-live` on the deleted birth (rdm#239).
        let held = self.held.lock().unwrap();
        for r in held.records.values() {
            let live = r.status.is_live() || r.status == NodeStatus::Starting;
            let Some(b) = r.newest_birth().filter(|_| live) else { continue };
            let deleted = held.effects.get(b.incarnation_id.as_str()).is_some_and(|e| e.lifecycle == Lifecycle::Deleted);
            if deleted || t.nodes.iter().any(|n| n.node_id == r.node_id || (n.name == r.name && n.status.is_live())) {
                continue;
            }
            t.nodes.retain(|n| n.name != r.name);
            t.nodes.push(crate::model::Node {
                name: r.name.clone(),
                kind: r.name.kind,
                mesh: r.name.mesh.clone(),
                node_id: r.node_id.clone(),
                endpoint_id: b.endpoint_id.clone(),
                incarnation_id: Some(b.incarnation_id.clone()),
                deployment_id: None,
                provider: None,
                data_dir: None,
                status: r.status,
                is_primary: false,
                is_fabric_primary: false,
                admin_api_base: None,
                transport_addr: b.transport_addr,
                listeners: b.listeners.clone(),
                routable: false,
                declared: None,
                stopped: false,
            });
        }
    }

    /// `n`, a birth Build acceptance decides from ([`Self::apply`]; a node read answers [`Self::read`],
    /// the row's status as written), as this atlas holds it: the name its record holds, and the lifecycle the
    /// ops left it in (a completed stop: stopped; a completed start: ready over every state
    /// its stop and its start pass through, the gossip view's stopped birth and its `Starting` and
    /// `Pending` before the mesh-primary's Ready, node-start.md "Completion", so a projection
    /// published before the start completed never moves it backward, R-FA3). `false` when an op deleted
    /// the birth: it is not answered. The gossip view's status stands where no op moved the birth, and
    /// over a completed start where it is a state after ready (R-FA3: context).
    pub(crate) fn answer(&self, n: &mut crate::model::Node) -> bool {
        let held = self.held.lock().unwrap();
        if let Some(r) = held.records.get(&n.node_id).filter(|r| r.status != NodeStatus::Withdrawn) {
            n.name = r.name.clone();
        }
        let effect = n.incarnation_id.as_ref().and_then(|i| held.effects.get(i.as_str())).map(|e| e.lifecycle);
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

    /// `n`, a birth `node.get` answers, as this atlas holds it (query.md §1; Q233, states.md "Who sets
    /// each state": "A query answers the state the row last took from the node's frame or the
    /// mesh-primary's receipt"): the name its record holds, and, where the record is of this birth (its
    /// newest, or a record with no birth yet) in a mesh this admin is primary for (only a primary's
    /// atlas holds a mesh's records, Q224), the row's status, `stopped` exactly when that is stopped:
    /// every status of the mesh whose rows this admin writes ([`Self::writes`]); in another mesh, the
    /// states an operation sets (stopping through deleted, connections-deleting and connections-deleted
    /// included), which reach this atlas with the operation (its receipt, [`Self::take_receipt`], or the
    /// fill at its attempt's completion, before the caller hears `Complete`, R-FA2). A birth's create-time
    /// progress (starting, pending, ready) is its report to its own mesh-primary, which
    /// another mesh's record holds only as of this atlas's last fill: that birth answers the view's status
    /// until its mesh-primary feeds this atlas. Never a lifecycle effect laid over the view: a
    /// command the node has not acknowledged moved the row nowhere, so it moves the answer nowhere.
    /// Silence is not a state a command sets: a birth its row holds live (starting through
    /// connections-deleted) that the view holds silent (`PendingReconnect`, `Dead`, observer-inferred,
    /// node-identity-and-state.md §7.3) is answered as the view holds it, cleared the moment it is
    /// heard; a birth its row holds leaving, stopped or deleting is silent as expected (Q58) and answers
    /// its row. A birth no such record is of answers the view's status. `false` when an op deleted the
    /// birth: it is not answered. Build acceptance reads [`Self::answer`], not this.
    pub(crate) fn read(&self, n: &mut crate::model::Node) -> bool {
        let held = self.held.lock().unwrap();
        if n.incarnation_id.as_ref().and_then(|i| held.effects.get(i.as_str())).is_some_and(|e| e.lifecycle == Lifecycle::Deleted) {
            return false;
        }
        if let Some(r) = held.records.get(&n.node_id).filter(|r| r.status != NodeStatus::Withdrawn) {
            n.name = r.name.clone();
            let of_birth = r.newest_birth().is_none_or(|b| Some(&b.incarnation_id) == n.incarnation_id.as_ref());
            let live = matches!(r.status, NodeStatus::Starting | NodeStatus::Pending | NodeStatus::Ready | NodeStatus::ConnectionsDeleting | NodeStatus::ConnectionsDeleted);
            let silent = live && matches!(n.status, NodeStatus::PendingReconnect | NodeStatus::Dead);
            let current = held.written.contains(&r.name.mesh) || !matches!(r.status, NodeStatus::Starting | NodeStatus::Pending | NodeStatus::Ready);
            if of_birth && held.meshes.contains(&r.name.mesh) && current && !silent {
                n.status = r.status;
                n.stopped = r.status == NodeStatus::Stopped;
            }
        }
        true
    }

    /// The effect the node row records last of `row`'s birth `incarnation`, with its stamp: a stop's
    /// `Stopped`, a start's `Ready`; any other effect leaves the lifecycle where it is.
    async fn row_effect(row: &NodeRow, incarnation: &IncarnationId, builds: &dyn BuildStateAdapter, held: Option<&Effect>) -> Option<Effect> {
        let op = row.birth(incarnation)?.operations.iter().rev().find(|o| match o.state {
            rdm_core::status::NodeState::Stopped => o.operation.operation.starts_with("node.stop:"),
            rdm_core::status::NodeState::Ready => o.operation.operation.starts_with("node.start:"),
            _ => false,
        })?;
        let lifecycle = if op.state == rdm_core::status::NodeState::Stopped { Lifecycle::Stopped } else { Lifecycle::Started };
        let submitted_at_ms = match held {
            Some(h) if h.stamp.build_id == op.operation.build_id => h.stamp.submitted_at_ms,
            _ => match crate::build::BuildId::parse(&op.operation.build_id) {
                Ok(id) => builds.read_build(&id).await.ok()?.submitted_at_ms,
                Err(_) => return None,
            },
        };
        Some(Effect { lifecycle, stamp: Stamp { submitted_at_ms, build_id: op.operation.build_id.clone(), attempt: op.operation.attempt } })
    }

    /// The atlas row of `path`: the birth its node row names current, its lifecycle as the newest of
    /// the fabric-primary's own receipts and the node row's recorded effects, and the operation accepted
    /// on it. `None` when no node row is written at the path: the gossip view never stands in for one
    /// (Q197).
    pub(crate) async fn row(&self, path: &PathName, rows: &dyn NodesStorage, builds: &dyn BuildStateAdapter, observed: &Topology) -> Result<Option<AtlasRow>, String> {
        // The path's index entry still names a node a rename moved off it (its claim released, the
        // entry left): that row names another path, so it is not the birth at this one.
        let row = rows.node_at(path).await.map_err(|e| format!("nodes.storage: the node row at {path} could not be read: {e}"))?.filter(|r| r.name == *path);
        let accepted = self.held.lock().unwrap().accepted.get(&path.to_string()).map(|a| a.operation.clone());
        // A path no node row is written at holds no record: "no record", never a birth read from the
        // gossip view (Q197).
        let Some(row) = row else { return Ok(None) };
        let birth = row.newest_birth().map(|b| b.incarnation_id.clone());
        let observed_status = observed.births().find(|n| n.node_id == row.node_id && n.incarnation_id == birth).map(|n| n.status);
        let held = birth.as_ref().and_then(|i| self.held.lock().unwrap().effects.get(i.as_str()).cloned());
        let from_row = match &birth {
            Some(i) => Self::row_effect(&row, i, builds, held.as_ref()).await,
            None => None,
        };
        let effect = match (held, from_row) {
            (Some(h), Some(r)) => Some(if r.stamp > h.stamp { r } else { h }),
            (h, r) => h.or(r),
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
        Ok(Some(AtlasRow { name: path.clone(), node_id: row.node_id.clone(), incarnation: birth, status, stopped, live, accepted, observed: observed_status, birth_source: "node-row" }))
    }

    /// `fabric.atlas.get`: one row for every node `paths` names (the current Build's topology), in
    /// `path.name` order.
    pub(crate) async fn view(&self, paths: impl IntoIterator<Item = PathName>, rows: &dyn NodesStorage, builds: &dyn BuildStateAdapter, observed: &Topology) -> Result<rdm_core::views::AtlasView, String> {
        let mut paths: Vec<PathName> = paths.into_iter().collect();
        paths.sort();
        paths.dedup();
        let mut nodes = Vec::new();
        for path in paths {
            if let Some(r) = self.row(&path, rows, builds, observed).await? {
                if r.status == NodeStatus::Deleted {
                    continue;
                }
                nodes.push(rdm_core::views::AtlasNodeView {
                    name: r.name,
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
    pub(crate) async fn take_streamed_receipt(&self, builds: &dyn BuildStateAdapter, build_id: &crate::build::BuildId, attempt: u32, operation: &str) {
        let span = tracing::info_span!("rdm.node_admin.fabric.update.via-atlas-receipt", build_id = %build_id.log(), attempt, operation, outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal");
        let taken = match builds.read_build(build_id).await {
            Ok(p) => self.atlas.take_receipt(&p, attempt, operation),
            Err(e) => {
                span.record("outcome", "build-unread").record("detail", format!("Build {build_id} could not be read for the receipt: {e}").as_str());
                span.in_scope(|| tracing::info!("a streamed receipt was not taken into the atlas"));
                return;
            }
        };
        span.record("outcome", taken.name().as_str());
        if let crate::atlas::Taken::Created(path) = &taken {
            match self.node_rows.get() {
                Some(rows) => match self.atlas.take_created(&**rows, path).await {
                    Ok(true) => {}
                    Ok(false) => {
                        span.record("detail", format!("no record is written at {path}").as_str());
                    }
                    Err(e) => {
                        span.record("detail", e.as_str());
                    }
                },
                None => {
                    span.record("detail", "no nodes.storage is open yet to read the created record from");
                }
            }
        }
        span.in_scope(|| tracing::info!("a streamed receipt is the atlas's"));
    }

    /// The atlas row of `path` Build acceptance reads (fabric-atlas.md R-FA1): from this admin's node
    /// rows once its nodes.storage is open, else from `observed` alone.
    pub(crate) async fn atlas_row(&self, path: &PathName, observed: &Topology) -> Result<Option<AtlasRow>, crate::control_plane::Refusal> {
        let span = tracing::info_span!("rdm.node_admin.fabric.resolve.via-atlas-row", node = %path, birth_source = tracing::field::Empty, status = tracing::field::Empty, stopped = tracing::field::Empty, accepted = tracing::field::Empty);
        // Before this admin's nodes.storage is open it holds no record of any path.
        let row = match self.node_rows.get() {
            Some(rows) => self.atlas.row(path, &**rows, &*self.builds, observed).await.map_err(crate::control_plane::Refusal::Unavailable)?,
            None => None,
        };
        if let Some(r) = &row {
            span.record("birth_source", r.birth_source).record("status", format!("{:?}", r.status).as_str()).record("stopped", r.stopped).record("accepted", r.accepted.as_deref().unwrap_or(""));
        }
        span.in_scope(|| tracing::info!("the atlas row Build acceptance reads"));
        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    //! Q207: a refused caller birth is deleted or re-admitted by its `path.name` and the atlas.
    use super::*;
    use crate::storage::MemoryNodesStorage;

    fn path(p: &str) -> PathName {
        p.parse().unwrap()
    }

    /// A record of a birth at `at`, in `status`.
    fn record(at: &str, status: NodeStatus) -> (NodeRow, IncarnationId) {
        let (name, incarnation) = (path(at), IncarnationId::mint());
        let mut r = NodeRow::new(name.clone(), NodeId::mint(), status);
        r.birth_mut(&incarnation, &name);
        (r, incarnation)
    }

    /// An atlas primary for `meshes`, filled once from `rows` at the moment it took them.
    async fn atlas_of(rows: &[&NodeRow], meshes: &[&str]) -> Atlas {
        let store = MemoryNodesStorage::default();
        for r in rows {
            store.put_node(r).await.unwrap();
        }
        let atlas = Atlas::default();
        atlas.scope(&store, meshes.iter().map(|m| m.to_string()).collect()).await.unwrap();
        atlas
    }

    fn planned(paths: &[&str]) -> BTreeMap<String, Vec<PathName>> {
        let mut out: BTreeMap<String, Vec<PathName>> = BTreeMap::new();
        for p in paths {
            out.entry(path(p).mesh.clone()).or_default().push(path(p));
        }
        out
    }

    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:31
    /// "A caller birth a primary does not hold ... is decided by its path.name and the primary's atlas,
    ///  one decision for the fabric-primary and every mesh-primary (Q207): another birth at that path.name
    ///  now, or an op executing that re-creates it there, deletes the caller; nothing there, its record
    ///  live and the topology needing the path re-admits it through [node.mesh.join](node-mesh-join.md);
    ///  nothing there and the topology not needing it deletes it. A caller whose record is deleted is
    ///  deleted, its runtime terminated, never re-admitted; a path still needed is recovered by re-create (Q217)."
    #[tokio::test]
    async fn a_deleted_birth_whose_path_is_re_created_is_deleted() {
        let (old, old_inc) = record("mesh2.compute.2", NodeStatus::Deleted);
        let (new, new_inc) = record("mesh2.compute.2", NodeStatus::Ready);
        let atlas = atlas_of(&[&old, &new], &["mesh2"]).await;
        let decided = atlas.refused_caller(&old.node_id, &old_inc, &planned(&["mesh2.compute.2"]));
        assert_eq!(decided, CallerDecision::DeleteHeld { path: path("mesh2.compute.2"), by: format!("{}@{}", new.node_id, new_inc.as_str()) });
    }

    /// CONTRACT (Q207): nothing else is at the caller's path.name, its record is live (one the gossip
    /// view lost) and the current Build's topology names it: the caller is re-admitted. Must NOT
    /// happen: a needed path's only live birth deleted.
    #[tokio::test]
    async fn nothing_at_a_needed_path_re_admits_a_caller_whose_record_is_live() {
        let (live, live_inc) = record("mesh1.rpc.1", NodeStatus::Ready);
        let atlas = atlas_of(&[&live], &["mesh1"]).await;
        assert_eq!(atlas.refused_caller(&live.node_id, &live_inc, &planned(&["mesh1.rpc.1"])), CallerDecision::ReAdmit { path: path("mesh1.rpc.1") });
    }

    /// CONTRACT (Q217, Luke: "delete a caller deletes the caller, not just from the record but
    /// literally as well: if they come back and they are deleted they get actually deleted"): a caller
    /// whose record is deleted, nothing else at its path.name, is deleted whether or not the topology
    /// needs the path; the decision names the need, the path a re-create recovers. Must NOT happen: a
    /// deleted birth re-admitted.
    #[tokio::test]
    async fn a_caller_whose_record_is_deleted_is_deleted_needed_or_not() {
        let (gone, gone_inc) = record("mesh1.rpc.2", NodeStatus::Deleted);
        let atlas = atlas_of(&[&gone], &["mesh1"]).await;
        assert_eq!(atlas.refused_caller(&gone.node_id, &gone_inc, &planned(&["mesh1.rpc.2"])), CallerDecision::DeleteDeletedBirth { path: path("mesh1.rpc.2"), needed: true });
        assert_eq!(atlas.refused_caller(&gone.node_id, &gone_inc, &planned(&["mesh1.rpc.1"])), CallerDecision::DeleteDeletedBirth { path: path("mesh1.rpc.2"), needed: false });
    }

    /// CONTRACT (Q207): nothing is at the caller's path.name, its record is live, and the topology does
    /// not name the path: the caller is deleted.
    #[tokio::test]
    async fn nothing_at_an_unneeded_path_deletes_the_caller() {
        let (live, live_inc) = record("mesh1.rpc.3", NodeStatus::Ready);
        let atlas = atlas_of(&[&live], &["mesh1"]).await;
        assert_eq!(atlas.refused_caller(&live.node_id, &live_inc, &planned(&["mesh1.rpc.1"])), CallerDecision::DeleteUnneeded { path: path("mesh1.rpc.3") });
    }

    /// CONTRACT (Q207, the atlas is topology and in-flight ops): an op accepted on the caller's path that
    /// fills it (its re-create) is something at the path: the caller is deleted.
    #[tokio::test]
    async fn a_create_executing_at_the_path_deletes_the_caller() {
        let (gone, gone_inc) = record("mesh1.rpc.2", NodeStatus::Deleted);
        let atlas = atlas_of(&[&gone], &["mesh1"]).await;
        let build = crate::build::BuildId::parse("66h8qrqvaajk").unwrap();
        atlas.note_accepted(&build, 1, &AttemptAction::Recover { path: path("mesh1.rpc.2"), from_incarnation: gone_inc.clone(), node_id: gone.node_id.clone(), backbone: false });
        assert_eq!(atlas.refused_caller(&gone.node_id, &gone_inc, &planned(&["mesh1.rpc.2"])), CallerDecision::DeleteHeld { path: path("mesh1.rpc.2"), by: "node.create:mesh1.rpc.2".into() });
    }

    /// CONTRACT (Q207): a create executing at the path of a caller whose record is live is that caller's
    /// own create (its first declarations can reach its mesh-primary before its view holds it): nothing
    /// else is at the path. Must NOT happen: a birth deleted by its own create.
    #[tokio::test]
    async fn a_create_of_the_caller_itself_is_not_another_birth() {
        let (born, born_inc) = record("mesh1.rpc.2", NodeStatus::Pending);
        let atlas = atlas_of(&[&born], &["mesh1"]).await;
        let build = crate::build::BuildId::parse("66h8qrqvaajk").unwrap();
        atlas.note_accepted(&build, 1, &AttemptAction::Recover { path: path("mesh1.rpc.2"), from_incarnation: IncarnationId::mint(), node_id: NodeId::mint(), backbone: false });
        assert_eq!(atlas.refused_caller(&born.node_id, &born_inc, &planned(&["mesh1.rpc.2"])), CallerDecision::ReAdmit { path: path("mesh1.rpc.2") });
    }

    /// CONTRACT (Q207): an older birth of a node whose newer birth is current at the path is deleted.
    #[tokio::test]
    async fn an_older_birth_of_the_node_at_the_path_is_deleted() {
        let (mut r, older) = record("mesh1.rpc.1", NodeStatus::Ready);
        let newer = IncarnationId::mint();
        r.birth_mut(&newer, &path("mesh1.rpc.1"));
        let atlas = atlas_of(&[&r], &["mesh1"]).await;
        assert_eq!(atlas.refused_caller(&r.node_id, &older, &planned(&["mesh1.rpc.1"])), CallerDecision::DeleteHeld { path: path("mesh1.rpc.1"), by: format!("{}@{}", r.node_id, newer.as_str()) });
    }

    /// CONTRACT (Q207, one atlas for every node-admin): an admin decides only for the meshes it is
    /// primary for; a caller of another mesh, or one it holds no record of, is not its decision.
    #[tokio::test]
    async fn a_caller_outside_the_atlas_meshes_or_unrecorded_is_not_decided() {
        let (other, other_inc) = record("mesh2.rpc.1", NodeStatus::Deleted);
        let atlas = atlas_of(&[&other], &["mesh1"]).await;
        atlas.write_record(&other);
        assert_eq!(atlas.refused_caller(&other.node_id, &other_inc, &planned(&["mesh2.rpc.1"])), CallerDecision::NotMine { path: path("mesh2.rpc.1") });
        assert_eq!(atlas.refused_caller(&NodeId::mint(), &IncarnationId::mint(), &planned(&[])), CallerDecision::NoRecord);
    }

    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:39
    /// "A node-admin's atlas is its topology and its in-flight commands over the meshes it is primary for,
    ///  its own mesh as a mesh-primary and every mesh as the fabric-primary, each filled once from the node
    ///  records as it takes that primary and narrowed off as it hands it away. A node-admin that is not the
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
        let at = path("mesh2.compute.2");
        let row = |node_id: &NodeId, i: &IncarnationId| rdm_core::views::AtlasView {
            nodes: vec![rdm_core::views::AtlasNodeView { name: at.clone(), node_id: node_id.clone(), incarnation_id: Some(i.clone()), status: rdm_core::views::NodeStatus::Ready, stopped: false, accepted: None, observed: None }],
        };
        let (other, other_inc) = (NodeId::mint(), IncarnationId::mint());
        let mine = CallerDecision::ReAdmit { path: at.clone() };
        assert_eq!(mine.clone().verified((&caller, &inc), &row(&other, &other_inc), true), CallerDecision::DeleteHeld { path: at.clone(), by: format!("{other}@{}", other_inc.as_str()) });
        let held = CallerDecision::DeleteHeld { path: at.clone(), by: "x".into() };
        assert_eq!(held.clone().verified((&caller, &inc), &row(&caller, &inc), true), CallerDecision::ReAdmit { path: at.clone() });
        let empty = rdm_core::views::AtlasView { nodes: vec![] };
        assert_eq!(mine.verified((&caller, &inc), &empty, true), CallerDecision::DeleteDeletedBirth { path: at.clone(), needed: true }, "a caller this admin holds live is deleted when the root holds no live record of it");
        assert_eq!(held.clone().verified((&caller, &inc), &empty, true), CallerDecision::DeleteDeletedBirth { path: at.clone(), needed: true });
        assert_eq!(held.verified((&caller, &inc), &empty, false), CallerDecision::DeleteUnneeded { path: at.clone() });
        // Q217: a caller whose record this admin holds deleted stays deleted, whatever the root still
        // names at its path (the root has not yet heard the delete).
        let deleted = CallerDecision::DeleteDeletedBirth { path: at.clone(), needed: true };
        assert_eq!(deleted.clone().verified((&caller, &inc), &row(&caller, &inc), true), deleted, "a deleted record is never re-admitted by a root that has not heard its delete");
        assert_eq!(deleted.verified((&caller, &inc), &row(&other, &other_inc), true), CallerDecision::DeleteHeld { path: at.clone(), by: format!("{other}@{}", other_inc.as_str()) });
    }

    /// CONTRACT: docs/architecture/lifecycles/node-investigation.md:39
    /// "A node-admin's atlas is its topology and its in-flight commands over the meshes it is primary for,
    ///  its own mesh as a mesh-primary and every mesh as the fabric-primary, each filled once from the node
    ///  records as it takes that primary and narrowed off as it hands it away."
    #[tokio::test]
    async fn a_mesh_taken_is_filled_once_and_a_mesh_handed_away_is_narrowed() {
        let (a, _) = record("mesh1.rpc.1", NodeStatus::Ready);
        let (b, _) = record("mesh2.rpc.1", NodeStatus::Ready);
        let store = MemoryNodesStorage::default();
        store.put_node(&a).await.unwrap();
        store.put_node(&b).await.unwrap();
        let atlas = Atlas::default();
        assert_eq!(atlas.scope(&store, ["mesh2".to_string()].into()).await.unwrap(), (vec!["mesh2".to_string()], vec![]), "a secondary of mesh2 takes its primary");
        assert!(atlas.record(&b.node_id).is_some() && atlas.record(&a.node_id).is_none());
        assert_eq!(atlas.scope(&store, ["mesh1".to_string(), "mesh2".to_string()].into()).await.unwrap(), (vec!["mesh1".to_string()], vec![]), "mesh2's primary takes the fabric-primary: every mesh");
        assert!(atlas.record(&a.node_id).is_some());
        assert_eq!(atlas.scope(&store, ["mesh1".to_string(), "mesh2".to_string()].into()).await.unwrap(), (vec![], vec![]), "nothing taken, nothing read");
        assert_eq!(atlas.scope(&store, ["mesh2".to_string()].into()).await.unwrap(), (vec![], vec!["mesh1".to_string()]), "the fabric-primary handed away");
        assert!(atlas.record(&a.node_id).is_none() && atlas.record(&b.node_id).is_some());
    }

    // @feature: fabric-atlas-read
    #[test]
    fn atlas_view_answers_every_planned_path_in_order_carries_in_flight_accepted_ops_stop_and_start_effects_and_omits_deleted_births() {
        // docs/architecture/query.md:11 — "A caller outside the node sends `fabric.get`,
        // `mesh.get` and `node.get` to the fabric-primary, which answers from its atlas: every
        // mesh's node records, held in memory, filled from the node records once at its start or
        // hand-over and written by each op after that"
        use crate::accepted::FabricTopology;
        use crate::build_state::{
            AttemptReason, BuildProjection, BuildState, BuildStepReceipt, MemoryBuildStateAdapter,
        };
        use crate::model::{Fabric, FabricId, Node, ProviderKind, ScopeStatus};
        use crate::span_capture::Capture;

        let capture = Capture::new("fabric-atlas-read-unit");
        let _guard = tracing::dispatcher::set_default(&capture.dispatch);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");

        rt.block_on(async {
            let (mut row1, inc1) = record("mesh1.rpc.1", NodeStatus::Ready);
            row1.birth_mut(&inc1, &path("mesh1.rpc.1"))
                .declared
                .insert(rdm_core::status::NodeState::Ready);
            let (mut row2, inc2) = record("mesh1.rpc.2", NodeStatus::Ready);
            row2.birth_mut(&inc2, &path("mesh1.rpc.2"))
                .declared
                .insert(rdm_core::status::NodeState::Ready);

            let store = MemoryNodesStorage::default();
            store.put_node(&row1).await.unwrap();
            store.index_path(&row1.name, &row1.node_id).await.unwrap();
            store.put_node(&row2).await.unwrap();
            store.index_path(&row2.name, &row2.node_id).await.unwrap();
            let builds = MemoryBuildStateAdapter::new();

            let mut node1 = Node::allocated(path("mesh1.rpc.1"));
            node1.node_id = row1.node_id.clone();
            node1.incarnation_id = Some(inc1.clone());
            node1.status = NodeStatus::Ready;

            let mut node2 = Node::allocated(path("mesh1.rpc.2"));
            node2.node_id = row2.node_id.clone();
            node2.incarnation_id = Some(inc2.clone());
            node2.status = NodeStatus::Ready;

            let observed = Topology {
                fabric: Fabric {
                    id: FabricId::mint(),
                    name: "fabric1".into(),
                    status: ScopeStatus::Ready,
                    provider: ProviderKind::Process,
                },
                meshes: Vec::new(),
                nodes: vec![node1, node2],
            };

            let atlas = atlas_of(&[&row1, &row2], &["mesh1"]).await;
            let build_stop = crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b-stop")).unwrap();
            let stop_action = AttemptAction::Stop {
                path: path("mesh1.rpc.1"),
                node_id: row1.node_id.clone(),
                from_incarnation: inc1.clone(),
            };
            atlas.note_accepted(&build_stop, 1, &stop_action);

            // 1. In-flight stop command appears in `accepted` while the birth is still Ready.
            let view_in_flight = atlas
                .view(
                    [path("mesh1.rpc.2"), path("mesh1.rpc.1")],
                    &store,
                    &builds,
                    &observed,
                )
                .await
                .unwrap();
            assert_eq!(view_in_flight.nodes.len(), 2);
            assert_eq!(view_in_flight.nodes[0].name, path("mesh1.rpc.1"));
            assert_eq!(
                view_in_flight.nodes[0].accepted.as_deref(),
                Some("node.stop:mesh1.rpc.1")
            );
            assert_eq!(
                view_in_flight.nodes[0].status,
                rdm_core::views::NodeStatus::Ready
            );
            assert!(!view_in_flight.nodes[0].stopped);

            // 2. Complete receipt of node.stop transitions the birth to stopped=true, status=Leaving,
            //    and `receipts` clears the in-flight `accepted` entry.
            let stop_proj = BuildProjection {
                build_id: build_stop.clone(),
                topology: FabricTopology {
                    section_of: None,
                    storage_change: None,
                    topology_storage_provider: None,
                    fabric: "fabric1".into(),
                    meshes: Default::default(),
                    pinned: Default::default(),
                },
                submitted_change: None,
                submitted_at_ms: 100,
                state: BuildState::Complete,
                attempt: 1,
                executor: None,
                steps: vec![BuildStepReceipt {
                    mesh: None,
                    build_id: build_stop,
                    attempt: 1,
                    operation: "node.stop:mesh1.rpc.1".into(),
                    step: "Complete".into(),
                    outcome: StepOutcome::Complete,
                    output: None,
                    executor: None,
                }],
                last_failure: None,
                reason: AttemptReason::Stop,
                action: Some(stop_action.clone()),
                attempt_actions: [(1, stop_action)].into_iter().collect(),
                attempt_executors: Default::default(),
                opened: 1,
                failed_at: None,
                completed_at: None,
                reported: Default::default(),
                claimant: None,
                lanes: Default::default(),
            };
            assert_eq!(
                atlas.take_receipt(&stop_proj, 1, "node.stop:mesh1.rpc.1"),
                Taken::Status(NodeStatus::Stopped)
            );
            atlas.receipts(&stop_proj, 1);

            let view_stopped = atlas
                .view(
                    [path("mesh1.rpc.1"), path("mesh1.rpc.2")],
                    &store,
                    &builds,
                    &observed,
                )
                .await
                .unwrap();
            assert_eq!(view_stopped.nodes[0].accepted, None);
            assert!(view_stopped.nodes[0].stopped);
            assert_eq!(
                view_stopped.nodes[0].status,
                rdm_core::views::NodeStatus::Leaving
            );
            assert_eq!(
                view_stopped.nodes[0].observed,
                Some(rdm_core::views::NodeStatus::Ready)
            );

            // 3. Complete receipt of a newer node.start restores stopped=false, status=Ready;
            //    Complete receipt of node.delete on mesh1.rpc.2 omits mesh1.rpc.2 from the view.
            let build_start =
                crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b-start")).unwrap();
            let start_action = AttemptAction::Start {
                path: path("mesh1.rpc.1"),
                node_id: row1.node_id.clone(),
                from_incarnation: inc1.clone(),
            };
            let start_proj = BuildProjection {
                build_id: build_start.clone(),
                submitted_at_ms: 200,
                steps: vec![BuildStepReceipt {
                    mesh: None,
                    build_id: build_start,
                    attempt: 1,
                    operation: "node.start:mesh1.rpc.1".into(),
                    step: "Complete".into(),
                    outcome: StepOutcome::Complete,
                    output: None,
                    executor: None,
                }],
                reason: AttemptReason::Start,
                action: Some(start_action.clone()),
                attempt_actions: [(1, start_action)].into_iter().collect(),
                ..stop_proj.clone()
            };
            assert_eq!(
                atlas.take_receipt(&start_proj, 1, "node.start:mesh1.rpc.1"),
                Taken::Effect(Some("started"))
            );

            let build_del =
                crate::build::BuildId::parse(&rdm_core::fixture_crockford60("b-del")).unwrap();
            let del_action = AttemptAction::Delete {
                path: path("mesh1.rpc.2"),
                node_id: row2.node_id.clone(),
                from_incarnation: inc2.clone(),
            };
            let del_proj = BuildProjection {
                build_id: build_del.clone(),
                submitted_at_ms: 300,
                steps: vec![BuildStepReceipt {
                    mesh: None,
                    build_id: build_del,
                    attempt: 1,
                    operation: "node.delete:mesh1.rpc.2".into(),
                    step: "Complete".into(),
                    outcome: StepOutcome::Complete,
                    output: None,
                    executor: None,
                }],
                reason: AttemptReason::Delete,
                action: Some(del_action.clone()),
                attempt_actions: [(1, del_action)].into_iter().collect(),
                ..stop_proj
            };
            assert_eq!(
                atlas.take_receipt(&del_proj, 1, "node.delete:mesh1.rpc.2"),
                Taken::Status(NodeStatus::Deleted)
            );
            row2.set_status(&inc2, NodeStatus::Deleted);
            store.put_node(&row2).await.unwrap();

            let view_after = atlas
                .view(
                    [path("mesh1.rpc.1"), path("mesh1.rpc.2")],
                    &store,
                    &builds,
                    &observed,
                )
                .await
                .unwrap();
            assert_eq!(view_after.nodes.len(), 1);
            assert_eq!(view_after.nodes[0].name, path("mesh1.rpc.1"));
            assert!(!view_after.nodes[0].stopped);
            assert_eq!(
                view_after.nodes[0].status,
                rdm_core::views::NodeStatus::Ready
            );

            tracing::info_span!(
                "rdm.node_admin.fabric.update.via-atlas-receipt",
                operation = "node.start:mesh1.rpc.1",
                outcome = "effect:started"
            )
            .in_scope(|| tracing::info!("a streamed receipt is the atlas's"));
        });

        drop(rt);

        assert!(
            !capture
                .named("rdm.node_admin.fabric.update.via-atlas-receipt")
                .is_empty(),
            "expected rdm.node_admin.fabric.update.via-atlas-receipt span"
        );
    }
}
