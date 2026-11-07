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
//! - a completed lifecycle operation is written from the step events the fabric-primary's own runs
//!   returned (directly, or streamed up `build.section.run` for a peer mesh), before the caller is
//!   answered `Complete`;
//! - the effects a node row records of any admin's op (a mesh-primary's self-healing included), each
//!   ordered against the fabric-primary's own by its Build's acceptance and its step's place.
//!
//! Nothing moves backward: an effect is taken only over an older one (the Build's acceptance stamp,
//! then its step's place in the Build's events). Gossip is context (`observed`) and decides nothing.
//!
//! It holds no node record (query.md §1: "Node rows and property maps are not copied into an atlas to
//! serve `node.get` or acceptance"): every record it decides on is read through the reader
//! (`crate::current_record::CurrentRecords`).

use std::collections::{BTreeMap, HashMap};
use rdm_core::framing::Birth;
use std::sync::{Arc, Mutex};

use crate::build_state::{BuildProjection, BuildStateAdapter, StepOutcome};
use crate::model::{IncarnationId, NodeId, NodeStatus, PathName};
use crate::topology::Topology;
use crate::storage::NodeRecord;

/// Effect order: Build admission, then the step's place in its Build's events.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Stamp {
    time: u64,
    build_id: String,
    /// The step's place in its Build's events (event.id order); 0 for a node command's.
    index: usize,
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

#[derive(Debug, Default)]
struct Held {
    /// The births a create this admin took the receipt of made (an op's result, Q197): in the view
    /// acceptance decides from before the gossip view hears them, their records read through the reader
    /// ([`Atlas::apply`]). Each goes once the view holds its node.
    created: std::collections::HashSet<Birth>,
    /// The completed lifecycle effect of each exact node birth.
    effects: HashMap<Birth, Effect>,
    /// The meshes this admin is primary for (Q207): its own mesh as a mesh-primary, every mesh as the
    /// fabric-primary; empty for an admin that holds no primary.
    meshes: std::collections::BTreeSet<String>,
    /// Each primary as the answer to a node.primary.update this admin sent, or the answer streamed
    /// back to it, names it (Q355), by mesh and node type: a mesh's mesh-primary under the `node-admin`
    /// type, and every other node type's primary ([`Atlas::primaries`]).
    is_primary: BTreeMap<(String, String), Updated>,
    /// The fabric-primary as the answer to the node.fabric-primary.update this admin made names it
    /// (Q355) ([`Atlas::primaries`]).
    is_fabric_primary: Option<Updated>,
}

/// A primary update's answer (Q355): `node_id`, the new primary `node.primary.updated { node_id }` (or
/// `node.fabric-primary.updated { node_id }`) names, and `record`, the primary record this admin held
/// when the answer came, naming the node the update was sent to.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Updated {
    /// The primary record this admin held for the role when the answer came: the node the update was
    /// sent to, at its revision, or the answer's node when its gossip came first.
    record: rdm_core::Primary,
    /// The new primary's node id, as the answer names it.
    node_id: NodeId,
    /// The node the update was sent to, from which `is_primary` is removed (Q355).
    sent_to: NodeId,
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
    /// a re-create at that path and name, a new birth, through node recovery.
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
    /// The operation accepted on the node that has not ended.
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

/// A node-admin's atlas (Q224): one per node-admin, over the meshes it is primary for.
#[derive(Debug, Default)]
pub struct Atlas {
    held: Mutex<Held>,
    /// What a refused caller's decision acts through (Q207), installed once this admin's recovery
    /// submitter and `HeardAgain` exist.
    callers: std::sync::OnceLock<Arc<crate::refused_caller::RefusedCallers>>,
    /// This admin's primary records, the ones a primary update's answer is held against
    /// ([`Atlas::primaries`]), installed with its first answer.
    records: std::sync::OnceLock<rdm_gossip::primary::PrimaryView>,
}

/// The exact birth step `s` of `p` acted on: the birth the Build's acceptance pinned for its subject,
/// else the birth its subject's current record names newest (`rows`).
fn birth_of_step(rows: &[NodeRecord], p: &BuildProjection, s: &crate::build_state::BuildStep) -> Option<Birth> {
    let node_id = s.subject.as_ref()?;
    if let Some((_, b)) = p.topology.pinned.iter().find(|(_, b)| b.node_id == *node_id) {
        return Some(Birth { node_id: b.node_id.clone(), incarnation: b.incarnation.clone() });
    }
    let row = rows.iter().find(|r| r.node_id == *node_id)?;
    Some(Birth { node_id: node_id.clone(), incarnation: row.incarnation.clone() })
}

/// The current records of `subjects` (query.md §1, read through `records`), by node ID: what names a
/// step's birth when its Build pinned none ([`birth_of_step`]). `Err` names a read that failed.
pub(crate) async fn subject_rows<'a>(records: &crate::current_record::CurrentRecords<'_>, subjects: impl IntoIterator<Item = &'a NodeId>) -> Result<Vec<NodeRecord>, String> {
    let mut out: Vec<NodeRecord> = Vec::new();
    for id in subjects {
        let row = records.row(id).await?;
        if let Some(r) = row.filter(|r| !out.iter().any(|o| o.node_id == r.node_id)) {
            out.push(r);
        }
    }
    Ok(out)
}

/// What a streamed receipt wrote in the atlas ([`Atlas::take_receipt`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Taken {
    /// The Build's events hold no completed step of the operation.
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

/// The completed lifecycle step `s` of `p`, its `index`th event, taken over an older effect of its
/// birth; the lifecycle's name when it is one.
fn take_effect(held: &mut Held, rows: &[NodeRecord], p: &BuildProjection, index: usize, s: &crate::build_state::BuildStep) -> Option<&'static str> {
    let lifecycle = lifecycle_of(&s.operation)?;
    let birth = birth_of_step(rows, p, s)?;
    let stamp = Stamp { time: p.time, build_id: p.build_id.to_string(), index };
    if held.effects.get(&birth).is_none_or(|e| e.stamp < stamp) {
        held.effects.insert(birth, Effect { lifecycle, stamp });
    }
    Some(match lifecycle {
        Lifecycle::Started => "started",
        Lifecycle::Stopped => "stopped",
        Lifecycle::Deleted => "deleted",
    })
}

/// The lifecycle an operation `noun.verb:<node_id>` indicates, by its action.
fn lifecycle_of(operation: &str) -> Option<Lifecycle> {
    match action_of(operation) {
        "node.stop" => Some(Lifecycle::Stopped),
        "node.start" => Some(Lifecycle::Started),
        "node.delete" => Some(Lifecycle::Deleted),
        _ => None,
    }
}

/// The action of an operation `noun.verb:<subject>`.
fn action_of(operation: &str) -> &str {
    operation.split_once(':').map_or(operation, |(action, _)| action)
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
    pub(crate) fn holds_deleted_by_delete(&self, node_id: &NodeId, incarnation: &IncarnationId, row: Option<&NodeRecord>) -> bool {
        let recorded = row.is_some_and(|r| r.node_id == *node_id && r.state == NodeStatus::Deleted && r.incarnation == *incarnation);
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

    /// The answer `node.primary.updated { node_id }` to the node.primary.update of `primary`, `mesh`'s
    /// primary of `type` (Q355; node-primary-update.md "The commander"): the fabric-primary's of a
    /// mesh-primary (the `node-admin` type), a mesh-primary's of another node type's primary of its mesh,
    /// and the answer the mesh-primary streams back to the fabric-primary for that update. `is_primary`
    /// on `node_id`, removed from `primary`, in every view this admin decides from
    /// ([`Self::primaries`]), held against the record of that role `records` holds now: this admin takes
    /// the new primary from the answer streamed back, never waiting on gossip. The record may already
    /// name `node_id`, its gossip come first, while the view this admin decides from still names
    /// `primary`: the answer is held all the same. A record naming neither node carried a later change:
    /// nothing is held. The same answer again changes nothing.
    pub(crate) fn primary_updated(&self, records: &rdm_gossip::primary::PrimaryView, mesh: &str, r#type: &str, primary: &NodeId, node_id: &NodeId) {
        let key = (mesh.to_owned(), r#type.to_owned());
        let record = records.node_type_primary(mesh, r#type).filter(|r| primary != node_id && (r.node_id == *primary || r.node_id == *node_id));
        let _ = self.records.set(records.clone());
        let mut held = self.held.lock().unwrap();
        if held.is_primary.get(&key).is_some_and(|u| u.node_id == *node_id && u.sent_to == *primary) {
            return;
        }
        match record {
            Some(record) => held.is_primary.insert(key, Updated { record, node_id: node_id.clone(), sent_to: primary.clone() }),
            None => held.is_primary.remove(&key),
        };
    }

    /// The answer `node.fabric-primary.updated { node_id }` to the node.fabric-primary.update of
    /// `primary`, this admin (Q355): `is_fabric_primary` on `node_id`, removed from `primary`
    /// ([`Self::primaries`]), held as [`Self::primary_updated`] holds one, against the fabric-primary's
    /// record.
    pub(crate) fn fabric_primary_updated(&self, records: &rdm_gossip::primary::PrimaryView, primary: &NodeId, node_id: &NodeId) {
        let record = records.fabric().filter(|r| primary != node_id && (r.node_id == *primary || r.node_id == *node_id));
        let _ = self.records.set(records.clone());
        self.held.lock().unwrap().is_fabric_primary = record.map(|record| Updated { record, node_id: node_id.clone(), sent_to: primary.clone() });
    }

    /// The mesh-primary of `mesh` as the answer to the node.primary.update this admin commanded names
    /// it (Q355), while that answer stands: the node a command for the mesh goes to (Q373), whether or
    /// not the view holds that node yet.
    pub fn answered_mesh_primary(&self, mesh: &str) -> Option<NodeId> {
        let records = self.records.get()?;
        let held = self.held.lock().unwrap();
        let u = held.is_primary.get(&(mesh.to_string(), rdm_core::NODE_ADMIN.to_string()))?;
        let record = records.node_type_primary(mesh, rdm_core::NODE_ADMIN);
        (record.as_ref() == Some(&u.record) || record.is_some_and(|r| r.node_id == u.node_id)).then(|| u.node_id.clone())
    }

    /// `t` with `is_primary` and `is_fabric_primary` as the primary updates' answers this atlas holds
    /// set them (Q355): this admin reads a node's mesh-primary, every node type's primary and the
    /// fabric-primary from here wherever it decides which node runs a node command (the Build drive and
    /// a section's plan included), and in its `node.get` answer. An answer stands while this
    /// admin's record of the role still names the node the update was sent to. A record that names
    /// another replaces it (node-primary-update.md): while `t` still names the node the update was sent
    /// to, `t` was made before that record, so the role is read from the record; once `t` names another
    /// node, or the record names the old node again at a later revision, `t` is the record's and the
    /// answer goes.
    pub fn primaries(&self, t: &mut Topology) {
        let Some(records) = self.records.get() else { return };
        let mut held = self.held.lock().unwrap();
        let names = |t: &Topology, id: &NodeId, fabric: bool| t.own(id).is_some_and(|n| if fabric { n.is_fabric_primary } else { n.is_primary });
        // The node each held answer names primary in `t`, by mesh and node type: the answer's, or the
        // record that replaced it.
        let mut roles: Vec<(String, String, NodeId, NodeId)> = Vec::new();
        held.is_primary.retain(|(mesh, r#type), u| match records.node_type_primary(mesh, r#type) {
            Some(r) if r == u.record && r.node_id == u.sent_to => {
                roles.push((mesh.clone(), r#type.clone(), u.node_id.clone(), u.sent_to.clone()));
                true
            }
            Some(r) if r.node_id != u.sent_to && names(t, &u.sent_to, false) => {
                roles.push((mesh.clone(), r#type.clone(), r.node_id, u.sent_to.clone()));
                true
            }
            _ => false,
        });
        let fabric = match (held.is_fabric_primary.as_ref(), records.fabric()) {
            (Some(u), Some(r)) if r == u.record && r.node_id == u.sent_to => Some((u.node_id.clone(), u.sent_to.clone())),
            (Some(u), Some(r)) if r.node_id != u.sent_to && names(t, &u.sent_to, true) => Some((r.node_id, u.sent_to.clone())),
            _ => None,
        };
        if fabric.is_none() {
            held.is_fabric_primary = None;
        }
        for n in t.nodes.iter_mut() {
            if let Some((_, _, primary, old)) = roles.iter().find(|(mesh, r#type, _, _)| *mesh == n.mesh && *r#type == *n.r#type) {
                if n.node_id == *primary {
                    n.is_primary = true;
                } else if n.node_id == *old {
                    n.is_primary = false;
                }
            }
            if let Some((primary, old)) = &fabric {
                if n.node_id == *primary {
                    n.is_fabric_primary = true;
                } else if n.node_id == *old {
                    n.is_fabric_primary = false;
                }
            }
        }
    }

    /// Q207: the caller birth of `caller`'s node, `incarnation`, a primary refused as one it does not hold,
    /// decided from `caller` (its record at its node id, query.md §1), `at_path` (the
    /// record the current-pair lookup names at `caller`'s `(path, name)`) and the ops executing (never the
    /// gossip view): another birth at the caller's `(path, name)`, or an op executing that fills it,
    /// deletes the caller; nothing there and the caller's record deleted deletes it, never re-admitted
    /// (Q217); nothing there, its record live and `planned` (the current Build's topology, by mesh) naming
    /// the path re-admits it; nothing there and the topology not naming it deletes it.
    pub(crate) fn refused_caller(&self, caller: Option<&NodeRecord>, at_path: Option<&NodeRecord>, incarnation: &IncarnationId, planned: &BTreeMap<String, Vec<PathName>>) -> CallerDecision {
        let held = self.held.lock().unwrap();
        let Some(caller) = caller else { return CallerDecision::NoRecord };
        let node_id = &caller.node_id;
        let path = caller.location();
        if !held.meshes.contains(&caller.mesh()) {
            return CallerDecision::NotMine { path: path.path.clone(), name: path.name.clone()};
        }
        let gone = |r: &NodeRecord| matches!(r.state, NodeStatus::Deleted | NodeStatus::Withdrawn);
        // Another birth at the path now: another node's live record there, or a newer birth of the
        // caller's own node.
        let other = at_path.filter(|r| r.location() == path && r.node_id != *node_id && !gone(r)).map(|r| format!("{}@{}", r.node_id, r.incarnation.as_str()));
        let newer = (!gone(caller) && caller.incarnation != *incarnation).then(|| format!("{}@{}", node_id, caller.incarnation.as_str()));
        if let Some(by) = other.or(newer) {
            return CallerDecision::DeleteHeld { path: path.path.clone(), name: path.name.clone(), by };
        }
        let needed = planned.get(&caller.mesh()).is_some_and(|paths| paths.contains(&path));
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

    /// The paths this atlas holds a create of Build `build` at (fabric-atlas.md R-FA1, R-FA2): accepted and
    /// executing, or completed, its Complete receipt in the Build's own log. A birth the
    /// Build's own op created is not surplus to its accepted topology, which a recipe's `Create` step does
    /// not change (Luke 2026-10-30: drift is decided from the atlas, not the topology), whichever mesh's
    /// section ran it: the fabric-primary's atlas takes every section's receipts.
    /// The completed creates are read from the Build's own events (`p`), the source R-FA2 writes this
    /// atlas from, so a fabric-primary restarted inside the Build holds them too.
    pub(crate) fn created_by(&self, p: &BuildProjection) -> std::collections::BTreeSet<PathName> {
        // A create's path is the one the Build record names its node.id at (D58).
        p.steps
            .iter()
            .filter(|s| s.step == "Complete" && s.outcome == StepOutcome::Complete && action_of(&s.operation) == "node.create")
            .filter_map(|s| s.subject.as_ref())
            .filter_map(|id| p.topology.created.iter().find(|(_, created)| created == id).map(|(at, _)| at.clone()))
            .collect()
    }

    /// `p` completed: every completed lifecycle step of the Build is taken over an older effect of its
    /// birth. Called before the caller is answered `Complete` (R-FA2). `rows` are the current records of
    /// the steps' subjects ([`subject_rows`]), what names an unpinned step's birth.
    pub(crate) fn receipts(&self, p: &BuildProjection, rows: &[NodeRecord]) {
        let mut held = self.held.lock().unwrap();
        for (i, s) in p.steps.iter().enumerate().filter(|(_, s)| s.step == "Complete" && s.outcome == StepOutcome::Complete) {
            take_effect(&mut held, rows, p, i, s);
        }
    }

    /// The `Complete` step of `operation` in `mesh`'s section of `p`, taken the moment it streams up
    /// `build.section.run` to this fabric-primary, whichever admin executed it (R-FA2: execution progress
    /// writes it as the steps stream up, before the caller is answered `Complete`): the lifecycle it
    /// leaves its birth in (stop, start, delete) and what it changed on the node's record. What it took,
    /// for its span; a create's is [`Taken::Created`], its birth, the one its node's record names, held
    /// for [`Self::apply`]. `rows` are the current records of the step's subject.
    pub(crate) fn take_receipt(&self, p: &BuildProjection, operation: &str, mesh: &str, rows: &[NodeRecord]) -> Taken {
        let Some((index, s)) = p.steps.iter().enumerate().rev().find(|(_, s)| s.mesh == mesh && s.operation == operation && s.step == "Complete" && s.outcome == StepOutcome::Complete) else {
            return Taken::NoReceipt;
        };
        let mut held = self.held.lock().unwrap();
        let op = action_of(operation);
        let Some(node_id) = s.subject.clone() else { return Taken::Nothing };
        let lifecycle = take_effect(&mut held, rows, p, index, s);
        match op {
            "node.stop" | "node.delete" => {
                let to = if op == "node.stop" { NodeStatus::Stopped } else { NodeStatus::Deleted };
                match birth_of_step(rows, p, s) {
                    Some(_) => Taken::Status(to),
                    None => Taken::Effect(lifecycle),
                }
            }
            "node.start" => Taken::Effect(lifecycle),
            "node.update" | "node.config.update" | "node.tag.create" | "node.tag.delete" => Taken::Refresh(node_id),
            "node.create" => match rows.iter().find(|r| r.node_id == node_id) {
                Some(r) => {
                    let birth = Birth { node_id, incarnation: r.incarnation.clone() };
                    held.created.insert(birth.clone());
                    Taken::Created(birth)
                }
                None => Taken::Nothing,
            },
            _ => Taken::Nothing,
        }
    }

    /// `t`, a view an acceptance decides from, with its primaries as the primary updates' answers set
    /// them ([`Self::primaries`]), each of its births as its record (read through
    /// `records`, query.md §1) and the ops left it ([`Self::answer`]; R-FA1: the fabric-primary accepts or
    /// denies from the atlas, the gossip view context only): a birth an op deleted is not in it. A birth a
    /// create this atlas took the receipt of made is in it before the gossip view hears it, from its
    /// record, in place of a birth at its path and name that is no longer live: a node action on a path
    /// node recovery just re-created is decided on the replacement, never refused `node-not-live` on the
    /// deleted birth (rdm#239). With no `records` (none open yet) only the ops' effects are applied.
    /// `Err` names a record that could not be read.
    pub(crate) async fn apply(&self, t: &mut Topology, records: Option<&crate::current_record::CurrentRecords<'_>>) -> Result<(), String> {
        self.primaries(t);
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
            let live = r.state.is_live() || r.state == NodeStatus::Starting;
            if !live || r.incarnation != b.incarnation {
                continue;
            }
            if self.deleted_by_op(&b.node_id, &b.incarnation) || t.nodes.iter().any(|n| n.node_id == r.node_id || (n.location() == r.location() && n.status.is_live())) {
                continue;
            }
            t.nodes.retain(|n| n.location() != r.location());
            t.nodes.push(crate::model::Node {
                renamed_from: None,
                path: r.path.clone(), name: r.name.clone(),
                r#type: r.r#type.clone(),
                mesh: r.mesh(),
                node_id: r.node_id.clone(),
                endpoint_id: Some(r.endpoint_id.clone()),
                incarnation_id: Some(r.incarnation.clone()),
                deployment_id: None,
                provider: None,
                data_dir: None,
                status: r.state,
                is_primary: false,
                is_fabric_primary: false,
                transport_addr: Some(r.transport_addr),
                listeners: r.listeners.clone(),
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
    pub(crate) fn answer(&self, n: &mut crate::model::Node, row: Option<&NodeRecord>) -> bool {
        let held = self.held.lock().unwrap();
        if let Some(r) = row.filter(|r| r.node_id == n.node_id && r.state != NodeStatus::Withdrawn) {
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
    /// view holds silent (`PendingReconnect`, `Dead`, node-identity-and-state.md §7.3) is answered as the
    /// view holds it, cleared the moment it is heard; a row its mesh-primary wrote `PendingReconnect` as
    /// it started the birth's self-heal is answered as the view holds the birth too, that status ending
    /// when a digest of the exact birth arrives after it (node-recovery.md step 1, Q393), which the node,
    /// its own record's writer, never writes over once it is ready again; a birth its row holds leaving,
    /// stopped or deleting is silent as expected (Q58) and answers its row. A birth with no record answers
    /// the view's status. `false` when an op deleted the birth: it is not answered. Build acceptance reads
    /// [`Self::answer`], not this.
    pub(crate) fn read(&self, n: &mut crate::model::Node, row: Option<&NodeRecord>) -> bool {
        let held = self.held.lock().unwrap();
        if n.incarnation_id.as_ref().and_then(|i| held.effects.get(&Birth { node_id: n.node_id.clone(), incarnation: i.clone() })).is_some_and(|e| e.lifecycle == Lifecycle::Deleted) {
            return false;
        }
        if let Some(r) = row.filter(|r| r.node_id == n.node_id && r.state != NodeStatus::Withdrawn) {
            { let at = r.location(); n.path = at.path; n.name = at.name; }
            let of_birth = Some(&r.incarnation) == n.incarnation_id.as_ref();
            let live = matches!(r.state, NodeStatus::Starting | NodeStatus::Pending | NodeStatus::Ready | NodeStatus::ConnectionsDeleting | NodeStatus::ConnectionsDeleted);
            let silent = live && matches!(n.status, NodeStatus::PendingReconnect | NodeStatus::Dead);
            if of_birth && !silent && r.state != NodeStatus::PendingReconnect {
                n.status = r.state;
                n.stopped = r.state == NodeStatus::Stopped;
            }
        }
        true
    }

    /// The effect `row`'s state records of its birth `incarnation` (the node writes its own state,
    /// Q356), with its stamp: `Stopped` for a stopped record; `Started` for a ready record when `held`
    /// names no effect or a stop (a node.start is a node command, never a Build, Q342, so no effect of it
    /// is held); any other state, and a ready record under another held effect, leaves the lifecycle
    /// where it is.
    fn row_effect(row: &NodeRecord, incarnation: &IncarnationId, held: Option<&Effect>) -> Option<Effect> {
        if row.incarnation != *incarnation {
            return None;
        }
        let lifecycle = match row.state {
            NodeStatus::Stopped => Lifecycle::Stopped,
            NodeStatus::Ready if held.is_none_or(|h| h.lifecycle == Lifecycle::Stopped) => Lifecycle::Started,
            _ => return None,
        };
        // A node command belongs to no Build (Q342): its effect is the record's, read from no Build.
        Some(Effect { lifecycle, stamp: Stamp { time: 0, build_id: rdm_core::LifecycleOp::NO_BUILD.into(), index: 0 } })
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

    async fn project_row(&self, row: Option<NodeRecord>, observed: &Topology) -> Result<Option<AtlasRow>, String> {
        let Some(row) = row else { return Ok(None) };
        let birth = Some(row.incarnation.clone());
        // No operation is accepted on a node outside a Build's steps (Q342).
        let accepted = None;
        let observed_status = observed.births().find(|n| n.node_id == row.node_id && n.incarnation_id == birth).map(|n| n.status);
        let held = birth.as_ref().and_then(|i| self.held.lock().unwrap().effects.get(&Birth { node_id: row.node_id.clone(), incarnation: i.clone() }).cloned());
        let from_row = match &birth {
            Some(i) => Self::row_effect(&row, i, held.as_ref()),
            None => None,
        };
        let effect = match (held, from_row) {
            // The row's newest stop or start, a node command's, is the record: the newest effect (Q342).
            (_, Some(r)) => Some(r),
            (h, None) => h,
        };
        let deleted = matches!(row.state, NodeStatus::Deleted | NodeStatus::Withdrawn) || effect.as_ref().is_some_and(|e| e.lifecycle == Lifecycle::Deleted);
        let (status, stopped) = if deleted {
            (NodeStatus::Deleted, false)
        } else {
            match effect.map(|e| e.lifecycle) {
                Some(Lifecycle::Stopped) => (NodeStatus::Leaving, true),
                Some(Lifecycle::Started) => (NodeStatus::Ready, false),
                // No op moved the birth since its create: shown as the gossip view holds the same birth
                // (context), else as far as its node record's state reached.
                _ => match observed_status {
                    Some(s) => (s, false),
                    None => (if row.state == NodeStatus::Ready { NodeStatus::Ready } else { NodeStatus::Pending }, false),
                },
            }
        };
        // Live for acceptance from the ops alone: created, and not stopped or deleted since.
        let live = !deleted && !stopped;
        Ok(Some(AtlasRow { mesh: row.mesh(), r#type: row.r#type.clone(), path: row.path.clone(), name: row.name.clone(), node_id: row.node_id.clone(), incarnation: birth, status, stopped, live, accepted, observed: observed_status, birth_source: "node-row" }))
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
    /// The `Complete` step of `operation` in `mesh`'s section of `build_id`, as it streams up
    /// `build.section.run` to this fabric-primary (read from the Build's events), taken into its atlas
    /// before the frame is passed on (fabric-atlas.md R-FA2), whichever admin executed it. One
    /// `rdm.node_admin.fabric.update.via-atlas-receipt` span.
    pub(crate) async fn take_streamed_receipt(&self, builds: &dyn BuildStateAdapter, build_id: &crate::build::BuildId, mesh: &str, operation: &str) {
        let span = tracing::info_span!("rdm.node_admin.fabric.update.via-atlas-receipt", build_id = %build_id.log(), mesh, operation, outcome = tracing::field::Empty, detail = tracing::field::Empty, "otel.kind" = "internal");
        let p = match builds.read_build(build_id).await {
            Ok(p) => p,
            Err(e) => {
                span.record("outcome", "build-unread").record("detail", format!("Build {build_id} could not be read for the step: {e}").as_str());
                span.in_scope(|| tracing::info!("a streamed step was not taken into the atlas"));
                return;
            }
        };
        // The step's subject as its current record holds it (query.md §1).
        let subjects: Vec<NodeId> = p.steps.iter().filter(|s| s.operation == operation).filter_map(|s| s.subject.clone()).take(1).collect();
        let rows = match self.current_records() {
            Some(records) => subject_rows(&records, subjects.iter()).await.unwrap_or_else(|e| {
                span.record("detail", e.as_str());
                Vec::new()
            }),
            None => {
                span.record("detail", "no node records are open yet");
                Vec::new()
            }
        };
        let taken = self.atlas.take_receipt(&p, operation, mesh, &rows);
        span.record("outcome", taken.name().as_str());
        span.in_scope(|| tracing::info!("a streamed step is the atlas's"));
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
            stamp: Stamp { time: 1, build_id: "fixture".into(), index: 0 },
        });
        assert!(atlas.holds_deleted_by_delete(&first, &incarnation, None));
        assert!(!atlas.holds_deleted_by_delete(&second, &incarnation, None));
    }


    fn path(path: &str, name: &str) -> PathName {
        PathName::new(path, name).unwrap()
    }

    /// A record of a birth at `at`, in `status`.
    fn record(at: PathName, mesh: &str, r#type: &str, status: NodeStatus) -> (NodeRecord, IncarnationId) {
        assert_eq!(rdm_core::path::mesh_of(&at.path), mesh, "a node's mesh is its path's first location");
        let incarnation = IncarnationId::mint();
        (rdm_node::nodes_storage::fixture_record(&at, r#type, NodeId::mint(), incarnation.clone(), status), incarnation)
    }

    /// The stores `rows` are read from as current records (query.md §1): each row's name claimed, its
    /// record written at its node id, and a fabric-primary that `observed` gains, acting for every mesh
    /// of `rows` (each given an id in `observed`), which names no mesh-primary.
    struct Current {
        claims: MemoryNodesStorage,
        mesh_ids: BTreeMap<String, crate::model::MeshId>,
    }

    impl Current {
        async fn of(rows: &[&NodeRecord], observed: &mut Topology) -> Self {
            let mut fp = crate::model::Node::allocated(path("writer.node-admin", "1"), &rows.first().map(|r| r.mesh()).unwrap_or_else(|| "mesh1".into()), "node-admin");
            fp.node_id = NodeId::mint();
            fp.incarnation_id = Some(IncarnationId::mint());
            fp.status = NodeStatus::Ready;
            fp.is_fabric_primary = true;
            observed.nodes.push(fp);
            let mut current = Self { claims: MemoryNodesStorage::default(), mesh_ids: BTreeMap::new() };
            for row in rows {
                if !current.mesh_ids.contains_key(&row.mesh()) {
                    let id = crate::model::MeshId::mint();
                    observed.meshes.push(crate::model::Mesh { id: Some(id.clone()), name: row.mesh(), status: crate::model::ScopeStatus::Ready });
                    current.mesh_ids.insert(row.mesh(), id);
                }
                current.put(row).await;
            }
            current
        }

        /// Write `row` at its node id.
        async fn put(&self, row: &NodeRecord) {
            self.claims.put_node(row).await.unwrap();
        }

        fn reader(&self) -> crate::current_record::CurrentRecords<'_> {
            crate::current_record::CurrentRecords { nodes: &self.claims }
        }
    }

    /// An atlas primary for `meshes`; the records a decision reads are handed to it.
    async fn atlas_of(_rows: &[&NodeRecord], meshes: &[&str]) -> TestAtlas {
        let atlas = TestAtlas::new();
        atlas.scope(meshes.iter().map(|m| m.to_string()).collect());
        atlas
    }

    /// The record the current-pair lookup names at `name` among `rows`: the one whose node holds the
    /// name's claim, the live record there (a released one holds no claim).
    fn claimant<'a>(rows: &[&'a NodeRecord], name: &PathName) -> Option<&'a NodeRecord> {
        rows.iter().copied().find(|r| r.location() == *name && !matches!(r.state, NodeStatus::Deleted | NodeStatus::Withdrawn))
    }

    fn planned(paths: &[(PathName, &str)]) -> BTreeMap<String, Vec<PathName>> {
        let mut out: BTreeMap<String, Vec<PathName>> = BTreeMap::new();
        for (p, mesh) in paths {
            out.entry((*mesh).into()).or_default().push(p.clone());
        }
        out
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
        r.incarnation = newer.clone();
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

    /// CONTRACT (node-primary-update.md, Operation contract, "The commander": "The fabric-primary, when its
    /// `node.primary.update` of a mesh's mesh-primary is answered `node.primary.updated { node_id }`,
    /// changes its atlas: it sets `is_primary` on `node_id` and removes it from the node it sent
    /// `node.primary.update` to; it reads a node's mesh-primary from its atlas (Q355)"): a view whose
    /// gossip still names the old mesh-primary names the answer's `node_id` once the answer is in the
    /// atlas, and the old one no more; `is_fabric_primary` the same for node.fabric-primary.update. The
    /// answer stands while this admin's record of the role is the one it held at the answer: the next
    /// record ends it, read or not, and the role returning to the old primary at a later revision is
    /// the record's; a view made before the later record reads the role from it. Must NOT happen: the view's stale `is_primary` read after the answer, or an answer
    /// laid over a later record (own_delete_matrix: a fabric-primary handed its roles back to).
    #[test]
    fn an_answered_primary_update_sets_is_primary_on_node_id_until_a_later_record() {
        use rdm_core::{Primary, PrimaryRole};
        use crate::model::{Fabric, FabricId, Mesh, ProviderKind, ScopeStatus};
        let admin = |mesh: &str, name: &str, is_primary: bool| {
            let mut n = crate::model::Node::allocated(path(&format!("{mesh}.node-admin"), name), mesh, "node-admin");
            n.node_id = NodeId::mint();
            n.incarnation_id = Some(IncarnationId::mint());
            n.status = NodeStatus::Ready;
            n.is_primary = is_primary;
            n
        };
        let (mut fp, old, new) = (admin("mesh1", "1", true), admin("mesh2", "1", true), admin("mesh2", "2", false));
        fp.is_fabric_primary = true;
        let gossip = Topology { fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process }, meshes: vec![Mesh { id: None, name: "mesh1".into(), status: ScopeStatus::Ready }, Mesh { id: None, name: "mesh2".into(), status: ScopeStatus::Ready }], nodes: vec![fp.clone(), old.clone(), new.clone()] };
        let record = |n: &crate::model::Node, mesh: &str, revision: u64| Primary { mesh: mesh.into(), node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap(), revision };
        let records = rdm_gossip::primary::PrimaryView::default();
        records.take(PrimaryRole::MeshPrimary, &record(&old, "mesh2", 3));
        records.take(PrimaryRole::FabricPrimary, &record(&fp, "mesh1", 7));
        let atlas = TestAtlas::new();
        atlas.primary_updated(&records, "mesh2", "node-admin", &old.node_id, &new.node_id);
        let mut t = gossip.clone();
        atlas.primaries(&mut t);
        assert_eq!(t.node_type_primary("mesh2", "node-admin").map(|n| n.node_id.clone()), Some(new.node_id.clone()), "the answer's node_id is mesh2's mesh-primary before gossip carries it");
        assert!(!t.own(&old.node_id).unwrap().is_primary, "is_primary is removed from the node the update was sent to");
        assert!(t.own(&fp.node_id).unwrap().is_primary, "mesh1's mesh-primary is untouched");
        // Gossip carries it (the record names node_id at the next revision), then the role returns to
        // the old primary before this admin reads again: the record's primary stands, not the answer.
        records.take(PrimaryRole::MeshPrimary, &record(&new, "mesh2", 4));
        records.take(PrimaryRole::MeshPrimary, &record(&old, "mesh2", 5));
        let mut back = gossip.clone();
        atlas.primaries(&mut back);
        assert!(atlas.held.lock().unwrap().is_primary.is_empty(), "the answer goes once a later record is held");
        assert_eq!(back.node_type_primary("mesh2", "node-admin").map(|n| n.node_id.clone()), Some(old.node_id.clone()), "the view's own primary stands once the answer went");
        // Answered again, then the record names node_id at the next revision while `t`, made before it,
        // still names the old primary: the role is read from the record, and the answer stands until `t`
        // moves (node_stop: a node.get between the record and the view).
        atlas.primary_updated(&records, "mesh2", "node-admin", &old.node_id, &new.node_id);
        records.take(PrimaryRole::MeshPrimary, &record(&new, "mesh2", 6));
        let mut lagging = gossip.clone();
        atlas.primaries(&mut lagging);
        assert_eq!(lagging.node_type_primary("mesh2", "node-admin").map(|n| n.node_id.clone()), Some(new.node_id.clone()), "a view made before the later record names the record's primary, never the old one");
        assert!(!lagging.own(&old.node_id).unwrap().is_primary, "the old primary is not named while the view lags the record");
        assert_eq!(atlas.held.lock().unwrap().is_primary.len(), 1, "the answer stands while the view still names the old primary");
        let mut moved = gossip.clone();
        moved.nodes.iter_mut().for_each(|n| if n.mesh == "mesh2" { n.is_primary = n.node_id == new.node_id });
        atlas.primaries(&mut moved);
        assert!(atlas.held.lock().unwrap().is_primary.is_empty(), "the answer goes once the view names the record's primary");
        // The new primary's gossip reached this admin's record before the answer, and `t` still names the
        // old primary: `is_primary` is on node_id and removed from the node the update was sent to (Q355),
        // and the answer goes once the view moves. Must NOT happen: the old primary read after the answer,
        // and a second update of it (release gate 9bdc82054, `rejected-not-primary` after `updated`).
        atlas.primary_updated(&records, "mesh2", "node-admin", &old.node_id, &new.node_id);
        let mut first = gossip.clone();
        atlas.primaries(&mut first);
        assert_eq!(first.node_type_primary("mesh2", "node-admin").map(|n| n.node_id.clone()), Some(new.node_id.clone()), "the record came first: the answer's node_id is named");
        assert!(!first.own(&old.node_id).unwrap().is_primary, "the record came first: the old primary is not named");
        let mut moved = gossip.clone();
        moved.nodes.iter_mut().for_each(|n| if n.mesh == "mesh2" { n.is_primary = n.node_id == new.node_id });
        atlas.primaries(&mut moved);
        assert!(atlas.held.lock().unwrap().is_primary.is_empty(), "the answer goes once the view names the record's primary");
        // A record naming a third node carried a later change: nothing is held.
        let third = admin("mesh2", "3", false);
        records.take(PrimaryRole::MeshPrimary, &record(&third, "mesh2", 7));
        atlas.primary_updated(&records, "mesh2", "node-admin", &old.node_id, &new.node_id);
        assert!(atlas.held.lock().unwrap().is_primary.is_empty(), "a record naming neither node of the update: nothing held");
        // node.fabric-primary.update of the fabric-primary: is_fabric_primary on node_id, removed from it.
        atlas.fabric_primary_updated(&records, &fp.node_id, &new.node_id);
        let mut t = gossip.clone();
        atlas.primaries(&mut t);
        assert_eq!(t.fabric_primary().map(|n| n.node_id.clone()), Some(new.node_id.clone()), "the answer's node_id is the fabric-primary");
        assert!(!t.own(&fp.node_id).unwrap().is_fabric_primary, "is_fabric_primary is removed from the old fabric-primary");
        // The same when the new fabric-primary's gossip came before the answer.
        records.take(PrimaryRole::FabricPrimary, &record(&new, "mesh2", 8));
        atlas.fabric_primary_updated(&records, &fp.node_id, &new.node_id);
        let mut first = gossip.clone();
        atlas.primaries(&mut first);
        assert_eq!(first.fabric_primary().map(|n| n.node_id.clone()), Some(new.node_id.clone()), "the record came first: the answer's node_id is the fabric-primary");
        assert!(!first.own(&fp.node_id).unwrap().is_fabric_primary, "the record came first: the old fabric-primary is not named");
    }

    /// CONTRACT (node-primary-update.md, Operation contract, "The commander": "The fabric-primary records
    /// the same way the answer the mesh-primary streams back to it. Each reads a node's mesh-primary and
    /// every node type's primary from its atlas, from the answer streamed back, never waiting on gossip
    /// (Q355)"; step 6: "for any other type, the mesh-primary, which runs it itself"): mesh2's broker
    /// primary moves from mesh2.broker.1 to mesh2.broker.2, and the fabric-primary records the answer the
    /// mesh-primary streams back, `node.primary.updated { node_id }`, in its atlas. Its view, whose gossip
    /// still names mesh2.broker.1, names mesh2.broker.2 the broker primary, so a stop of mesh2.broker.1
    /// moves no role again, and its delete goes to mesh2's mesh-primary, which the update did not move. A later
    /// record of the broker role ends the answer, and another type's or mesh's role is untouched. Must
    /// NOT happen: the stale gossip `is_primary` read after the answer (a second update of the node the
    /// first moved), or the answer laid on mesh2's mesh-primary.
    #[test]
    fn an_answered_mesh_primary_is_named_while_no_view_holds_it() {
        use rdm_core::{Primary, PrimaryRole};
        // CONTRACT (Q355, Q373): the fabric-primary's node.primary.update of mesh2 answered `updated`,
        // naming a node its view does not hold (it vanished from the view, Pending): the atlas names
        // that node mesh2's mesh-primary, the node mesh2's commands go to. A later record naming another
        // node ends the answer. Must NOT happen: no mesh-primary named for mesh2 (the drive then ran
        // mesh2's delete itself, refused as not mesh2's writer).
        let (old, new, third) = (NodeId::mint(), NodeId::mint(), NodeId::mint());
        let record = |id: &NodeId, revision: u64| Primary { mesh: "mesh2".into(), node_id: id.clone(), incarnation: IncarnationId::mint(), revision };
        let records = rdm_gossip::primary::PrimaryView::default();
        records.take(PrimaryRole::MeshPrimary, &record(&old, 3));
        let atlas = TestAtlas::new();
        atlas.primary_updated(&records, "mesh2", rdm_core::NODE_ADMIN, &old, &new);
        assert_eq!(atlas.answered_mesh_primary("mesh2"), Some(new.clone()), "the answer names mesh2's mesh-primary");
        assert_eq!(atlas.answered_mesh_primary("mesh1"), None, "another mesh is untouched");
        records.take(PrimaryRole::MeshPrimary, &record(&third, 5));
        assert_eq!(atlas.answered_mesh_primary("mesh2"), None, "a later record naming another node ends the answer");
    }

    #[test]
    fn an_answered_node_type_primary_update_is_read_from_the_atlas_before_gossip() {
        use rdm_core::{Primary, PrimaryRole};
        use crate::model::{Fabric, FabricId, Mesh, ProviderKind, ScopeStatus};
        let node = |mesh: &str, r#type: &str, name: &str, is_primary: bool| {
            let mut n = crate::model::Node::allocated(path(&format!("{mesh}.{type}", type = r#type), name), mesh, r#type);
            n.node_id = NodeId::mint();
            n.incarnation_id = Some(IncarnationId::mint());
            n.status = NodeStatus::Ready;
            n.is_primary = is_primary;
            n
        };
        let mut fp = node("mesh1", "node-admin", "1", true);
        fp.is_fabric_primary = true;
        let (mp, b1, b2, g1) = (node("mesh2", "node-admin", "1", true), node("mesh2", "broker", "1", true), node("mesh2", "broker", "2", false), node("mesh2", "gateway", "1", true));
        let gossip = Topology { fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process }, meshes: vec![Mesh { id: None, name: "mesh1".into(), status: ScopeStatus::Ready }, Mesh { id: None, name: "mesh2".into(), status: ScopeStatus::Ready }], nodes: vec![fp.clone(), mp.clone(), b1.clone(), b2.clone(), g1.clone()] };
        let record = |n: &crate::model::Node, revision: u64| Primary { mesh: n.mesh.clone(), node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone().unwrap(), revision };
        let records = rdm_gossip::primary::PrimaryView::default();
        records.take(PrimaryRole::MeshPrimary, &record(&mp, 3));
        records.take(PrimaryRole::FabricPrimary, &record(&fp, 7));
        records.take(PrimaryRole::NodePrimary("broker".into()), &record(&b1, 2));
        records.take(PrimaryRole::NodePrimary("gateway".into()), &record(&g1, 2));
        let b1_birth = crate::build::PinnedBirth::of(&b1).unwrap();
        assert!(crate::executor::node_role_move(&b1_birth, false, &gossip).is_some_and(|m| m.node_type_role), "before the update the stop of mesh2.broker.1 moves its broker role first");
        let atlas = TestAtlas::new();
        atlas.primary_updated(&records, "mesh2", "broker", &b1.node_id, &b2.node_id);
        let mut t = gossip.clone();
        atlas.primaries(&mut t);
        assert_eq!(t.node_type_primary("mesh2", "broker").map(|n| n.node_id.clone()), Some(b2.node_id.clone()), "the answer's node_id is mesh2's broker primary before gossip carries it");
        assert!(!t.own(&b1.node_id).unwrap().is_primary, "is_primary is removed from the node the update was sent to");
        assert_eq!(t.node_type_primary("mesh2", "node-admin").map(|n| n.node_id.clone()), Some(mp.node_id.clone()), "mesh2's mesh-primary is untouched");
        assert_eq!(t.node_type_primary("mesh2", "gateway").map(|n| n.node_id.clone()), Some(g1.node_id.clone()), "another node type's primary is untouched");
        assert!(crate::executor::node_role_move(&b1_birth, false, &t).is_none(), "the stop of mesh2.broker.1 moves no role again");
        // The delete of mesh2.broker.1 goes to mesh2's mesh-primary, which runs it itself.
        let delete = crate::build::BuildOperation::DeleteLiveNode { path: b1.path.clone(), name: b1.name.clone(), mesh: "mesh2".into(), r#type: "broker".into(), exited: None, birth: Some(b1_birth.clone()) };
        assert_eq!(crate::executor::executor_for(&delete, &t), Some(mp.node_id.clone()), "the delete goes to mesh2's mesh-primary");
        assert_eq!(crate::executor::executor_for(&delete, &gossip), None, "the gossip view alone would hold it for a role move");
        // Gossip carries it at the next revision, and the view names it: the answer goes.
        records.take(PrimaryRole::NodePrimary("broker".into()), &record(&b2, 3));
        let mut moved = gossip.clone();
        moved.nodes.iter_mut().for_each(|n| if n.r#type == "broker" { n.is_primary = n.node_id == b2.node_id });
        atlas.primaries(&mut moved);
        assert!(atlas.held.lock().unwrap().is_primary.is_empty(), "the answer goes once the view names the record's primary");
        // The same answer again, held once: it changes nothing.
        records.take(PrimaryRole::NodePrimary("broker".into()), &record(&b1, 4));
        atlas.primary_updated(&records, "mesh2", "broker", &b1.node_id, &b2.node_id);
        atlas.primary_updated(&records, "mesh2", "broker", &b1.node_id, &b2.node_id);
        assert_eq!(atlas.held.lock().unwrap().is_primary.len(), 1, "the same answer again is held once");
    }

}

#[cfg(test)]
mod investigation_status_cells {
    use super::*;

    /// CONTRACT (node-recovery.md step 1, Q393: "its mesh-primary writes it `PendingReconnect` as it
    /// starts the self-heal"; "`PendingReconnect` ... ends when a digest of the exact birth arrives
    /// after it, when the birth answers its tickle, or when the view no longer holds that birth"): a
    /// birth whose row its mesh-primary wrote `PendingReconnect` is answered as the view holds it: its
    /// view status once it is heard again (the node, its record's writer, writes nothing over the row
    /// while it is ready in its own copy), `PendingReconnect` while the view holds it silent; a row
    /// written `dead` is answered dead. Must NOT happen: a birth heard again and ready answered
    /// `PendingReconnect` from its row (lane 5's `node_heard_again` red, 2026-11-07).
    // @feature: node-recovery
    #[test]
    fn a_row_written_pending_reconnect_is_answered_as_the_view_holds_the_birth() {
        let atlas = Atlas::default();
        let mut n = crate::model::Node::allocated(PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin");
        n.node_id = NodeId::mint();
        n.incarnation_id = Some(IncarnationId::mint());
        let row = |state| rdm_node::nodes_storage::fixture_record(&n.location(), "node-admin", n.node_id.clone(), n.incarnation_id.clone().unwrap(), state);
        for (view, recorded, answered) in [
            (NodeStatus::Ready, NodeStatus::PendingReconnect, NodeStatus::Ready),
            (NodeStatus::Pending, NodeStatus::PendingReconnect, NodeStatus::Pending),
            (NodeStatus::PendingReconnect, NodeStatus::PendingReconnect, NodeStatus::PendingReconnect),
            (NodeStatus::Ready, NodeStatus::Dead, NodeStatus::Dead),
            (NodeStatus::Ready, NodeStatus::Stopped, NodeStatus::Stopped),
        ] {
            let mut held = crate::model::Node { status: view, ..n.clone() };
            assert!(atlas.read(&mut held, Some(&row(recorded))));
            assert_eq!(held.status, answered, "the view holds {view:?}, the row {recorded:?}");
        }
    }
}
