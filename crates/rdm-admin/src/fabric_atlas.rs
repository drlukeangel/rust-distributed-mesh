//! The fabric-primary's `FabricAtlas` (docs/plans/proposal/fabric-atlas.md, Q194, Q197): the union of
//! the ops currently executing and the current topology state, read by Build acceptance and answered
//! by `fabrics.atlas.get`.
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
use std::sync::Mutex;

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
    /// node.start completed: the birth is ready for traffic again (node-start.md "Completion").
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
    /// Where the birth was read from: `node-row`, or `view` when no node row is written at the path.
    pub(crate) birth_source: &'static str,
}

/// The fabric-primary's `FabricAtlas`.
#[derive(Debug, Default)]
pub struct FabricAtlas {
    held: Mutex<Held>,
}

/// The births and operation keys an action is accepted on.
fn accepted_on(action: &AttemptAction) -> Vec<(PathName, String)> {
    match action {
        AttemptAction::DeleteConnections { path, .. } => vec![(path.clone(), format!("node.connections.delete:{path}"))],
        AttemptAction::Stop { path, .. } => vec![(path.clone(), format!("node.stop:{path}"))],
        AttemptAction::Start { path, .. } => vec![(path.clone(), format!("node.start:{path}"))],
        AttemptAction::Delete { path, .. } => vec![(path.clone(), format!("node.delete:{path}"))],
        AttemptAction::Recover { path, .. } => vec![(path.clone(), format!("node.create:{path}"))],
        AttemptAction::Terminate { path, from_incarnation, .. } => vec![(path.clone(), format!("node.terminate:{path}@{}", from_incarnation.as_str()))],
        AttemptAction::Recipe { steps, .. } => {
            let mut out: Vec<(PathName, String)> = Vec::new();
            for step in steps {
                if !out.iter().any(|(p, _)| p == step.node()) {
                    out.push((step.node().clone(), crate::build::recipe_step_key(step)));
                }
            }
            out
        }
    }
}

/// The birth a completed step of `p` acted on at `path`: the stop receipt's own birth, else the one the
/// attempt's action is fenced to.
fn birth_of_step(p: &BuildProjection, attempt: u32, path: &PathName, output: Option<&serde_json::Value>) -> Option<IncarnationId> {
    if let Some(op) = output.and_then(|v| serde_json::from_value::<rdm_core::LifecycleOp>(v.clone()).ok()) {
        return Some(op.incarnation);
    }
    match p.attempt_actions.get(&attempt)? {
        AttemptAction::DeleteConnections { path: at, from_incarnation } | AttemptAction::Stop { path: at, from_incarnation } | AttemptAction::Start { path: at, from_incarnation } | AttemptAction::Delete { path: at, from_incarnation } | AttemptAction::Recover { path: at, from_incarnation, .. } => {
            (at == path).then(|| from_incarnation.clone())
        }
        AttemptAction::Recipe { pinned, .. } => pinned.iter().find(|(at, _)| at == path).map(|(_, b)| b.incarnation.clone()),
        AttemptAction::Terminate { .. } => None,
    }
}

/// What a streamed receipt wrote in the atlas ([`FabricAtlas::take_receipt`]).
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

impl FabricAtlas {
    /// Hold `row` as its node's record: a write an op made (a writer's record, an op's result).
    pub fn write_record(&self, row: &NodeRow) {
        let mut held = self.held.lock().unwrap();
        held.births_read.extend(row.births.iter().map(|b| (row.node_id.clone(), b.incarnation_id.as_str().to_string())));
        held.records.insert(row.node_id.clone(), row.clone());
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

    /// `node.config.update` at the fabric-primary (node-config-update.md step 1): the map written to
    /// the node in the atlas before the command goes down. The config it held before, to put back when
    /// the command is refused; `None` when the atlas holds no record of the node.
    pub(crate) fn write_config(&self, node_id: &NodeId, config: &BTreeMap<String, String>) -> Option<BTreeMap<String, String>> {
        let mut held = self.held.lock().unwrap();
        let r = held.records.get_mut(node_id)?;
        Some(std::mem::replace(&mut r.config, config.clone()))
    }

    /// Put `config` back on the node `node_id` where the atlas still holds `put` (a refused command).
    pub(crate) fn restore_config(&self, node_id: &NodeId, put: &BTreeMap<String, String>, config: BTreeMap<String, String>) {
        if let Some(r) = self.held.lock().unwrap().records.get_mut(node_id) {
            if r.config == *put {
                r.config = config;
            }
        }
    }

    /// The record the atlas holds for `node_id`.
    pub fn record(&self, node_id: &NodeId) -> Option<NodeRow> {
        self.held.lock().unwrap().records.get(node_id).cloned()
    }

    /// The record the atlas holds at `path`: the one whose node holds the name now.
    pub fn record_at(&self, path: &PathName) -> Option<NodeRow> {
        self.held.lock().unwrap().records.values().find(|r| r.name == *path).cloned()
    }

    /// Fill the records of each mesh in `meshes` from the node records, read whole (one listing per
    /// mesh): at the fabric-primary's start or hand-over (Q204), and once a Build's attempt completed
    /// (its op results on every mesh it names, Q197). The number of records held after.
    pub async fn fill(&self, rows: &dyn NodesStorage, meshes: impl IntoIterator<Item = String>) -> Result<usize, String> {
        let mut read = Vec::new();
        for mesh in meshes {
            let listed = rows.nodes(&mesh).await.map_err(|e| format!("nodes.storage: the node records of {mesh} could not be read: {e}"))?;
            read.push((mesh, listed));
        }
        let mut held = self.held.lock().unwrap();
        for (mesh, listed) in read {
            held.records.retain(|_, r| r.name.mesh != mesh);
            for r in listed {
                let births: Vec<(NodeId, String)> = r.births.iter().map(|b| (r.node_id.clone(), b.incarnation_id.as_str().to_string())).collect();
                held.births_read.extend(births);
                held.records.insert(r.node_id.clone(), r);
            }
        }
        Ok(held.records.len())
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
                Some(AttemptAction::Recipe { pinned, .. }) => pinned.iter().find(|(at, _)| *at == path).map(|(_, b)| b.node_id.clone()),
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
                // The recipe's node.name.update: the name the step names (the receipt carries none).
                let to = match p.attempt_actions.get(&attempt) {
                    Some(AttemptAction::Recipe { steps, .. }) => steps.iter().find_map(|st| match st {
                        rdm_core::build::RecipeStep::Update { node, name } if *node == path => Some(name.clone()),
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
            "node.config.update" | "node.tags.create" | "node.tags.delete" => {
                // In place, to the create spec the accepted Build names for the path (build.md §1, "The
                // Build applies the diff"): its config; its tags added (create) or the ones it does not
                // name removed (delete).
                let Some(spec) = p.topology.mesh(&path.mesh).and_then(|m| m.spec(&path)).cloned() else { return Taken::Nothing };
                let Some(node_id) = node_of(&held) else { return Taken::Nothing };
                let Some(r) = held.records.get_mut(&node_id) else { return Taken::Nothing };
                match op {
                    "node.config.update" => r.config = spec.config,
                    "node.tags.create" => r.tags.extend(spec.tags),
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
    }

    /// `n`, a birth a node read answers, as this atlas holds it (node-rpc-envelope.md node.get: the
    /// node reads are answered from the `FabricAtlas`): the name its record holds, and the lifecycle the
    /// ops left it in (a completed stop: stopped; a completed start of a birth the gossip view still
    /// holds stopped: ready for traffic). `false` when an op deleted the birth: it is not answered. The
    /// gossip view's status stands where no op moved the birth (R-FA3: context).
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
            Some(Lifecycle::Started) if n.stopped => {
                n.stopped = false;
                n.status = NodeStatus::ReadyForTraffic;
            }
            _ => {}
        }
        true
    }

    /// The effect the node row records last of `row`'s birth `incarnation`, with its stamp: a stop's
    /// `Stopped`, a start's `ReadyForTraffic`; any other effect leaves the lifecycle where it is.
    async fn row_effect(row: &NodeRow, incarnation: &IncarnationId, builds: &dyn BuildStateAdapter, held: Option<&Effect>) -> Option<Effect> {
        let op = row.birth(incarnation)?.operations.iter().rev().find(|o| match o.state {
            rdm_core::status::NodeState::Stopped => o.operation.operation.starts_with("node.stop:"),
            rdm_core::status::NodeState::ReadyForTraffic => o.operation.operation.starts_with("node.start:"),
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
    /// on it. With no node row at the path, the birth `observed` holds there, its own status deciding
    /// liveness, and `birth_source` says so. `None` when neither holds a birth at the path.
    pub(crate) async fn row(&self, path: &PathName, rows: &dyn NodesStorage, builds: &dyn BuildStateAdapter, observed: &Topology) -> Result<Option<AtlasRow>, String> {
        // The path's index entry still names a node a rename moved off it (its claim released, the
        // entry left): that row names another path, so it is not the birth at this one.
        let row = rows.node_at(path).await.map_err(|e| format!("nodes.storage: the node row at {path} could not be read: {e}"))?.filter(|r| r.name == *path);
        let accepted = self.held.lock().unwrap().accepted.get(&path.to_string()).map(|a| a.operation.clone());
        let Some(row) = row else { return Ok(self.observed_row(path, observed)) };
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
                Some(Lifecycle::Started) => (NodeStatus::ReadyForTraffic, false),
                // No op moved the birth since its create: shown as the gossip view holds the same birth
                // (context), else as far as its node row's declarations reached.
                _ => match observed_status {
                    Some(s) => (s, false),
                    None => {
                        let ready = birth.as_ref().and_then(|i| row.birth(i)).is_some_and(|b| b.declared.contains(&rdm_core::status::NodeState::ReadyForTraffic));
                        (if ready { NodeStatus::ReadyForTraffic } else { NodeStatus::Pending }, false)
                    }
                },
            }
        };
        // Live for acceptance from the ops alone: created, and not stopped or deleted since.
        let live = !deleted && !stopped;
        Ok(Some(AtlasRow { name: path.clone(), node_id: row.node_id.clone(), incarnation: birth, status, stopped, live, accepted, observed: observed_status, birth_source: "node-row" }))
    }

    /// The row of `path` where no node row is written at it: the birth the gossip view holds there,
    /// its own status deciding liveness (`birth_source` `view`). `None` when the view holds none.
    fn observed_row(&self, path: &PathName, observed: &Topology) -> Option<AtlasRow> {
        let accepted = self.held.lock().unwrap().accepted.get(&path.to_string()).map(|a| a.operation.clone());
        // The view's birth as the ops left it (R-FA3: the view merges in only what the fabric-primary
        // did not do itself): one an op renamed off the path or deleted is not at it.
        let mut n = observed.node(path)?.clone();
        if !self.answer(&mut n) || n.name != *path {
            return None;
        }
        Some(AtlasRow { name: path.clone(), node_id: n.node_id.clone(), incarnation: n.incarnation_id.clone(), status: n.status, stopped: n.stopped, live: n.status.is_live() && !n.stopped, accepted, observed: Some(n.status), birth_source: "view" })
    }

    /// `fabrics.atlas.get`: one row for every node `paths` names (the current Build's topology), in
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
    /// into its `FabricAtlas` before the frame is passed on (fabric-atlas.md R-FA2), whichever admin
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
        if let crate::fabric_atlas::Taken::Created(path) = &taken {
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
        let row = match self.node_rows.get() {
            Some(rows) => self.atlas.row(path, &**rows, &*self.builds, observed).await.map_err(crate::control_plane::Refusal::Unavailable)?,
            None => self.atlas.observed_row(path, observed),
        };
        if let Some(r) = &row {
            span.record("birth_source", r.birth_source).record("status", format!("{:?}", r.status).as_str()).record("stopped", r.stopped).record("accepted", r.accepted.as_deref().unwrap_or(""));
        }
        span.in_scope(|| tracing::info!("the atlas row Build acceptance reads"));
        Ok(row)
    }
}
