//! The multi-mesh soak driver (i143.e9.s2, rafka-v2 #2787): continuous testkit-store operations,
//! recorded in the operation ledger, while the action model generates legal random topology and
//! fault actions that run as Builds, as node commands or as provider faults on exact runtimes.
//!
//! One seed is the whole schedule: [`crate::model::generate`] draws every action (one per round,
//! from the model state the previous rounds produced), the traffic stream draws its own targets
//! and values from a second generator seeded from the same seed, and every choice is recorded as
//! a [`crate::sim::Scheduler`] step. The driver composes what exists and owns no second copy of
//! it:
//!
//! - [`crate::model`]: the legal actions, their preconditions, the shrinker;
//! - [`crate::ledger`]: every issued operation is classified once from its typed outcome and the
//!   count and state algebras reconcile it at the end;
//! - [`crate::process_faults`] / [`crate::container_faults`]: faults act on the exact runtime a
//!   birth published; [`crate::wedge`] judges every hold;
//! - [`crate::elections`]: the roles a view advertises equal the ones the public candidates
//!   compute.
//!
//! Topology actions are Node RPC calls on the typed objects, each awaited on its reply stream: grow
//! = `node.create` and shrink = `node.delete`, each a Build; restart = `node.stop` then `node.start`,
//! node commands that belong to no Build (Q183, Q342); hand-off = the delete of the mesh's
//! role-holding node-admin then a create into the freed ordinal, two Builds. A Build's re-birth is
//! confirmed from the Build's end and its `rdm.build.step` events (`AllocateIdentity` of the node.id
//! its record names at the path, read through the [`BuildRead`] the test binary hands the driver), a
//! node recovery's from the view and the recovery's `rdm.node.investigated` event, before anything
//! is judged.
//!
//! The fabric-primary node-admin is never signalled, killed, held or silenced: a fault that names
//! it is redrawn and the redraw is recorded. It leaves only through a hand-off Build. The
//! bootstrap node-admin (the harness's own control address) is never a fault target or removed.
//!
//! A violation ends the run at once with the seed, the executed action sequence, and a minimized
//! legal reproduction ([`Repro`]): the shrinker's result over the model, replayed from the initial
//! model, ending in the failing action, reaching the same topology before it, and keeping every
//! row that names a node the failure involves. The minimized sequence is a legal reproduction of
//! the topology and the story of the involved nodes; it is not re-executed against a live estate.

use crate::container_faults;
use crate::elections::primaries_as_expected;
use crate::estate::{wait_for, Estate, ProbeHandle};
use crate::ledger::{Bucket, Ledger, MutationIntent, OperationId};
use crate::model::{generate_among, replay, shrink, Action, Capabilities, ClassBounds, Failure, Model, NodeClass, Rng};
use crate::netfault::{udp_ports, Partition};
use crate::process_faults::{cpu_ticks, proc_state, ExactRuntime, Fault};
use crate::scenario::Operation as StoreOp;
use crate::sim::Scheduler;
use crate::wedge::{judge, Control, Evidence, Family, FaultAck, Primitive, Progress as WedgeProgress, Reconciliation, Recovery, Release, Routing};
use rdm_client::{birth_of_view, Accepted, CallEnd, FabricDesired, FabricView, Frame, MeshDesired, MeshNode, NodeView, PathName, ScopeStatus, WorkflowStream};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The opcode of the testkit store on Node RPC.
const TESTKIT_STORE_OP: u8 = 0x70;
/// The keys the traffic stream writes: disjoint from the model's own proof keys (`0..STORE_KEYS`).
const TRAFFIC_KEYS: std::ops::Range<u64> = 100..108;
/// The bound on one action's birth sequence to complete (a Build and its convergence).
const CONVERGE: Duration = Duration::from_secs(150);

fn s(v: &Value) -> String {
    v.as_str().unwrap_or("").to_string()
}

/// The name of an action's kind.
pub fn kind(a: &Action) -> &'static str {
    match a {
        Action::Grow { class: NodeClass::NodeAdmin, .. } => "grow-admin",
        Action::Grow { .. } => "grow-rpc",
        Action::Shrink { .. } => "shrink",
        Action::Restart { .. } => "restart",
        Action::HandOff { .. } => "hand-off",
        Action::Unheard { .. } => "unheard",
        Action::Heal { .. } => "heal",
        Action::Kill { .. } => "kill",
        Action::Wedge { .. } => "wedge",
        Action::Store(StoreOp::Put { .. }) => "store-put",
        Action::Store(StoreOp::Cas { .. }) => "store-cas",
        Action::Store(StoreOp::Delete { .. }) => "store-delete",
    }
}

/// The node an action names, when it names one.
fn node_of(a: &Action) -> Option<&PathName> {
    match a {
        Action::Shrink { node } | Action::Restart { node } | Action::Kill { node } | Action::Wedge { node } => Some(node),
        Action::Store(StoreOp::Put { target, .. } | StoreOp::Cas { target, .. } | StoreOp::Delete { target, .. }) => Some(target),
        _ => None,
    }
}

/// What a run is configured with.
#[derive(Debug, Clone)]
pub struct Config {
    pub seed: u64,
    pub secs: u64,
    /// `(mesh, [(class, count)])` as born.
    pub shape: Vec<(&'static str, Vec<(NodeClass, u32)>)>,
    pub bounds: BTreeMap<NodeClass, ClassBounds>,
    /// The legal actions this run draws from.
    pub profile: Profile,
    /// The provider can make a mesh unheard by the others: the container provider always, the
    /// process provider when the host drops UDP between loopback ports (`netfault`).
    pub network_faults: bool,
    /// The topology staleness floor and the backbone gossip interval the estate runs at
    /// (`rdm_gossip::topology`): the windows a silence is observed within.
    pub floor: Duration,
    pub backbone: Duration,
    /// The longest one offline-tickle ladder takes: the direct connect, then every via-peer carrier at
    /// once, each one call of the default budget
    /// (`rdm_admin::offline::tickle_ladder_at_default_budget`).
    pub tickle_ladder: Duration,
    /// How long a fault is held past the view's mark before it is lifted (the floors story, S6):
    /// a held runtime stays held, and a cut stays cut with traffic on both sides, for this long
    /// after the staleness floor marked it; `None` lifts a hold at the mark and heals a cut on the
    /// next row. Set past the hung-hold floors (`RDM_HUNG_HOLD_FLOORS` staleness floors) it proves
    /// what node-recovery.md promises of a held runtime the provider reads `OsStopped`
    /// (deployment-provider.md §2.5: it opens no R-FM1 hold) and of a cut mesh: silence is not
    /// death, the same births come back.
    pub hold: Option<Duration>,
}

impl Config {
    /// Two meshes of two node-admins and three nodes; a mesh holds 2..=3 node-admins and
    /// 3..=4 nodes.
    pub fn mm(seed: u64, secs: u64, floor: Duration, backbone: Duration, tickle_ladder: Duration) -> Self {
        Self {
            seed,
            secs,
            shape: vec![("mesh1", vec![(NodeClass::NodeAdmin, 2), (NodeClass::RpcNode, 3)]), ("mesh2", vec![(NodeClass::NodeAdmin, 2), (NodeClass::RpcNode, 3)])],
            bounds: BTreeMap::from([(NodeClass::NodeAdmin, ClassBounds { min: 2, max: 3 }), (NodeClass::RpcNode, ClassBounds { min: 3, max: 4 })]),
            profile: Profile::All,
            network_faults: false,
            floor,
            backbone,
            tickle_ladder,
            hold: None,
        }
    }

    /// Two meshes of `admins` node-admins, `compute` compute, `gateway` gateway and `broker` broker
    /// nodes each (the R-shape roles); a role class may lose one node and regain it, or grow by
    /// one. `profile` names the actions the run draws from.
    #[allow(clippy::too_many_arguments)]
    pub fn roles(seed: u64, secs: u64, per_mesh: [(NodeClass, u32); 4], profile: Profile, network_faults: bool, floor: Duration, backbone: Duration, tickle_ladder: Duration) -> Self {
        let mut bounds = BTreeMap::new();
        for (class, n) in per_mesh {
            bounds.insert(class, if class == NodeClass::NodeAdmin { ClassBounds { min: n, max: n + 1 } } else { ClassBounds { min: n - 1, max: n + 1 } });
        }
        Self { seed, secs, shape: vec![("mesh1", per_mesh.to_vec()), ("mesh2", per_mesh.to_vec())], bounds, profile, network_faults, floor, backbone, tickle_ladder, hold: None }
    }

    /// How long a silenced node takes to be marked unheard in the view of the observer that judges
    /// it. Pending-reconnect: the staleness floor, then the views converge. True offline (`dead`):
    /// the mark at the floor, ladder 1 half a floor later, ladder 2 at least one floor after ladder 1
    /// found no path, each ladder at most `tickle_ladder`, then the mesh primary's write reaches the
    /// view (offline.rs, node-identity-and-state.md section 7.3).
    /// The mesh primary tickles the silent nodes it watches one after another, so each of the
    /// `silent` nodes (the held node and every member of a silenced mesh) costs two ladders.
    pub fn unheard_within(&self, true_offline: bool, silent: usize) -> Duration {
        if true_offline {
            self.floor * 5 / 2 + self.tickle_ladder * 2 * silent.max(1) as u32 + self.backbone * 4 + Duration::from_secs(10)
        } else {
            self.floor * 3 + self.backbone * 2 + Duration::from_secs(30)
        }
    }

    /// The initial model of the shape on a provider.
    pub fn model(&self, provider: &str) -> Model {
        let meshes: Vec<(&str, Vec<(NodeClass, u32)>)> = self.shape.iter().map(|(m, classes)| (*m, classes.clone())).collect();
        Model::of(&meshes, self.bounds.clone(), Capabilities { network_faults: provider == "container" || self.network_faults })
    }
}

/// The legal actions a run draws from: the traffic and fault profile of one soak. The generator
/// is the same seeded draw in every profile; a profile only narrows the candidates it picks among.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    /// Every legal action.
    All,
    /// Restarts, kills and holds of role nodes (every class but the node-admin); the continuous
    /// traffic stream is the testkit-store load in every profile.
    Routing,
    /// Restarts, kills and holds of node-admins, and mesh-primary hand-offs (the fabric-primary's
    /// included), under traffic.
    Primary,
    /// A mesh made unheard by the others, held, healed, converged, repeated.
    Partition,
    /// Restart, delete and capacity re-add of role nodes under traffic.
    Lifecycle,
    /// Holds past the floors (S6): a runtime held and a mesh cut, each kept past the staleness and
    /// the hung-hold floors (`Config::hold`) with traffic on, then released or healed; the same
    /// births come back. Role nodes and non-primary node-admins alike.
    Floors,
}


impl Profile {
    pub fn allows(self, a: &Action, model: &Model) -> bool {
        let class = node_of(a).and_then(|p| model.node(p)).map(|(_, n)| n.class);
        match self {
            Profile::All => true,
            Profile::Routing => matches!(a, Action::Restart { .. } | Action::Kill { .. } | Action::Wedge { .. }) && class.is_some_and(|c| c != NodeClass::NodeAdmin),
            Profile::Primary => match a {
                Action::HandOff { .. } => true,
                Action::Restart { .. } | Action::Kill { .. } | Action::Wedge { .. } => class == Some(NodeClass::NodeAdmin),
                _ => false,
            },
            Profile::Partition => matches!(a, Action::Unheard { .. } | Action::Heal { .. }),
            Profile::Lifecycle => match a {
                Action::Grow { class, .. } => *class != NodeClass::NodeAdmin,
                Action::Shrink { .. } | Action::Restart { .. } => class.is_some_and(|c| c != NodeClass::NodeAdmin),
                _ => false,
            },
            Profile::Floors => matches!(a, Action::Wedge { .. } | Action::Unheard { .. } | Action::Heal { .. }),
        }
    }
}

/// Append `found` to the run's `violations.jsonl`, one line each, as they are found: the run's folder
/// holds every violation whatever ends the run. A write that fails is said on stderr; the run goes on.
fn write_found(path: &std::path::Path, found: &[Violation]) {
    use std::io::Write as _;
    if found.is_empty() {
        return;
    }
    let lines: String = found.iter().map(|v| serde_json::to_string(v).unwrap_or_default() + "\n").collect();
    if let Err(e) = std::fs::OpenOptions::new().create(true).append(true).open(path).and_then(|mut f| f.write_all(lines.as_bytes())) {
        eprintln!("SOAK violations.jsonl at {}: {e}", path.display());
    }
}

/// One violated rule, by name, at a round and action.
#[derive(Debug, Clone, Serialize)]
pub struct Violation {
    pub rule: String,
    pub round: usize,
    /// Index of the executed action it broke at.
    pub step: usize,
    pub action: String,
    pub detail: String,
}

/// The minimized legal reproduction of a failure.
#[derive(Debug, Clone, Serialize)]
pub struct Repro {
    pub seed: u64,
    pub rule: String,
    pub failing_step: usize,
    pub failing_action: Action,
    /// The nodes the failure involves: every row naming one is kept.
    pub involved: Vec<PathName>,
    pub original_len: usize,
    pub minimized: Vec<Action>,
    pub candidates_tried: usize,
    /// The live facts of the failing round: the round's log entry (the node ids, the Build, what
    /// the convergence was waiting on). The model carries none of them.
    pub live: Value,
    /// What "minimized" means here.
    pub definition: &'static str,
}

const REPRO_DEFINITION: &str = "a sequence legal from the initial model that ends in the failing action, reaches the same topology (paths and classes) before it, and keeps every row naming an involved node; the shrinker removes windows while the rule still fails; the sequence is not re-executed against a live estate";

/// The operation ledger's account of a run.
#[derive(Debug, Clone, Serialize, Default)]
pub struct LedgerReport {
    pub issued: usize,
    pub buckets: BTreeMap<String, usize>,
    /// `bucket:detail` of every classification.
    pub details: BTreeMap<String, usize>,
    pub applied_mutations: usize,
    /// Stores that survived to the end and were reconciled against their final state.
    pub stores_reconciled: usize,
    /// Operations on a store no live node holds: counted by the count algebra, their store is gone
    /// with the last birth over it.
    pub ops_on_deleted_births: usize,
    /// `(node id, key)` pairs a delete touched: the ledger holds no delete intent, so the state
    /// algebra does not judge those keys.
    pub keys_excluded_from_state: usize,
    /// Indeterminate mutations: `(operation, final state shows it applied)`.
    pub indeterminate: Vec<(u64, bool)>,
    /// Probe invocations that printed no typed outcome (the operation stays unclassified and the
    /// count algebra names it).
    pub probe_failures: Vec<String>,
    pub violations: Vec<String>,
}

/// The result of a run.
#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub seed: u64,
    pub secs: u64,
    pub provider: String,
    pub rounds: usize,
    pub elapsed_s: u64,
    pub actions: BTreeMap<String, usize>,
    pub skipped: BTreeMap<String, usize>,
    pub ledger: LedgerReport,
    pub violations: Vec<Violation>,
    pub repro: Option<Repro>,
    pub executed: Vec<Action>,
    pub schedule: Vec<Value>,
    pub rounds_log: Vec<Value>,
    /// Every issued operation with its target, key, intent and typed classification, in issue order.
    pub operations: Vec<Value>,
}

impl Report {
    pub fn ok(&self) -> bool {
        self.violations.is_empty() && self.ledger.violations.is_empty()
    }
}

/// What a panicking run leaves: enough to report the seed and shrink.
#[derive(Debug, Default)]
pub struct Progress {
    pub executed: Vec<Action>,
    pub initial: Option<Model>,
}

#[derive(Debug, Clone)]
struct OpMeta {
    path: String,
    /// The name, beside its path.
    name: String,
    target_id: String,
    key: u64,
    /// The trace of the probe invocation that carried the operation, from the `traceparent` it printed.
    trace_id: Option<String>,
    /// The gateway that carried the call, when one did.
    via: Option<String>,
    /// The route the probe printed for the call (`direct`, `via-peer`, ...).
    route: Option<String>,
}

impl OpMeta {
    /// The `(path, name)` lookup index key of path and name.
    fn location(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.path.clone(), name: self.name.clone() } }
}

#[derive(Default)]
struct OpView {
    ledger: Ledger,
    meta: Vec<OpMeta>,
    excluded: BTreeSet<(String, u64)>,
    /// `(node id, key)` -> the value the last successful write left, for compare-and-swap.
    last: BTreeMap<(String, u64), String>,
    probe_failures: Vec<String>,
    /// Node id -> the data dir its view named. A store is its data dir: a recovered node is born
    /// over the lost node's dir with a new node id (node-recovery.md), and holds what it stored.
    data_dirs: BTreeMap<String, Option<String>>,
}

#[derive(Default, Clone)]
struct View {
    /// The fabric-primary's birth: the traffic's probes enter at it and read its view.
    admin: Option<MeshNode>,
    rpcs: Vec<(PathName, String)>,
    /// The gateways that carry a quarter of the traffic to its exact target (`--via`).
    gateways: Vec<String>,
}

struct Shared {
    op_view: Mutex<OpView>,
    view: Mutex<View>,
    stop: AtomicBool,
}

/// One testkit-store operation of a stream.
struct Call {
    path: String,
    /// The name, beside its path.
    name: String,
    target_id: String,
    kind: &'static str,
    key: u64,
    value: Option<String>,
    expected: Option<String>,
    /// The gateway that carries the call to its exact target (`--via path:<gateway>`).
    via: Option<String>,
}

/// Issue `call` once: recorded in the ledger before it is sent, classified from the printed typed
/// outcome after. No retry: an outcome is recorded, never repeated.
async fn issue(shared: &Shared, probe: &ProbeHandle, admin: &MeshNode, call: Call) {
    let id: OperationId = {
        let mut b = shared.op_view.lock().unwrap();
        let mutation = match (call.kind, &call.value) {
            ("put" | "cas", Some(v)) => Some(MutationIntent { key: call.key.to_string(), value: v.clone().into_bytes() }),
            _ => None,
        };
        if call.kind == "delete" {
            b.excluded.insert((call.target_id.clone(), call.key));
        }
        let id = b.ledger.issue(TESTKIT_STORE_OP, call.target_id.clone(), mutation);
        debug_assert_eq!(id.0 as usize, b.meta.len());
        b.meta.push(OpMeta { path: call.path.clone(), name: call.name.clone(), target_id: call.target_id.clone(), key: call.key, trace_id: None, via: call.via.clone(), route: None });
        id
    };
    let target = format!("exact:{}", call.target_id);
    let mut args: Vec<String> = vec![call.kind.to_string(), "--target".into(), target, "--key".into(), call.key.to_string()];
    match call.kind {
        "put" => args.extend(["--value".into(), call.value.clone().unwrap_or_default()]),
        "cas" => {
            if let Some(e) = &call.expected {
                args.extend(["--expected".into(), e.clone()]);
            }
            args.extend(["--value".into(), call.value.clone().unwrap_or_default()]);
        }
        _ => {}
    }
    if let Some(gw) = &call.via {
        args.extend(["--via".into(), format!("path:{gw}")]);
    }
    let (p, a) = (probe.clone(), admin.clone());
    let printed = tokio::task::spawn_blocking(move || {
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        p.run(&a, &refs)
    })
    .await;
    let mut b = shared.op_view.lock().unwrap();
    if let Ok(Ok(out)) = &printed {
        b.meta[id.0 as usize].trace_id = out["traceparent"].as_str().and_then(|tp| tp.split('-').nth(1)).filter(|t| t.len() == 32).map(String::from);
        b.meta[id.0 as usize].route = out["route"].as_str().map(String::from);
    }
    match printed {
        Ok(Ok(out)) => match b.ledger.classify_probe(id, &out) {
            Ok(()) => {
                if out["outcome"] == "Reply" && call.value.is_some() && matches!(call.kind, "put" | "cas") {
                    let applied = call.kind == "put" && out["reply"]["result"]["stored"] == true || call.kind == "cas" && out["reply"]["result"]["swapped"] == true;
                    if applied {
                        b.last.insert((call.target_id.clone(), call.key), call.value.clone().unwrap_or_default());
                    }
                }
            }
            Err(e) => b.probe_failures.push(format!("{id}: {e}")),
        },
        Ok(Err(e)) => b.probe_failures.push(format!("{id}: {e}")),
        Err(e) => b.probe_failures.push(format!("{id}: the probe task failed: {e}")),
    }
}

/// The continuous stream: seeded targets, kinds, keys and values, one operation at a time, until
/// stopped. Targets come from the latest published view; a stale target is a typed outcome.
async fn traffic(shared: Arc<Shared>, probe: ProbeHandle, seed: u64) {
    let mut rng = Rng(seed ^ 0x7AFF_1C00_7AFF_1C00);
    let mut n = 0u64;
    while !shared.stop.load(Ordering::Relaxed) {
        let view = shared.view.lock().unwrap().clone();
        let (Some(admin), false) = (view.admin.clone(), view.rpcs.is_empty()) else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        n += 1;
        let (path, id) = view.rpcs[rng.below(view.rpcs.len() as u64) as usize].clone();
        let key = TRAFFIC_KEYS.start + rng.below(TRAFFIC_KEYS.end - TRAFFIC_KEYS.start);
        let pick = rng.below(10);
        let value = format!("t{n}");
        // A quarter of the calls are carried to their exact target by a gateway (same-mesh and
        // cross-mesh alike); the rest go direct from the controller.
        // A node is never its own carrier (a node does not dial itself).
        let carriers: Vec<&String> = view.gateways.iter().filter(|g| **g != id).collect();
        let via = (!carriers.is_empty() && rng.below(4) == 0).then(|| carriers[rng.below(carriers.len() as u64) as usize].clone());
        let call = match pick {
            0..=4 => Call { path: path.path.clone(), name: path.name.clone(), target_id: id, kind: "put", key, value: Some(value), expected: None, via },
            5..=6 => {
                let expected = shared.op_view.lock().unwrap().last.get(&(id.clone(), key)).cloned();
                Call { path: path.path.clone(), name: path.name.clone(), target_id: id, kind: "cas", key, value: Some(value), expected, via }
            }
            _ => Call { path: path.path.clone(), name: path.name.clone(), target_id: id, kind: "get", key, value: None, expected: None, via },
        };
        issue(&shared, &probe, &admin, call).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A live node-admin as the entry admin's view names it: its `path/name pair`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Admin {
    path: String,
    /// The name, beside its path.
    name: String,
    node_id: rdm_core::NodeId,
}

impl Admin {
    /// The `(path, name)` lookup index key of path and name.
    fn location(&self) -> rdm_core::PathName { rdm_core::PathName { path: self.path.clone(), name: self.name.clone() } }
}

impl Admin {
    fn path(&self) -> PathName { self.location() }
}

fn location(n: &Value) -> PathName {
    crate::estate::location(n)
}

/// The node-admin's explicit identity and descriptive location.
fn admin_of(n: &Value) -> Option<Admin> {
    Some(Admin { path: n["path"].as_str()?.to_string(), name: n["name"].as_str()?.to_string(), node_id: serde_json::from_value(n["node_id"].clone()).ok()? })
}

/// Every live admin, from the entry admin's view.
fn admins_of(nodes: &[Value]) -> Vec<Admin> {
    nodes.iter().filter(|n| n["type"] == "node-admin" && !matches!(n["state"].as_str(), Some("dead" | "pending-reconnect"))).filter_map(admin_of).collect()
}

/// `node.get` of every node of every mesh `admin` holds, as that admin's own view holds them, as JSON: a read that
/// may hit an admin whose process just died answers `None`, never a panic.
async fn views_at(estate: &Estate, admin: &Admin) -> Option<Vec<Value>> {
    estate.node_views_at(&admin.path()).await.map(|v| Estate::values(&v))
}

/// `fabric.get` at `admin`, only when it answers within 2 s for this estate's Fabric.
async fn fabric_at(estate: &Estate, admin: &Admin) -> Option<FabricView> {
    estate.fabric_view_at(&admin.path()).await
}

/// Control follows whoever answers for this Fabric: the fabric's advertised primary if it answers, else the first
/// live admin from `known` that does. The admin control now starts at; `None` when nobody answers.
async fn relocate_control(estate: &mut Estate, known: &[Admin]) -> Option<Admin> {
    let entry = estate.admin.clone();
    let mut candidates: Vec<&Admin> = known.iter().filter(|a| a.path() == entry).collect();
    candidates.extend(known.iter().filter(|a| a.path() != entry));
    for c in candidates {
        if let Some(f) = fabric_at(estate, c).await {
            if let Some(primary) = f.fabric_primary.as_ref() {
                if let Some(target) = known.iter().find(|a| &a.node_id == primary) {
                    if estate.fabric_view_at(&target.location()).await.is_some() {
                        estate.enter(&target.location()).await;
                        return Some(target.clone());
                    }
                }
            }
            estate.enter(&c.path()).await;
            return Some(c.clone());
        }
    }
    None
}

/// A workflow call the fabric-primary did not accept, by name.
fn refused(call: &str, end: CallEnd) -> Broken {
    broken("topology-action-accepted", format!("{call}: {end}"))
}

/// The bound of [`after_work_in_flight`]: as long as a round waits for its Build.
const BUILD_IN_PROGRESS_BOUND: Duration = Duration::from_secs(120);

/// `submit`, submitted again while it is refused `build-in-progress` (one Build at a time, build.md
/// §1) or `busy` (one command or step on a node at a time at its primary, node-recovery.md step 1,
/// Q413, once the call's own Send is spent): a caller that must go after the work in flight follows
/// it to its end and submits again. A Build another caller submitted between rounds is that Build in
/// flight. A domain answer: asked again after the one schedule's next delay (node-rpc.md §43), within
/// [`BUILD_IN_PROGRESS_BOUND`]; any other refusal, or the bound passed, is the call's.
async fn after_work_in_flight<T, F, Fut>(mut submit: F) -> Result<T, CallEnd>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, CallEnd>>,
{
    let until = Instant::now() + BUILD_IN_PROGRESS_BOUND;
    let pacing = rdm_transport::Pacing::from_cadences();
    let mut turn = 0u32;
    loop {
        turn = turn.saturating_add(1);
        let after = pacing.after(turn);
        match submit().await {
            Err(CallEnd::BuildInProgress { .. }) if Instant::now() + after < until => tokio::time::sleep(after).await,
            Err(CallEnd::Rejected { reason, .. }) if reason == "busy" && Instant::now() + after < until => tokio::time::sleep(after).await,
            other => return other,
        }
    }
}

/// The Build a call opened, as a round row records it; `null` for a node command (node.stop,
/// node.start), which belongs to no Build (Q342).
fn accepted_json(a: Option<&Accepted>) -> Value {
    match a {
        Some(a) => json!({"build_id": a.build_id.to_string()}),
        None => Value::Null,
    }
}

/// The node count of every class per mesh the model holds.
fn expected_counts(model: &Model) -> BTreeMap<String, BTreeMap<&'static str, u32>> {
    let classes: BTreeSet<NodeClass> = model.bounds.keys().copied().chain(model.meshes.values().flat_map(|m| m.nodes.values().map(|n| n.class))).collect();
    model.meshes.keys().map(|m| (m.clone(), classes.iter().map(|c| (c.type_name(), model.count(m, *c))).collect())).collect()
}

/// A node that serves the testkit store: every type but the node-admin.
fn serves_testkit_store(n: &Value) -> bool {
    n["type"].as_str().is_some_and(|k| !rdm_core::is_node_admin(&k))
}

/// The fabric has converged when every live admin's view holds the same births: the expected
/// counts, every one ready, the same (path, incarnation) set everywhere; a node-admin
/// that is not the fabric-primary may instead hold a peer mesh unheard whole (every node of it
/// `pending-reconnect`, its pruner's mark) and agree on the rest. Returns the live admins that
/// agree and, per admin, the peer meshes it holds unheard.
/// Every live admin's view against the expected shape and against the entry admin's births, or why
/// not, by name: which check failed, the counts it saw against the shape it expected, the admin whose
/// births differ and which. The reason is the round's `waiting_on`.
async fn converged_everywhere(estate: &Estate, expected: &BTreeMap<String, BTreeMap<&'static str, u32>>) -> Result<(Vec<Admin>, BTreeMap<rdm_core::NodeId, BTreeSet<String>>), String> {
    let nodes = estate.node_values().await;
    let shape_ok = |nodes: &[Value]| {
        expected.iter().all(|(m, counts)| {
            let ready = |k: &str| nodes.iter().filter(|n| crate::estate::mesh_of(&n) == m.as_str() && n["type"] == k && n["state"] == "ready").count() as u32;
            let all = |k: &str| nodes.iter().filter(|n| crate::estate::mesh_of(&n) == m.as_str() && n["type"] == k && !matches!(n["state"].as_str(), Some("dead"))).count() as u32;
            counts.iter().all(|(kind, n)| ready(kind) == *n && all(kind) == *n)
        })
    };
    let births = |nodes: &[Value]| -> BTreeSet<(String, String)> { nodes.iter().filter(|n| n["state"] == "ready").map(|n| (s(&n["node_id"]), s(&n["incarnation"]))).collect() };
    // The counts `nodes` shows of each expected (mesh, type): ready, then every node not dead.
    let counts = |nodes: &[Value]| -> String {
        let mut out = Vec::new();
        for (m, kinds) in expected {
            for (kind, want) in kinds {
                let ready = nodes.iter().filter(|n| crate::estate::mesh_of(&n) == m.as_str() && n["type"] == *kind && n["state"] == "ready").count();
                let all = nodes.iter().filter(|n| crate::estate::mesh_of(&n) == m.as_str() && n["type"] == *kind && !matches!(n["state"].as_str(), Some("dead"))).count();
                if ready as u32 != *want || all as u32 != *want {
                    let states: Vec<String> = nodes.iter().filter(|n| crate::estate::mesh_of(&n) == m.as_str() && n["type"] == *kind).map(|n| format!("{}:{}", s(&n["name"]), s(&n["state"]))).collect();
                    out.push(format!("{m}.{kind} expected {want}, ready {ready}, not dead {all} [{}]", states.join(" ")));
                }
            }
        }
        out.join("; ")
    };
    let birth_diff = |want: &BTreeSet<(String, String)>, theirs: &BTreeSet<(String, String)>| -> String {
        let missing: Vec<String> = want.difference(theirs).map(|(i, c)| format!("{i}/{c}")).collect();
        let extra: Vec<String> = theirs.difference(want).map(|(i, c)| format!("{i}/{c}")).collect();
        format!("births the entry admin holds ready and it does not: {missing:?}; it holds ready and the entry admin does not: {extra:?}")
    };
    if !shape_ok(&nodes) {
        return Err(format!("the entry admin's view is not the expected shape: {}", counts(&nodes)));
    }
    let want = births(&nodes);
    let mut agreeing = Vec::new();
    let mut unheard_at: BTreeMap<rdm_core::NodeId, BTreeSet<String>> = BTreeMap::new();
    for admin in admins_of(&nodes) {
        let Some(theirs) = views_at(estate, &admin).await else {
            return Err(format!("{:?} does not answer node.get", admin.name));
        };
        if shape_ok(&theirs) && births(&theirs) == want {
            agreeing.push(admin);
            continue;
        }
        // What the pages promise of an admin's view: the fabric-primary's holds every mesh it hears
        // (gossip.md §3.2), a node-admin's holds its own mesh (node-recovery.md). A peer mesh
        // a node-admin has not heard on the backbone past the staleness floor is marked
        // `pending-reconnect` by its own pruner, every node of it, and re-fed; nothing else is
        // promised of that copy until the mesh is heard again (gossip.md §6; Q86, Luke 2026-10-20:
        // "the gossip isn't repaired, you get evicted after 30 s"). Such an admin agrees on
        // everything but the meshes it holds unheard whole.
        let is_fp = nodes.iter().any(|n| n["node_id"] == admin.node_id.as_str() && n["is_fabric_primary"] == true);
        let own_mesh = nodes.iter().find(|n| n["node_id"] == admin.node_id.as_str()).map(|n| s(&crate::estate::mesh_of(&n))).unwrap_or_default();
        let unheard: BTreeSet<String> = expected
            .keys()
            .filter(|m| **m != own_mesh && !theirs.iter().any(|n| crate::estate::mesh_of(&n) == m.as_str() && n["state"] != "pending-reconnect"))
            .cloned()
            .collect();
        if is_fp || unheard.is_empty() {
            return Err(format!("{:?} (fabric-primary: {is_fp}) disagrees: {}; {}", admin.name, counts(&theirs), birth_diff(&want, &births(&theirs))));
        }
        let heard = |ns: &[Value]| -> Vec<Value> { ns.iter().filter(|n| !unheard.contains(&s(&crate::estate::mesh_of(&n)))).cloned().collect() };
        let (mine, theirs_heard) = (heard(&nodes), heard(&theirs));
        let heard_expected: BTreeMap<String, BTreeMap<&'static str, u32>> = expected.iter().filter(|(m, _)| !unheard.contains(*m)).map(|(m, c)| (m.clone(), c.clone())).collect();
        let shape_heard = |ns: &[Value]| {
            heard_expected.iter().all(|(m, counts)| {
                let ready = |k: &str| ns.iter().filter(|n| crate::estate::mesh_of(&n) == m.as_str() && n["type"] == k && n["state"] == "ready").count() as u32;
                let all = |k: &str| ns.iter().filter(|n| crate::estate::mesh_of(&n) == m.as_str() && n["type"] == k && !matches!(n["state"].as_str(), Some("dead"))).count() as u32;
                counts.iter().all(|(kind, n)| ready(kind) == *n && all(kind) == *n)
            })
        };
        if !shape_heard(&theirs_heard) || births(&theirs_heard) != births(&mine) {
            return Err(format!("{:?}, holding {unheard:?} unheard, disagrees on what it heard: {}; {}", admin.name, counts(&theirs_heard), birth_diff(&births(&mine), &births(&theirs_heard))));
        }
        unheard_at.insert(admin.node_id.clone(), unheard);
        agreeing.push(admin);
    }
    Ok((agreeing, unheard_at))
}

/// How the driver reads Build `id` as the fabric's Build records and its events hold it, as the JSON the
/// Build view serializes to (build.md §5): `state` (`pending`, `running`, `complete`, `incomplete`,
/// `failed`), `topology` with the node.id it `created` at each path (D58), `steps` (one per
/// `rdm.build.step` event: `mesh`, `operation`, `step`, `outcome`, `subject`, `source`), `failure` and
/// `unfinished`. The read is the test binary's: the records and the events are read from the fabric's
/// store root and folded (`build_state::read_committed`, `BuildProjection::of`, Q292), which the
/// scenario crate's shipped code cannot link (`tools/mesh-audit/src/deps.rs`, rule 4).
pub type BuildRead = for<'a> fn(&'a Estate, &'a str) -> std::pin::Pin<Box<dyn std::future::Future<Output = Value> + Send + 'a>>;

/// The node.id Build `build_id` created at `path`, once the Build ended `complete` with the
/// `AllocateIdentity` step of the create its record names there ended `complete` ([`created_at`]); the
/// birth's incarnation is the view's. `Ok(None)` while the Build runs; `Err` names a Build that ended
/// `failed` or `incomplete` (Q346), or one that completed with no such create.
async fn born_at(estate: &Estate, read: BuildRead, build_id: &str, path: &PathName) -> Result<Option<String>, String> {
    let b = read(estate, build_id).await;
    match b["state"].as_str() {
        Some("failed") => return Err(format!("Build {build_id} ended failed: {}", b["failure"])),
        Some("incomplete") => return Err(format!("Build {build_id} ended incomplete: {}", b["unfinished"])),
        Some("complete") => {}
        _ => return Ok(None),
    }
    created_at(&b, path).map(Some).ok_or_else(|| format!("Build {build_id} ended complete and no AllocateIdentity of a create at {path:?} ended complete: {}", b["steps"]))
}

/// The node.id the Build view `b` created at `path`: the id its record's `created` names there (D58),
/// when that create's `AllocateIdentity` step event (`node.create:<node.id>`) ended `complete`.
fn created_at(b: &Value, path: &PathName) -> Option<String> {
    let id: rdm_core::NodeId = b["topology"]["created"].as_array()?.iter().find(|e| e[0]["path"] == path.path.as_str() && e[0]["name"] == path.name.as_str()).and_then(|e| serde_json::from_value(e[1].clone()).ok())?;
    let op = rdm_core::OperationId::node("node.create", &id).to_string();
    b["steps"].as_array()?.iter().any(|st| st["operation"] == op.as_str() && st["step"] == "AllocateIdentity" && st["outcome"] == "complete").then(|| id.to_string())
}

/// The `rdm.node.investigated` events of the exact birth `(node.id, incarnation)` on the estate's
/// fabric store root (build.md §5; node-investigation.md: the primary that investigated writes its
/// result as one event), each its `data`: `incarnation`, `path`, `name`, `result` (`ready`,
/// `rejoined`, or `re-created`: node recovery, never a Build, Q342) and `inspected`.
fn investigated(estate: &Estate, birth: &(String, String)) -> Vec<Value> {
    let file = estate.fabric_store_root().join(rdm_admin::store_root::EVENTS_FILE);
    rdm_admin::store_log::read_file(&file)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|l| serde_json::from_slice::<Value>(&l.object).ok())
        .filter(|e| e["type"] == "rdm.node.investigated" && e["subject"] == birth.0.as_str() && e["data"]["incarnation"] == birth.1.as_str())
        .map(|e| e["data"].clone())
        .collect()
}

/// Every live admin's view holds exactly `birth` at `path`, ready, and the replaced
/// incarnation `old` nowhere live.
async fn birth_held_everywhere(estate: &Estate, path: &PathName, birth: &(String, String), old: Option<&(String, String)>) -> Result<(), String> {
    let nodes = estate.node_values().await;
    for admin in admins_of(&nodes) {
        let Some(theirs) = views_at(estate, &admin).await else { return Err(format!("{:?}: node.get unanswered", admin.name)) };
        let at_path: Vec<String> = theirs.iter().filter(|n| location(n) == *path).map(|n| format!("{}/{}:{}", s(&n["node_id"]), s(&n["incarnation"]), s(&n["state"]))).collect();
        let holds = theirs.iter().any(|n| location(n) == *path && n["node_id"] == birth.0.as_str() && n["incarnation"] == birth.1.as_str() && n["state"] == "ready");
        let old_live: Vec<String> = theirs.iter().filter(|n| old.is_some_and(|old| n["node_id"] == old.0.as_str() && n["incarnation"] == old.1.as_str()) && !matches!(n["state"].as_str(), Some("dead" | "pending-reconnect"))).map(|n| format!("{:?}:{}", location(n), s(&n["state"]))).collect();
        if !holds {
            return Err(format!("{:?} holds {at_path:?} at {path:?}, not {}/{} ready", admin.name, birth.0, birth.1));
        }
        if !old_live.is_empty() {
            return Err(format!("{:?} still holds the replaced birth {:?} live: {old_live:?}", admin.name, old));
        }
    }
    Ok(())
}

/// No live admin's view holds `path` live any more.
async fn gone_everywhere(estate: &Estate, path: &PathName) -> Result<(), String> {
    let nodes = estate.node_values().await;
    for admin in admins_of(&nodes) {
        let Some(v) = views_at(estate, &admin).await else { return Err(format!("{:?}: node.get unanswered", admin.name)) };
        let live: Vec<String> = v.iter().filter(|n| location(n) == *path && !matches!(n["state"].as_str(), Some("dead"))).map(|n| format!("{}:{}", s(&n["node_id"]), s(&n["state"]))).collect();
        if !live.is_empty() {
            return Err(format!("{:?} still holds {path:?}: {live:?}", admin.name));
        }
    }
    Ok(())
}

/// Until the view of an admin that answers marks every held birth of `births` (`(path, node_id)`)
/// unheard ([`marks_unheard`]), within `within`. `Err` names what the views held at each path.
///
/// `direct`: the paths are judged by the admins of their own mesh (the direct observers of a held
/// node), any one of them marking every path; otherwise by the admin that answers for the Fabric.
async fn until_unheard(estate: &mut Estate, known: &[Admin], births: &[(PathName, String)], true_offline: bool, direct: bool, within: Duration) -> Result<Duration, String> {
    let paths: Vec<PathName> = births.iter().map(|(p, _)| p.clone()).collect();
    let began = Instant::now();
    let until = Instant::now() + within;
    let mut last = String::new();
    while Instant::now() < until {
        if let Some(entry) = relocate_control(estate, known).await {
            // Only the target mesh's primary tickles it, so only its view records `dead`: true
            // offline is read there; pending-reconnect from any admin that answers.
            let mut judge = entry.clone();
            if true_offline {
                if let (Some(v), Some(mesh)) = (views_at(estate, &entry).await, births.first().and_then(|(_, id)| estate.node_record(id)).and_then(|r| crate::estate::mesh_of(&r["data"]).as_str().map(str::to_string))) {
                    if let Some(primary) = v.iter().find(|n| n["type"] == "node-admin" && crate::estate::mesh_of(&n) == mesh && n["is_primary"] == true && n["state"] == "ready").and_then(admin_of) {
                        judge = primary;
                    }
                }
            }
            let mut judges = vec![judge.clone()];
            if direct && !true_offline {
                if let (Some(v), Some(mesh)) = (views_at(estate, &entry).await, births.first().and_then(|(_, id)| estate.node_record(id)).and_then(|r| crate::estate::mesh_of(&r["data"]).as_str().map(str::to_string))) {
                    let own: Vec<Admin> = v.iter().filter(|n| n["type"] == "node-admin" && crate::estate::mesh_of(&n) == mesh && n["state"] == "ready").filter_map(admin_of).filter(|a| known.contains(a)).collect();
                    if !own.is_empty() {
                        judges = own;
                    }
                }
            }
            for judge in judges {
              if let Some(nodes) = views_at(estate, &judge).await {
                if marks_unheard(&nodes, births, true_offline) {
                    return Ok(began.elapsed());
                }
                last = paths.iter().map(|p| format!("{p:?}:{:?}", nodes.iter().filter(|n| location(n) == *p).map(|n| format!("{}@{}", s(&n["state"]), s(&n["node_id"]))).collect::<Vec<_>>())).collect::<Vec<_>>().join(" ");
              }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // What every admin that answers holds, so the refusal says who did not mark it.
    let mut views = Vec::new();
    for admin in known {
        let held = match views_at(estate, admin).await {
            Some(v) => paths.iter().map(|p| format!("{p:?}:{:?}", v.iter().filter(|n| location(n) == *p).map(|n| format!("{}@{}", s(&n["state"]), s(&n["node_id"]))).collect::<Vec<_>>())).collect::<Vec<_>>().join(" "),
            None => "unanswered".into(),
        };
        views.push(format!("{:?} -> {held}", admin.name));
    }
    Err(format!("not marked unheard within {}s: {last}; every admin's view: {views:?}", within.as_secs()))
}

/// Whether `view` marks every held birth of `births` (`(path, node_id)`) unheard: the birth's own row
/// `dead`, or also `pending-reconnect` unless `true_offline`. A held birth the view no longer holds
/// was deleted by node recovery on a verdict other than `reached`, at once (node-recovery.md step 3,
/// Q110, Q111), so its `dead` stands only until that delete; the replacement at its path is another
/// birth and is never read as the held one.
fn marks_unheard(view: &[Value], births: &[(PathName, String)], true_offline: bool) -> bool {
    births.iter().all(|(_, id)| {
        view.iter().filter(|n| n["node_id"] == id.as_str()).all(|n| match n["state"].as_str() {
            Some("dead") => true,
            Some("pending-reconnect") => !true_offline,
            _ => false,
        })
    })
}

fn primaries(nodes: &[Value]) -> (bool, String) {
    match primaries_as_expected(nodes) {
        Ok(()) => (true, String::new()),
        Err(e) => (false, e),
    }
}

fn fabric_primaries(nodes: &[Value]) -> String {
    crate::elections::advertised_fabric_primaries(nodes).join(",")
}

/// What acted on the exact birth `(node.id, incarnation)` since the Build view `before`, each as one row:
/// every `rdm.build.step` event Build `after` gained since `before` (the same Build, its steps in
/// event.id order) whose subject is the birth's node.id and whose operation creates, deletes, stops or
/// starts it (build.md §5), and every `rdm.node.investigated` event of the birth, of `investigated`, whose
/// `result` is `re-created`: node recovery re-creates it, its mesh-primary's own workflow and never a
/// Build (node-recovery.md, Q111, Q342, Q423). A step on another node, a role move among them, did nothing
/// to it; an investigation that ends `ready` or `rejoined` keeps the same birth.
fn acted_on(before: &Value, after: &Value, investigated: &[Value], birth: &(String, String)) -> Vec<String> {
    const ACTS: [&str; 4] = ["node.create", "node.delete", "node.stop", "node.start"];
    let mut out = Vec::new();
    if before["build_id"] == after["build_id"] {
        let from = before["steps"].as_array().map_or(0, Vec::len);
        for st in after["steps"].as_array().into_iter().flatten().skip(from) {
            let op = s(&st["operation"]);
            let action = op.split(':').next().unwrap_or_default();
            if st["subject"] == birth.0.as_str() && ACTS.contains(&action) {
                out.push(format!("Build {} mesh {}: {op} {} {}", s(&after["build_id"]), s(&st["mesh"]), s(&st["step"]), st["outcome"]));
            }
        }
    }
    for e in investigated.iter().filter(|e| e["result"] == "re-created" && e["incarnation"] == birth.1.as_str()) {
        out.push(format!("node recovery: investigated {} at {}.{}, result {}, inspected {}", birth.0, s(&e["path"]), s(&e["name"]), s(&e["result"]), s(&e["inspected"])));
    }
    out
}

/// A mesh made unheard by every other mesh, until its Heal row.
enum Cut {
    /// Container provider: a packet filter in the namespaces of the mesh's containers.
    Container(container_faults::Silenced),
    /// Process provider: UDP between the mesh's transport sockets and every other mesh's dropped
    /// by an nftables table; dropping the value lifts it.
    Udp { partition: Partition, members: Vec<PathName> },
}

impl Cut {
    fn members_len(&self) -> usize {
        match self {
            Cut::Container(s) => s.members.len(),
            Cut::Udp { members, .. } => members.len(),
        }
    }

    fn lift(self) -> Result<(), String> {
        match self {
            Cut::Container(s) => s.lift(),
            Cut::Udp { partition, .. } => {
                drop(partition);
                Ok(())
            }
        }
    }
}

fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

/// The node-admins' spans as they are written: each poll reads the new bytes of every node-admin
/// evidence file since the last poll and keeps the spans `keep` selects, in start order. A wait that
/// polls every few hundred milliseconds reads this, never `Estate::spans` (the whole span set: at
/// `4aa58cf0` the partition soak's span set reached 1.5 GB and every poll of the cut's re-feed wait
/// parsed it, 4-5 s each, through which the driver's own reads of the fabric-primary starved: 84 gaps
/// of 3.5-5 s between its requests, the cut waits growing from 10 s to 72 s, and round 41's
/// `node.get` unanswered within its 2 s).
struct SpanTail {
    dir: std::path::PathBuf,
    offsets: BTreeMap<std::path::PathBuf, u64>,
    kept: Vec<Value>,
    keep: fn(&Value) -> bool,
}

impl SpanTail {
    /// A tail that begins at the files' current ends: what is written from now on is read.
    fn from_now(dir: std::path::PathBuf, keep: fn(&Value) -> bool) -> Self {
        let mut tail = Self { dir, offsets: BTreeMap::new(), kept: Vec::new(), keep };
        for path in tail.files() {
            let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            tail.offsets.insert(path, len);
        }
        tail
    }

    fn files(&self) -> Vec<std::path::PathBuf> {
        std::fs::read_dir(&self.dir)
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.contains("node-admin") && n.ends_with(".spans.jsonl")))
            .collect()
    }

    /// Every kept span so far, the new bytes of each file read first.
    fn poll(&mut self) -> &[Value] {
        use std::io::{Read, Seek, SeekFrom};
        let mut grew = false;
        for path in self.files() {
            let off = self.offsets.get(&path).copied().unwrap_or(0);
            let Ok(mut f) = std::fs::File::open(&path) else { continue };
            let len = f.metadata().map(|m| m.len()).unwrap_or(0);
            if len <= off {
                continue;
            }
            let mut buf = Vec::with_capacity((len - off) as usize);
            if f.seek(SeekFrom::Start(off)).is_err() || f.read_to_end(&mut buf).is_err() {
                continue;
            }
            // Only whole lines: a line still being written is read by the next poll.
            let Some(end) = buf.iter().rposition(|b| *b == b'\n') else { continue };
            for line in buf[..=end].split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
                if let Ok(v) = serde_json::from_slice::<Value>(line) {
                    if (self.keep)(&v) {
                        self.kept.push(v);
                        grew = true;
                    }
                }
            }
            self.offsets.insert(path, off + end as u64 + 1);
        }
        if grew {
            self.kept.sort_by_key(|sp| sp["start_unix_nano"].as_u64().unwrap_or(0));
        }
        &self.kept
    }
}

/// The peer mesh the fabric-primary of `nodes` (its own view) holds unheard, if any, with why: a peer
/// mesh whose node-admin primary that view holds off ready. The `BuildService` refuses every action on a node
/// of a mesh unheard on the backbone past the staleness floor (R-D1, `unheard-mesh`, control_plane.rs),
/// the floor the mesh-primary's silence mark is also taken at (node-recovery.md step 1), so a peer mesh
/// whose primary is marked is one the driver does not act on until it is heard again.
/// The driver acts on a peer mesh only when this is `None`: at `4aa58cf0` the authority soak
/// restarted `mesh2.node-admin.2` 6 s after its mesh-primary was released from a hold, 2.6 s into the
/// fabric-primary's silence of mesh2, and the restart was refused `unheard-mesh` although every view had
/// converged.
fn held_unheard_in(nodes: &[Value]) -> Option<String> {
    let fp = nodes.iter().find(|n| n["is_fabric_primary"] == true && n["state"] == "ready")?;
    nodes
        .iter()
        .find(|n| n["type"] == "node-admin" && n["is_primary"] == true && crate::estate::mesh_of(&n) != crate::estate::mesh_of(&fp) && n["state"] != "ready")
        .map(|n| format!("{} holds {} unheard: its primary {} is {}", s(&fp["node_id"]), s(&crate::estate::mesh_of(&n)), s(&n["node_id"]), s(&n["state"])))
}

/// What the view says of a silenced birth after its release.
#[derive(Debug, PartialEq, Eq)]
enum HeldBirth {
    /// The same birth reads ready.
    Ready,
    /// The same birth is held, off ready: marked unheard or running its start sequence again.
    Returning,
    /// No row of that birth: another birth stands at the path, or none.
    Replaced,
}

fn judge_held_birth(nodes: &[Value], node_id: &str, inc: &str) -> HeldBirth {
    let of_birth: Vec<&Value> = nodes.iter().filter(|n| n["node_id"] == node_id && n["incarnation"] == inc).collect();
    if of_birth.is_empty() {
        HeldBirth::Replaced
    } else if of_birth.iter().any(|n| n["state"] == "ready") {
        HeldBirth::Ready
    } else {
        HeldBirth::Returning
    }
}

/// A fault or an execution step that broke a rule.
struct Broken {
    rule: &'static str,
    detail: String,
}

fn broken(rule: &'static str, detail: impl Into<String>) -> Broken {
    Broken { rule, detail: detail.into() }
}

/// The driver. Owns the estate for the run; [`Self::run`] returns it for the caller to stop.
pub struct Driver {
    pub estate: Estate,
    cfg: Config,
    initial: Model,
    model: Model,
    rng: Rng,
    sched: Scheduler,
    shared: Arc<Shared>,
    progress: Arc<Mutex<Progress>>,
    known: Vec<Admin>,
    /// How a Build is read as the fabric-primary's log holds it (the test binary's).
    read: BuildRead,
    build_id: String,
    executed: Vec<Action>,
    actions: BTreeMap<String, usize>,
    skipped: BTreeMap<String, usize>,
    rounds_log: Vec<Value>,
    silenced: BTreeMap<String, Cut>,
    /// When each silenced mesh was cut, and the births every ready node held then: a cut held past
    /// the floors (`Config::hold`) is healed only after, and the same births must be back after it.
    cut_at: BTreeMap<String, Instant>,
    cut_births: BTreeMap<String, BTreeMap<PathName, (String, String)>>,
    started: Instant,
}

impl Driver {
    /// Bring the estate to the born shape through the rectifier and hold the born model.
    pub async fn new(mut estate: Estate, cfg: Config, read: BuildRead) -> Self {
        estate.set_seed(cfg.seed);
        let initial = cfg.model(&estate.owner.provider);
        let desired = FabricDesired { fabric: "fabric1".into(), meshes: cfg.shape.iter().map(|(m, classes)| MeshDesired::of(*m, classes.iter().map(|(class, n)| (class.type_name(), *n)))).collect() };
        let reconciled = tokio::time::timeout(Duration::from_secs(120), estate.resize(&desired)).await.unwrap_or_else(|_| panic!("the formation Build of {desired:?} did not complete within 120s"));
        let build_id = reconciled.build_id.to_string();
        let want: BTreeMap<(String, String), usize> = cfg.shape.iter().flat_map(|(m, classes)| classes.iter().map(move |(class, n)| ((m.to_string(), class.type_name().to_string()), *n as usize))).collect();
        estate.settles_to_counts(want, Duration::from_secs(60)).await;
        let expected = expected_counts(&initial);
        wait_for("every live admin holds the same births", cfg.floor * 2 + cfg.backbone * 2 + Duration::from_secs(60), || async { converged_everywhere(&estate, &expected).await.ok() }).await;
        let known = admins_of(&estate.node_values().await);
        let progress = Arc::new(Mutex::new(Progress { executed: Vec::new(), initial: Some(initial.clone()) }));
        let shared = Arc::new(Shared { op_view: Mutex::new(OpView::default()), view: Mutex::new(View::default()), stop: AtomicBool::new(false) });
        Self {
            estate,
            rng: Rng(cfg.seed),
            sched: Scheduler::new(cfg.seed),
            initial: initial.clone(),
            model: initial,
            cfg,
            shared,
            progress,
            known,
            read,
            build_id,
            executed: Vec::new(),
            actions: BTreeMap::new(),
            skipped: BTreeMap::new(),
            rounds_log: Vec::new(),
            silenced: BTreeMap::new(),
            cut_at: BTreeMap::new(),
            cut_births: BTreeMap::new(),
            started: Instant::now(),
        }
    }

    /// The peer mesh the fabric-primary of `nodes` holds unheard now, if any, with why
    /// ([`held_unheard_in`]). The driver acts on a peer mesh only when this is `None`.
    fn held_unheard(&mut self, nodes: &[Value]) -> Option<String> {
        held_unheard_in(nodes)
    }

    /// What a panicking run has done so far; read it after a join error.
    pub fn progress(&self) -> Arc<Mutex<Progress>> {
        self.progress.clone()
    }

    fn publish_view(&self, nodes: &[Value]) {
        let admin = nodes
            .iter()
            .find(|n| n["type"] == "node-admin" && n["is_fabric_primary"] == true && n["state"] == "ready")
            .or_else(|| nodes.iter().find(|n| n["type"] == "node-admin" && n["state"] == "ready"))
            .and_then(|n| serde_json::from_value::<NodeView>(n.clone()).ok())
            .as_ref()
            .and_then(birth_of_view);
        let rpcs = nodes.iter().filter(|n| serves_testkit_store(n) && n["state"] == "ready").map(|n| (location(n), s(&n["node_id"]))).collect();
        let gateways = nodes.iter().filter(|n| n["type"] == "gateway" && n["state"] == "ready").map(|n| s(&n["node_id"])).collect();
        *self.shared.view.lock().unwrap() = View { admin, rpcs, gateways };
        let mut b = self.shared.op_view.lock().unwrap();
        for n in nodes.iter().filter(|n| serves_testkit_store(n)) {
            b.data_dirs.insert(s(&n["node_id"]), n["storage"]["data_dir"].as_str().map(str::to_string));
        }
    }

    /// The live reason an action cannot run now, if any. The model knows no authority.
    fn live_refusal(&self, a: &Action, nodes: &[Value]) -> Option<String> {
        let node = |p: &PathName| nodes.iter().find(|n| location(n) == *p && n["state"] == "ready");
        let guarded = |p: &PathName, what: &str| -> Option<String> {
            let n = node(p)?;
            if n["is_fabric_primary"] == true {
                return Some(format!("{what}: {p:?} is the fabric-primary; it leaves only by a hand-off Build"));
            }
            if *p == self.estate.entry_birth().location() && n["type"] == "node-admin" && self.estate.bootstrap_pid().is_some() {
                return Some(format!("{what}: {p:?} is the harness's control address (the bootstrap node-admin)"));
            }
            None
        };
        // The restart of a node that holds its node type's primary role is
        // refused by name, `no-secondary`, while no other node of its type in its mesh is
        // ready to take the role (node-primary-update.md "No-successor cases"; a
        // desired-state shrink of a node type's only node skips the update, so a shrink is not guarded).
        let no_secondary = |n: &Value, what: &str| -> Option<String> {
            if n["type"] == "node-admin" || n["is_primary"] != true {
                return None;
            }
            let secondary = nodes.iter().any(|o| crate::estate::mesh_of(&o) == crate::estate::mesh_of(&n) && o["type"] == n["type"] && (crate::estate::location(&o) != crate::estate::location(&n)) && o["state"] == "ready");
            (!secondary).then(|| format!("{what}: {:?} holds its node-type primary role and no other {} of {} is ready: node.primary.update has no secondary", location(n), s(&n["type"]), s(&crate::estate::mesh_of(&n))))
        };
        // A hand-off is a delete then a spawn, and a spawn lands at the lowest free ordinal of its
        // class: it lands on the deleted node's path/name pair only when no lower ordinal is free.
        let lands_on_same_path = |p: &PathName| -> Option<String> {
            let (mesh, node) = self.model.node(p)?;
            let free = (1..node.ordinal).map(|i| PathName::new(format!("{mesh}.{}", node.class.segment()), i.to_string()).expect("generated model location"))
                .find(|location| !self.model.meshes[mesh].nodes.contains_key(location))?;
            Some(format!("hand-off: {free:?} is free, so a spawn after deleting {p:?} lands there"))
        };
        match a {
            Action::Kill { node: p } | Action::Wedge { node: p } => guarded(p, kind(a)),
            Action::Shrink { node: p } => guarded(p, kind(a)),
            Action::Restart { node: p } => node(p).and_then(|n| {
                if n["is_fabric_primary"] == true {
                    return Some(format!("restart: {p:?} is the fabric-primary; it leaves only by a hand-off Build"));
                }
                no_secondary(n, "restart")
            }),
            Action::HandOff { mesh } => {
                let primary = nodes.iter().find(|n| crate::estate::mesh_of(&n) == mesh.as_str() && n["type"] == "node-admin" && n["is_primary"] == true && n["state"] == "ready");
                match primary {
                    None => Some(format!("hand-off: {mesh} has no ready mesh primary")),
                    Some(p) => guarded(&location(p), "hand-off").filter(|r| r.contains("bootstrap")).or_else(|| lands_on_same_path(&location(p))),
                }
            }
            Action::Unheard { mesh } => {
                let fp = nodes.iter().any(|n| crate::estate::mesh_of(&n) == mesh.as_str() && n["is_fabric_primary"] == true);
                fp.then(|| format!("unheard: {mesh} holds the fabric-primary; the silenced set never holds it"))
            }
            Action::Grow { .. } | Action::Heal { .. } | Action::Store(_) => None,
        }
    }

    /// The next executable action: drawn from the model, redrawn (and recorded) when the live
    /// estate refuses it.
    fn next_action(&mut self, nodes: &[Value]) -> Option<Action> {
        // A silence is held for its observation and healed by the next row: a fabric that lives
        // silenced is not the fabric the invariants describe.
        if let Some((mesh, _)) = self.model.meshes.iter().find(|(_, m)| m.unheard) {
            // The floors story holds the cut: until the hold has elapsed since the cut, the rows are
            // testkit-store traffic on both sides, and the Heal row comes after.
            if let (Some(hold), Some(at)) = (self.cfg.hold, self.cut_at.get(mesh).copied()) {
                if at.elapsed() < hold {
                    let seed = self.rng.next();
                    if let Some(a) = generate_among(seed, &self.model, 1, |a| matches!(a, Action::Store(_))).into_iter().next() {
                        self.sched.record("hold-cut", &format!("{mesh}: {} ms of {} ms held; traffic row", at.elapsed().as_millis(), hold.as_millis()));
                        return Some(a);
                    }
                }
            }
            let a = Action::Heal { mesh: mesh.clone() };
            self.sched.record("heal-after-silence", &a.to_string());
            return Some(a);
        }
        // The floors story holds and cuts in turn: a hold, then a cut, so a run of any length proves
        // both cycles; the draw still picks which node or mesh.
        let cut_next = self.cfg.profile == Profile::Floors && self.actions.get("wedge").copied().unwrap_or(0) > self.actions.get("unheard").copied().unwrap_or(0);
        for _ in 0..64 {
            let seed = self.rng.next();
            let profile = self.cfg.profile;
            let drawn = generate_among(seed, &self.model, 1, |a| profile.allows(a, &self.model) && (profile != Profile::Floors || matches!(a, Action::Unheard { .. }) == cut_next));
            let a = drawn.into_iter().next()?;
            self.sched.record("draw", &format!("{seed:#x}:{}", kind(&a)));
            match self.live_refusal(&a, nodes) {
                None => return Some(a),
                Some(why) => {
                    self.sched.record("redraw", &why);
                    let code = ["fabric-primary", "control address", "is free, so a spawn", "no ready mesh primary"].iter().find(|c| why.contains(**c)).copied().unwrap_or("other");
                    *self.skipped.entry(format!("{}: {code}", kind(&a))).or_default() += 1;
                }
            }
        }
        None
    }

    /// Run until the deadline or the first violation.
    pub async fn run(mut self) -> (Report, Estate) {
        // The initial fabric refuses a node delete and restart until it has first reached ready (states.md "The initial fabric before ready").
        self.estate.until_fabric_ready(Duration::from_secs(120)).await;
        // The host's counters of a container fabric's bridge, the reference a failed control call's are read against.
        crate::host_net::keep(&self.estate, "start", None).await;
        let tr = tokio::spawn(traffic(self.shared.clone(), self.estate.probe_handle(), self.cfg.seed));
        self.sched.record("start", &format!("seed={} secs={}", self.cfg.seed, self.cfg.secs));
        let deadline = Instant::now() + Duration::from_secs(self.cfg.secs);
        let mut violations: Vec<Violation> = Vec::new();
        // Each violation is written to the run's folder the moment it is found, and the run keeps going:
        // it ends at its duration, or when nothing more can run (no admin answers, no legal action).
        let found = self.estate.artifacts.join("violations.jsonl");
        let mut round = 0usize;
        // A cut owes its Heal row: the run ends on a healed fabric, never inside a hold.
        while Instant::now() < deadline || !self.silenced.is_empty() {
            let seen = violations.len();
            round += 1;
            if relocate_control(&mut self.estate, &self.known).await.is_none() {
                violations.push(Violation { rule: "control-api-answers".into(), round, step: self.executed.len(), action: "-".into(), detail: format!("no admin answers fabric.get: {:?}", self.known.iter().map(|a| &a.name).collect::<Vec<_>>()) });
                write_found(&found, &violations[seen..]);
                break;
            }
            let nodes = self.estate.node_values().await;
            self.known = admins_of(&nodes);
            self.publish_view(&nodes);
            let Some(action) = self.next_action(&nodes) else {
                violations.push(Violation { rule: "a-legal-action-exists".into(), round, step: self.executed.len(), action: "-".into(), detail: format!("no drawn action is executable on the live estate; skipped: {:?}", self.skipped) });
                write_found(&found, &violations[seen..]);
                break;
            };
            let step = self.executed.len();
            let started = Instant::now();
            let mut entry = json!({"round": round, "step": step, "kind": kind(&action), "action": action, "at_s": self.started.elapsed().as_secs()});
            self.sched.record("action", &action.to_string());
            // The model state is advanced with the action: convergence is judged against it, and
            // a failing action is the last row of the executed sequence.
            self.model.apply(step, &action).expect("a drawn action is legal in the state it was drawn in");
            self.executed.push(action.clone());
            *self.actions.entry(kind(&action).to_string()).or_default() += 1;
            {
                let mut p = self.progress.lock().unwrap();
                p.executed.push(action.clone());
            }
            let executed = self.execute(&action, &nodes, round, &mut entry).await;
            if let Err(b) = &executed {
                violations.push(Violation { rule: b.rule.into(), round, step, action: action.to_string(), detail: b.detail.clone() });
            }
            entry["wall_ms"] = json!(started.elapsed().as_millis() as u64);
            if entry.get("converged_ms").is_some() {
                entry["converged_ms"] = entry["wall_ms"].clone();
            }
            self.rounds_log.push(entry);
            if executed.is_ok() {
                // The checkpoint: the invariants that held after this action, by name, on its row.
                match self.invariants(round).await {
                    Ok(held) => {
                        if let Some(row) = self.rounds_log.last_mut() {
                            row["invariants_held"] = json!(held);
                            if held.is_empty() {
                                row["invariants_deferred"] = json!("a mesh is cut; they are judged at its Heal row");
                            }
                        }
                    }
                    Err(b) => violations.push(Violation { rule: b.rule.into(), round, step, action: action.to_string(), detail: b.detail }),
                }
            }
            write_found(&found, &violations[seen..]);
        }
        // A mesh left silenced (nothing more could run) is healed before anything is judged or stopped.
        let seen = violations.len();
        for (mesh, sil) in std::mem::take(&mut self.silenced) {
            if let Err(e) = sil.lift() {
                violations.push(Violation { rule: "silence-lifts".into(), round, step: self.executed.len().saturating_sub(1), action: format!("lift {mesh}"), detail: e });
            }
        }
        write_found(&found, &violations[seen..]);
        self.shared.stop.store(true, Ordering::Relaxed);
        let _ = tr.await;
        let (ledger, ledger_violations) = self.reconcile_ledger().await;
        crate::host_net::keep(&self.estate, "end", None).await;
        let mut report = Report {
            seed: self.cfg.seed,
            secs: self.cfg.secs,
            provider: self.estate.owner.provider.clone(),
            rounds: round,
            elapsed_s: self.started.elapsed().as_secs(),
            actions: self.actions.clone(),
            skipped: self.skipped.clone(),
            ledger,
            violations,
            repro: None,
            executed: self.executed.clone(),
            schedule: self.sched.events_json(),
            rounds_log: self.rounds_log.clone(),
            operations: {
                let b = self.shared.op_view.lock().unwrap();
                b.ledger
                    .operations()
                    .iter()
                    .zip(b.meta.iter())
                    .map(|(o, m)| json!({"op_seq": o.id.0, "target": m.location(), "target_node_id": m.target_id, "key": m.key, "trace_id": m.trace_id, "via": m.via, "route": m.route, "intent": o.mutation.as_ref().map(|i| json!({"key": i.key, "value": String::from_utf8_lossy(&i.value)})), "classification": o.classification}))
                    .collect()
            },
        };
        report.ledger.violations = ledger_violations.iter().map(|(_, d)| d.clone()).collect();
        // The first broken rule, shrunk. A ledger violation fails at the last executed row.
        let first = report.violations.first().cloned().or_else(|| {
            ledger_violations.first().map(|(rule, d)| Violation { rule: rule.clone(), round, step: self.executed.len().saturating_sub(1), action: self.executed.last().map(|a| a.to_string()).unwrap_or_default(), detail: d.clone() })
        });
        if let Some(v) = first {
            let involved: BTreeSet<PathName> = ledger_violations.iter().filter(|(r, _)| *r == v.rule).flat_map(|(_, d)| self.paths_named_in(d)).chain(self.executed.get(v.step).and_then(|a| node_of(a).cloned())).collect();
            let mut repro = reproduce(self.cfg.seed, &self.initial, &self.executed, &v.rule, v.step, &v.detail, involved);
            repro.live = report.rounds_log.iter().find(|e| e["step"] == v.step).cloned().unwrap_or(Value::Null);
            report.repro = Some(repro);
        }
        (report, self.estate)
    }

    /// The node paths a ledger violation's text names (a violation is about operations on stores;
    /// the view maps their target ids to paths).
    fn paths_named_in(&self, detail: &str) -> Vec<PathName> {
        let b = self.shared.op_view.lock().unwrap();
        let mut out = Vec::new();
        for (i, m) in b.meta.iter().enumerate() {
            if detail.contains(&format!("op#{i} ")) || detail.contains(&format!("op#{i}\n")) || detail.ends_with(&format!("op#{i}")) {
                out.push(m.location());
            }
        }
        out
    }

    async fn execute(&mut self, action: &Action, nodes: &[Value], round: usize, entry: &mut Value) -> Result<(), Broken> {
        let expected = expected_counts(&self.model);
        let before = (self.read)(&self.estate, &self.build_id).await;
        let node_json = |p: &PathName| nodes.iter().find(|n| location(n) == *p).cloned();
        // The exact birth the action removes, and the Build whose create names the node that replaces
        // it; none when node recovery replaces it, which is never a Build (Q342).
        let mut removed: Option<(PathName, (String, String))> = None;
        let mut watch: Option<String> = None;
        // A restarted birth: the same node id and incarnation come back ready (node-stop.md "Kept").
        let mut restarted: Option<(PathName, (String, String))> = None;
        let mut deleted_node_id: Option<String> = None;
        // A birth held while its runtime ran: it comes back as itself.
        let mut held: Option<(PathName, (String, String))> = None;
        let mut gone: Option<PathName> = None;
        let mut no_build_for_drift = false;
        match action {
            Action::Grow { mesh, class } => {
                let kind = class.type_name();
                let spawn = after_work_in_flight(|| self.estate.create_node(&mesh, class.type_name())).await.map_err(|end| refused(&format!("node.create {kind} in {mesh}"), end))?;
                entry["accepted"] = accepted_json(spawn.accepted());
                entry["build"] = self.await_build(spawn).await?;
            }
            Action::Shrink { node } => {
                let n = node_json(node).ok_or_else(|| broken("model-matches-view", format!("{node:?} is not in the view")))?;
                let delete = after_work_in_flight(|| async { self.estate.delete_node(&node.clone()).await }).await.map_err(|end| refused(&format!("node.delete {node:?}"), end))?;
                entry["accepted"] = accepted_json(delete.accepted());
                entry["build"] = self.await_build(delete).await?;
                gone = Some(node.clone());
                deleted_node_id = Some(s(&n["node_id"]));
            }
            Action::Restart { node } => {
                let n = node_json(node).ok_or_else(|| broken("model-matches-view", format!("{node:?} is not in the view")))?;
                // A restart is node.stop then node.start (Q183): the birth is kept, the same node id and
                // incarnation start again on the live process; nothing is replaced.
                let stop = after_work_in_flight(|| async { self.estate.nodes_obj().await.stop(&node.clone()).await }).await.map_err(|end| refused(&format!("node.stop {node:?}"), end))?;
                entry["accepted"] = accepted_json(stop.accepted());
                self.await_build(stop).await?;
                self.estate.until_stopped_at_the_fabric_primary(&node.clone()).await;
                let start = after_work_in_flight(|| async { self.estate.nodes_obj().await.start(&node.clone()).await }).await.map_err(|end| refused(&format!("node.start {node:?}"), end))?;
                entry["accepted_start"] = accepted_json(start.accepted());
                self.await_build(start).await?;
                restarted = Some((node.clone(), (s(&n["node_id"]), s(&n["incarnation"]))));
            }
            Action::HandOff { mesh } => {
                let n = nodes
                    .iter()
                    .find(|n| crate::estate::mesh_of(&n) == mesh.as_str() && n["type"] == "node-admin" && n["is_primary"] == true && n["state"] == "ready")
                    .cloned()
                    .ok_or_else(|| broken("model-matches-view", format!("{mesh} has no ready mesh primary")))?;
                entry["handed_off_from"] = json!({"node": n["name"], "node_id": n["node_id"], "fabric_primary": n["is_fabric_primary"]});
                // The fabric-primary admits a primary's delete only when its own view holds a successor
                // (build.md §4.4: "A primary's stop, connections delete or delete has a successor | the
                // fabric-primary's view"), and its view learns a peer mesh's births from that mesh's
                // primary's topology, after the atlas `node.get` answers from: the hand-off waits for a ready
                // node-admin of the mesh other than its primary in what the fabric-primary holds itself.
                let primary_name = location(&n);
                if let Some(fp) = nodes.iter().find(|x| x["is_fabric_primary"] == true).map(location) {
                    let est = &self.estate;
                    let successor = wait_for_opt(&format!("{fp:?} holds a ready node-admin of {mesh} besides {primary_name:?}"), CONVERGE, || async {
                        est.held_values_at(&fp).await.into_iter().find(|x| crate::estate::mesh_of(&x) == mesh.as_str() && x["type"] == "node-admin" && location(x) != primary_name && x["state"] == "ready").map(|x| location(&x))
                    })
                    .await;
                    entry["successor_in_fabric_primary_view"] = json!(successor);
                }
                let (b2, old) = self.delete_then_create(&n, entry).await?;
                removed = Some((location(&n), old));
                watch = Some(b2);
                deleted_node_id = Some(s(&n["node_id"]));
            }
            Action::Kill { node } => {
                // Node recovery re-creates the killed birth at its path and name under a new node.id
                // (node-recovery.md): its mesh-primary's own workflow, never a Build (Q342).
                let n = node_json(node).ok_or_else(|| broken("model-matches-view", format!("{node:?} is not in the view")))?;
                removed = Some((node.clone(), (s(&n["node_id"]), s(&n["incarnation"]))));
                deleted_node_id = Some(s(&n["node_id"]));
                no_build_for_drift = true;
                self.kill(node, entry).await?;
            }
            Action::Wedge { node } => {
                let n = node_json(node).ok_or_else(|| broken("model-matches-view", format!("{node:?} is not in the view")))?;
                held = Some((node.clone(), (s(&n["node_id"]), s(&n["incarnation"]))));
                self.wedge(&n, round, &before, entry).await?;
            }
            Action::Unheard { mesh } => {
                self.unheard(mesh, nodes, entry).await?;
                // The silenced side stays silenced until its Heal row: nothing converges across it.
                return Ok(());
            }
            Action::Heal { mesh } => {
                let sil = self.silenced.remove(mesh).ok_or_else(|| broken("model-matches-estate", format!("{mesh} holds no silence to lift")))?;
                let healed_at_ns = now_ns();
                sil.lift().map_err(|e| broken("silence-lifts", e))?;
                let held_ms = self.cut_at.remove(mesh).map(|at| at.elapsed().as_millis() as u64);
                entry["healed"] = json!({"mesh": mesh, "healed_at_ns": healed_at_ns, "held_ms": held_ms});
                // A cut held past the floors (S6): every path that was ready at the cut is ready again with
                // one live birth. Which side's birth stands is not pinned (Q4): a birth unreached across the
                // cut may have been re-created (Q110, Q111), and the one it superseded is refused and ended.
                if let (Some(_), Some(births)) = (self.cfg.hold, self.cut_births.remove(mesh)) {
                    let floor = self.cfg.floor;
                    let est = &self.estate;
                    let one_each = wait_for_opt(&format!("{mesh} healed: one live birth ready at every path ready at the cut"), floor * 4 + Duration::from_secs(90), || async {
                        let nodes = est.node_values().await;
                        let missing: Vec<PathName> = births
                            .keys()
                            .filter(|name| nodes.iter().filter(|n| location(n) == **name).count() != 1 || !nodes.iter().any(|n| location(n) == **name && n["state"] == "ready"))
                            .cloned()
                            .collect();
                        missing.is_empty().then(|| {
                            let same = births.iter().filter(|(name, (id, _))| nodes.iter().any(|n| location(n) == **name && n["node_id"] == id.as_str())).count();
                            (nodes.len(), same)
                        })
                    })
                    .await;
                    match one_each {
                        Some((n, same)) => entry["healed"]["births"] = json!({"paths": births.len(), "same_birth": same, "recreated": births.len() - same, "nodes": n}),
                        None => return Err(broken("silence-is-not-death", format!("{mesh} healed after {held_ms:?} ms: not every path ready at the cut holds one ready birth again"))),
                    }
                }
            }
            Action::Store(op) => {
                let (target, key) = match op {
                    StoreOp::Put { target, key, .. } | StoreOp::Cas { target, key, .. } | StoreOp::Delete { target, key } => (target, *key),
                };
                let n = node_json(target).filter(|n| n["state"] == "ready").ok_or_else(|| broken("model-matches-view", format!("{target:?} is not ready in the view")))?;
                let probe = self.estate.probe_handle();
                let admin = self.shared.view.lock().unwrap().admin.clone().unwrap_or_else(|| self.estate.entry_birth());
                let call = match op {
                    StoreOp::Put { value, .. } => Call { path: target.path.clone(), name: target.name.clone(), target_id: s(&n["node_id"]), kind: "put", key, value: Some(value.clone()), expected: None, via: None },
                    StoreOp::Cas { expected, value, .. } => Call { path: target.path.clone(), name: target.name.clone(), target_id: s(&n["node_id"]), kind: "cas", key, value: Some(value.clone()), expected: Some(expected.clone()), via: None },
                    StoreOp::Delete { .. } => Call { path: target.path.clone(), name: target.name.clone(), target_id: s(&n["node_id"]), kind: "delete", key, value: None, expected: None, via: None },
                };
                issue(&self.shared, &probe, &admin, call).await;
                return Ok(());
            }
        }
        if !self.silenced.is_empty() {
            return Ok(());
        }
        // Converge: the birth sequence is confirmed before anything is judged: the operation's Build
        // ends complete with the create of the node.id its record names at the path, or node recovery
        // puts a birth of a new node.id at the path (its `rdm.node.investigated` event recorded with the
        // round); every live admin holds exactly that birth ready and the replaced one nowhere; every
        // live admin holds the same births.
        let until = Instant::now() + CONVERGE;
        let mut held_wait: Option<Instant> = None;
        loop {
            if relocate_control(&mut self.estate, &self.known).await.is_some() {
                let view = self.estate.node_values().await;
                self.publish_view(&view);
                let ready_at = |path: &PathName, id: Option<&str>, not: &str| view.iter().find(|n| location(n) == *path && n["state"] == "ready" && n["node_id"] != not && id.is_none_or(|id| n["node_id"] == id)).map(|n| (s(&n["node_id"]), s(&n["incarnation"])));
                let born = match (&watch, &removed) {
                    (Some(b), Some((path, old))) => born_at(&self.estate, self.read, b, path).await.map_err(|f| broken("build-completes", f))?.and_then(|id| ready_at(path, Some(&id), &old.0)),
                    (None, Some((path, old))) => {
                        let born = ready_at(path, None, &old.0);
                        if born.is_some() {
                            entry["investigated"] = json!(investigated(&self.estate, old));
                        }
                        born
                    }
                    _ => None,
                };
                let sequence_done = match (&removed, &born, &gone) {
                    _ if restarted.is_some() => {
                        let (path, birth) = restarted.as_ref().expect("checked");
                        match birth_held_everywhere(&self.estate, path, birth, None).await {
                            Ok(()) => true,
                            Err(why) => {
                                entry["waiting_on"] = json!(why);
                                false
                            }
                        }
                    }
                    (Some((path, old)), Some(birth), _) => {
                        entry["born"] = json!({"path": path, "node_id": birth.0, "incarnation": birth.1, "replaced": old});
                        match birth_held_everywhere(&self.estate, path, birth, Some(old)).await {
                            Ok(()) => true,
                            Err(why) => {
                                entry["waiting_on"] = json!(why);
                                false
                            }
                        }
                    }
                    (Some(_), None, _) => {
                        entry["waiting_on"] = json!("no ready birth of another node.id at the path yet: the Build has not ended complete, or node recovery has not re-created it");
                        false
                    }
                    (None, _, Some(path)) => match gone_everywhere(&self.estate, path).await {
                        Ok(()) => true,
                        Err(why) => {
                            entry["waiting_on"] = json!(why);
                            false
                        }
                    },
                    (None, _, None) => true,
                };
                if sequence_done {
                    let converged = converged_everywhere(&self.estate, &expected).await;
                    if let Err(why) = &converged {
                        entry["waiting_on"] = json!(why);
                    }
                    if let Ok((agreeing, unheard_at)) = converged {
                        if !unheard_at.is_empty() {
                            entry["peer_mesh_unheard_at"] = json!(unheard_at);
                        }
                        // Converged views are not the whole fabric: the fabric-primary may still hold a
                        // peer mesh unheard (a held mesh-primary just released, a healed cut), and every
                        // action on that mesh is refused until it is heard.
                        match self.held_unheard(&view) {
                            None => {
                                if let Some(since) = held_wait {
                                    entry["held_unheard_ms"] = json!(since.elapsed().as_millis() as u64);
                                    entry.as_object_mut().map(|o| o.remove("waiting_on"));
                                }
                                self.known = agreeing;
                                break;
                            }
                            Some(why) => {
                                entry["waiting_on"] = json!(why);
                                held_wait.get_or_insert_with(Instant::now);
                            }
                        }
                    }
                }
            }
            if Instant::now() >= until {
                let mut views = serde_json::Map::new();
                for admin in &self.known {
                    let v = match views_at(&self.estate, admin).await {
                        Some(nodes) => {
                            let mut by: BTreeMap<String, Vec<String>> = BTreeMap::new();
                            for n in &nodes {
                                by.entry(s(&crate::estate::mesh_of(&n))).or_default().push(format!("{:?}:{}", location(n), s(&n["state"])));
                            }
                            json!(by)
                        }
                        None => json!("unanswered"),
                    };
                    views.insert(admin.node_id.to_string(), v);
                }
                return Err(broken("birth-sequence-converges", format!("the birth sequence did not complete within {}s: {entry}; views {}", CONVERGE.as_secs(), Value::Object(views))));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        entry["converged_ms"] = json!(entry["wall_ms"].as_u64().unwrap_or(0));
        // A silenced birth that node recovery did not re-create is the same birth back; one it re-created
        // (a verdict other than `reached`, Q110, Q111) was checked replaced with its runtime ended.
        if let Some((path, inc)) = held.as_ref().filter(|_| entry["recreated"] != true) {
            // The released birth is commanded back and runs its start sequence before it is ready
            // (node-recovery.md §3): judged another birth only when no row of it stands; otherwise
            // waited for within the converge window.
            let wait_until = Instant::now() + CONVERGE;
            loop {
                let now_nodes = self.estate.node_values().await;
                match judge_held_birth(&now_nodes, &inc.0, &inc.1) {
                    HeldBirth::Ready => break,
                    // A recovery that acted on the held birth after the release read (its verdict,
                    // `os-stopped` read while held, is the one decision: node-investigation.md, Q273) replaced
                    // it: a recreate read late, never silence replacing a running runtime.
                    HeldBirth::Replaced => {
                        let late = acted_on(&before, &(self.read)(&self.estate, &self.build_id).await, &investigated(&self.estate, inc), inc);
                        if late.is_empty() {
                            return Err(broken("silence-is-not-death", format!("{path:?} came back as another birth than {inc:?}: silence replaced a running runtime")));
                        }
                        entry["recreated"] = json!(true);
                        entry["recreated_late"] = json!(late);
                        break;
                    }
                    HeldBirth::Returning if Instant::now() >= wait_until => {
                        return Err(broken("silence-is-not-death", format!("{path:?}: the released birth {inc:?} did not read ready within {}s", CONVERGE.as_secs())));
                    }
                    HeldBirth::Returning => tokio::time::sleep(Duration::from_millis(250)).await,
                }
            }
        }
        // The replaced node's id names no node: a call to it is refused, never dispatched.
        if let Some(old) = &deleted_node_id {
            let r = self.estate.probe(&["get", "--target", &format!("exact:{old}"), "--key", "9999"]);
            entry["deleted_call"] = r["outcome"].clone();
            if r["outcome"] == "Reply" {
                return Err(broken("deleted-node-is-not-dispatched", format!("a call to the deleted node {old} was dispatched: {r}")));
            }
        }
        // A loss is recovered by node recovery, never a Build (Q342): the current Build stays.
        let fabric = self.estate.try_fabric_view().await.map_err(|end| broken("fabric-build-id-readable", format!("fabric.get at the entry admin {:?}: {end}", self.estate.admin)))?;
        let now_build = fabric.build_id.unwrap_or_default();
        if no_build_for_drift && now_build != self.build_id {
            return Err(broken("no-build-for-drift", format!("a Build was minted for drift: {} -> {now_build}", self.build_id)));
        }
        self.build_id = now_build;
        Ok(())
    }

    /// A workflow stream to its `Complete`, within 120 s; a stream that ends `FabricPrimaryMoved` (a planned fabric-primary
    /// hand-off) is followed at the new role. The Build the call opened, as the round row records it.
    async fn await_build(&mut self, mut stream: WorkflowStream) -> Result<Value, Broken> {
        let accepted = stream.accepted().cloned();
        let what = match &accepted {
            Some(a) => format!("Build {}", a.build_id),
            None => format!("the node command {:?}", stream.kind()),
        };
        let until = tokio::time::Instant::now() + Duration::from_secs(120);
        loop {
            match tokio::time::timeout_at(until, stream.next()).await {
                Err(_) => return Err(broken("build-completes", format!("{what} did not complete within 120s"))),
                Ok(None) => return Err(broken("build-completes", format!("the stream of {what} ended with no terminal frame"))),
                Ok(Some(Err(end))) => return Err(broken("build-completes", format!("the stream of {what} broke: {end}"))),
                Ok(Some(Ok(Frame::Complete))) => return Ok(accepted_json(accepted.as_ref())),
                Ok(Some(Ok(Frame::Failed { step, reason }))) => return Err(broken("build-completes", format!("{what} failed at {step:?}: {reason}"))),
                Ok(Some(Ok(Frame::FabricPrimaryMoved { fabric_primary }))) => {
                    let Some(accepted) = accepted.as_ref() else {
                        return Err(broken("build-completes", format!("{what} ended FabricPrimaryMoved, which no node command's stream carries")));
                    };
                    stream = self
                        .estate
                        .nodes_obj()
                        .await
                        .resume(stream.kind().clone(), accepted)
                        .await
                        .map_err(|end| broken("build-completes", format!("the fabric-primary role moved to {fabric_primary:?} and the new role does not stream Build {}: {end}", accepted.build_id)))?;
                }
                Ok(Some(Ok(_))) => {}
            }
        }
    }

    /// `node.delete` of `n`, await the Build, `node.create` of the same kind into the freed ordinal: two
    /// Builds (Q345). Returns the spawn's Build and the deleted incarnation.
    async fn delete_then_create(&mut self, n: &Value, entry: &mut Value) -> Result<(String, (String, String)), Broken> {
        let (path, mesh) = (location(n), s(&crate::estate::mesh_of(&n)));
        let r#type: String = serde_json::from_value(n["type"].clone()).unwrap_or_else(|e| panic!("{path:?}: the view's type {} is no node type: {e}", n["type"]));
        let nodes = self.estate.nodes_obj().await;
        let target = rdm_core::NodeId::parse(&s(&n["node_id"])).unwrap_or_else(|e| panic!("{path:?}: the view's node_id {} is no node.id: {e}", n["node_id"]));
        let delete = after_work_in_flight(|| nodes.delete(vec![target.clone()])).await.map_err(|end| refused(&format!("node.delete {path:?}"), end))?;
        entry["accepted"] = accepted_json(delete.accepted());
        entry["build"] = self.await_build(delete).await?;
        // A deleted fabric-primary or entry admin takes the entry with it: the create goes to the admin
        // that holds the control role after the delete, as each round enters it.
        if relocate_control(&mut self.estate, &self.known).await.is_none() {
            return Err(broken("control-api-answers", format!("after node.delete {path:?}, no admin answers fabric.get: {:?}", self.known.iter().map(|a| &a.name).collect::<Vec<_>>())));
        }
        let spawn = after_work_in_flight(|| self.estate.create_node(&mesh, &r#type)).await.map_err(|end| refused(&format!("node.create {:?} in {mesh}", r#type), end))?;
        entry["regrow"] = accepted_json(spawn.accepted());
        let build = spawn.accepted().expect("a create opens a Build").build_id.to_string();
        self.await_build(spawn).await?;
        Ok((build, (s(&n["node_id"]), s(&n["incarnation"]))))
    }

    async fn published(&self, node: &PathName) -> Result<ExactRuntime, Broken> {
        let dir = self.estate.data_dir(node).await;
        ExactRuntime::published(std::path::Path::new(&dir)).map_err(|r| broken("fault-names-an-exact-runtime", format!("{node:?}: no exact runtime published in {dir}: {r:?}")))
    }

    /// The exact runtime of `node` ends: SIGKILL of the published process (process provider), or
    /// `docker kill` of the exact container (container provider: SIGKILL to its init, acknowledged by
    /// the daemon as not running). A container whose interface is removed is not a death: its
    /// runtime keeps running, silent, and the provider reports it `Running`, so its mesh-primary holds
    /// it under R-FM1 (`held-running`, RDM_HUNG_HOLD_FLOORS floors) instead of re-birthing it
    /// (node-recovery.md steps 3-4; the soaks at 768dfaee). Only once the fabric has first been
    /// ready: the driver arms no death during fabric start
    /// ([`crate::estate::Estate::fabric_ready`], Q74).
    async fn kill(&mut self, node: &PathName, entry: &mut Value) -> Result<(), Broken> {
        let ready = self.estate.fabric_ready();
        if self.estate.owner.provider == "container" {
            let c = container_faults::ExactContainer::of(&self.estate, node).await.map_err(|r| broken("fault-names-an-exact-runtime", format!("kill {node:?}: {r:?}")))?;
            let applied = c.apply(container_faults::Fault::Kill(ready)).map_err(|r| broken("fault-is-acknowledged", format!("kill {node:?}: {r:?}")))?;
            if !applied.exited {
                return Err(broken("fault-is-acknowledged", format!("kill {node:?}: the daemon did not show the container exited: {applied:?}")));
            }
            entry["killed"] = json!({"container": c.id, "status_after": applied.status_after, "exit_code": applied.exit_code});
            return Ok(());
        }
        let rt = self.published(node).await?;
        let applied = rt.apply(Fault::Kill(ready)).map_err(|r| broken("fault-is-acknowledged", format!("kill {node:?}: {r:?}")))?;
        if !applied.exited {
            return Err(broken("fault-is-acknowledged", format!("kill {node:?}: the OS did not show the exit: {applied:?}")));
        }
        entry["killed"] = json!({"pid": rt.pid, "start": rt.start});
        Ok(())
    }

    /// Hold the node's exact runtime until the view stops calling it ready, judge the hold with
    /// the semantic wedge detector, release it. The same birth must come back.
    async fn wedge(&mut self, n: &Value, round: usize, before: &Value, entry: &mut Value) -> Result<(), Broken> {
        let path = location(n);
        let birth = (s(&n["node_id"]), s(&n["incarnation"]));
        let container = self.estate.owner.provider == "container";
        let is_rpc = serves_testkit_store(n);
        let exact = format!("exact:{}", s(&n["node_id"]));
        let floor = self.cfg.floor;
        let key = (9000 + round).to_string();
        let probe = self.estate.probe_handle();
        let admin = self.estate.entry_birth();
        let run = |args: Vec<String>| {
            let (p, a) = (probe.clone(), admin.clone());
            async move { tokio::task::spawn_blocking(move || p.run(&a, &args.iter().map(String::as_str).collect::<Vec<_>>())).await.ok().and_then(|r| r.ok()).unwrap_or(Value::Null) }
        };
        if is_rpc {
            let put = run(vec!["put".into(), "--target".into(), exact.clone(), "--key".into(), key.clone(), "--value".into(), "before-hold".into()]).await;
            if put["outcome"] != "Reply" {
                crate::host_net::keep(&self.estate, &format!("round-{round:04}.{path:?}"), Some(&path)).await;
                return Err(broken("healthy-control-before-fault", format!("{path:?}: the control put before the hold: {put}")));
            }
        }
        let nodes0 = self.estate.node_values().await;
        let (roles_before, detail_before) = primaries(&nodes0);
        let fp_before = fabric_primaries(&nodes0);
        // Where the fabric-primary belongs as the hold begins: the one advertised.
        let home = fp_before.clone();
        entry["fabric_role"] = json!({"advertised": fp_before, "home": home});
        let mut ev = Evidence::new(Family::SilentRuntime, format!("silent-runtime:{path:?}"));
        // The hold: SIGSTOP of the exact runtime, or `docker pause` of the exact container.
        let release: Box<dyn Fn() -> Result<Value, String> + Send>;
        let mut hold_rt: Option<ExactRuntime> = None;
        let held_from = Instant::now();
        // A terminate of the held birth during the hold would be death from silence: read as it is written.
        let mut terminates = SpanTail::from_now(self.estate.evidence.clone(), |sp| sp["name"] == "rdm.node_admin.deployment.delete.via-path-investigation" && sp["attributes"]["outcome"] == "unreached");
        if container {
            let id = self.estate.container_of(&path).await.ok_or_else(|| broken("fault-names-an-exact-runtime", format!("{path:?}: no running container")))?;
            container_faults::pause(&id).map_err(|e| broken("fault-applies", e))?;
            ev.primitive = Some(Primitive { armed: true, ack: json!({"paused": id}) });
            let id2 = id.clone();
            release = Box::new(move || container_faults::unpause(&id2).map(|_| json!({"unpaused": id2})));
        } else {
            let rt = self.published(&path).await?;
            let stopped = rt.apply(Fault::Stop).map_err(|r| broken("fault-is-acknowledged", format!("stop {path:?}: {r:?}")))?;
            ev.primitive = Some(Primitive { armed: true, ack: serde_json::to_value(&stopped).unwrap_or(Value::Null) });
            let rt2 = rt.clone();
            release = Box::new(move || rt2.apply(Fault::Continue).map(|a| serde_json::to_value(a).unwrap_or(Value::Null)).map_err(|r| format!("{r:?}")));
            hold_rt = Some(rt);
        }
        // The container provider waits for the view to mark the held node unheard (pending-reconnect
        // or dead). A dead mark there was measured at 81 s after the hold against the 79.5 s
        // the tickle's two ladders derive (spans of the i143-2787-soak-container run): the coin is
        // drawn all the same, so the schedule is the same on both providers.
        let coin = self.rng.next() % 2 == 0;
        // Only a node's own mesh-primary tickles it, so only it records `dead`. A held mesh-primary has
        // no other tickler: the admins that look at it find it `os-stopped`, which proves no exit and
        // opens no hold (deployment-provider.md §2.5), so it is held pending-reconnect, never dead.
        let holds_mesh_primary = nodes0.iter().any(|x| location(x) == path && x["type"] == "node-admin" && x["is_primary"] == true);
        let true_offline = coin && !container && !holds_mesh_primary;
        // Control never waits on the held node's own control API: a held admin answers nothing.
        let held_name = Some(location(n));
        self.known.retain(|a| Some(&a.location()) != held_name.as_ref());
        if held_name.as_ref() == Some(&self.estate.admin) {
            if let Some(other) = self.known.first().map(Admin::path) {
                self.estate.enter(&other).await;
            }
        }
        entry["held_until"] = json!(if true_offline { "dead" } else { "pending-reconnect" });
        let known = self.known.clone();
        // On containers the view marked a held node unheard 29 s to 46 s after the hold (spans of
        // the i143-2787-soak-container runs); the container cell for the silence (#2784) observes
        // the same consequence within 90 s, and so does the soak there.
        let within = if container { Duration::from_secs(90) } else { self.cfg.unheard_within(true_offline, 1 + self.silenced.values().map(|x| x.members_len()).sum::<usize>()) };
        let marked = until_unheard(&mut self.estate, &known, &[(path.clone(), s(&n["node_id"]))], true_offline, true, within).await;
        if let Ok(after) = &marked {
            entry["unheard_after_ms"] = json!(after.as_millis() as u64);
        }
        let mut reads = Vec::new();
        let mut outcomes = Vec::new();
        if marked.is_ok() && is_rpc {
            for _ in 0..2 {
                let o = run(vec!["get".into(), "--target".into(), exact.clone(), "--key".into(), key.clone()]).await;
                reads.push(json!({"proc_state": hold_rt.as_ref().and_then(|r| proc_state(r.pid)).map(String::from), "cpu_ticks": hold_rt.as_ref().map(|r| cpu_ticks(r.pid)), "outcome": o["outcome"]}).to_string());
                outcomes.push(o);
            }
        } else if marked.is_ok() {
            for _ in 0..2 {
                reads.push(json!({"proc_state": hold_rt.as_ref().and_then(|r| proc_state(r.pid)).map(String::from), "cpu_ticks": hold_rt.as_ref().map(|r| cpu_ticks(r.pid))}).to_string());
            }
        }
        // The floors story (S6): the runtime stays held past the staleness floor. The provider reads it
        // `OsStopped` (deployment-provider.md §2.5), a verdict other than `reached`, so node recovery
        // re-creates the node at its path (node-recovery.md, Q110, Q111): the path holds one birth, the
        // held one or its replacement, and nothing terminates the held runtime while it reads
        // `OsStopped` (node-recovery.md step 3: recovery leaves it as it is).
        if let (Some(hold), Ok(_)) = (self.cfg.hold, &marked) {
            let mut checks = Vec::new();
            while held_from.elapsed() < hold {
                tokio::time::sleep(floor.min(hold.saturating_sub(held_from.elapsed())).max(Duration::from_millis(200))).await;
                let now_nodes = self.estate.node_values().await;
                let at_path: Vec<&Value> = now_nodes.iter().filter(|x| location(x) == path).collect();
                let targeted: Vec<Value> = terminates.poll().iter().filter(|sp| sp["attributes"]["node_id"] == n["node_id"]).cloned().collect();
                checks.push(json!({
                    "held_ms": held_from.elapsed().as_millis() as u64,
                    "proc_state": hold_rt.as_ref().and_then(|r| proc_state(r.pid)).map(String::from),
                    "view_status": at_path.first().map(|x| x["state"].clone()),
                    "births_at_path": at_path.len(),
                }));
                // Two births at the path are never right. None is the instant between the recovery's
                // delete of the held birth and its create of the replacement (node-recovery.md steps
                // 3-4, a delete then a create): the path holds its replacement again within a floor,
                // or the recovery has stalled.
                let mut births = at_path.len();
                if births == 0 {
                    let est = &self.estate;
                    births = wait_for_opt(&format!("{path:?}: a birth at the path again after the recovery's delete"), floor, || async {
                        let n = est.node_values().await.iter().filter(|x| location(x) == path).count();
                        (n > 0).then_some(n)
                    })
                    .await
                    .unwrap_or(0);
                    checks.push(json!({"held_ms": held_from.elapsed().as_millis() as u64, "births_at_path_again": births}));
                }
                if births != 1 {
                    entry["held_past_floors"] = json!({"hold_ms": hold.as_millis() as u64, "checks": checks});
                    return Err(broken("silence-is-not-death", format!("{path:?} held {} ms: the path holds {births} births", held_from.elapsed().as_millis())));
                }
                if !targeted.is_empty() {
                    entry["held_past_floors"] = json!({"hold_ms": hold.as_millis() as u64, "checks": checks});
                    return Err(broken("silence-is-not-death", format!("{path:?} held {} ms: its runtime was terminated while held and reading OsStopped: {}", held_from.elapsed().as_millis(), targeted[0])));
                }
            }
            entry["held_past_floors"] = json!({"hold_ms": hold.as_millis() as u64, "checks": checks});
        }
        let held_at_last_read = hold_rt.as_ref().map(|r| proc_state(r.pid) == Some('T')).unwrap_or(true);
        let nodes_during = self.estate.node_values().await;
        let (roles_during, detail_during) = primaries(&nodes_during);
        let during = nodes_during.iter().find(|x| location(x) == path).cloned();
        let build_during = (self.read)(&self.estate, &self.build_id).await;
        // What acted on the held birth while it was held: a step of the current Build on its node.id, or
        // node recovery's re-create (its `rdm.node.investigated` event, `result` `re-created`); a step on
        // another node, a role move among them, did nothing to it (fabric-recovery.md step 4).
        let acted_during = acted_on(before, &build_during, &investigated(&self.estate, &birth), &birth);
        entry["acted_on_held"] = json!({"build_steps": [before["steps"].as_array().map_or(0, Vec::len), build_during["steps"].as_array().map_or(0, Vec::len)], "acted": acted_during});
        let alive = hold_rt.as_ref().map(|r| r.check().is_ok()).unwrap_or(true);
        let replaced = during.as_ref().is_none_or(|x| x["node_id"] != n["node_id"]);
        let released = release();
        if let Err(e) = marked {
            return Err(broken("fault-is-observed", format!("{path:?} held: {e}")));
        }
        let released = released.map_err(|e| broken("fault-is-released", format!("{path:?}: {e}")))?;
        ev.fault = Some(FaultAck { held: true, names: Some(json!({"node": path, "view_status": during.as_ref().map(|x| x["state"].clone())})), held_at_last_read });
        ev.progress = Some(WedgeProgress { reads, complete_while_held: outcomes.iter().any(|o| o["outcome"] == "Reply") });
        ev.routing = Some(Routing { expected_routable: false, observed_routable: outcomes.iter().any(|o| o["outcome"] == "Reply"), observed: format!("view {}; calls {}", during.as_ref().map(|x| s(&x["state"])).unwrap_or_default(), outcomes.iter().map(|o| s(&o["outcome"])).collect::<Vec<_>>().join(",")) });
        ev.release = Some(Release { acked: true, hold_ended: hold_rt.as_ref().map(|r| proc_state(r.pid) != Some('T')).unwrap_or(true) });
        let inc = s(&n["incarnation"]);
        // After the release the path is ready again: as the held birth, or, when node recovery re-created
        // it during the hold, as its replacement, the held runtime refused at admission and terminated
        // (Q110: "if it comes back it's denied ... then you kill it").
        // A recovery that has acted on the held birth by the release (since Q273 an `os-stopped` runtime is
        // a verdict and is recovered within seconds: node-investigation.md) is read to its end: the path is
        // back only as the replacement, never as the birth being deleted.
        // Read at each poll, never once at the release: a recovery whose verdict is written in the same
        // instant as the release (its verdict reached as the hold ends) acts on the held birth after
        // the release, so the held birth's brief return as itself is not the end; the path is back only
        // as the replacement once any recovery has acted.
        let back = {
            let est = &self.estate;
            let held_rt = hold_rt.clone();
            let back = wait_for_opt(&format!("{path:?} ready as its own birth, or as its replacement with the held runtime ended"), floor * 2 + Duration::from_secs(60), || async {
                let acting = !acted_on(before, &(self.read)(est, &self.build_id).await, &investigated(est, &birth), &birth).is_empty();
                est.node_value_opt(&path).await.filter(|x| {
                    x["state"] == "ready" && ((x["incarnation"] == inc.as_str() && !acting) || (x["node_id"] != n["node_id"] && held_rt.as_ref().is_none_or(|r| r.check().is_err())))
                })
            })
            .await;
            let back = back.ok_or_else(|| broken("silence-is-not-death", format!("{path:?} is neither back as {inc} nor replaced with the held runtime ended after the release")))?;
            // A recovery that acted after the held birth was read back as itself (its verdict written
            // between that read and this one) is read to its end too: the path is back as the
            // replacement.
            let acted_since = !acted_on(before, &(self.read)(est, &self.build_id).await, &investigated(est, &birth), &birth).is_empty();
            if back["node_id"] == n["node_id"] && acted_since {
                wait_for_opt(&format!("{path:?} replaced with the held runtime ended, a recovery having acted"), floor * 2 + Duration::from_secs(60), || async {
                    est.node_value_opt(&path).await.filter(|x| x["state"] == "ready" && x["node_id"] != n["node_id"] && held_rt.as_ref().is_none_or(|r| r.check().is_err()))
                })
                .await
                .ok_or_else(|| broken("silence-is-not-death", format!("{path:?}: a recovery acted on {inc} after the release, and no replacement is ready with the held runtime ended")))?
            } else {
                back
            }
        };
        let recreated = back["node_id"] != n["node_id"];
        entry["recreated"] = json!(recreated);
        let target_now = if recreated { format!("exact:{}", s(&back["node_id"])) } else { exact.clone() };
        let kept = if is_rpc { run(vec!["get".into(), "--target".into(), target_now, "--key".into(), key.clone()]).await } else { Value::Null };
        ev.recovery = Some(Recovery { work_complete: if is_rpc { kept["outcome"] == "Reply" } else { true }, marker_after: json!({"proc_state": hold_rt.as_ref().and_then(|r| proc_state(r.pid)).map(String::from), "outcome": kept["outcome"]}).to_string() });
        // The roles go home one floor after the node at the lost path/name pair is Ready (Q89): the "after"
        // read waits for that, bounded, before it judges the role at its home.
        let nodes_after = {
            let est = &self.estate;
            let home = home.clone();
            let settled = wait_for_opt(&format!("{path:?}: the fabric-primary role home again after the release"), floor * 3 + Duration::from_secs(30), || async {
                let ns = est.node_values().await;
                (fabric_primaries(&ns) == home).then_some(ns)
            })
            .await;
            match settled {
                Some(ns) => ns,
                None => self.estate.node_values().await,
            }
        };
        let (roles_after, detail_after) = primaries(&nodes_after);
        ev.control = Some(Control {
            roles_as_expected: [roles_before, roles_during, roles_after],
            roles_detail: [detail_before, detail_during, detail_after.clone()].join(" | "),
            fabric_primary: [fp_before, fabric_primaries(&nodes_during), fabric_primaries(&nodes_after)],
            fabric_home: home,
            primary_may_move: false,
            incarnation: [inc.clone(), during.as_ref().map(|x| s(&x["incarnation"])).unwrap_or_default(), s(&back["incarnation"])],
            // The held birth's runs: none before the hold, and the step events that acted on it during it.
            runs: [0, acted_during.len() as u64],
            exact_runtime_alive: alive,
            replaced_during: replaced || recreated,
            held_past_floor: recreated,
        });
        let mut rec = Reconciliation::default();
        let build_after = (self.read)(&self.estate, &self.build_id).await;
        let acted_after = acted_on(before, &build_after, &investigated(&self.estate, &birth), &birth);
        if recreated {
            // The re-created node is a new node over a newly allocated data dir (Q111: the data dir
            // follows the verdict; `os-stopped` is not `exited`): it serves at the path, and only the
            // recovery's re-create acted on the held birth.
            if is_rpc {
                rec.check("the re-created node serves at the path", kept["outcome"] == "Reply" && kept["reply"]["executing_node"] == back["node_id"], kept.to_string());
            }
            rec.check("only node recovery acted on the held birth: its rdm.node.investigated event's result is re-created", !acted_after.is_empty() && acted_after.iter().all(|r| r.starts_with("node recovery")), format!("{acted_after:?}"));
        } else {
            if is_rpc {
                rec.check("the value stored before the hold is served after it", kept["reply"]["result"] == json!({"found": true, "value": "before-hold"}), kept.to_string());
                rec.check("the same birth serves it", kept["reply"]["incarnation_id"] == n["incarnation"] && kept["reply"]["executing_node"] == n["node_id"], kept.to_string());
            }
            rec.check(
                "nothing since the hold began deleted, created, stopped or started the held birth: no Build step on its node.id and no node recovery re-create",
                acted_after.is_empty(),
                format!("{acted_after:?}"),
            );
        }
        rec.check("the primaries equal the public candidates' after the release", roles_after, detail_after);
        ev.reconciliation = Some(rec);
        entry["wedge"] = json!({"released": released, "view_while_held": during.as_ref().map(|x| x["state"].clone())});
        match judge(&ev) {
            Ok(v) => {
                entry["wedge"]["verdict"] = serde_json::to_value(&v).unwrap_or(Value::Null);
                Ok(())
            }
            Err(r) => Err(broken("wedge-detector-judges-the-hold", format!("{path:?}: {r}"))),
        }
    }

    /// Silence a mesh against the rest of the estate (container provider) until its Heal row.
    async fn unheard(&mut self, mesh: &str, nodes: &[Value], entry: &mut Value) -> Result<(), Broken> {
        self.cut_at.insert(mesh.to_string(), Instant::now());
        self.cut_births.insert(mesh.to_string(), nodes.iter().filter(|n| n["state"] == "ready").map(|n| (location(n), (s(&n["node_id"]), s(&n["incarnation"])))).collect());
        let members: Vec<PathName> = nodes.iter().filter(|n| crate::estate::mesh_of(&n) == mesh && n["state"] == "ready").map(|n| location(n)).collect();
        let fp = nodes.iter().find(|n| n["is_fabric_primary"] == true).map(location).ok_or_else(|| broken("fabric-primary-known", "no fabric-primary in the held view"))?;
        if self.estate.owner.provider != "container" {
            return self.cut_udp(mesh, nodes, members, entry).await;
        }
        let cut_at_ns = now_ns();
        let sil = container_faults::silence(&self.estate, &members, &fp).await.map_err(|e| broken("fault-applies", e))?;
        if !sil.active().map_err(|e| broken("fault-is-observed", e))? {
            return Err(broken("fault-is-observed", format!("{mesh}: the silence chain is not installed on every member")));
        }
        // Seen from the other mesh: control is read through its admins.
        let others: Vec<Admin> = self.known.iter().filter(|a| !nodes.iter().any(|n| crate::estate::mesh_of(&n) == mesh && n["node_id"] == a.node_id.as_str())).cloned().collect();
        // The window the container cell for this fault (#2784) observes the same consequence in; the
        // soak measures the time it took (`unheard_after_ms`).
        let within = Duration::from_secs(90);
        self.silenced.insert(mesh.to_string(), Cut::Container(sil));
        let births: Vec<(PathName, String)> = nodes.iter().filter(|n| crate::estate::mesh_of(&n) == mesh && n["state"] == "ready").map(|n| (location(n), s(&n["node_id"]))).collect();
        let after = until_unheard(&mut self.estate, &others, &births, false, false, within).await.map_err(|e| broken("fault-is-observed", format!("{mesh} silenced: {e}")))?;
        // The cut window the cell reads (`cut_at_ns` to its Heal row's `healed_at_ns`), as the process
        // provider's udp-drop row records it: a run's cuts are counted and judged from these rows.
        entry["silenced"] = json!({"mesh": mesh, "members": members, "fault": "silence", "cut_at_ns": cut_at_ns, "unheard_after_ms": after.as_millis() as u64});
        Ok(())
    }

    /// Process provider: every UDP path between `mesh` and the other meshes is dropped (the host's
    /// acknowledgement is the rule install). Held until every node-admin has re-fed every peer-mesh
    /// node-admin twice (`rdm.mesh.connection.update.via-refeed`, reason `stale-held-member`), the
    /// repair the architecture gives an unheard peer mesh; each node-admin of the cut mesh still holds
    /// its own mesh ready, because no node is cut from a member of its own mesh.
    async fn cut_udp(&mut self, mesh: &str, nodes: &[Value], members: Vec<PathName>, entry: &mut Value) -> Result<(), Broken> {
        let others: Vec<PathName> = nodes.iter().filter(|n| crate::estate::mesh_of(&n) != mesh && n["state"] == "ready").map(|n| location(n)).collect();
        let (pa, pb) = (udp_ports(nodes, &members), udp_ports(nodes, &others));
        if pa.len() != members.len() || pb.len() != others.len() {
            return Err(broken("fault-names-an-exact-runtime", format!("{mesh}: {} of {} members and {} of {} others publish a transport socket", pa.len(), members.len(), pb.len(), others.len())));
        }
        let partition = Partition::start(&pa, &pb).map_err(|why| broken("fault-applies", format!("RDM_REQUIRE_NETFAULT: this host cannot drop traffic between {mesh} and the other meshes: {why}")))?;
        // The re-feeds are read from the node-admins' evidence as it is written, from the cut on.
        let mut refeeds = SpanTail::from_now(self.estate.evidence.clone(), |sp| sp["name"] == "rdm.mesh.connection.update.via-refeed" && sp["attributes"]["reason"] == "stale-held-member");
        let cut_at_ns = now_ns();
        self.silenced.insert(mesh.to_string(), Cut::Udp { partition, members: members.clone() });
        let at_cut: Vec<(PathName, String, String)> = nodes.iter().filter(|n| n["type"] == "node-admin" && n["state"] == "ready").map(|n| (location(n), s(&crate::estate::mesh_of(&n)), s(&n["node_id"]))).collect();
        let window = self.cfg.floor;
        let until = Instant::now() + window * 6 + Duration::from_secs(40);
        let held_at = loop {
            let att: Vec<&Value> = refeeds.poll().iter().filter(|sp| sp["start_unix_nano"].as_u64().unwrap_or(0) >= cut_at_ns).collect();
            // A node-admin node recovery re-created during the cut (Q110, Q111) is a new birth on new ports,
            // outside the cut: only the admins still on their birth of the cut re-feed and are re-fed.
            let now_nodes = self.estate.node_values().await;
            let admins: Vec<(&String, &String)> = at_cut.iter().filter(|(_, _, id)| now_nodes.iter().any(|n| n["node_id"] == id.as_str())).map(|(_, mesh, id)| (id, mesh)).collect();
            let all = admins.iter().all(|(obs, om)| admins.iter().filter(|(_, pm)| pm != om).all(|(peer, _)| att.iter().filter(|sp| sp["attributes"]["node"] == obs.as_str() && sp["attributes"]["peer_node"] == peer.as_str()).count() >= 2));
            if all {
                break now_ns();
            }
            if Instant::now() >= until {
                return Err(broken("fault-is-observed", format!("{mesh} cut: not every node-admin re-fed every peer-mesh node-admin twice within {}s ({} re-feeds since the cut)", (window * 6 + Duration::from_secs(40)).as_secs(), att.len())));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        for (name, node_mesh, _) in &at_cut {
            let own: Vec<&str> = nodes.iter().filter(|n| s(&crate::estate::mesh_of(&n)) == *node_mesh && n["state"] == "ready").filter_map(|n| n["node_id"].as_str()).collect();
            // What the admin holds itself (query.md §2: node.topology.get, answered from its own topology),
            // never the view read, which a cut-off mesh-primary carries up to the fabric-primary and cannot
            // answer during the cut (query.md §1).
            let theirs = self.estate.held_values_at(name).await;
            if theirs.is_empty() {
                return Err(broken("cut-leaves-own-mesh-ready", format!("{name:?} does not answer node.topology.get during the cut")));
            }
            let missing: Vec<&&str> = own.iter().filter(|p| !theirs.iter().any(|n| n["node_id"] == **p && n["state"] == "ready")).collect();
            if !missing.is_empty() {
                return Err(broken("cut-leaves-own-mesh-ready", format!("{name:?} no longer holds its own mesh members ready during the cut: {missing:?}")));
            }
        }
        entry["silenced"] = json!({"mesh": mesh, "members": members, "fault": "udp-drop", "udp_ports": {"cut": pa, "others": pb}, "cut_at_ns": cut_at_ns, "held_after_ms": (held_at - cut_at_ns) / 1_000_000});
        Ok(())
    }

    /// The invariants that hold at every converged point; a violation names itself.
    async fn invariants(&mut self, round: usize) -> Result<Vec<&'static str>, Broken> {
        if !self.silenced.is_empty() {
            return Ok(Vec::new());
        }
        let nodes = self.estate.node_values().await;
        let mut per_path: BTreeMap<PathName, usize> = BTreeMap::new();
        for n in nodes.iter().filter(|n| !matches!(n["state"].as_str(), Some("dead" | "pending-reconnect"))) {
            *per_path.entry(location(n)).or_default() += 1;
        }
        if let Some((p, c)) = per_path.iter().find(|(_, c)| **c > 1) {
            return Err(broken("one-current-birth-per-path", format!("round {round}: {c} current births at {p:?}")));
        }
        let mut writers: BTreeSet<String> = BTreeSet::new();
        for admin in admins_of(&nodes) {
            let Some(f) = fabric_at(&self.estate, &admin).await else {
                return Err(broken("live-admin-answers", format!("round {round}: {:?} (named ready in the view) does not answer fabric.get", admin.name)));
            };
            if let Some(p) = &f.fabric_primary {
                writers.insert(p.to_string());
            }
            if f.build_id.as_deref() != Some(self.build_id.as_str()) {
                return Err(broken("fabric-build-id-held", format!("round {round}: {:?} holds the current build_id {:?} not {}", admin.name, f.build_id, self.build_id)));
            }
            let heard: BTreeSet<String> = f.meshes.iter().filter(|m| m.status == ScopeStatus::Ready).map(|m| m.name.clone()).collect();
            for (mesh, _) in &self.cfg.shape {
                if !heard.contains(*mesh) {
                    return Err(broken("no-peer-mesh-permanently-unheard", format!("round {round}: {:?} does not hold {mesh} ready", admin.name)));
                }
            }
        }
        if writers.len() != 1 {
            return Err(broken("one-fabric-primary", format!("round {round}: fabric primaries named across live admins: {writers:?}")));
        }
        let (ok, why) = primaries(&nodes);
        if !ok {
            return Err(broken("primaries-equal-the-public-candidates", format!("round {round}: {why}")));
        }
        for n in nodes.iter().filter(|n| serves_testkit_store(n) && n["state"] == "ready") {
            let r = self.estate.probe(&["get", "--target", &format!("exact:{}", s(&n["node_id"])), "--key", "9998"]);
            if r["outcome"] != "Reply" || r["reply"]["incarnation_id"] != n["incarnation"] {
                let name = location(n);
                crate::host_net::keep(&self.estate, &format!("round-{round:04}.{name:?}"), Some(&name)).await;
                return Err(broken("every-rpc-node-reachable-on-its-current-birth", format!("round {round}: {:?} not reachable on its current birth: {r}", n["name"])));
            }
        }
        if let Some(row) = self.rounds_log.last_mut() {
            row["authority"] = json!({"fabric_primary": writers.iter().next(), "mesh_primaries": nodes.iter().filter(|n| n["type"] == "node-admin" && n["is_primary"] == true).map(|n| n["name"].clone()).collect::<Vec<_>>(), "primaries": nodes.iter().filter(|n| n["is_primary"] == true).map(|n| n["name"].clone()).collect::<Vec<_>>()});
        }
        Ok(vec![
            "one-current-birth-per-path",
            "live-admin-answers",
            "fabric-build-id-held",
            "no-peer-mesh-permanently-unheard",
            "one-fabric-primary",
            "primaries-equal-the-public-candidates",
            "every-rpc-node-reachable-on-its-current-birth",
        ])
    }

    /// The ledger's count algebra over every operation and its state algebra against every store
    /// that survived to the end. Returns the account and every violation with the invariant it
    /// fails.
    async fn reconcile_ledger(&mut self) -> (LedgerReport, Vec<(String, String)>) {
        let mut found: Vec<(String, String)> = Vec::new();
        let nodes = if relocate_control(&mut self.estate, &self.known).await.is_some() { self.estate.node_values().await } else { Vec::new() };
        let alive: BTreeMap<String, PathName> = nodes.iter().filter(|n| serves_testkit_store(n) && n["state"] == "ready").map(|n| (s(&n["node_id"]), location(n))).collect();
        let (meta, excluded, probe_failures, mut data_dirs) = {
            let b = self.shared.op_view.lock().unwrap();
            (b.meta.clone(), b.excluded.clone(), b.probe_failures.clone(), b.data_dirs.clone())
        };
        for n in nodes.iter().filter(|n| serves_testkit_store(n)) {
            data_dirs.insert(s(&n["node_id"]), n["storage"]["data_dir"].as_str().map(str::to_string));
        }
        let mut report = LedgerReport { probe_failures, keys_excluded_from_state: excluded.len(), ..Default::default() };
        let (counts, global) = {
            let b = self.shared.op_view.lock().unwrap();
            (b.ledger.buckets(), b.ledger.reconcile())
        };
        report.issued = meta.len();
        report.buckets = counts.iter().map(|(k, v)| (format!("{k:?}"), *v)).collect();
        {
            let b = self.shared.op_view.lock().unwrap();
            for o in b.ledger.operations() {
                if let Some(c) = &o.classification {
                    *report.details.entry(format!("{:?}:{}", c.bucket, c.detail)).or_default() += 1;
                }
            }
        }
        if let Err(refusal) = global {
            for v in &refusal.0 {
                found.push((v.invariant().to_string(), v.to_string()));
            }
        }
        // The state algebra, store by store. A store is its data dir: every birth over the dir a live
        // node holds is folded into that node's final state.
        let ids: BTreeSet<String> = meta.iter().map(|m| m.target_id.clone()).collect();
        let mut held: BTreeMap<String, (String, PathName)> = BTreeMap::new();
        for (id, path) in &alive {
            match data_dirs.get(id).cloned().flatten() {
                None => found.push((crate::ledger::DETECTS_LOST_APPLIED_MUTATION.to_string(), format!("{path:?} ({id}): its view names no data dir, so its store is not told from another birth's"))),
                Some(dir) => {
                    if let Some((other, other_path)) = held.insert(dir.clone(), (id.clone(), path.clone())) {
                        found.push((crate::ledger::DETECTS_LOST_APPLIED_MUTATION.to_string(), format!("{path:?} ({id}) and {other_path:?} ({other}) are both live over the data dir {dir}")));
                    }
                }
            }
        }
        let mut reconciled: BTreeSet<String> = BTreeSet::new();
        let probe = self.estate.probe_handle();
        let admin = self.estate.entry_birth();
        for (dir, (id, path)) in &held {
            let births: BTreeSet<String> = ids.iter().filter(|t| data_dirs.get(*t).is_some_and(|d| d.as_deref() == Some(dir.as_str()))).cloned().collect();
            if births.is_empty() {
                continue;
            }
            reconciled.extend(births.iter().cloned());
            let excluded_keys: BTreeSet<u64> = excluded.iter().filter(|(t, _)| births.contains(t)).map(|(_, k)| *k).collect();
            let keys: BTreeSet<u64> = meta.iter().filter(|m| births.contains(&m.target_id)).map(|m| m.key).filter(|k| !excluded_keys.contains(k)).collect();
            let mut state: BTreeMap<String, Vec<u8>> = BTreeMap::new();
            let mut unreadable = Vec::new();
            for k in keys {
                let (p, a, t, ks) = (probe.clone(), admin.clone(), format!("exact:{id}"), k.to_string());
                let out = tokio::task::spawn_blocking(move || p.run(&a, &["get", "--target", &t, "--key", &ks])).await.ok().and_then(|r| r.ok()).unwrap_or(Value::Null);
                match (out["outcome"].as_str(), &out["reply"]["result"]) {
                    (Some("Reply"), r) if r["found"] == true => {
                        state.insert(k.to_string(), r["value"].as_str().unwrap_or_default().as_bytes().to_vec());
                    }
                    (Some("Reply"), r) if r["found"] == false => {}
                    _ => unreadable.push((k, out)),
                }
            }
            if !unreadable.is_empty() {
                found.push((crate::ledger::DETECTS_LOST_APPLIED_MUTATION.to_string(), format!("{path:?} ({id}): the final store could not be read for keys {:?}", unreadable.iter().map(|(k, o)| format!("{k}: {o}")).collect::<Vec<_>>())));
                continue;
            }
            let sub = {
                let b = self.shared.op_view.lock().unwrap();
                b.ledger.select(|o| births.contains(&o.target_node_id) && !o.mutation.as_ref().is_some_and(|m| m.key.parse::<u64>().is_ok_and(|k| excluded_keys.contains(&k))))
            };
            match sub.reconcile_state(&state) {
                Ok(r) => {
                    report.stores_reconciled += 1;
                    report.applied_mutations += r.applied_mutations;
                    report.indeterminate.extend(r.indeterminate_mutations.iter().map(|(o, a)| (o.0, *a)));
                }
                Err(refusal) => {
                    for v in &refusal.0 {
                        found.push((v.invariant().to_string(), format!("{path:?} ({id}): {v}")));
                    }
                }
            }
        }
        report.ops_on_deleted_births = meta.iter().filter(|m| !reconciled.contains(&m.target_id)).count();
        (report, found)
    }
}

/// Poll `check` until it yields `Some` within `within`: `None` when it never does.
async fn wait_for_opt<T, F, Fut>(_what: &str, within: Duration, mut check: F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + within;
    loop {
        if let Some(v) = check().await {
            return Some(v);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The minimized legal reproduction of a failure: the executed sequence shrunk, over the model,
/// while it stays legal, ends in the failing action, reaches the same topology before it and
/// keeps every row naming an involved node.
pub fn reproduce(seed: u64, initial: &Model, executed: &[Action], rule: &str, step: usize, detail: &str, involved: BTreeSet<PathName>) -> Repro {
    let prefix: Vec<Action> = executed.iter().take(step + 1).cloned().collect();
    let failing = prefix.last().cloned();
    let Some(failing) = failing else {
        return Repro { seed, rule: rule.into(), failing_step: step, failing_action: Action::Heal { mesh: String::new() }, involved: involved.into_iter().collect(), original_len: 0, minimized: Vec::new(), candidates_tried: 0, live: Value::Null, definition: REPRO_DEFINITION };
    };
    let pre_shape = replay(initial, &prefix[..prefix.len() - 1]).map(|m| m.shape()).unwrap_or_default();
    let keep: Vec<&Action> = prefix.iter().filter(|a| node_of(a).is_some_and(|n| involved.contains(n))).collect();
    let property = |init: &Model, cand: &[Action]| -> Result<(), Failure> {
        let last = cand.last();
        let fail = |at: usize, why: &str| Failure { rule: rule.to_string(), step: at, action: failing.clone(), detail: why.to_string() };
        if last != Some(&failing) {
            return Ok(());
        }
        let Ok(before) = replay(init, &cand[..cand.len() - 1]) else { return Ok(()) };
        if before.shape() != pre_shape {
            return Ok(());
        }
        // Every row naming an involved node, in order, as the original sequence had them.
        let mut want = keep.iter();
        let mut next = want.next();
        for a in cand {
            if next.is_some_and(|k| *k == a) {
                next = want.next();
            }
        }
        if next.is_some() {
            return Ok(());
        }
        Err(fail(cand.len() - 1, detail))
    };
    match shrink(initial, &prefix, property) {
        Some(sh) => Repro { seed, rule: rule.into(), failing_step: step, failing_action: failing, involved: involved.into_iter().collect(), original_len: prefix.len(), minimized: sh.minimized, candidates_tried: sh.candidates_tried, live: Value::Null, definition: REPRO_DEFINITION },
        None => Repro { seed, rule: rule.into(), failing_step: step, failing_action: failing, involved: involved.into_iter().collect(), original_len: prefix.len(), minimized: prefix, candidates_tried: 0, live: Value::Null, definition: REPRO_DEFINITION },
    }
}

/// The report of a run whose driver panicked: the seed and the sequence executed so far, shrunk
/// to the action it panicked in.
pub fn panicked(seed: u64, progress: &Progress, message: &str) -> Repro {
    let initial = progress.initial.clone().expect("the driver recorded its initial model");
    let step = progress.executed.len().saturating_sub(1);
    let involved: BTreeSet<PathName> = progress.executed.get(step).and_then(|a| node_of(a).cloned()).into_iter().collect();
    reproduce(seed, &initial, &progress.executed, "driver-panic", step, message, involved)
}

/// The buckets a report counts, for a one-line summary.
pub fn summary(r: &Report) -> String {
    let b = |k: Bucket| r.ledger.buckets.get(&format!("{k:?}")).copied().unwrap_or(0);
    format!(
        "SOAK seed={} provider={} rounds={} issued={} reply={} not_sent={} unserved={} rejected_stale={} indeterminate={} violations={} ledger_violations={}",
        r.seed,
        r.provider,
        r.rounds,
        r.ledger.issued,
        b(Bucket::Reply),
        b(Bucket::NotSent),
        b(Bucket::Unserved),
        b(Bucket::RejectedStale),
        b(Bucket::Indeterminate),
        r.violations.len(),
        r.ledger.violations.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONTRACT (node-recovery.md §3: a birth heard again is commanded back through node.mesh.join and
    /// runs its start sequence before it is ready): right after a wedge's release the same
    /// birth reads pending-reconnect, and that is the birth returning, never another birth. Must NOT
    /// happen: the authority soak at 3cf384224, round 18, judging it "silence replaced a running
    /// runtime" at the release instant while the birth was running, ready 0.3 s later.
    #[test]
    fn a_released_birth_marked_pending_reconnect_is_returning_never_replaced() {
        let row = |inc: &str, status: &str| json!({"path": "mesh2.node-admin", "name": "1", "node_id": "100000000000", "incarnation": inc, "state": status});
        assert_eq!(judge_held_birth(&[row("qx6", "pending-reconnect")], "100000000000", "qx6"), HeldBirth::Returning);
        assert_eq!(judge_held_birth(&[row("qx6", "ready")], "100000000000", "qx6"), HeldBirth::Ready);
        assert_eq!(judge_held_birth(&[row("zzz", "ready")], "100000000000", "qx6"), HeldBirth::Replaced);
        assert_eq!(judge_held_birth(&[], "100000000000", "qx6"), HeldBirth::Replaced);
    }

    /// CONTRACT (build.md §1, one Build at a time; `CallEnd::BuildInProgress`: a caller that must go
    /// after the Build in flight follows it and submits again): a workflow call refused
    /// `build-in-progress` is submitted again until it is accepted, and any other refusal is the
    /// call's at once. Must NOT happen: a round broken by a Build in flight that another caller submitted
    /// between rounds (the routing and randomized soaks at 14034530d: `node.delete` refused while a returned
    /// birth's termination ran).
    #[tokio::test(start_paused = true)]
    async fn a_call_refused_build_in_progress_is_submitted_again_and_any_other_refusal_is_the_calls() {
        let current = rdm_client::BuildId::parse(&rdm_core::fixture_crockford60("inflight")).unwrap();
        let tries = std::sync::atomic::AtomicU32::new(0);
        let accepted = after_work_in_flight(|| {
            let n = tries.fetch_add(1, Ordering::SeqCst);
            let current = current.clone();
            async move { if n < 3 { Err(CallEnd::BuildInProgress { current }) } else { Ok(n) } }
        })
        .await;
        assert_eq!(accepted.ok(), Some(3), "accepted on the fourth submit, after three build-in-progress refusals");
        let other = after_work_in_flight(|| async { Err::<u32, _>(CallEnd::Rejected { reason: "unknown-node".into(), detail: String::new() }) }).await;
        assert!(matches!(other, Err(CallEnd::Rejected { ref reason, .. }) if reason == "unknown-node"), "{other:?}");
    }

    /// A held birth is marked unheard by its own row: `dead`, or `pending-reconnect` when the hold is
    /// not written dead. Node recovery re-creates it at once on a verdict other than `reached`
    /// (node-recovery.md step 3, Q110, Q111), so its `dead` stands only until recovery's delete (0.45 s
    /// in the routing soak at 119663355); the path's replacement, Pending or ready, is
    /// another birth and never makes the held one heard.
    #[test]
    fn a_held_birth_node_recovery_replaced_is_marked_unheard() {
        let row = |id: &str, status: &str| json!({"name": "mesh2.broker.2", "node_id": id, "state": status});
        let held = [(PathName::new("mesh2.broker", "2").unwrap(), "y2reptx3zt5y".to_string())];
        assert!(marks_unheard(&[row("y2reptx3zt5y", "dead")], &held, true));
        assert!(!marks_unheard(&[row("y2reptx3zt5y", "pending-reconnect")], &held, true));
        assert!(marks_unheard(&[row("y2reptx3zt5y", "pending-reconnect")], &held, false));
        assert!(!marks_unheard(&[row("y2reptx3zt5y", "ready")], &held, false));
        for status in ["pending", "ready"] {
            assert!(marks_unheard(&[row("461hjdtmy4t4", status)], &held, true), "the replacement {status} at the path");
        }
    }
        use crate::model::generate;

    /// A failure at the last row of a generated sequence shrinks to a legal sequence that ends in
    /// the failing action, reaches the same topology before it and keeps the involved node's rows.
    #[test]
    fn a_failure_reports_the_seed_and_a_minimized_legal_reproduction() {
        let cfg = Config::mm(1_432_787, 1, Duration::from_secs(3), Duration::from_millis(500), Duration::from_secs(30));
        let initial = cfg.model("process");
        let seq = generate(cfg.seed, &initial, 60);
        let at = seq.iter().rposition(|a| matches!(a, Action::Restart { .. })).expect("a restart in 60 rows");
        let Action::Restart { node } = &seq[at] else { unreachable!() };
        let repro = reproduce(cfg.seed, &initial, &seq, "planted", at, "planted failure", BTreeSet::from([node.clone()]));
        assert_eq!(repro.seed, cfg.seed);
        assert_eq!(repro.minimized.last(), Some(&seq[at]));
        assert!(repro.minimized.len() < at + 1, "{} -> {}", at + 1, repro.minimized.len());
        replay(&initial, &repro.minimized).expect("the minimized sequence is legal");
        let before = |rows: &[Action]| replay(&initial, &rows[..rows.len() - 1]).unwrap().shape();
        assert_eq!(before(&repro.minimized), before(&seq[..=at]), "the same topology before the failing action");
        let named: Vec<&Action> = seq[..=at].iter().filter(|a| node_of(a) == Some(node)).collect();
        let kept: Vec<&Action> = repro.minimized.iter().filter(|a| node_of(a) == Some(node)).collect();
        assert_eq!(named, kept, "every row naming the involved node is kept");
    }

    /// CONTRACT (R-D1; node-recovery.md step 1): the fabric-primary holds a peer mesh unheard while its
    /// own view holds that mesh's primary off ready, and holds nothing once it reads ready again; its own
    /// mesh is never held.
    #[test]
    fn a_peer_mesh_is_held_while_the_fabric_primarys_view_holds_its_primary_off_ready() {
        let row = |mesh: &str, id: &str, primary: bool, fabric: bool, status: &str| json!({ "path": format!("{mesh}.node-admin"), "type": "node-admin", "node_id": id, "is_primary": primary, "is_fabric_primary": fabric, "state": status });
        let fp = row("mesh1", "100000000000", true, true, "ready");
        assert!(held_unheard_in(&[fp.clone(), row("mesh2", "200000000000", true, false, "ready")]).is_none(), "a heard peer mesh is not held");
        let why = held_unheard_in(&[fp.clone(), row("mesh2", "200000000000", true, false, "pending_reconnect")]).expect("a marked peer mesh-primary holds its mesh");
        assert!(why.contains("mesh2") && why.contains("200000000000"), "{why}");
        assert!(held_unheard_in(&[fp, row("mesh1", "300000000000", false, false, "pending_reconnect")]).is_none(), "the fabric-primary's own mesh is never held");
    }

    /// The held birth of the floors soak on `b6695529` (seed 1431101, step 33): mesh1.broker.2.
    fn held_fixture() -> (PathName, (String, String)) {
        (PathName::new("mesh1.broker", "2").unwrap(), ("100000000000".into(), "200000000000".into()))
    }

    /// One `rdm.build.step` event of Build `a00000000000` as the Build view lists it (build.md §5).
    fn step(mesh: &str, action: &str, node_id: &str, step: &str) -> Value {
        let op = rdm_core::OperationId::node(action, &rdm_core::NodeId::parse(node_id).unwrap()).to_string();
        json!({ "build_id": "a00000000000", "mesh": mesh, "operation": op, "step": step, "outcome": "complete", "subject": node_id, "source": "mesh1.node-admin.1" })
    }

    fn build(steps: Vec<Value>) -> Value {
        json!({ "build_id": "a00000000000", "state": "running", "steps": steps })
    }

    /// CONTRACT (build.md §5: a Build's progress is its `rdm.build.step` events; node-recovery.md: node
    /// recovery is never a Build, Q342, and its result is one `rdm.node.investigated` event): what acted
    /// on a held birth is a step the current Build gained on its node.id that creates, deletes, stops or
    /// starts it, or an investigation of that exact birth that ends `re-created`. Must NOT happen: a role
    /// move, a step on another node, a step the Build held before the hold, or a `rejoined` read as
    /// acting on it.
    #[test]
    fn only_a_step_on_the_held_node_or_its_recovery_acts_on_a_held_birth() {
        let (_, birth) = held_fixture();
        let before = build(vec![step("mesh1", "node.create", &birth.0, "AllocateIdentity")]);
        assert!(acted_on(&before, &before, &[], &birth).is_empty(), "a step the Build held before the hold");
        let moved = build(vec![step("mesh1", "node.create", &birth.0, "AllocateIdentity"), step("mesh1", "node.fabric-primary.update", &birth.0, "Complete"), step("mesh1", "node.delete", "400000000000", "TerminateRuntime")]);
        assert!(acted_on(&before, &moved, &[], &birth).is_empty(), "a role move and a step on another node do nothing to it");
        let deleted = build(vec![step("mesh1", "node.create", &birth.0, "AllocateIdentity"), step("mesh1", "node.delete", &birth.0, "TerminateRuntime")]);
        let rows = acted_on(&before, &deleted, &[], &birth);
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert!(rows[0].contains("node.delete:100000000000") && rows[0].contains("TerminateRuntime"), "{rows:?}");
        let other = json!({ "build_id": "b00000000000", "steps": deleted["steps"] });
        assert!(acted_on(&before, &other, &[], &birth).is_empty(), "another Build's steps are not gained by this one");
        let investigated = |inc: &str, result: &str| json!({ "incarnation": inc, "path": "mesh1.broker", "name": "2", "result": result, "inspected": "os-stopped" });
        assert!(acted_on(&before, &before, &[investigated(&birth.1, "rejoined")], &birth).is_empty(), "a rejoined birth is the same birth, back");
        assert!(acted_on(&before, &before, &[investigated(&birth.1, "ready")], &birth).is_empty(), "a ready birth is the same birth");
        assert!(acted_on(&before, &before, &[investigated("800000000000", "re-created")], &birth).is_empty(), "another incarnation is another birth");
        let recovered = acted_on(&before, &before, &[investigated(&birth.1, "re-created")], &birth);
        assert_eq!(recovered.len(), 1, "{recovered:?}");
        assert!(recovered[0].starts_with("node recovery") && recovered[0].contains("os-stopped"), "{recovered:?}");
    }

    /// CONTRACT (D58: the fabric-primary mints the node.id of each create at acceptance and writes it in
    /// the record; build.md §5): the node.id a Build created at a path is the one its record's `created`
    /// names there, once that create's `AllocateIdentity` step event ended `complete`. Must NOT happen:
    /// a create of another path, or one whose step failed, read as the birth.
    #[test]
    fn the_birth_a_build_created_is_the_node_id_its_record_names_once_its_allocation_completed() {
        let (path, birth) = held_fixture();
        let created = json!([[{"path": path.path, "name": path.name}, birth.0], [{"path": "mesh1.broker", "name": "3"}, "300000000000"]]);
        let mut b = build(vec![step("mesh1", "node.create", "300000000000", "AllocateIdentity")]);
        b["topology"] = json!({ "created": created });
        assert_eq!(created_at(&b, &path), None, "only another path's create completed");
        let mut failed = step("mesh1", "node.create", &birth.0, "AllocateIdentity");
        failed["outcome"] = json!({ "failed": { "reason": "launch refused", "code": "FAILED_PRECONDITION" } });
        b["steps"].as_array_mut().unwrap().push(failed);
        assert_eq!(created_at(&b, &path), None, "its allocation failed");
        b["steps"].as_array_mut().unwrap().push(step("mesh1", "node.create", &birth.0, "AllocateIdentity"));
        assert_eq!(created_at(&b, &path), Some(birth.0.clone()));
        assert_eq!(created_at(&b, &PathName::new("mesh1.broker", "4").unwrap()), None, "the record creates nothing there");
    }
}
