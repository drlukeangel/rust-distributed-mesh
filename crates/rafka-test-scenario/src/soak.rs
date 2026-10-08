//! The multi-mesh soak driver (i143.e9.s2, rafka-v2 #2787): continuous proof-store operations,
//! recorded in the operation ledger, while the action model generates legal random topology and
//! fault actions that run as Builds the node-admin rectifier executes or as provider faults on
//! exact runtimes.
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
//! - [`crate::elections`]: the seats a view advertises equal the ones the public candidates
//!   compute.
//!
//! Topology actions go through the control API only (grow = spawn, shrink = delete, restart =
//! restart, replace = delete then spawn into the freed ordinal, hand-off = replace of the mesh's
//! seat-holding node-admin). A restart or a replacement is confirmed from the Build's own receipts
//! (`AllocateIdentity`) before anything is judged.
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
use crate::elections::seats_as_expected;
use crate::estate::{wait_for, Estate, ProbeHandle};
use crate::ledger::{Bucket, Ledger, MutationIntent, OperationId};
use crate::model::{generate, replay, shrink, Action, Capabilities, ClassBounds, Failure, Model, NodeClass, Rng};
use crate::process_faults::{cpu_ticks, proc_state, ExactRuntime, Fault};
use crate::scenario::Operation as ProofOp;
use crate::sim::Scheduler;
use crate::wedge::{judge, Control, Evidence, Family, FaultAck, Primitive, Progress as WedgeProgress, Reconciliation, Recovery, Release, Routing};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The op code of the proof store on Node RPC.
const PROOF_STORE_OP: u8 = 0x70;
/// The keys the traffic stream writes: disjoint from the model's own proof keys (`0..PROOF_KEYS`).
const TRAFFIC_KEYS: std::ops::Range<u64> = 100..108;
/// The bound on one action's birth sequence to complete (a Build and its convergence).
const CONVERGE: Duration = Duration::from_secs(150);

fn s(v: &Value) -> String {
    v.as_str().unwrap_or("").to_string()
}

fn num(v: &Value) -> u64 {
    v.as_u64().or_else(|| v.as_str().and_then(|a| a.parse().ok())).unwrap_or(0)
}

/// The name of an action's kind.
pub fn kind(a: &Action) -> &'static str {
    match a {
        Action::Grow { class: NodeClass::NodeAdmin, .. } => "grow-admin",
        Action::Grow { .. } => "grow-rpc",
        Action::Shrink { .. } => "shrink",
        Action::Restart { .. } => "restart",
        Action::Replace { .. } => "replace",
        Action::HandOff { .. } => "hand-off",
        Action::Unheard { .. } => "unheard",
        Action::Heal { .. } => "heal",
        Action::Kill { .. } => "kill",
        Action::Wedge { .. } => "wedge",
        Action::Proof(ProofOp::Put { .. }) => "proof-put",
        Action::Proof(ProofOp::Cas { .. }) => "proof-cas",
        Action::Proof(ProofOp::Delete { .. }) => "proof-delete",
    }
}

/// The node an action names, when it names one.
fn node_of(a: &Action) -> Option<&str> {
    match a {
        Action::Shrink { node } | Action::Restart { node } | Action::Replace { node } | Action::Kill { node } | Action::Wedge { node } => Some(node),
        Action::Proof(ProofOp::Put { target, .. } | ProofOp::Cas { target, .. } | ProofOp::Delete { target, .. }) => Some(target),
        _ => None,
    }
}

/// What a run is configured with.
#[derive(Debug, Clone)]
pub struct Config {
    pub seed: u64,
    pub secs: u64,
    /// `(mesh, node_admins, rpc_nodes)` as born.
    pub shape: Vec<(&'static str, u32, u32)>,
    pub bounds: BTreeMap<NodeClass, ClassBounds>,
    /// The membership staleness floor and the backbone gossip interval the estate runs at
    /// (`rafka_mesh_transport::membership`): the windows a silence is observed within.
    pub floor: Duration,
    pub backbone: Duration,
    /// The longest one offline-tickle round takes: the direct ping plus every via-peer carrier, each
    /// one call of the default budget (`rafka_node_rpc::CallOptions::default().budget` x (1 +
    /// `rafka_node_admin_core::offline::VIA_PEER_TICKLE_FANOUT`)).
    pub tickle_round: Duration,
}

impl Config {
    /// Two meshes of two node-admins and three rpc nodes; a mesh holds 2..=3 node-admins and
    /// 3..=4 rpc nodes.
    pub fn mm(seed: u64, secs: u64, floor: Duration, backbone: Duration, tickle_round: Duration) -> Self {
        Self {
            seed,
            secs,
            shape: vec![("mesh1", 2, 3), ("mesh2", 2, 3)],
            bounds: BTreeMap::from([(NodeClass::NodeAdmin, ClassBounds { min: 2, max: 3 }), (NodeClass::RpcNode, ClassBounds { min: 3, max: 4 })]),
            floor,
            backbone,
            tickle_round,
        }
    }

    /// How long a silenced node takes to be marked unheard in the view of the observer that judges
    /// it. Pending-reconnect: the staleness floor, then the views converge. True offline (`dead`):
    /// the mark at the floor, round 1 half a floor later, round 2 at least one floor after round 1
    /// found no path, each round at most `tickle_round`, then the mesh primary's write reaches the
    /// view (offline.rs, fabric-node-lifecycle.md section 7.3).
    /// The mesh primary tickles the silent nodes it watches one after another, so each of the
    /// `silent` nodes (the held node and every member of a silenced mesh) costs two rounds.
    pub fn unheard_within(&self, true_offline: bool, silent: usize) -> Duration {
        if true_offline {
            self.floor * 5 / 2 + self.tickle_round * 2 * silent.max(1) as u32 + self.backbone * 4 + Duration::from_secs(10)
        } else {
            self.floor * 3 + self.backbone * 2 + Duration::from_secs(30)
        }
    }

    /// The initial model of the shape on a provider.
    pub fn model(&self, provider: &str) -> Model {
        Model::new(&self.shape, self.bounds.clone(), Capabilities { network_faults: provider == "container" })
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
    pub involved: Vec<String>,
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
    /// Operations whose target birth no longer exists: counted by the count algebra, their store
    /// is gone with it.
    pub ops_on_retired_births: usize,
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
    target_path: String,
    target_id: String,
    key: u64,
}

#[derive(Default)]
struct OpBook {
    ledger: Ledger,
    meta: Vec<OpMeta>,
    excluded: BTreeSet<(String, u64)>,
    /// `(node id, key)` -> the value the last successful write left, for compare-and-swap.
    last: BTreeMap<(String, u64), String>,
    probe_failures: Vec<String>,
}

#[derive(Default, Clone)]
struct View {
    /// The fabric-primary's control API: the traffic's probes read the view through it.
    admin: Option<String>,
    rpcs: Vec<(String, String)>,
}

struct Shared {
    book: Mutex<OpBook>,
    view: Mutex<View>,
    stop: AtomicBool,
}

/// One proof-store operation of a stream.
struct Call {
    target_path: String,
    target_id: String,
    kind: &'static str,
    key: u64,
    value: Option<String>,
    expected: Option<String>,
}

/// Issue `call` once: recorded in the ledger before it is sent, classified from the printed typed
/// outcome after. No retry: an outcome is recorded, never repeated.
async fn issue(shared: &Shared, probe: &ProbeHandle, admin: &str, call: Call) {
    let id: OperationId = {
        let mut b = shared.book.lock().unwrap();
        let mutation = match (call.kind, &call.value) {
            ("put" | "cas", Some(v)) => Some(MutationIntent { key: call.key.to_string(), value: v.clone().into_bytes() }),
            _ => None,
        };
        if call.kind == "delete" {
            b.excluded.insert((call.target_id.clone(), call.key));
        }
        let id = b.ledger.issue(PROOF_STORE_OP, call.target_id.clone(), mutation);
        debug_assert_eq!(id.0 as usize, b.meta.len());
        b.meta.push(OpMeta { target_path: call.target_path.clone(), target_id: call.target_id.clone(), key: call.key });
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
    let (p, a) = (probe.clone(), admin.to_string());
    let printed = tokio::task::spawn_blocking(move || {
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        p.run(&a, &refs)
    })
    .await;
    let mut b = shared.book.lock().unwrap();
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
        let call = match pick {
            0..=4 => Call { target_path: path, target_id: id, kind: "put", key, value: Some(value), expected: None },
            5..=6 => {
                let expected = shared.book.lock().unwrap().last.get(&(id.clone(), key)).cloned();
                Call { target_path: path, target_id: id, kind: "cas", key, value: Some(value), expected }
            }
            _ => Call { target_path: path, target_id: id, kind: "get", key, value: None, expected: None },
        };
        issue(&shared, &probe, &admin, call).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Every live admin's control API, from the entry admin's view.
fn admin_bases(nodes: &[Value]) -> Vec<String> {
    nodes.iter().filter(|n| n["kind"] == "node_admin" && !matches!(n["status"].as_str(), Some("dead" | "pending-reconnect"))).filter_map(|n| n["admin_api_base"].as_str().map(String::from)).collect()
}

/// A read that may hit an admin whose process just died: `None`, never a panic.
async fn try_get(base: &str, path: &str) -> Option<Value> {
    let r = reqwest::Client::new().get(format!("{base}{path}")).timeout(Duration::from_secs(2)).send().await.ok()?;
    if r.status().as_u16() != 200 {
        return None;
    }
    r.json().await.ok()
}

/// Control follows whoever answers for this Fabric: the fabric's advertised holder if it answers,
/// else the first live admin from `known` that does. False when nobody answers.
async fn relocate_control(estate: &mut Estate, known: &[String]) -> bool {
    let mut candidates: Vec<String> = vec![estate.admin.clone()];
    candidates.extend(known.iter().cloned());
    for c in candidates.clone() {
        if let Some(f) = estate.fabric_at(&c).await {
            if let Some(h) = f["admin_api_base"].as_str().filter(|b| !b.is_empty()) {
                if estate.fabric_at(h).await.is_some() {
                    estate.admin = h.to_string();
                    return true;
                }
            }
            estate.admin = c;
            return true;
        }
    }
    false
}

/// `(admins, rpc nodes)` per mesh the model holds.
fn expected_counts(model: &Model) -> BTreeMap<String, (u32, u32)> {
    model.meshes.keys().map(|m| (m.clone(), (model.count(m, NodeClass::NodeAdmin), model.count(m, NodeClass::RpcNode)))).collect()
}

/// The fabric has converged when every live admin's view holds the same births: the expected
/// counts, every one ready for traffic, the same (path, incarnation) set everywhere. Returns the
/// live admin bases that agree.
async fn converged_everywhere(estate: &Estate, expected: &BTreeMap<String, (u32, u32)>) -> Option<Vec<String>> {
    let nodes = estate.nodes().await;
    let shape_ok = |nodes: &[Value]| {
        expected.iter().all(|(m, (a, r))| {
            let ready = |k: &str| nodes.iter().filter(|n| n["mesh"] == m.as_str() && n["kind"] == k && n["status"] == "ready-for-traffic").count() as u32;
            let all = |k: &str| nodes.iter().filter(|n| n["mesh"] == m.as_str() && n["kind"] == k && !matches!(n["status"].as_str(), Some("dead"))).count() as u32;
            ready("node_admin") == *a && ready("rpc_node") == *r && all("node_admin") == *a && all("rpc_node") == *r
        })
    };
    let births = |nodes: &[Value]| -> BTreeSet<(String, String)> { nodes.iter().filter(|n| n["status"] == "ready-for-traffic").map(|n| (s(&n["name"]), s(&n["incarnation_id"]))).collect() };
    if !shape_ok(&nodes) {
        return None;
    }
    let want = births(&nodes);
    let mut agreeing = Vec::new();
    for base in admin_bases(&nodes) {
        let v = try_get(&base, "/api/nodes").await?;
        let theirs: Vec<Value> = v["nodes"].as_array().cloned().unwrap_or_default();
        if !shape_ok(&theirs) || births(&theirs) != want {
            return None;
        }
        agreeing.push(base);
    }
    Some(agreeing)
}

/// The birth a round's operation produced at `path`, from the Build's own receipts: the last
/// completed `AllocateIdentity` for an operation on `path` in an attempt after `after_attempt`.
/// `Ok(None)` while the Build has not completed it; `Err` names a failed Build.
async fn born_at(estate: &Estate, build_id: &str, path: &str, after_attempt: u64) -> Result<Option<(String, String)>, String> {
    let (_, b) = estate.get(&format!("/api/builds?id={build_id}")).await;
    // A Build still reporting an attempt up to `after_attempt` is reporting an earlier attempt's
    // state: this operation's attempt has not been claimed here yet.
    if num(&b["attempt"]) <= after_attempt && after_attempt > 0 {
        return Ok(None);
    }
    match b["state"].as_str() {
        Some("failed") => return Err(format!("Build {build_id} failed: {}", b["last_failure"])),
        Some("complete") => {}
        _ => return Ok(None),
    }
    let suffix = format!(":{path}");
    Ok(b["steps"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|st| st["step"] == "AllocateIdentity" && st["outcome"] == "complete" && num(&st["attempt"]) > after_attempt && st["operation"].as_str().is_some_and(|o| o.ends_with(&suffix)))
        .last()
        .map(|st| (s(&st["output"]["node_id"]), s(&st["output"]["incarnation"]))))
}

/// Every live admin's view holds exactly `birth` at `path`, ready for traffic, and the replaced
/// incarnation `old` nowhere live.
async fn birth_held_everywhere(estate: &Estate, path: &str, birth: &(String, String), old: &str) -> Result<(), String> {
    let nodes = estate.nodes().await;
    for base in admin_bases(&nodes) {
        let Some(v) = try_get(&base, "/api/nodes").await else { return Err(format!("{base}: /api/nodes unanswered")) };
        let theirs = v["nodes"].as_array().cloned().unwrap_or_default();
        let at_path: Vec<String> = theirs.iter().filter(|n| n["name"] == path).map(|n| format!("{}/{}:{}", s(&n["node_id"]), s(&n["incarnation_id"]), s(&n["status"]))).collect();
        let holds = theirs.iter().any(|n| n["name"] == path && n["node_id"] == birth.0.as_str() && n["incarnation_id"] == birth.1.as_str() && n["status"] == "ready-for-traffic");
        let old_live: Vec<String> = theirs.iter().filter(|n| n["incarnation_id"] == old && !matches!(n["status"].as_str(), Some("dead" | "pending-reconnect"))).map(|n| format!("{}:{}", s(&n["name"]), s(&n["status"]))).collect();
        if !holds {
            return Err(format!("{base} holds {at_path:?} at {path}, not {}/{} ready-for-traffic", birth.0, birth.1));
        }
        if !old_live.is_empty() {
            return Err(format!("{base} still holds the replaced incarnation {old} live: {old_live:?}"));
        }
    }
    Ok(())
}

/// No live admin's view holds `path` live any more.
async fn gone_everywhere(estate: &Estate, path: &str) -> Result<(), String> {
    let nodes = estate.nodes().await;
    for base in admin_bases(&nodes) {
        let Some(v) = try_get(&base, "/api/nodes").await else { return Err(format!("{base}: /api/nodes unanswered")) };
        let live: Vec<String> = v["nodes"].as_array().into_iter().flatten().filter(|n| n["name"] == path && !matches!(n["status"].as_str(), Some("dead"))).map(|n| format!("{}:{}", s(&n["node_id"]), s(&n["status"]))).collect();
        if !live.is_empty() {
            return Err(format!("{base} still holds {path}: {live:?}"));
        }
    }
    Ok(())
}

/// Until the view of an admin that answers marks every one of `paths` unheard (`dead`, or also
/// `pending-reconnect` unless `true_offline`), within `within`. `Err` names what the views held.
///
/// `direct`: the paths are judged by the admins of their own mesh (the direct observers of a held
/// node), any one of them marking every path; otherwise by the admin that answers for the Fabric.
async fn until_unheard(estate: &mut Estate, known: &[String], paths: &[String], true_offline: bool, direct: bool, within: Duration) -> Result<Duration, String> {
    let began = Instant::now();
    let until = Instant::now() + within;
    let mut last = String::new();
    while Instant::now() < until {
        if relocate_control(estate, known).await {
            // Only the target mesh's primary tickles it, so only its view records `dead`: true
            // offline is read there; pending-reconnect from any admin that answers.
            let mut judge = estate.admin.clone();
            if true_offline {
                if let (Some(v), Some(mesh)) = (try_get(&estate.admin, "/api/nodes").await, paths.first().and_then(|p| p.split('.').next())) {
                    if let Some(base) = v["nodes"].as_array().into_iter().flatten().find(|n| n["kind"] == "node_admin" && n["mesh"] == mesh && n["is_primary"] == true && n["status"] == "ready-for-traffic").and_then(|n| n["admin_api_base"].as_str()) {
                        judge = base.to_string();
                    }
                }
            }
            let mut judges = vec![judge.clone()];
            if direct && !true_offline {
                if let (Some(v), Some(mesh)) = (try_get(&estate.admin, "/api/nodes").await, paths.first().and_then(|p| p.split('.').next())) {
                    let own: Vec<String> = v["nodes"].as_array().into_iter().flatten().filter(|n| n["kind"] == "node_admin" && n["mesh"] == mesh && n["status"] == "ready-for-traffic").filter_map(|n| n["admin_api_base"].as_str().map(String::from)).filter(|b| known.contains(b)).collect();
                    if !own.is_empty() {
                        judges = own;
                    }
                }
            }
            for judge in judges {
              if let Some(v) = try_get(&judge, "/api/nodes").await {
                let nodes = v["nodes"].as_array().cloned().unwrap_or_default();
                let unheard = |p: &String| {
                    nodes.iter().filter(|n| n["name"] == p.as_str()).all(|n| match n["status"].as_str() {
                        Some("dead") => true,
                        Some("pending-reconnect") => !true_offline,
                        _ => false,
                    })
                };
                if paths.iter().all(unheard) {
                    return Ok(began.elapsed());
                }
                last = paths.iter().map(|p| format!("{p}:{:?}", nodes.iter().filter(|n| n["name"] == p.as_str()).map(|n| s(&n["status"])).collect::<Vec<_>>())).collect::<Vec<_>>().join(" ");
              }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // What every admin that answers holds, so the refusal says who did not mark it.
    let mut views = Vec::new();
    for base in known.iter().chain(std::iter::once(&estate.admin)) {
        let held = match try_get(base, "/api/nodes").await {
            Some(v) => paths.iter().map(|p| format!("{p}:{:?}", v["nodes"].as_array().into_iter().flatten().filter(|n| n["name"] == p.as_str()).map(|n| s(&n["status"])).collect::<Vec<_>>())).collect::<Vec<_>>().join(" "),
            None => "unanswered".into(),
        };
        views.push(format!("{base} -> {held}"));
    }
    Err(format!("not marked unheard within {}s: {last}; every admin's view: {views:?}", within.as_secs()))
}

fn seats(nodes: &[Value]) -> (bool, String) {
    match seats_as_expected(nodes) {
        Ok(()) => (true, String::new()),
        Err(e) => (false, e),
    }
}

fn fabric_primaries(nodes: &[Value]) -> String {
    crate::elections::advertised_fabric_primaries(nodes).join(",")
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
    known: Vec<String>,
    build_id: String,
    executed: Vec<Action>,
    actions: BTreeMap<String, usize>,
    skipped: BTreeMap<String, usize>,
    rounds_log: Vec<Value>,
    silenced: BTreeMap<String, container_faults::Silenced>,
    started: Instant,
}

impl Driver {
    /// Bring the estate to the born shape through the rectifier and hold the born model.
    pub async fn new(mut estate: Estate, cfg: Config) -> Self {
        estate.set_seed(cfg.seed);
        let initial = cfg.model(&estate.owner.provider);
        let desired = json!({"fabric": "fabric1", "meshes": cfg.shape.iter().map(|(m, a, r)| json!({"name": m, "node_admin": a, "rpc_node": r})).collect::<Vec<_>>()});
        let (status, a) = estate.post("/api/build", &desired).await;
        assert_eq!(status, 202, "{a}");
        let build_id = s(&a["build_id"]);
        estate.await_build(&build_id, Duration::from_secs(120)).await;
        estate.settled_shape(&cfg.shape, Duration::from_secs(60)).await;
        let expected = expected_counts(&initial);
        wait_for("every live admin holds the same births", cfg.floor * 2 + cfg.backbone * 2 + Duration::from_secs(60), || converged_everywhere(&estate, &expected)).await;
        let known = admin_bases(&estate.nodes().await);
        let progress = Arc::new(Mutex::new(Progress { executed: Vec::new(), initial: Some(initial.clone()) }));
        let shared = Arc::new(Shared { book: Mutex::new(OpBook::default()), view: Mutex::new(View::default()), stop: AtomicBool::new(false) });
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
            build_id,
            executed: Vec::new(),
            actions: BTreeMap::new(),
            skipped: BTreeMap::new(),
            rounds_log: Vec::new(),
            silenced: BTreeMap::new(),
            started: Instant::now(),
        }
    }

    /// What a panicking run has done so far; read it after a join error.
    pub fn progress(&self) -> Arc<Mutex<Progress>> {
        self.progress.clone()
    }

    fn publish_view(&self, nodes: &[Value]) {
        let admin = nodes.iter().find(|n| n["kind"] == "node_admin" && n["is_fabric_primary"] == true && n["status"] == "ready-for-traffic").or_else(|| nodes.iter().find(|n| n["kind"] == "node_admin" && n["status"] == "ready-for-traffic")).and_then(|n| n["admin_api_base"].as_str().map(String::from));
        let rpcs = nodes.iter().filter(|n| n["kind"] == "rpc_node" && n["status"] == "ready-for-traffic").map(|n| (s(&n["name"]), s(&n["node_id"]))).collect();
        *self.shared.view.lock().unwrap() = View { admin, rpcs };
    }

    /// The live reason an action cannot run now, if any. The model knows no authority.
    fn live_refusal(&self, a: &Action, nodes: &[Value]) -> Option<String> {
        let node = |p: &str| nodes.iter().find(|n| n["name"] == p && n["status"] == "ready-for-traffic");
        let guarded = |p: &str, what: &str| -> Option<String> {
            let n = node(p)?;
            if n["is_fabric_primary"] == true {
                return Some(format!("{what}: {p} is the fabric-primary; it leaves only by a hand-off Build"));
            }
            if p == "mesh1.admin.1" && n["kind"] == "node_admin" && self.estate.bootstrap_pid().is_some() {
                return Some(format!("{what}: {p} is the harness's control address (the bootstrap node-admin)"));
            }
            None
        };
        // A replacement is a delete then a spawn, and a spawn lands at the lowest free ordinal of
        // its class: it replaces the node at the same path.name only when no lower ordinal is free.
        let lands_on_same_path = |p: &str| -> Option<String> {
            let (mesh, _) = self.model.node(p)?;
            let mut it = p.rsplitn(2, '.');
            let ordinal: u32 = it.next()?.parse().ok()?;
            let stem = it.next()?;
            let free = (1..ordinal).find(|i| !self.model.meshes[mesh].nodes.contains_key(&format!("{stem}.{i}")))?;
            Some(format!("replace: {stem}.{free} is free, so a spawn after deleting {p} lands there, not at {p}"))
        };
        match a {
            Action::Kill { node: p } | Action::Wedge { node: p } => guarded(p, kind(a)),
            Action::Replace { node: p } => guarded(p, kind(a)).or_else(|| lands_on_same_path(p)),
            Action::Shrink { node: p } => guarded(p, kind(a)),
            Action::Restart { node: p } => node(p).filter(|n| n["is_fabric_primary"] == true).map(|_| format!("restart: {p} is the fabric-primary; it leaves only by a hand-off Build")),
            Action::HandOff { mesh } => {
                let primary = nodes.iter().find(|n| n["mesh"] == mesh.as_str() && n["kind"] == "node_admin" && n["is_primary"] == true && n["status"] == "ready-for-traffic");
                match primary {
                    None => Some(format!("hand-off: {mesh} has no ready mesh primary")),
                    Some(p) => guarded(&s(&p["name"]), "hand-off").filter(|r| r.contains("bootstrap")).or_else(|| lands_on_same_path(&s(&p["name"]))),
                }
            }
            Action::Unheard { mesh } => {
                let fp = nodes.iter().any(|n| n["mesh"] == mesh.as_str() && n["is_fabric_primary"] == true);
                fp.then(|| format!("unheard: {mesh} holds the fabric-primary; the silenced set never holds it"))
            }
            Action::Grow { .. } | Action::Heal { .. } | Action::Proof(_) => None,
        }
    }

    /// The next executable action: drawn from the model, redrawn (and recorded) when the live
    /// estate refuses it.
    fn next_action(&mut self, nodes: &[Value]) -> Option<Action> {
        // A silence is held for its observation and healed by the next row: a fabric that lives
        // silenced is not the fabric the invariants describe.
        if let Some((mesh, _)) = self.model.meshes.iter().find(|(_, m)| m.unheard) {
            let a = Action::Heal { mesh: mesh.clone() };
            self.sched.record("heal-after-silence", &a.to_string());
            return Some(a);
        }
        for _ in 0..64 {
            let seed = self.rng.next();
            let drawn = generate(seed, &self.model, 1);
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
        let tr = tokio::spawn(traffic(self.shared.clone(), self.estate.probe_handle(), self.cfg.seed));
        self.sched.record("start", &format!("seed={} secs={}", self.cfg.seed, self.cfg.secs));
        let deadline = Instant::now() + Duration::from_secs(self.cfg.secs);
        let mut violations: Vec<Violation> = Vec::new();
        let mut round = 0usize;
        while Instant::now() < deadline && violations.is_empty() {
            round += 1;
            if !relocate_control(&mut self.estate, &self.known).await {
                violations.push(Violation { rule: "control-api-answers".into(), round, step: self.executed.len(), action: "-".into(), detail: "no admin answers its control API".into() });
                break;
            }
            let nodes = self.estate.nodes().await;
            self.known = admin_bases(&nodes);
            self.publish_view(&nodes);
            let Some(action) = self.next_action(&nodes) else {
                violations.push(Violation { rule: "a-legal-action-exists".into(), round, step: self.executed.len(), action: "-".into(), detail: format!("no drawn action is executable on the live estate; skipped: {:?}", self.skipped) });
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
            if let Err(b) = self.execute(&action, &nodes, round, &mut entry).await {
                violations.push(Violation { rule: b.rule.into(), round, step, action: action.to_string(), detail: b.detail });
            }
            entry["wall_ms"] = json!(started.elapsed().as_millis() as u64);
            self.rounds_log.push(entry);
            if violations.is_empty() {
                if let Err(b) = self.invariants(round).await {
                    violations.push(Violation { rule: b.rule.into(), round, step, action: action.to_string(), detail: b.detail });
                }
            }
        }
        // A mesh left silenced is healed before anything is judged or stopped.
        for (mesh, sil) in std::mem::take(&mut self.silenced) {
            if let Err(e) = sil.lift() {
                violations.push(Violation { rule: "silence-lifts".into(), round, step: self.executed.len().saturating_sub(1), action: format!("lift {mesh}"), detail: e });
            }
        }
        self.shared.stop.store(true, Ordering::Relaxed);
        let _ = tr.await;
        let (ledger, ledger_violations) = self.reconcile_ledger().await;
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
        };
        report.ledger.violations = ledger_violations.iter().map(|(_, d)| d.clone()).collect();
        // The first broken rule, shrunk. A ledger violation fails at the last executed row.
        let first = report.violations.first().cloned().or_else(|| {
            ledger_violations.first().map(|(rule, d)| Violation { rule: rule.clone(), round, step: self.executed.len().saturating_sub(1), action: self.executed.last().map(|a| a.to_string()).unwrap_or_default(), detail: d.clone() })
        });
        if let Some(v) = first {
            let involved: BTreeSet<String> = ledger_violations.iter().filter(|(r, _)| *r == v.rule).flat_map(|(_, d)| self.paths_named_in(d)).chain(self.executed.get(v.step).and_then(|a| node_of(a).map(String::from))).collect();
            let mut repro = reproduce(self.cfg.seed, &self.initial, &self.executed, &v.rule, v.step, &v.detail, involved);
            repro.live = report.rounds_log.iter().find(|e| e["step"] == v.step).cloned().unwrap_or(Value::Null);
            report.repro = Some(repro);
        }
        (report, self.estate)
    }

    /// The node paths a ledger violation's text names (a violation is about operations on stores;
    /// the book maps their target ids to paths).
    fn paths_named_in(&self, detail: &str) -> Vec<String> {
        let b = self.shared.book.lock().unwrap();
        let mut out = Vec::new();
        for (i, m) in b.meta.iter().enumerate() {
            if detail.contains(&format!("op#{i} ")) || detail.contains(&format!("op#{i}\n")) || detail.ends_with(&format!("op#{i}")) {
                out.push(m.target_path.clone());
            }
        }
        out
    }

    async fn execute(&mut self, action: &Action, nodes: &[Value], round: usize, entry: &mut Value) -> Result<(), Broken> {
        let expected = expected_counts(&self.model);
        let attempt_before = num(&self.estate.get(&format!("/api/builds?id={}", self.build_id)).await.1["attempt"]);
        let node_json = |p: &str| nodes.iter().find(|n| n["name"] == p).cloned();
        // The exact birth the action removes, and the Build whose receipts name the birth that
        // replaces it (with the attempt it must come after).
        let mut removed: Option<(String, String)> = None;
        let mut watch: Option<(String, u64)> = None;
        let mut retired_node_id: Option<String> = None;
        // A birth held while its runtime ran: it comes back as itself.
        let mut held: Option<(String, String)> = None;
        let mut gone: Option<String> = None;
        let mut no_build_for_drift = false;
        match action {
            Action::Grow { mesh, class } => {
                let kind = if *class == NodeClass::NodeAdmin { "node_admin" } else { "rpc_node" };
                let (st, b) = self.estate.post("/api/nodes/spawn", &json!({"mesh": mesh, "kind": kind})).await;
                entry["accepted"] = json!(st);
                if st != 202 {
                    return Err(broken("topology-action-accepted", format!("spawn {kind} in {mesh}: {st} {b}")));
                }
                self.await_build(&s(&b["build_id"]), entry).await?;
            }
            Action::Shrink { node } => {
                let n = node_json(node).ok_or_else(|| broken("model-matches-view", format!("{node} is not in the view")))?;
                let (st, b) = self.estate.delete(&format!("/api/nodes/{node}")).await;
                entry["accepted"] = json!(st);
                if st != 202 {
                    return Err(broken("topology-action-accepted", format!("delete {node}: {st} {b}")));
                }
                self.await_build(&s(&b["build_id"]), entry).await?;
                gone = Some(node.clone());
                retired_node_id = Some(s(&n["node_id"]));
            }
            Action::Restart { node } => {
                let n = node_json(node).ok_or_else(|| broken("model-matches-view", format!("{node} is not in the view")))?;
                let (st, b) = self.estate.post(&format!("/api/nodes/{node}/restart"), &json!({})).await;
                entry["accepted"] = json!(st);
                if st != 202 {
                    return Err(broken("topology-action-accepted", format!("restart {node}: {st} {b}")));
                }
                removed = Some((node.clone(), s(&n["incarnation_id"])));
                // The birth is read from the attempt this restart opened, never an earlier one's.
                watch = Some((s(&b["build_id"]), Estate::attempt_of(&b) - 1));
            }
            Action::Replace { node } => {
                let n = node_json(node).ok_or_else(|| broken("model-matches-view", format!("{node} is not in the view")))?;
                let (b2, old) = self.replace(&n, entry).await?;
                removed = Some((node.clone(), old));
                watch = Some((b2, 0));
                retired_node_id = Some(s(&n["node_id"]));
            }
            Action::HandOff { mesh } => {
                let n = nodes
                    .iter()
                    .find(|n| n["mesh"] == mesh.as_str() && n["kind"] == "node_admin" && n["is_primary"] == true && n["status"] == "ready-for-traffic")
                    .cloned()
                    .ok_or_else(|| broken("model-matches-view", format!("{mesh} has no ready mesh primary")))?;
                entry["handed_off_from"] = json!({"node": n["name"], "node_id": n["node_id"], "fabric_primary": n["is_fabric_primary"]});
                let (b2, old) = self.replace(&n, entry).await?;
                removed = Some((s(&n["name"]), old));
                watch = Some((b2, 0));
                retired_node_id = Some(s(&n["node_id"]));
            }
            Action::Kill { node } => {
                let n = node_json(node).ok_or_else(|| broken("model-matches-view", format!("{node} is not in the view")))?;
                removed = Some((node.clone(), s(&n["incarnation_id"])));
                watch = Some((self.build_id.clone(), attempt_before));
                retired_node_id = Some(s(&n["node_id"]));
                no_build_for_drift = true;
                self.kill(node, entry).await?;
            }
            Action::Wedge { node } => {
                let n = node_json(node).ok_or_else(|| broken("model-matches-view", format!("{node} is not in the view")))?;
                held = Some((node.clone(), s(&n["incarnation_id"])));
                self.wedge(&n, round, attempt_before, entry).await?;
            }
            Action::Unheard { mesh } => {
                self.unheard(mesh, nodes, entry).await?;
                // The silenced side stays silenced until its Heal row: nothing converges across it.
                return Ok(());
            }
            Action::Heal { mesh } => {
                let sil = self.silenced.remove(mesh).ok_or_else(|| broken("model-matches-estate", format!("{mesh} holds no silence to lift")))?;
                sil.lift().map_err(|e| broken("silence-lifts", e))?;
                entry["healed"] = json!(mesh);
            }
            Action::Proof(op) => {
                let (target, key) = match op {
                    ProofOp::Put { target, key, .. } | ProofOp::Cas { target, key, .. } | ProofOp::Delete { target, key } => (target, *key),
                };
                let n = node_json(target).filter(|n| n["status"] == "ready-for-traffic").ok_or_else(|| broken("model-matches-view", format!("{target} is not ready in the view")))?;
                let probe = self.estate.probe_handle();
                let admin = self.shared.view.lock().unwrap().admin.clone().unwrap_or_else(|| self.estate.admin.clone());
                let call = match op {
                    ProofOp::Put { value, .. } => Call { target_path: target.clone(), target_id: s(&n["node_id"]), kind: "put", key, value: Some(value.clone()), expected: None },
                    ProofOp::Cas { expected, value, .. } => Call { target_path: target.clone(), target_id: s(&n["node_id"]), kind: "cas", key, value: Some(value.clone()), expected: Some(expected.clone()) },
                    ProofOp::Delete { .. } => Call { target_path: target.clone(), target_id: s(&n["node_id"]), kind: "delete", key, value: None, expected: None },
                };
                issue(&self.shared, &probe, &admin, call).await;
                return Ok(());
            }
        }
        if !self.silenced.is_empty() {
            return Ok(());
        }
        // Converge: the birth sequence is confirmed before anything is judged: the operation's
        // Build (or the fault's attempt) completes; its AllocateIdentity names the new birth at the
        // path; every live admin holds exactly that birth ready and the replaced one nowhere; every
        // live admin holds the same births.
        let until = Instant::now() + CONVERGE;
        loop {
            if relocate_control(&mut self.estate, &self.known).await {
                let view = self.estate.nodes().await;
                self.publish_view(&view);
                let born = match (&watch, &removed) {
                    (Some((b, after)), Some((path, _))) => born_at(&self.estate, b, path, *after).await.map_err(|f| broken("build-completes", f))?,
                    _ => None,
                };
                let sequence_done = match (&removed, &born, &gone) {
                    (Some((path, old)), Some(birth), _) => {
                        entry["born"] = json!({"path": path, "node_id": birth.0, "incarnation": birth.1, "replaced": old});
                        match birth_held_everywhere(&self.estate, path, birth, old).await {
                            Ok(()) => true,
                            Err(why) => {
                                entry["waiting_on"] = json!(why);
                                false
                            }
                        }
                    }
                    (Some(_), None, _) => {
                        entry["waiting_on"] = json!("the Build has not completed the rebirth");
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
                    if let Some(agreeing) = converged_everywhere(&self.estate, &expected).await {
                        self.known = agreeing;
                        break;
                    }
                }
            }
            if Instant::now() >= until {
                let mut views = serde_json::Map::new();
                for base in self.known.iter().chain(std::iter::once(&self.estate.admin)) {
                    let v = match try_get(base, "/api/nodes").await {
                        Some(v) => {
                            let mut by: BTreeMap<String, Vec<String>> = BTreeMap::new();
                            for n in v["nodes"].as_array().into_iter().flatten() {
                                by.entry(s(&n["mesh"])).or_default().push(format!("{}:{}", s(&n["name"]), s(&n["status"])));
                            }
                            json!(by)
                        }
                        None => json!("unanswered"),
                    };
                    views.insert(base.clone(), v);
                }
                return Err(broken("birth-sequence-converges", format!("the birth sequence did not complete within {}s: {entry}; views {}", CONVERGE.as_secs(), Value::Object(views))));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        entry["converged_ms"] = json!(entry["wall_ms"].as_u64().unwrap_or(0));
        // A silenced birth whose runtime ran is held, never replaced: the same birth is back.
        if let Some((path, inc)) = &held {
            let now_nodes = self.estate.nodes().await;
            if !now_nodes.iter().any(|n| n["name"] == path.as_str() && n["incarnation_id"] == inc.as_str() && n["status"] == "ready-for-traffic") {
                return Err(broken("silence-is-not-death", format!("{path} came back as another birth than {inc}: silence replaced a running runtime")));
            }
        }
        // The replaced node's id names no node: a call to it is refused, never dispatched.
        if let Some(old) = &retired_node_id {
            let r = self.estate.probe(&["get", "--target", &format!("exact:{old}"), "--key", "9999"]);
            entry["retired_call"] = r["outcome"].clone();
            if r["outcome"] == "Reply" {
                return Err(broken("retired-node-is-not-dispatched", format!("a call to the retired node {old} was dispatched: {r}")));
            }
        }
        // A loss opens attempts of the accepted Build, never a Build.
        let (_, f) = self.estate.get("/api/fabric").await;
        let now_build = s(&f["build_id"]);
        if no_build_for_drift && now_build != self.build_id {
            return Err(broken("no-build-for-drift", format!("a Build was minted for drift: {} -> {now_build}", self.build_id)));
        }
        self.build_id = now_build;
        Ok(())
    }

    async fn await_build(&mut self, id: &str, entry: &mut Value) -> Result<(), Broken> {
        let until = Instant::now() + Duration::from_secs(120);
        loop {
            relocate_control(&mut self.estate, &self.known).await;
            let (_, b) = self.estate.get(&format!("/api/builds?id={id}")).await;
            match b["state"].as_str() {
                Some("complete") => {
                    entry["build"] = json!({"id": id, "attempt": b["attempt"]});
                    return Ok(());
                }
                Some("failed") => return Err(broken("build-completes", format!("Build {id} failed: {}", b["last_failure"]))),
                _ => {}
            }
            if Instant::now() >= until {
                return Err(broken("build-completes", format!("Build {id} did not complete within 120s: {b}")));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// DELETE `n`, await the Build, spawn the same kind into the freed ordinal. Returns the
    /// spawn's Build and the replaced incarnation.
    async fn replace(&mut self, n: &Value, entry: &mut Value) -> Result<(String, String), Broken> {
        let (path, mesh, kind) = (s(&n["name"]), s(&n["mesh"]), s(&n["kind"]));
        let (st, b) = self.estate.delete(&format!("/api/nodes/{path}")).await;
        entry["accepted"] = json!(st);
        if st != 202 {
            return Err(broken("topology-action-accepted", format!("delete {path}: {st} {b}")));
        }
        self.await_build(&s(&b["build_id"]), entry).await?;
        let (st2, b2) = self.estate.post("/api/nodes/spawn", &json!({"mesh": mesh, "kind": kind})).await;
        entry["regrow"] = json!(st2);
        if st2 != 202 {
            return Err(broken("topology-action-accepted", format!("spawn {kind} in {mesh}: {st2} {b2}")));
        }
        Ok((s(&b2["build_id"]), s(&n["incarnation_id"])))
    }

    async fn published(&self, node: &str) -> Result<ExactRuntime, Broken> {
        let dir = self.estate.data_dir_of(node).await;
        ExactRuntime::published(std::path::Path::new(&dir)).map_err(|r| broken("fault-names-an-exact-runtime", format!("{node}: no exact runtime published in {dir}: {r:?}")))
    }

    /// The exact runtime of `node` ends: SIGKILL of the published process (process provider), the
    /// container's interface removed (container provider: its transport dies with it and the
    /// runtime exits).
    async fn kill(&mut self, node: &str, entry: &mut Value) -> Result<(), Broken> {
        if self.estate.owner.provider == "container" {
            let u = container_faults::unplug(&self.estate, node).map_err(|e| broken("fault-applies", e))?;
            entry["unplugged"] = json!(u.id);
            return Ok(());
        }
        let rt = self.published(node).await?;
        let applied = rt.apply(Fault::Kill).map_err(|r| broken("fault-is-acknowledged", format!("kill {node}: {r:?}")))?;
        if !applied.exited {
            return Err(broken("fault-is-acknowledged", format!("kill {node}: the OS did not show the exit: {applied:?}")));
        }
        entry["killed"] = json!({"pid": rt.pid, "start": rt.start});
        Ok(())
    }

    /// Hold the node's exact runtime until the view stops calling it ready, judge the hold with
    /// the semantic wedge detector, release it. The same birth must come back.
    async fn wedge(&mut self, n: &Value, round: usize, attempt_before: u64, entry: &mut Value) -> Result<(), Broken> {
        let path = s(&n["name"]);
        let container = self.estate.owner.provider == "container";
        let is_rpc = n["kind"] == "rpc_node";
        let exact = format!("exact:{}", s(&n["node_id"]));
        let floor = self.cfg.floor;
        let key = (9000 + round).to_string();
        let probe = self.estate.probe_handle();
        let admin = self.estate.admin.clone();
        let run = |args: Vec<String>| {
            let (p, a) = (probe.clone(), admin.clone());
            async move { tokio::task::spawn_blocking(move || p.run(&a, &args.iter().map(String::as_str).collect::<Vec<_>>())).await.ok().and_then(|r| r.ok()).unwrap_or(Value::Null) }
        };
        if is_rpc {
            let put = run(vec!["put".into(), "--target".into(), exact.clone(), "--key".into(), key.clone(), "--value".into(), "before-hold".into()]).await;
            if put["outcome"] != "Reply" {
                return Err(broken("healthy-control-before-fault", format!("{path}: the control put before the hold: {put}")));
            }
        }
        let nodes0 = self.estate.nodes().await;
        let (seats_before, detail_before) = seats(&nodes0);
        let fp_before = fabric_primaries(&nodes0);
        let mut ev = Evidence::new(Family::SilentRuntime, format!("silent-runtime:{path}"));
        // The hold: SIGSTOP of the exact runtime, or `docker pause` of the exact container.
        let release: Box<dyn Fn() -> Result<Value, String> + Send>;
        let mut hold_rt: Option<ExactRuntime> = None;
        if container {
            let id = self.estate.container_of(&path).ok_or_else(|| broken("fault-names-an-exact-runtime", format!("{path}: no running container")))?;
            container_faults::pause(&id).map_err(|e| broken("fault-applies", e))?;
            ev.primitive = Some(Primitive { armed: true, ack: json!({"paused": id}) });
            let id2 = id.clone();
            release = Box::new(move || container_faults::unpause(&id2).map(|_| json!({"unpaused": id2})));
        } else {
            let rt = self.published(&path).await?;
            let stopped = rt.apply(Fault::Stop).map_err(|r| broken("fault-is-acknowledged", format!("stop {path}: {r:?}")))?;
            ev.primitive = Some(Primitive { armed: true, ack: serde_json::to_value(&stopped).unwrap_or(Value::Null) });
            let rt2 = rt.clone();
            release = Box::new(move || rt2.apply(Fault::Continue).map(|a| serde_json::to_value(a).unwrap_or(Value::Null)).map_err(|r| format!("{r:?}")));
            hold_rt = Some(rt);
        }
        // The container provider waits for the view to mark the held node unheard (pending-reconnect
        // or dead). A true-offline mark there was measured at 81 s after the hold against the 79.5 s
        // the tickle's two rounds derive (spans of the i143-2787-soak-container run): the coin is
        // drawn all the same, so the schedule is the same on both providers.
        let coin = self.rng.next() % 2 == 0;
        let true_offline = coin && !container;
        // Control never waits on the held node's own control API: a held admin answers nothing.
        let held_base = n["admin_api_base"].as_str().map(String::from);
        self.known.retain(|b| Some(b) != held_base.as_ref());
        if held_base.as_deref() == Some(self.estate.admin.as_str()) {
            if let Some(other) = self.known.first() {
                self.estate.admin = other.clone();
            }
        }
        entry["held_until"] = json!(if true_offline { "dead" } else { "pending-reconnect" });
        let known = self.known.clone();
        // On containers the view marked a held node unheard 29 s to 46 s after the hold (spans of
        // the i143-2787-soak-container runs); the container cell for the silence (#2784) observes
        // the same consequence within 90 s, and so does the soak there.
        let within = if container { Duration::from_secs(90) } else { self.cfg.unheard_within(true_offline, 1 + self.silenced.values().map(|x| x.members.len()).sum::<usize>()) };
        let marked = until_unheard(&mut self.estate, &known, &[path.clone()], true_offline, true, within).await;
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
        let held_at_last_read = hold_rt.as_ref().map(|r| proc_state(r.pid) == Some('T')).unwrap_or(true);
        let nodes_during = self.estate.nodes().await;
        let (seats_during, detail_during) = seats(&nodes_during);
        let during = nodes_during.iter().find(|x| x["name"] == path.as_str()).cloned();
        let attempt_during = num(&self.estate.get(&format!("/api/builds?id={}", self.build_id)).await.1["attempt"]);
        let alive = hold_rt.as_ref().map(|r| r.check().is_ok()).unwrap_or(true);
        let replaced = during.as_ref().is_none_or(|x| x["node_id"] != n["node_id"]);
        let released = release();
        if let Err(e) = marked {
            return Err(broken("fault-is-observed", format!("{path} held: {e}")));
        }
        let released = released.map_err(|e| broken("fault-is-released", format!("{path}: {e}")))?;
        ev.fault = Some(FaultAck { held: true, names: Some(json!({"node": path, "view_status": during.as_ref().map(|x| x["status"].clone())})), held_at_last_read });
        ev.progress = Some(WedgeProgress { reads, complete_while_held: outcomes.iter().any(|o| o["outcome"] == "Reply") });
        ev.routing = Some(Routing { expected_routable: false, observed_routable: outcomes.iter().any(|o| o["outcome"] == "Reply"), observed: format!("view {}; calls {}", during.as_ref().map(|x| s(&x["status"])).unwrap_or_default(), outcomes.iter().map(|o| s(&o["outcome"])).collect::<Vec<_>>().join(",")) });
        ev.release = Some(Release { acked: true, hold_ended: hold_rt.as_ref().map(|r| proc_state(r.pid) != Some('T')).unwrap_or(true) });
        let inc = s(&n["incarnation_id"]);
        let back = {
            let est = &self.estate;
            let back = wait_for_opt(&format!("{path} back as its own birth"), floor * 2 + Duration::from_secs(60), || async {
                est.node_opt(&path).await.filter(|x| x["incarnation_id"] == inc.as_str() && x["status"] == "ready-for-traffic")
            })
            .await;
            back.ok_or_else(|| broken("silence-is-not-death", format!("{path} did not come back as {inc} after the release")))?
        };
        let kept = if is_rpc { run(vec!["get".into(), "--target".into(), exact.clone(), "--key".into(), key.clone()]).await } else { Value::Null };
        ev.recovery = Some(Recovery { work_complete: if is_rpc { kept["outcome"] == "Reply" } else { true }, marker_after: json!({"proc_state": hold_rt.as_ref().and_then(|r| proc_state(r.pid)).map(String::from), "outcome": kept["outcome"]}).to_string() });
        let nodes_after = self.estate.nodes().await;
        let (seats_after, detail_after) = seats(&nodes_after);
        ev.control = Some(Control {
            seats_as_expected: [seats_before, seats_during, seats_after],
            seats_detail: [detail_before, detail_during, detail_after.clone()].join(" | "),
            fabric_primary: [fp_before, fabric_primaries(&nodes_during), fabric_primaries(&nodes_after)],
            authority_may_move: false,
            incarnation: [inc.clone(), during.as_ref().map(|x| s(&x["incarnation_id"])).unwrap_or_default(), s(&back["incarnation_id"])],
            attempts: [attempt_before, attempt_during],
            exact_runtime_alive: alive,
            replaced_during: replaced,
        });
        let mut rec = Reconciliation::default();
        if is_rpc {
            rec.check("the value stored before the hold is served after it", kept["reply"]["result"] == json!({"found": true, "value": "before-hold"}), kept.to_string());
            rec.check("the same birth serves it", kept["reply"]["incarnation_id"] == n["incarnation_id"] && kept["reply"]["executing_node"] == n["node_id"], kept.to_string());
        }
        rec.check("the Build gained no attempt", num(&self.estate.get(&format!("/api/builds?id={}", self.build_id)).await.1["attempt"]) == attempt_before, format!("attempt {attempt_before}"));
        rec.check("the seats equal the public candidates' after the release", seats_after, detail_after);
        ev.reconciliation = Some(rec);
        entry["wedge"] = json!({"released": released, "view_while_held": during.as_ref().map(|x| x["status"].clone())});
        match judge(&ev) {
            Ok(v) => {
                entry["wedge"]["verdict"] = serde_json::to_value(&v).unwrap_or(Value::Null);
                Ok(())
            }
            Err(r) => Err(broken("wedge-detector-judges-the-hold", format!("{path}: {r}"))),
        }
    }

    /// Silence a mesh against the rest of the estate (container provider) until its Heal row.
    async fn unheard(&mut self, mesh: &str, nodes: &[Value], entry: &mut Value) -> Result<(), Broken> {
        let members: Vec<String> = nodes.iter().filter(|n| n["mesh"] == mesh && n["status"] == "ready-for-traffic").map(|n| s(&n["name"])).collect();
        let fp = nodes.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).unwrap_or_default();
        let sil = container_faults::silence(&self.estate, &members, &fp).map_err(|e| broken("fault-applies", e))?;
        if !sil.active().map_err(|e| broken("fault-is-observed", e))? {
            return Err(broken("fault-is-observed", format!("{mesh}: the silence chain is not installed on every member")));
        }
        // Seen from the other mesh: control is read through its admins.
        let others: Vec<String> = self.known.iter().filter(|b| !nodes.iter().any(|n| n["mesh"] == mesh && n["admin_api_base"] == b.as_str())).cloned().collect();
        // The window the container cell for this fault (#2784) observes the same consequence in; the
        // soak measures the time it took (`unheard_after_ms`).
        let within = Duration::from_secs(90);
        self.silenced.insert(mesh.to_string(), sil);
        let after = until_unheard(&mut self.estate, &others, &members, false, false, within).await.map_err(|e| broken("fault-is-observed", format!("{mesh} silenced: {e}")))?;
        entry["silenced"] = json!({"mesh": mesh, "members": members, "unheard_after_ms": after.as_millis() as u64});
        Ok(())
    }

    /// The invariants that hold at every converged point; a violation names itself.
    async fn invariants(&mut self, round: usize) -> Result<(), Broken> {
        if !self.silenced.is_empty() {
            return Ok(());
        }
        let nodes = self.estate.nodes().await;
        let mut per_path: BTreeMap<String, usize> = BTreeMap::new();
        for n in nodes.iter().filter(|n| !matches!(n["status"].as_str(), Some("dead" | "pending-reconnect"))) {
            *per_path.entry(s(&n["name"])).or_default() += 1;
        }
        if let Some((p, c)) = per_path.iter().find(|(_, c)| **c > 1) {
            return Err(broken("one-current-birth-per-path", format!("round {round}: {c} current births at {p}")));
        }
        let mut writers: BTreeSet<String> = BTreeSet::new();
        for base in admin_bases(&nodes) {
            let Some(f) = try_get(&base, "/api/fabric").await else {
                return Err(broken("live-admin-answers", format!("round {round}: {base} (named ready-for-traffic in the view) does not answer /api/fabric")));
            };
            if let Some(p) = f["fabric_primary"].as_str().filter(|p| !p.is_empty()) {
                writers.insert(p.to_string());
            }
            if f["build_id"].as_str() != Some(self.build_id.as_str()) {
                return Err(broken("fabric-build-id-held", format!("round {round}: {base} holds Fabric.build_id {} not {}", f["build_id"], self.build_id)));
            }
            let heard: BTreeSet<String> = f["meshes"].as_array().into_iter().flatten().filter(|m| m["status"] == "ready-for-traffic").map(|m| s(&m["name"])).collect();
            for (mesh, _, _) in &self.cfg.shape {
                if !heard.contains(*mesh) {
                    return Err(broken("no-peer-mesh-permanently-unheard", format!("round {round}: {base} does not hold {mesh} ready-for-traffic")));
                }
            }
        }
        if writers.len() != 1 {
            return Err(broken("one-fabric-primary", format!("round {round}: fabric primaries named across live admins: {writers:?}")));
        }
        let (ok, why) = seats(&nodes);
        if !ok {
            return Err(broken("seats-equal-the-public-candidates", format!("round {round}: {why}")));
        }
        for n in nodes.iter().filter(|n| n["kind"] == "rpc_node" && n["status"] == "ready-for-traffic") {
            let r = self.estate.probe(&["get", "--target", &format!("exact:{}", s(&n["node_id"])), "--key", "9998"]);
            if r["outcome"] != "Reply" || r["reply"]["incarnation_id"] != n["incarnation_id"] {
                return Err(broken("every-rpc-node-reachable-on-its-current-birth", format!("round {round}: {} not reachable on its current birth: {r}", n["name"])));
            }
        }
        Ok(())
    }

    /// The ledger's count algebra over every operation and its state algebra against every store
    /// that survived to the end. Returns the account and every violation with the invariant it
    /// fails.
    async fn reconcile_ledger(&mut self) -> (LedgerReport, Vec<(String, String)>) {
        let mut found: Vec<(String, String)> = Vec::new();
        let nodes = if relocate_control(&mut self.estate, &self.known).await { self.estate.nodes().await } else { Vec::new() };
        let alive: BTreeMap<String, String> = nodes.iter().filter(|n| n["kind"] == "rpc_node" && n["status"] == "ready-for-traffic").map(|n| (s(&n["node_id"]), s(&n["name"]))).collect();
        let (meta, excluded, probe_failures) = {
            let b = self.shared.book.lock().unwrap();
            (b.meta.clone(), b.excluded.clone(), b.probe_failures.clone())
        };
        let mut report = LedgerReport { probe_failures, keys_excluded_from_state: excluded.len(), ..Default::default() };
        let (counts, global) = {
            let b = self.shared.book.lock().unwrap();
            (b.ledger.buckets(), b.ledger.reconcile())
        };
        report.issued = meta.len();
        report.buckets = counts.iter().map(|(k, v)| (format!("{k:?}"), *v)).collect();
        {
            let b = self.shared.book.lock().unwrap();
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
        // The state algebra, store by store.
        let ids: BTreeSet<String> = meta.iter().map(|m| m.target_id.clone()).collect();
        let probe = self.estate.probe_handle();
        let admin = self.estate.admin.clone();
        for id in ids {
            let Some(path) = alive.get(&id) else {
                report.ops_on_retired_births += meta.iter().filter(|m| m.target_id == id).count();
                continue;
            };
            let keys: BTreeSet<u64> = meta.iter().filter(|m| m.target_id == id).map(|m| m.key).filter(|k| !excluded.contains(&(id.clone(), *k))).collect();
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
                found.push((crate::ledger::DETECTS_LOST_APPLIED_MUTATION.to_string(), format!("{path} ({id}): the final store could not be read for keys {:?}", unreadable.iter().map(|(k, o)| format!("{k}: {o}")).collect::<Vec<_>>())));
                continue;
            }
            let sub = {
                let b = self.shared.book.lock().unwrap();
                b.ledger.select(|o| o.target_node_id == id && !o.mutation.as_ref().is_some_and(|m| m.key.parse::<u64>().is_ok_and(|k| excluded.contains(&(id.clone(), k)))))
            };
            match sub.reconcile_state(&state) {
                Ok(r) => {
                    report.stores_reconciled += 1;
                    report.applied_mutations += r.applied_mutations;
                    report.indeterminate.extend(r.indeterminate_mutations.iter().map(|(o, a)| (o.0, *a)));
                }
                Err(refusal) => {
                    for v in &refusal.0 {
                        found.push((v.invariant().to_string(), format!("{path} ({id}): {v}")));
                    }
                }
            }
        }
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
pub fn reproduce(seed: u64, initial: &Model, executed: &[Action], rule: &str, step: usize, detail: &str, involved: BTreeSet<String>) -> Repro {
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
    let involved: BTreeSet<String> = progress.executed.get(step).and_then(|a| node_of(a).map(String::from)).into_iter().collect();
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
        let named: Vec<&Action> = seq[..=at].iter().filter(|a| node_of(a) == Some(node.as_str())).collect();
        let kept: Vec<&Action> = repro.minimized.iter().filter(|a| node_of(a) == Some(node.as_str())).collect();
        assert_eq!(named, kept, "every row naming the involved node is kept");
    }
}
