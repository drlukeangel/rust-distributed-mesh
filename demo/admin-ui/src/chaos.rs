//! The Chaos tab's door onto the chaos kit (`rafka-chaos`).
//!
//! A fault is aimed at one node's EXACT runtime, read from the runtime record the node's birth
//! published in its data dir, and answers with the kit's typed outcome: applied (with the OS
//! observation that acknowledged it) or refused (with the reason, nothing signalled). A network
//! cut of one node or of a whole peer mesh drops UDP between its transport ports and every other
//! node's, until healed.
//!
//! The fabric-primary is never a target: a fault, or a cut of a mesh that holds it, is refused by
//! name before the kit is asked. There is no action here that touches more than one node's
//! runtime, and none that takes every node-admin down.

use rafka_chaos::netfault::{estate_processes, Partition};
use rafka_chaos::process_faults::{ExactRuntime, Fault};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// What a fault needs to know of one node, from node-admin's view of it.
#[derive(Debug, Clone)]
pub struct NodeFacts {
    /// The node's `path.name`.
    pub name: String,
    /// The mesh it belongs to.
    pub mesh: String,
    /// It holds the fabric-primary seat.
    pub is_fabric_primary: bool,
    /// Its birth's data dir, where the runtime record lives.
    pub data_dir: Option<String>,
    /// The UDP port its mesh transport is bound to.
    pub transport_port: Option<u16>,
}

/// One fault or cut, with its typed outcome.
#[derive(Debug, Clone, Serialize)]
pub struct FaultRecord {
    /// Order of application.
    pub id: u64,
    /// When (ms since the Unix epoch).
    pub ts_ms: u64,
    /// The node or mesh aimed at.
    pub target: String,
    /// `kill`, `stop`, `continue`, `cut-node`, `cut-mesh` or `heal`.
    pub action: String,
    /// `applied` or `refused`.
    pub outcome: &'static str,
    /// The kit's observation, or the refusal and its reason.
    pub detail: Value,
}

/// A network cut in force.
#[derive(Debug, Clone, Serialize)]
pub struct Cut {
    /// Its id (the heal names it).
    pub id: u64,
    /// `node` or `mesh`.
    pub scope: String,
    /// The node or mesh cut from the rest.
    pub target: String,
    /// The UDP ports on the cut side, as of the last time the cut re-read its members.
    pub ports: Vec<u16>,
    /// The nodes on the cut side, as of the last time the cut re-read its members.
    pub members: Vec<String>,
    /// When it was made.
    pub since_ms: u64,
}

/// The faults applied through this UI and the cuts still in force.
#[derive(Default)]
pub struct Chaos {
    /// The estate's folder: every process launched under it is a member of its mesh from its
    /// launch, whether or not any admin has heard of it yet.
    estate_root: Option<PathBuf>,
    log: Mutex<Vec<FaultRecord>>,
    cuts: std::sync::Arc<Mutex<Vec<(Cut, Partition)>>>,
    next: std::sync::atomic::AtomicU64,
}

/// The refusal name for a fault aimed at the fabric-primary.
pub const FABRIC_PRIMARY_REFUSAL: &str = "fabric-primary-is-never-a-target";

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// The nodes of each side of a cut and their UDP ports. A node's ports are the ones node-admin
/// publishes for it joined with the ones the OS shows its process holding (found under
/// `estate_root` by the data dir it was launched with), so a node that no admin has heard of yet
/// is on its side from the moment it holds a socket.
fn sides(estate_root: Option<&Path>, nodes: &[NodeFacts], scope: &str, target: &str) -> (BTreeMap<String, BTreeSet<u16>>, BTreeMap<String, BTreeSet<u16>>) {
    let mut ports: BTreeMap<String, (String, BTreeSet<u16>)> = BTreeMap::new();
    for n in nodes {
        let e = ports.entry(n.name.clone()).or_insert_with(|| (n.mesh.clone(), BTreeSet::new()));
        e.1.extend(n.transport_port);
    }
    if let Some(root) = estate_root {
        for p in estate_processes(root) {
            let e = ports.entry(p.node()).or_insert_with(|| (p.mesh(), BTreeSet::new()));
            e.1.extend(p.udp_ports.iter().copied());
        }
    }
    let (mut a, mut b) = (BTreeMap::new(), BTreeMap::new());
    for (name, (mesh, ps)) in ports {
        let inside = if scope == "mesh" { mesh == target } else { name == target };
        if inside { a.insert(name, ps) } else { b.insert(name, ps) };
    }
    (a, b)
}

fn flat(m: &BTreeMap<String, BTreeSet<u16>>) -> Vec<u16> {
    m.values().flatten().copied().collect::<BTreeSet<_>>().into_iter().collect()
}

impl Chaos {
    /// A kit door for the estate rooted at `root`.
    pub fn with_estate_root(root: PathBuf) -> Self {
        Chaos { estate_root: Some(root), ..Chaos::default() }
    }

    fn record(&self, target: &str, action: &str, outcome: &'static str, detail: Value) -> FaultRecord {
        let r = FaultRecord { id: self.next.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1, ts_ms: now_ms(), target: target.into(), action: action.into(), outcome, detail };
        let mut log = self.log.lock().unwrap();
        log.push(r.clone());
        if log.len() > 200 {
            log.remove(0);
        }
        r
    }

    /// The faults applied, newest first.
    pub fn faults(&self) -> Vec<FaultRecord> {
        self.log.lock().unwrap().iter().rev().cloned().collect()
    }

    /// The cuts in force.
    pub fn cuts(&self) -> Vec<Cut> {
        self.cuts.lock().unwrap().iter().map(|(c, _)| c.clone()).collect()
    }

    /// Apply `action` (`kill`, `stop` or `continue`) to the exact runtime of `target`. The record
    /// is `refused` when the node is the fabric-primary, unknown, or the kit refuses.
    pub async fn fault(&self, nodes: &[NodeFacts], target: &str, action: &str) -> FaultRecord {
        let fault = match action {
            "kill" => Fault::Kill,
            "stop" => Fault::Stop,
            "continue" => Fault::Continue,
            other => return self.record(target, other, "refused", json!({"refusal": "unknown-action", "reason": format!("`{other}` is not kill, stop or continue")})),
        };
        let Some(node) = nodes.iter().find(|n| n.name == target) else {
            return self.record(target, action, "refused", json!({"refusal": "unknown-node", "reason": format!("node-admin's view holds no node `{target}`")}));
        };
        if node.is_fabric_primary {
            return self.record(target, action, "refused", json!({"refusal": FABRIC_PRIMARY_REFUSAL, "reason": format!("{target} holds the fabric-primary seat; a fault is never aimed at it")}));
        }
        // Only the admin that launched a node publishes its data dir: the OS shows the rest.
        let found = || self.estate_root.as_deref().and_then(|r| estate_processes(r).into_iter().find(|p| p.node() == target)).map(|p| p.data_dir.display().to_string());
        let Some(dir) = node.data_dir.clone().or_else(found) else {
            return self.record(target, action, "refused", json!({"refusal": "no-data-dir", "reason": format!("node-admin publishes no data dir for {target}, so its runtime record cannot be read")}));
        };
        let applied = tokio::task::spawn_blocking(move || ExactRuntime::published(Path::new(&dir)).and_then(|rt| rt.apply(fault))).await;
        match applied {
            Ok(Ok(a)) => self.record(target, action, "applied", serde_json::to_value(&a).unwrap_or(Value::Null)),
            Ok(Err(r)) => self.record(target, action, "refused", serde_json::to_value(&r).unwrap_or(Value::Null)),
            Err(e) => self.record(target, action, "refused", json!({"refusal": "fault-task-failed", "reason": e.to_string()})),
        }
    }

    /// Cut `target` (a node, or with `scope == "mesh"` every node of that mesh) from the rest of
    /// the fabric. A mesh that holds the fabric-primary is never cut.
    pub async fn cut(&self, nodes: &[NodeFacts], scope: &str, target: &str) -> FaultRecord {
        let action = format!("cut-{scope}");
        if !matches!(scope, "node" | "mesh") {
            return self.record(target, &action, "refused", json!({"refusal": "unknown-scope", "reason": format!("`{scope}` is not node or mesh")}));
        }
        let (a, b) = sides(self.estate_root.as_deref(), nodes, scope, target);
        if a.is_empty() {
            return self.record(target, &action, "refused", json!({"refusal": "unknown-target", "reason": format!("node-admin's view holds no {scope} `{target}`")}));
        }
        if let Some(fp) = nodes.iter().filter(|n| a.contains_key(&n.name)).find(|n| n.is_fabric_primary) {
            return self.record(target, &action, "refused", json!({"refusal": FABRIC_PRIMARY_REFUSAL, "reason": format!("{} holds the fabric-primary seat; the side holding it is never cut", fp.name)}));
        }
        let (pa, pb) = (flat(&a), flat(&b));
        if pa.is_empty() || pb.is_empty() {
            return self.record(target, &action, "refused", json!({"refusal": "no-transport-ports", "reason": "neither node-admin nor the OS shows a UDP port for one side of the cut"}));
        }
        let started = {
            let (x, y) = (pa.clone(), pb.clone());
            tokio::task::spawn_blocking(move || Partition::start(&x, &y)).await
        };
        match started {
            Ok(Ok(partition)) => {
                let cut = Cut { id: self.next.load(std::sync::atomic::Ordering::SeqCst) + 1, scope: scope.into(), target: target.into(), ports: pa.clone(), members: a.keys().cloned().collect(), since_ms: now_ms() };
                self.cuts.lock().unwrap().push((cut.clone(), partition));
                self.record(target, &action, "applied", json!({"cut": cut.id, "ports": cut.ports, "members": cut.members}))
            }
            Ok(Err(why)) => self.record(target, &action, "refused", json!({"refusal": "network-cut-unavailable", "reason": why})),
            Err(e) => self.record(target, &action, "refused", json!({"refusal": "fault-task-failed", "reason": e.to_string()})),
        }
    }

    /// Hold every cut in force over its side as the fabric is now: a node launched after the cut
    /// began (a member of the cut mesh, or of the rest) joins its side and is cut like the others.
    /// Returns the members that joined, per cut id.
    pub async fn hold_cuts(&self, nodes: &[NodeFacts]) -> Vec<(u64, Vec<String>)> {
        let cuts = self.cuts.clone();
        let root = self.estate_root.clone();
        let nodes = nodes.to_vec();
        tokio::task::spawn_blocking(move || {
            let mut joined = Vec::new();
            let mut cuts = cuts.lock().unwrap();
            for (cut, partition) in cuts.iter_mut() {
                let (a, b) = sides(root.as_deref(), &nodes, &cut.scope, &cut.target);
                let before: BTreeSet<String> = cut.members.iter().cloned().collect();
                let new: Vec<String> = a.keys().filter(|k| !before.contains(*k)).cloned().collect();
                match partition.extend(&flat(&a), &flat(&b)) {
                    Ok(0) => {}
                    Ok(_) => {
                        cut.ports = flat(&a);
                        cut.members = a.keys().cloned().collect();
                        if !new.is_empty() {
                            joined.push((cut.id, new));
                        }
                    }
                    Err(e) => tracing::warn!(cut = cut.id, "holding the cut over new members failed: {e}"),
                }
            }
            joined
        })
        .await
        .unwrap_or_default()
    }

    /// Lift the cut `id`.
    pub async fn heal(&self, id: u64) -> FaultRecord {
        let taken = {
            let mut cuts = self.cuts.lock().unwrap();
            cuts.iter().position(|(c, _)| c.id == id).map(|i| cuts.remove(i))
        };
        match taken {
            Some((cut, partition)) => {
                let _ = tokio::task::spawn_blocking(move || drop(partition)).await;
                self.record(&cut.target, "heal", "applied", json!({"cut": id}))
            }
            None => self.record(&format!("cut {id}"), "heal", "refused", json!({"refusal": "no-such-cut", "reason": format!("no network cut {id} is in force")})),
        }
    }
}
