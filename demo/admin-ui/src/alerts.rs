//! The Alerts tab: what an operator would want to be told, kept as a list.
//!
//! - CPU and RAM: a node whose load, as the mesh digest published it, rises above
//!   `RDM_CPU_ALERT_THRESHOLD` (cores, default 0.10) or `RDM_RAM_ALERT_THRESHOLD_GB` (default 0.5)
//!   raises one warn alert on the crossing, and one info alert when it falls back.
//! - Node state: a node going `pending…`, `dead` or out of node-admin's view, and a node that was
//!   unwell coming back `ready-for-traffic`.
//! - Chaos: every fault, cut and heal this UI applied, with its typed outcome.
//!
//! [`Alerts::observe`] judges one reading of the node list against the previous one; it holds the
//! state those judgements need and nothing else.

use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;

/// One alert.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Alert {
    /// Order raised.
    pub id: u64,
    /// When (ms since the Unix epoch).
    pub ts_ms: u64,
    /// `info`, `warn` or `error`.
    pub severity: &'static str,
    /// `cpu`, `ram`, `node-state`, `chaos`.
    pub kind: &'static str,
    /// What it says.
    pub message: String,
    /// The node it concerns.
    pub node: Option<String>,
    /// The mesh it concerns.
    pub mesh: Option<String>,
}

/// The thresholds, read once per observation.
#[derive(Debug, Clone, Copy)]
pub struct Thresholds {
    /// Cores.
    pub cpu_cores: f64,
    /// Gigabytes.
    pub ram_gb: f64,
}

impl Thresholds {
    /// From `RDM_CPU_ALERT_THRESHOLD` and `RDM_RAM_ALERT_THRESHOLD_GB`; a value that is set and does
    /// not parse is reported by the caller's log, never silently replaced.
    pub fn from_env() -> Self {
        let read = |var: &str, default: f64| std::env::var(var).ok().and_then(|v| v.trim().parse().ok()).unwrap_or(default);
        Thresholds { cpu_cores: read("RDM_CPU_ALERT_THRESHOLD", 0.10), ram_gb: read("RDM_RAM_ALERT_THRESHOLD_GB", 0.5) }
    }
}

/// The alert list and what judging the next reading needs.
#[derive(Default)]
pub struct Alerts {
    list: Vec<Alert>,
    next: u64,
    status: HashMap<String, String>,
    over_cpu: HashMap<String, bool>,
    over_ram: HashMap<String, bool>,
    seeded: bool,
}

const KEEP: usize = 500;

fn well(status: &str) -> bool {
    status == "ready-for-traffic"
}

impl Alerts {
    /// Raise one alert.
    pub fn raise(&mut self, now_ms: u64, severity: &'static str, kind: &'static str, message: String, node: Option<&str>, mesh: Option<&str>) {
        self.next += 1;
        self.list.push(Alert { id: self.next, ts_ms: now_ms, severity, kind, message, node: node.map(String::from), mesh: mesh.map(String::from) });
        if self.list.len() > KEEP {
            self.list.remove(0);
        }
    }

    /// The alerts, newest first.
    pub fn newest(&self) -> Vec<Alert> {
        self.list.iter().rev().cloned().collect()
    }

    /// Judge one reading: `nodes` are the topology view's nodes (name, mesh, status, load).
    pub fn observe(&mut self, now_ms: u64, nodes: &[Value], t: Thresholds) {
        let mut present = std::collections::HashSet::new();
        for n in nodes {
            let (Some(name), Some(status)) = (n["name"].as_str(), n["status"].as_str()) else { continue };
            let mesh = n["mesh"].as_str();
            present.insert(name.to_string());
            if self.seeded {
                match self.status.get(name).map(String::as_str) {
                    Some(prev) if prev != status => {
                        let (sev, what) = match (well(prev), well(status)) {
                            (_, true) => ("info", format!("{name} is back: {prev} -> {status}")),
                            (_, false) if status.contains("dead") => ("error", format!("{name} went {status} (was {prev})")),
                            _ => ("warn", format!("{name} went {status} (was {prev})")),
                        };
                        self.raise(now_ms, sev, "node-state", what, Some(name), mesh);
                    }
                    None => self.raise(now_ms, "info", "node-state", format!("{name} joined the view as {status}"), Some(name), mesh),
                    _ => {}
                }
            }
            self.status.insert(name.to_string(), status.to_string());
            if let Some(load) = n.get("load").filter(|l| l.is_object()) {
                let cpu = load["cpu_used_millicores"].as_f64().unwrap_or(0.0) / 1000.0;
                let ram = load["ram_used_bytes"].as_f64().unwrap_or(0.0) / 1e9;
                self.threshold("cpu", name, mesh, cpu > t.cpu_cores, format!("{name} CPU {cpu:.2} cores is above {:.2}", t.cpu_cores), format!("{name} CPU {cpu:.2} cores is back under {:.2}", t.cpu_cores), now_ms);
                self.threshold("ram", name, mesh, ram > t.ram_gb, format!("{name} RAM {ram:.2} GB is above {:.2}", t.ram_gb), format!("{name} RAM {ram:.2} GB is back under {:.2}", t.ram_gb), now_ms);
            }
        }
        if self.seeded {
            let gone: Vec<String> = self.status.keys().filter(|k| !present.contains(*k)).cloned().collect();
            for g in gone {
                self.status.remove(&g);
                self.raise(now_ms, "warn", "node-state", format!("{g} left node-admin's view"), Some(&g), g.split('.').next());
            }
        }
        self.seeded = true;
    }

    #[allow(clippy::too_many_arguments)]
    fn threshold(&mut self, kind: &'static str, name: &str, mesh: Option<&str>, over: bool, up: String, down: String, now_ms: u64) {
        let map = if kind == "cpu" { &mut self.over_cpu } else { &mut self.over_ram };
        let was = map.insert(name.to_string(), over).unwrap_or(false);
        match (was, over) {
            (false, true) => self.raise(now_ms, "warn", kind, up, Some(name), mesh),
            (true, false) => self.raise(now_ms, "info", kind, down, Some(name), mesh),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const T: Thresholds = Thresholds { cpu_cores: 0.10, ram_gb: 0.5 };

    fn n(name: &str, status: &str, cpu_milli: u64) -> Value {
        json!({"name": name, "mesh": "mesh1", "status": status, "load": {"cpu_used_millicores": cpu_milli, "ram_used_bytes": 1_000_000}})
    }

    #[test]
    fn a_node_going_unwell_and_back_and_a_cpu_crossing_each_raise_one_alert() {
        let mut a = Alerts::default();
        a.observe(1, &[n("mesh1.broker.1", "ready-for-traffic", 20)], T);
        assert!(a.newest().is_empty(), "the first reading is the baseline");
        a.observe(2, &[n("mesh1.broker.1", "pending-reconnect", 20)], T);
        a.observe(3, &[n("mesh1.broker.1", "pending-reconnect", 20)], T);
        a.observe(4, &[n("mesh1.broker.1", "ready-for-traffic", 500)], T);
        a.observe(5, &[n("mesh1.broker.1", "ready-for-traffic", 500)], T);
        a.observe(6, &[], T);
        let kinds: Vec<(&str, &str)> = a.newest().iter().rev().map(|x| (x.kind, x.severity)).collect();
        assert_eq!(kinds, [("node-state", "warn"), ("node-state", "info"), ("cpu", "warn"), ("node-state", "warn")]);
    }
}
