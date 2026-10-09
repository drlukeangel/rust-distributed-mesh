//! Link faults for the process estate, built on the loopback UDP cuts of [`crate::netfault`]:
//! a partition of an arbitrary node set against the rest, a link that flaps on a schedule, and
//! inbound UDP dropped to one node only.
//!
//! Every target is a node NAME resolved to its published UDP port in the admin's node view, and
//! refused by name when it is unknown, empty, the whole estate, or one the caller protects (a
//! fabric-primary). Where the host cannot run `iptables` (no root, no `sudo -n`) every fault
//! answers [`LinkRefusal::Unavailable`] with the reason: a test skips by that name, or fails with
//! it when `RDM_REQUIRE_NETFAULT=1`.
//!
//! Each application is one span: `rdm.testkit.fault.update.via-partition-subset`,
//! `rdm.testkit.fault.update.via-link-flap` or `rdm.testkit.fault.update.via-inbound-drop`, with
//! the typed outcome; its heal is `rdm.testkit.fault.remove.via-heal` carrying the fault.

use crate::netfault::Partition;
use serde::Serialize;
use serde_json::Value;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// The most flap cycles one call runs: a flap is bounded.
pub const MAX_FLAP_CYCLES: u32 = 20;
/// The longest one phase (cut or healed) of a flap lasts.
pub const MAX_FLAP_PHASE: Duration = Duration::from_secs(60);

static CHAINS: AtomicUsize = AtomicUsize::new(0);

/// Why a link fault was not applied. Nothing is cut.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "link_refusal", rename_all = "snake_case")]
pub enum LinkRefusal {
    /// The host cannot cut UDP (`iptables` is unavailable to this user).
    Unavailable {
        /// Why.
        reason: String,
    },
    /// A named node is not in the node view, or its view row names no UDP port.
    NoSuchNode {
        /// The name.
        name: String,
    },
    /// A set names no node.
    EmptySet {
        /// Which set (`subset`, `a`, `b`, `target`).
        set: String,
    },
    /// The subset is every node: there is no rest to cut it from.
    WholeEstate,
    /// A named node is one the caller protects.
    Protected {
        /// The name.
        name: String,
    },
    /// A flap asked for more cycles or a longer phase than a flap may run.
    Unbounded {
        /// What exceeded.
        what: String,
        /// The most allowed.
        max: u64,
    },
    /// A flap cut and healed some cycles, then the host refused the next.
    FlapInterrupted {
        /// Cycles completed.
        cycles_done: u32,
        /// Why the next failed.
        reason: String,
    },
}

fn name_of(n: &Value) -> String {
    n["name"].as_str().unwrap_or_default().to_string()
}

/// The UDP port each of `names` published, in the order named; refused by name when one is
/// unknown or names no port.
pub fn ports_of(nodes: &[Value], names: &[String]) -> Result<Vec<u16>, LinkRefusal> {
    names
        .iter()
        .map(|name| {
            nodes
                .iter()
                .find(|n| name_of(n) == *name)
                .and_then(|n| n["transport_addr"].as_str())
                .and_then(|a| a.rsplit(':').next())
                .and_then(|p| p.parse::<u16>().ok())
                .ok_or_else(|| LinkRefusal::NoSuchNode { name: name.clone() })
        })
        .collect()
}

fn refuse_protected(names: &[String], protected: &[String]) -> Result<(), LinkRefusal> {
    match names.iter().find(|n| protected.contains(n)) {
        Some(name) => Err(LinkRefusal::Protected { name: name.clone() }),
        None => Ok(()),
    }
}

fn non_empty(set: &str, names: &[String]) -> Result<(), LinkRefusal> {
    if names.is_empty() {
        Err(LinkRefusal::EmptySet { set: set.into() })
    } else {
        Ok(())
    }
}

fn heal_span(fault: &str, held: Duration) {
    tracing::info_span!("rdm.testkit.fault.remove.via-heal", fault, held_ms = held.as_millis() as u64).in_scope(|| tracing::info!("the cut is dropped"));
}

/// An arbitrary node set cut from every other node, until dropped.
pub struct PartitionSubset {
    cut: Option<Partition>,
    /// The nodes cut off.
    pub subset: Vec<String>,
    /// The nodes they are cut from.
    pub rest: Vec<String>,
    /// The subset's ports.
    pub subset_ports: Vec<u16>,
    /// The rest's ports.
    pub rest_ports: Vec<u16>,
    since: Instant,
}

impl PartitionSubset {
    /// Cut `subset` from every other node of `nodes`.
    pub fn start(nodes: &[Value], subset: &[String], protected: &[String]) -> Result<Self, LinkRefusal> {
        let span = tracing::info_span!("rdm.testkit.fault.update.via-partition-subset", subset = %subset.join(","), nodes = nodes.len(), outcome = tracing::field::Empty);
        let _g = span.enter();
        let out = Self::start_inner(nodes, subset, protected);
        span.record("outcome", tracing::field::display(match &out {
            Ok(p) => format!("cut: {} ports of {} against {} ports of {}", p.subset_ports.len(), p.subset.join(","), p.rest_ports.len(), p.rest.join(",")),
            Err(r) => serde_json::to_string(r).unwrap_or_default(),
        }));
        out
    }

    fn start_inner(nodes: &[Value], subset: &[String], protected: &[String]) -> Result<Self, LinkRefusal> {
        non_empty("subset", subset)?;
        refuse_protected(subset, protected)?;
        let subset_ports = ports_of(nodes, subset)?;
        let rest: Vec<String> = nodes.iter().map(name_of).filter(|n| !n.is_empty() && !subset.contains(n)).collect();
        if rest.is_empty() {
            return Err(LinkRefusal::WholeEstate);
        }
        let rest_ports = ports_of(nodes, &rest)?;
        let cut = Partition::start(&subset_ports, &rest_ports).map_err(|reason| LinkRefusal::Unavailable { reason })?;
        Ok(Self { cut: Some(cut), subset: subset.to_vec(), rest, subset_ports, rest_ports, since: Instant::now() })
    }
}

impl Drop for PartitionSubset {
    fn drop(&mut self) {
        self.cut.take();
        heal_span("partition-subset", self.since.elapsed());
    }
}

/// What a flap did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Flapped {
    /// Cycles run (each a cut then a heal).
    pub cycles: u32,
    /// Total milliseconds the link was cut.
    pub cut_ms: u64,
    /// Total milliseconds the link was healed between cuts.
    pub healed_ms: u64,
}

/// Cut and heal the link between node sets `a` and `b` `cycles` times: cut for `cut`, healed for
/// `healed`. Bounded by [`MAX_FLAP_CYCLES`] and [`MAX_FLAP_PHASE`]; the last phase is a heal.
/// `a` is the side whose link flaps and is never a protected node; `b` is whom it flaps against
/// and may include one (a protected node loses a peer for a moment, it is not the one faulted).
pub fn flap_link(nodes: &[Value], a: &[String], b: &[String], protected: &[String], cycles: u32, cut: Duration, healed: Duration) -> Result<Flapped, LinkRefusal> {
    let span = tracing::info_span!("rdm.testkit.fault.update.via-link-flap", a = %a.join(","), b = %b.join(","), cycles, cut_ms = cut.as_millis() as u64, healed_ms = healed.as_millis() as u64, outcome = tracing::field::Empty);
    let _g = span.enter();
    let out = flap_inner(nodes, a, b, protected, cycles, cut, healed);
    span.record("outcome", tracing::field::display(match &out {
        Ok(f) => serde_json::to_string(f).unwrap_or_default(),
        Err(r) => serde_json::to_string(r).unwrap_or_default(),
    }));
    out
}

fn flap_inner(nodes: &[Value], a: &[String], b: &[String], protected: &[String], cycles: u32, cut: Duration, healed: Duration) -> Result<Flapped, LinkRefusal> {
    non_empty("a", a)?;
    non_empty("b", b)?;
    refuse_protected(a, protected)?;
    if cycles == 0 || cycles > MAX_FLAP_CYCLES {
        return Err(LinkRefusal::Unbounded { what: format!("cycles {cycles}"), max: MAX_FLAP_CYCLES as u64 });
    }
    for (what, d) in [("cut", cut), ("healed", healed)] {
        if d > MAX_FLAP_PHASE {
            return Err(LinkRefusal::Unbounded { what: format!("{what} phase {} ms", d.as_millis()), max: MAX_FLAP_PHASE.as_millis() as u64 });
        }
    }
    let (pa, pb) = (ports_of(nodes, a)?, ports_of(nodes, b)?);
    let mut out = Flapped { cycles: 0, cut_ms: 0, healed_ms: 0 };
    for _ in 0..cycles {
        let held = Partition::start(&pa, &pb).map_err(|reason| if out.cycles == 0 { LinkRefusal::Unavailable { reason } } else { LinkRefusal::FlapInterrupted { cycles_done: out.cycles, reason } })?;
        let at = Instant::now();
        std::thread::sleep(cut);
        drop(held);
        heal_span("link-flap", at.elapsed());
        std::thread::sleep(healed);
        out.cycles += 1;
        out.cut_ms += cut.as_millis() as u64;
        out.healed_ms += healed.as_millis() as u64;
    }
    Ok(out)
}

/// The `iptables` invocations that drop inbound UDP to each of `ports` (any sender), in `chain`.
pub fn inbound_drop_rules(chain: &str, ports: &[u16]) -> Vec<Vec<String>> {
    ports.iter().map(|p| ["-A", chain, "-i", "lo", "-p", "udp", "--dport", &p.to_string(), "-j", "DROP"].iter().map(|s| s.to_string()).collect()).collect()
}

fn iptables(sudo: bool, args: &[&str]) -> Result<(), String> {
    let mut c = if sudo { Command::new("sudo") } else { Command::new("iptables") };
    if sudo {
        c.args(["-n", "iptables"]);
    }
    let out = c.arg("-w").args(args).output().map_err(|e| e.to_string())?;
    if out.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
    }
}

/// Inbound UDP to one node dropped, whoever sends it, until dropped. The node's own sends are not
/// touched: peers cannot reach it, and it can still reach them.
pub struct FirewallInbound {
    chain: String,
    sudo: bool,
    /// The node.
    pub target: String,
    /// The ports dropped.
    pub ports: Vec<u16>,
    since: Instant,
}

impl FirewallInbound {
    /// Drop inbound UDP to `target`.
    pub fn start(nodes: &[Value], target: &str, protected: &[String]) -> Result<Self, LinkRefusal> {
        let span = tracing::info_span!("rdm.testkit.fault.update.via-inbound-drop", target, outcome = tracing::field::Empty);
        let _g = span.enter();
        let out = Self::start_inner(nodes, target, protected);
        span.record("outcome", tracing::field::display(match &out {
            Ok(f) => format!("dropping inbound udp to ports {:?} of {}", f.ports, f.target),
            Err(r) => serde_json::to_string(r).unwrap_or_default(),
        }));
        out
    }

    fn start_inner(nodes: &[Value], target: &str, protected: &[String]) -> Result<Self, LinkRefusal> {
        non_empty("target", &[target.to_string()].into_iter().filter(|t| !t.is_empty()).collect::<Vec<_>>())?;
        refuse_protected(&[target.to_string()], protected)?;
        let ports = ports_of(nodes, &[target.to_string()])?;
        let chain = format!("RAFKA-INB-{}-{}", std::process::id(), CHAINS.fetch_add(1, Ordering::SeqCst));
        let sudo = iptables(false, &["-L", "INPUT", "-n"]).is_err();
        let unavailable = |e: String| LinkRefusal::Unavailable { reason: format!("iptables unavailable: {e}") };
        iptables(sudo, &["-N", &chain]).map_err(unavailable)?;
        let f = Self { chain, sudo, target: target.into(), ports, since: Instant::now() };
        iptables(sudo, &["-I", "INPUT", "-j", &f.chain]).map_err(|e| LinkRefusal::Unavailable { reason: format!("iptables: {e}") })?;
        for rule in inbound_drop_rules(&f.chain, &f.ports) {
            let args: Vec<&str> = rule.iter().map(String::as_str).collect();
            iptables(sudo, &args).map_err(|e| LinkRefusal::Unavailable { reason: format!("iptables: {e}") })?;
        }
        Ok(f)
    }
}

impl Drop for FirewallInbound {
    fn drop(&mut self) {
        let _ = iptables(self.sudo, &["-D", "INPUT", "-j", &self.chain]);
        let _ = iptables(self.sudo, &["-F", &self.chain]);
        let _ = iptables(self.sudo, &["-X", &self.chain]);
        heal_span("inbound-drop", self.since.elapsed());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn view() -> Vec<Value> {
        (1..=4).map(|i| json!({"name": format!("mesh1.rpc.{i}"), "transport_addr": format!("127.0.0.1:{}", 40000 + i)})).collect()
    }

    fn names(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn ports_come_from_the_view_and_an_unknown_node_is_refused_by_name() {
        assert_eq!(ports_of(&view(), &names(&["mesh1.rpc.2", "mesh1.rpc.4"])), Ok(vec![40002, 40004]));
        assert_eq!(ports_of(&view(), &names(&["mesh1.rpc.9"])), Err(LinkRefusal::NoSuchNode { name: "mesh1.rpc.9".into() }));
    }

    #[test]
    fn a_subset_cut_refuses_an_empty_whole_unknown_or_protected_set_before_touching_the_host() {
        let all: Vec<String> = view().iter().map(name_of).collect();
        assert_eq!(PartitionSubset::start(&view(), &[], &[]).err(), Some(LinkRefusal::EmptySet { set: "subset".into() }));
        assert_eq!(PartitionSubset::start(&view(), &all, &[]).err(), Some(LinkRefusal::WholeEstate));
        assert_eq!(PartitionSubset::start(&view(), &names(&["mesh1.rpc.1", "mesh1.rpc.2"]), &names(&["mesh1.rpc.2"])).err(), Some(LinkRefusal::Protected { name: "mesh1.rpc.2".into() }));
        assert_eq!(PartitionSubset::start(&view(), &names(&["nope"]), &[]).err(), Some(LinkRefusal::NoSuchNode { name: "nope".into() }));
    }

    #[test]
    fn a_flap_is_bounded_and_refuses_a_protected_or_empty_side_before_touching_the_host() {
        let (a, b) = (names(&["mesh1.rpc.1"]), names(&["mesh1.rpc.2"]));
        let ms = Duration::from_millis(1);
        assert!(matches!(flap_link(&view(), &a, &b, &[], 0, ms, ms), Err(LinkRefusal::Unbounded { max, .. }) if max == MAX_FLAP_CYCLES as u64));
        assert!(matches!(flap_link(&view(), &a, &b, &[], MAX_FLAP_CYCLES + 1, ms, ms), Err(LinkRefusal::Unbounded { .. })));
        assert!(matches!(flap_link(&view(), &a, &b, &[], 1, MAX_FLAP_PHASE + ms, ms), Err(LinkRefusal::Unbounded { .. })));
        assert_eq!(flap_link(&view(), &a, &[], &[], 1, ms, ms), Err(LinkRefusal::EmptySet { set: "b".into() }));
        assert_eq!(flap_link(&view(), &a, &b, &a, 1, ms, ms), Err(LinkRefusal::Protected { name: "mesh1.rpc.1".into() }));
    }

    #[test]
    fn an_inbound_drop_matches_the_destination_port_only_never_the_sender() {
        let rules = inbound_drop_rules("C", &[40001, 40002]);
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].join(" "), "-A C -i lo -p udp --dport 40001 -j DROP");
        assert!(rules.iter().all(|r| !r.iter().any(|a| a == "--sport")), "the node's own sends are not dropped");
        assert_eq!(FirewallInbound::start(&view(), "mesh1.rpc.3", &names(&["mesh1.rpc.3"])).err(), Some(LinkRefusal::Protected { name: "mesh1.rpc.3".into() }));
    }
}
