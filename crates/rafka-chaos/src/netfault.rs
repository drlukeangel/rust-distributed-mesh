//! Network faults for the process estate: UDP between loopback ports
//! dropped with `iptables` (as root, or through `sudo -n`). Where the host
//! cannot, [`Partition::start`] names why: a test skips by that name, or
//! fails with it when `RDM_REQUIRE_NETFAULT=1` (CI).

use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

/// One chain per cut of this process: a cell may hold several cuts at once.
static CUTS: AtomicUsize = AtomicUsize::new(0);

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// UDP between two sets of loopback ports dropped, until dropped itself. The sets can grow while
/// the cut stands ([`Partition::extend`]): a cut of a mesh holds for every member of that mesh,
/// including one born after the cut began.
pub struct Partition {
    chain: String,
    sudo: bool,
    a: BTreeSet<u16>,
    b: BTreeSet<u16>,
}

/// `multiport` takes at most 15 ports per rule.
const MULTIPORT_MAX: usize = 15;

impl Partition {
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

    /// `Err` names why the host cannot partition (a skip, or a CI failure).
    pub fn start(a: &[u16], b: &[u16]) -> Result<Self, String> {
        let chain = format!("RAFKA-PART-{}-{}", std::process::id(), CUTS.fetch_add(1, Ordering::SeqCst));
        let sudo = Self::iptables(false, &["-L", "INPUT", "-n"]).is_err();
        Self::iptables(sudo, &["-N", &chain]).map_err(|e| format!("iptables unavailable: {e}"))?;
        let mut p = Self { chain, sudo, a: BTreeSet::new(), b: BTreeSet::new() };
        Self::iptables(sudo, &["-I", "INPUT", "-j", &p.chain]).map_err(|e| format!("iptables: {e}"))?;
        p.extend(a, b)?;
        Ok(p)
    }

    /// Add `a` to the first side and `b` to the second: every pair not already dropped is dropped,
    /// in both directions. A port already on a side adds nothing. Returns how many ports joined.
    pub fn extend(&mut self, a: &[u16], b: &[u16]) -> Result<usize, String> {
        let new_a: Vec<u16> = a.iter().copied().filter(|p| !self.a.contains(p)).collect();
        let new_b: Vec<u16> = b.iter().copied().filter(|p| !self.b.contains(p)).collect();
        // New first-side ports against every second-side port, then the old first-side ports
        // against the new second-side ports.
        let all_b: Vec<u16> = self.b.iter().copied().chain(new_b.iter().copied()).collect();
        let old_a: Vec<u16> = self.a.iter().copied().collect();
        for x in &new_a {
            self.drop_between(*x, &all_b)?;
        }
        for x in &old_a {
            self.drop_between(*x, &new_b)?;
        }
        let joined = new_a.len() + new_b.len();
        self.a.extend(new_a);
        self.b.extend(new_b);
        Ok(joined)
    }

    /// Drop UDP between `port` and each of `others`, both ways, one rule per 15 ports.
    fn drop_between(&self, port: u16, others: &[u16]) -> Result<(), String> {
        let p = port.to_string();
        for chunk in others.chunks(MULTIPORT_MAX) {
            let list = chunk.iter().map(u16::to_string).collect::<Vec<_>>().join(",");
            for (side, other) in [("--sport", "--dports"), ("--dport", "--sports")] {
                Self::iptables(self.sudo, &["-A", &self.chain, "-i", "lo", "-p", "udp", side, &p, "-m", "multiport", other, &list, "-j", "DROP"])
                    .map_err(|e| format!("iptables: {e}"))?;
            }
        }
        Ok(())
    }
}

impl Drop for Partition {
    fn drop(&mut self) {
        let _ = Self::iptables(self.sudo, &["-D", "INPUT", "-j", &self.chain]);
        let _ = Self::iptables(self.sudo, &["-F", &self.chain]);
        let _ = Self::iptables(self.sudo, &["-X", &self.chain]);
    }
}

/// One process of an estate, found by the OS: the node data dir it was launched with and the
/// IPv4 UDP ports it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EstateProcess {
    /// The process id.
    pub pid: u32,
    /// Its `RDM_DATA_DIR`: `<estate root>/<node path.name>[-<suffix>]`.
    pub data_dir: PathBuf,
    /// The IPv4 UDP ports its sockets are bound to.
    pub udp_ports: Vec<u16>,
}

impl EstateProcess {
    /// The node `path.name` the data dir names (`mesh2.broker.1-7kenx5wvay7x` is `mesh2.broker.1`).
    pub fn node(&self) -> String {
        let leaf = self.data_dir.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
        match leaf.rsplit_once('-') {
            Some((name, suffix)) if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_alphanumeric()) && name.matches('.').count() == 2 => name.to_string(),
            _ => leaf,
        }
    }

    /// The mesh the node belongs to (the first segment of its `path.name`).
    pub fn mesh(&self) -> String {
        self.node().split('.').next().unwrap_or_default().to_string()
    }
}

/// Every running process whose `RDM_DATA_DIR` lies under `root`, with its UDP ports, asked of the
/// OS. A process is a member of its mesh from the moment it is launched, whether or not any
/// admin has heard of it yet.
pub fn estate_processes(root: &Path) -> Vec<EstateProcess> {
    let table = udp4_table();
    let mut out = Vec::new();
    let prefix = format!("RDM_DATA_DIR={}/", root.display());
    for e in std::fs::read_dir("/proc").into_iter().flatten().flatten() {
        let Some(pid) = e.file_name().to_str().and_then(|n| n.parse::<u32>().ok()) else { continue };
        let Ok(environ) = std::fs::read(format!("/proc/{pid}/environ")) else { continue };
        let Some(data_dir) = environ.split(|b| *b == 0).filter_map(|kv| std::str::from_utf8(kv).ok()).find_map(|kv| kv.strip_prefix(&prefix).map(|rest| root.join(rest))) else { continue };
        let mut ports = BTreeSet::new();
        for fd in std::fs::read_dir(format!("/proc/{pid}/fd")).into_iter().flatten().flatten() {
            if let Ok(target) = std::fs::read_link(fd.path()) {
                if let Some(inode) = target.to_string_lossy().strip_prefix("socket:[").and_then(|r| r.strip_suffix(']')).and_then(|i| i.parse::<u64>().ok()) {
                    if let Some(port) = table.get(&inode) {
                        ports.insert(*port);
                    }
                }
            }
        }
        out.push(EstateProcess { pid, data_dir, udp_ports: ports.into_iter().collect() });
    }
    out
}

/// Socket inode to local port, from the kernel's IPv4 UDP table.
fn udp4_table() -> BTreeMap<u64, u16> {
    let mut out = BTreeMap::new();
    for line in std::fs::read_to_string("/proc/net/udp").unwrap_or_default().lines().skip(1) {
        let cols: Vec<&str> = line.split_whitespace().collect();
        if cols.len() < 10 {
            continue;
        }
        let port = cols[1].rsplit(':').next().and_then(|p| u16::from_str_radix(p, 16).ok());
        if let (Some(port), Ok(inode)) = (port, cols[9].parse::<u64>()) {
            out.insert(inode, port);
        }
    }
    out
}

/// Udp ports.
pub fn udp_ports(nodes: &[Value], names: &[String]) -> Vec<u16> {
    let mut out = Vec::new();
    for n in nodes.iter().filter(|n| names.contains(&s(&n["name"]))) {
        out.push(s(&n["transport_addr"]).rsplit(':').next().unwrap().parse().unwrap());
    }
    out
}


#[cfg(test)]
mod tests {
    use super::*;

    fn proc_at(dir: &str) -> EstateProcess {
        EstateProcess { pid: 1, data_dir: PathBuf::from(dir), udp_ports: vec![] }
    }

    // @feature: chaos
    #[test]
    fn data_dir_names_its_node_and_mesh() {
        assert_eq!(proc_at("/r/mesh2.broker.1-7kenx5wvay7x").node(), "mesh2.broker.1");
        assert_eq!(proc_at("/r/mesh2.broker.1-7kenx5wvay7x").mesh(), "mesh2");
        assert_eq!(proc_at("/r/mesh1.admin.1").node(), "mesh1.admin.1");
        assert_eq!(proc_at("/r/mesh1.admin.1").mesh(), "mesh1");
    }

    // @feature: chaos
    #[test]
    fn estate_processes_finds_a_process_by_its_data_dir_and_its_udp_port() {
        let sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = sock.local_addr().unwrap().port();
        let root = std::env::temp_dir().join(format!("netfault-root-{}", std::process::id()));
        // A child launched with the estate's data dir env, holding no socket of its own: the
        // finder reads the OS for it, and for the test process (which holds the socket) by env.
        let mut child = Command::new("sleep").arg("30").env("RDM_DATA_DIR", root.join("mesh9.gateway.2-abc123")).spawn().unwrap();
        let found = estate_processes(&root);
        let _ = child.kill();
        let _ = child.wait();
        let c = found.iter().find(|p| p.pid == child.id()).expect("the child is found by its data dir");
        assert_eq!((c.node(), c.mesh()), ("mesh9.gateway.2".to_string(), "mesh9".to_string()));
        assert!(udp4_table().values().any(|p| *p == port), "the kernel table lists the bound port");
        drop(sock);
    }
}
