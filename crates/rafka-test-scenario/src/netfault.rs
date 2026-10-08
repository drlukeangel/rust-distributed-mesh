//! Network faults for the process estate: UDP between loopback ports
//! dropped with `iptables` (as root, or through `sudo -n`). Where the host
//! cannot, [`Partition::start`] names why: a test skips by that name, or
//! fails with it when `RDM_REQUIRE_NETFAULT=1` (CI).

use serde_json::Value;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

/// One chain per cut of this process: a cell may hold several cuts at once.
static CUTS: AtomicUsize = AtomicUsize::new(0);

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// UDP between two sets of loopback ports dropped, until dropped itself.
pub struct Partition {
    chain: String,
    sudo: bool,
}

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
        let p = Self { chain, sudo };
        Self::iptables(sudo, &["-I", "INPUT", "-j", &p.chain]).map_err(|e| format!("iptables: {e}"))?;
        for x in a {
            for y in b {
                for (from, to) in [(x, y), (y, x)] {
                    let (f, t) = (from.to_string(), to.to_string());
                    Self::iptables(sudo, &["-A", &p.chain, "-i", "lo", "-p", "udp", "--sport", &f, "--dport", &t, "-j", "DROP"])
                        .map_err(|e| format!("iptables: {e}"))?;
                }
            }
        }
        Ok(p)
    }
}

impl Drop for Partition {
    fn drop(&mut self) {
        let _ = Self::iptables(self.sudo, &["-D", "INPUT", "-j", &self.chain]);
        let _ = Self::iptables(self.sudo, &["-F", &self.chain]);
        let _ = Self::iptables(self.sudo, &["-X", &self.chain]);
    }
}

pub fn udp_ports(nodes: &[Value], names: &[String]) -> Vec<u16> {
    let mut out = Vec::new();
    for n in nodes.iter().filter(|n| names.contains(&s(&n["name"]))) {
        out.push(s(&n["transport_addr"]).rsplit(':').next().unwrap().parse().unwrap());
    }
    out
}

