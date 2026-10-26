//! Network faults for the process estate: UDP between loopback ports dropped by a packet-filter
//! table that the process which installed it OWNS.
//!
//! The table is an nftables table created with `flags owner`: the kernel deletes it when the
//! netlink socket that created it closes. That socket belongs to a helper (`nft -i`, as root or
//! through `sudo -n`) whose standard input is a pipe only the installing process holds. When the
//! installer ends, however it ends (a drop, a panic, SIGKILL), the kernel closes the pipe, the
//! helper reads end-of-file and exits, and the table goes with its socket: a fault cannot outlive
//! the process that installed it, and nothing has to run for it to end. Where the host cannot,
//! [`Held::install`] names why: a test skips by that name, or fails with it when
//! `RDM_REQUIRE_NETFAULT=1` (CI).

use serde_json::Value;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};

/// One table per fault of this process: a cell may hold several faults at once.
static TABLES: AtomicUsize = AtomicUsize::new(0);

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// A packet-filter table of loopback UDP drops, owned by the process that installed it and held
/// by a helper process that lives exactly as long as the pipe this value holds open.
pub struct Held {
    helper: Child,
    commands: Option<ChildStdin>,
    answers: BufReader<ChildStdout>,
}

impl Held {
    /// Install a table named for `fault` carrying one drop rule per entry of `rules` (nft rule
    /// text after `add rule ip <table> input`). `Ok` means the host acknowledged every rule:
    /// the table was listed back with each of them in it. `Err` names why the host cannot.
    pub fn install(fault: &str, rules: &[String]) -> Result<Self, String> {
        let table = format!("rafka_{fault}_{}_{}", std::process::id(), TABLES.fetch_add(1, Ordering::SeqCst));
        let direct = Command::new("nft").args(["list", "tables"]).output().is_ok_and(|o| o.status.success());
        let argv: &[&str] = if direct { &["nft", "-i"] } else { &["sudo", "-n", "nft", "-i"] };
        // The helper's errors and its listing share one stream, in the order nft produced them.
        let mut helper = Command::new("sh")
            .args(["-c", "exec \"$@\" 2>&1", "nft-holder"])
            .args(argv)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|e| format!("nft unavailable: {}: {e}", argv.join(" ")))?;
        let commands = helper.stdin.take().expect("the helper's stdin is piped");
        let answers = BufReader::new(helper.stdout.take().expect("the helper's stdout is piped"));
        let mut held = Self { helper, commands: Some(commands), answers: answers };
        let mut script = vec![
            format!("add table ip {table} {{ flags owner; }}"),
            format!("add chain ip {table} input {{ type filter hook input priority -1; policy accept; }}"),
        ];
        script.extend(rules.iter().map(|r| format!("add rule ip {table} input {r}")));
        script.push(format!("list table ip {table}"));
        // A helper that already ended (sudo refused) closes the pipe: the write fails and the
        // read below still returns what it said before it went.
        let _ = held.commands.as_mut().expect("held").write_all(format!("{}\n", script.join("\n")).as_bytes());
        let mut said = Vec::new();
        let mut line = String::new();
        while held.answers.read_line(&mut line).is_ok_and(|n| n > 0) {
            let text = line.trim_end().to_string();
            line.clear();
            let listed_end = text == "}";
            said.push(text);
            if listed_end {
                break;
            }
        }
        // The listing is the table's header line (nft may append a comment to it) through the closing
        // brace; what nft said before it is its refusals, and it carries every rule asked for.
        let header = format!("table ip {table} {{");
        let stands = said.iter().position(|l| l.starts_with(&header)).is_some_and(|at| said.last().is_some_and(|l| l == "}") && said[at..].iter().filter(|l| l.ends_with(" drop")).count() == rules.len());
        if stands {
            return Ok(held);
        }
        held.release();
        let status = held.helper.wait().map(|s| s.to_string()).unwrap_or_default();
        Err(format!("nft unavailable: {} ended with {status}: {}", argv.join(" "), if said.is_empty() { "it said nothing".to_string() } else { said.join(" | ") }))
    }

    /// The process whose lifetime is the fault's: when it has exited, the table is gone.
    pub fn holder_pid(&self) -> u32 {
        self.helper.id()
    }

    /// End the fault now: close the pipe and wait for the helper to exit, which removes the table.
    pub fn release(&mut self) {
        drop(self.commands.take());
        let _ = self.helper.wait();
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.release();
    }
}

fn port_set(ports: &[u16]) -> String {
    let mut ports = ports.to_vec();
    ports.sort_unstable();
    ports.dedup();
    format!("{{ {} }}", ports.iter().map(u16::to_string).collect::<Vec<_>>().join(", "))
}

/// The rules that drop UDP from every port of `a` to every port of `b`, and back, on loopback.
/// Neither set is cut from itself; an empty set cuts nothing.
pub fn partition_rules(a: &[u16], b: &[u16]) -> Vec<String> {
    if a.is_empty() || b.is_empty() {
        return Vec::new();
    }
    let (a, b) = (port_set(a), port_set(b));
    vec![format!("iifname \"lo\" udp sport {a} udp dport {b} drop"), format!("iifname \"lo\" udp sport {b} udp dport {a} drop")]
}

/// The rules that drop every UDP datagram on loopback from or to `ports`, whatever port is on the other end, so
/// a node born after the cut is cut from them too. An empty set cuts nothing.
pub fn isolation_rules(ports: &[u16]) -> Vec<String> {
    if ports.is_empty() {
        return Vec::new();
    }
    let set = port_set(ports);
    vec![format!("iifname \"lo\" udp sport {set} drop"), format!("iifname \"lo\" udp dport {set} drop")]
}

/// UDP between two sets of loopback ports dropped, until dropped itself or its installer ends.
pub struct Partition {
    held: Held,
}

impl Partition {
    /// `Err` names why the host cannot partition (a skip, or a CI failure).
    pub fn start(a: &[u16], b: &[u16]) -> Result<Self, String> {
        Ok(Self { held: Held::install("part", &partition_rules(a, b))? })
    }

    /// Every UDP datagram to or from `ports` dropped, births after the cut included.
    pub fn isolate(ports: &[u16]) -> Result<Self, String> {
        Ok(Self { held: Held::install("isolate", &isolation_rules(ports))? })
    }

    /// The process whose exit ends this cut.
    pub fn holder_pid(&self) -> u32 {
        self.held.holder_pid()
    }
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

    #[test]
    fn a_partition_drops_each_direction_by_both_ports_and_an_empty_side_cuts_nothing() {
        let rules = partition_rules(&[40002, 40001, 40001], &[40003]);
        assert_eq!(rules, vec![r#"iifname "lo" udp sport { 40001, 40002 } udp dport { 40003 } drop"#, r#"iifname "lo" udp sport { 40003 } udp dport { 40001, 40002 } drop"#]);
        assert!(partition_rules(&[], &[40003]).is_empty() && partition_rules(&[40001], &[]).is_empty());
    }

    #[test]
    fn an_isolation_drops_every_datagram_to_or_from_the_isolated_ports_whatever_the_other_port() {
        let rules = isolation_rules(&[40002, 40001]);
        assert_eq!(rules, vec![r#"iifname "lo" udp sport { 40001, 40002 } drop"#, r#"iifname "lo" udp dport { 40001, 40002 } drop"#]);
        assert!(isolation_rules(&[]).is_empty());
    }
}
