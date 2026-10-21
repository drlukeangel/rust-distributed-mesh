//! The host's view of a container estate's network, kept beside its evidence: what the host's own
//! counters said of the fabric's bridge when a control call failed, so a dial that timed out is read
//! against the bridge's link counters, the host's UDP and IP drop counters, the neighbour entry of the
//! destination and the connection-tracking counters, and the destination's own network namespace.
//! Evidence only: nothing here acts on the estate or its network.
//!
//! One file per capture, `<artifacts>/host-net/<label>.txt`: `start` (the run's reference once the
//! fabric is ready), `round-<n>.<path.name>` (a failed control call to that node) and `end`. Every
//! counter is cumulative: a capture is read as the delta from the one before it. Each section names
//! the command or file it holds, and a command that could not run is recorded with why.

use std::fmt::Write as _;
use std::path::Path;
use std::process::Command;

use crate::estate::Estate;

/// One command's stdout, or why there is none.
fn run(program: &str, args: &[&str]) -> String {
    match Command::new(program).args(args).output() {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).into_owned(),
        Ok(o) => format!("({program} exited {}: {})\n", o.status, String::from_utf8_lossy(&o.stderr).trim()),
        Err(e) => format!("({program} could not run: {e})\n"),
    }
}

fn read(path: &str) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| format!("({path} unreadable: {e})\n"))
}

fn on_path(program: &str) -> bool {
    std::env::var_os("PATH").is_some_and(|p| std::env::split_paths(&p).any(|d| d.join(program).is_file()))
}

/// Keep the host's view of `estate`'s bridge under `<artifacts>/host-net/<label>.txt`, with the
/// neighbour entry and network namespace of `node`'s running container when one is named. A
/// process estate has no bridge: nothing is kept.
pub async fn keep(estate: &Estate, label: &str, node: Option<&str>) {
    if estate.owner.provider != "container" || estate.fabric_id.is_empty() {
        return;
    }
    // The destination: its running container, that container's init (whose /proc/<pid>/net is the
    // container's namespace) and its address on the fabric's network.
    let container = match node {
        Some(n) => estate.container_of(n).await,
        None => None,
    };
    let (pid, ip) = container
        .as_deref()
        .map(|id| run("docker", &["inspect", "--format", "{{.State.Pid}} {{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", id]))
        .and_then(|o| o.trim().split_once(' ').map(|(p, i)| (p.to_string(), i.to_string())))
        .filter(|(p, _)| p.parse::<u32>().is_ok_and(|p| p > 0))
        .unzip();
    let dir = estate.artifacts.join("host-net");
    let text = capture(&estate.fabric_id, node, container.as_deref(), pid.as_deref(), ip.as_deref());
    let _ = std::fs::create_dir_all(&dir);
    let file = dir.join(format!("{label}.txt"));
    if let Err(e) = append(&file, &text) {
        eprintln!("[host-net] {}: {e}", file.display());
    }
}

fn append(file: &Path, text: &str) -> std::io::Result<()> {
    use std::io::Write as _;
    std::fs::OpenOptions::new().create(true).append(true).open(file)?.write_all(text.as_bytes())
}

fn capture(fabric_id: &str, node: Option<&str>, container: Option<&str>, pid: Option<&str>, ip: Option<&str>) -> String {
    let network = format!("rafka-{fabric_id}");
    let id = run("docker", &["network", "inspect", "--format", "{{.Id}}", &network]);
    // Docker names a bridge network's host interface `br-<the first 12 characters of its id>`.
    let bridge = id.trim().get(..12).filter(|h| h.bytes().all(|b| b.is_ascii_hexdigit())).map(|h| format!("br-{h}"));
    let mut t = String::new();
    let _ = writeln!(
        t,
        "# host-net at={} fabric={fabric_id} network={network} bridge={} node={} container={} pid={} ip={}",
        run("date", &["-u", "+%Y-%m-%dT%H:%M:%S.%6NZ"]).trim(),
        bridge.as_deref().unwrap_or("-"),
        node.unwrap_or("-"),
        container.unwrap_or("-"),
        pid.unwrap_or("-"),
        ip.unwrap_or("-"),
    );
    let mut section = |name: String, body: String| {
        let _ = write!(t, "\n## {name}\n{body}");
        if !body.ends_with('\n') {
            t.push('\n');
        }
    };
    match &bridge {
        Some(br) => {
            section(format!("ip -s -s link show {br}"), run("ip", &["-s", "-s", "link", "show", br]));
            section(format!("ip -s link show master {br}"), run("ip", &["-s", "link", "show", "master", br]));
        }
        None => section(format!("docker network inspect {network}"), format!("(no bridge: {})\n", id.trim())),
    }
    // Absolute values (-a), zeros too (-z), and the history file of other nstat users left as it is (-s).
    section("nstat -asz".into(), run("nstat", &["-asz"]));
    if let Some(br) = &bridge {
        if let Some(ip) = ip {
            let entry = run("ip", &["-s", "neigh", "show", "dev", br, ip]);
            section(format!("ip -s neigh show dev {br} {ip}"), if entry.trim().is_empty() { "(no entry)\n".into() } else { entry });
        }
        section(format!("ip -s neigh show dev {br}"), run("ip", &["-s", "neigh", "show", "dev", br]));
    }
    section("/proc/net/stat/arp_cache".into(), read("/proc/net/stat/arp_cache"));
    let thresh = ["gc_thresh1", "gc_thresh2", "gc_thresh3"].iter().map(|k| format!("{k} {}", read(&format!("/proc/sys/net/ipv4/neigh/default/{k}")).trim())).collect::<Vec<_>>().join("\n");
    section("/proc/sys/net/ipv4/neigh/default/gc_thresh{1,2,3}".into(), thresh);
    let conntrack = if !on_path("conntrack") {
        "(conntrack is not installed on this host)\n".to_string()
    } else {
        match Command::new("conntrack").arg("-S").output() {
            Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).into_owned(),
            plain => {
                let why = plain.map(|o| String::from_utf8_lossy(&o.stderr).trim().to_string()).unwrap_or_else(|e| e.to_string());
                format!("(conntrack -S: {why})\nsudo -n conntrack -S:\n{}", run("sudo", &["-n", "conntrack", "-S"]))
            }
        }
    };
    section("conntrack -S".into(), conntrack);
    let table = ["nf_conntrack_count", "nf_conntrack_max"].iter().map(|k| format!("{k} {}", read(&format!("/proc/sys/net/netfilter/{k}")).trim())).collect::<Vec<_>>().join("\n");
    section("/proc/sys/net/netfilter/nf_conntrack_{count,max}".into(), table);
    if let Some(pid) = pid {
        // The destination container's own network namespace, read through its init.
        for f in ["dev", "snmp", "udp", "arp"] {
            let path = format!("/proc/{pid}/net/{f}");
            section(path.clone(), read(&path));
        }
    }
    t
}
