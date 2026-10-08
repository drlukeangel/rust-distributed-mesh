//! Container fault backend (i143.e8.s6, rafka-v2 #2784): real Docker primitives against the exact
//! containers of a container estate. A silenced runtime keeps its immutable container: nothing
//! here stops, removes or restarts one.
//!
//! - silence: [`silence`] installs a packet filter in the network namespace of every container of a
//!   mesh so the rest of the estate is unheard (the interface, sockets and processes stay);
//!   [`Silenced::lift`] removes it. This is the network cut of a peer mesh that goes unheard.
//! - unplug: [`unplug`] removes one container's interface from its Fabric's Docker network and
//!   [`replug`] restores it at the same address. The runtime's transport dies with its interface
//!   (it exits: `rdm.mesh.node.delete.via-transport-stopped`), so this is a loss of the runtime, not
//!   a silence of it.
//! - freeze: [`pause`] / [`unpause`] hold a container's processes in place.
//! - provider observation: [`inspect`] reads one container's state straight from the Docker
//!   daemon, so a scenario can show that a silenced runtime never stopped, restarted or changed.
//!
//! The fabric-primary node-admin's container is never named by a helper here; a scenario that
//! silences a mesh chooses the side without it ([`silence`] refuses a node set that holds one).

use crate::estate::Estate;
use serde::Serialize;
use std::process::Command;

/// `docker <args>`: trimmed stdout, or an error naming the command and its stderr.
pub fn docker(args: &[&str]) -> Result<String, String> {
    let out = Command::new("docker").args(args).output().map_err(|e| format!("docker {}: {e}", args.join(" ")))?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        Err(format!("docker {}: {}", args.join(" "), String::from_utf8_lossy(&out.stderr).trim()))
    }
}

/// The Docker network of a Fabric.
pub fn network_of(estate: &Estate) -> String {
    format!("rafka-{}", estate.fabric_id)
}

/// A container disconnected from the fabric network, to be reconnected at the same address.
#[derive(Debug, Clone, Serialize)]
pub struct Unplugged {
    pub node: String,
    pub id: String,
    pub ip: String,
    pub network: String,
}

/// Disconnect the running container of `node` from its Fabric's network.
pub fn unplug(estate: &Estate, node: &str) -> Result<Unplugged, String> {
    let id = estate.container_of(node).ok_or_else(|| format!("{node}: no running container of fabric {}", estate.fabric_id))?;
    let network = network_of(estate);
    let ip = docker(&["inspect", "--format", &format!("{{{{(index .NetworkSettings.Networks \"{network}\").IPAddress}}}}"), &id])?;
    if ip.is_empty() {
        return Err(format!("{node}: container {id} holds no address on {network}"));
    }
    docker(&["network", "disconnect", "-f", &network, &id])?;
    Ok(Unplugged { node: node.into(), id, ip, network })
}

/// Reconnect an unplugged container at the address it held.
pub fn replug(u: &Unplugged) -> Result<(), String> {
    docker(&["network", "connect", "--ip", &u.ip, &u.network, &u.id]).map(|_| ())
}

/// The Docker network's gateway: the host's address on it, the source of every call the host makes
/// into the estate.
pub fn gateway_of(estate: &Estate) -> Result<String, String> {
    docker(&["network", "inspect", "--format", "{{range .IPAM.Config}}{{.Gateway}}{{end}}", &network_of(estate)])
}

/// A container's address on its Fabric's network.
pub fn address_of(estate: &Estate, id: &str) -> Result<String, String> {
    let network = network_of(estate);
    let ip = docker(&["inspect", "--format", &format!("{{{{(index .NetworkSettings.Networks \"{network}\").IPAddress}}}}"), id])?;
    if ip.is_empty() {
        Err(format!("container {id} holds no address on {network}"))
    } else {
        Ok(ip)
    }
}

/// The packet-filter chain a silence installs in each silenced container's own network namespace.
pub const SILENCE_CHAIN: &str = "RDM-SILENCE";

/// Run `/usr/sbin/<tool> <args>` in the network namespace of container `id` with `NET_ADMIN`, in a
/// throwaway container of the runtime image that mounts the host's tool directories read-only.
/// The target container is not touched: its interface, its sockets and its processes stay as they
/// are. `stdin` is fed to the tool.
fn in_netns(id: &str, tool: &str, args: &[&str], stdin: Option<&str>) -> Result<String, String> {
    use std::io::Write;
    use std::process::Stdio;
    let mut cmd = Command::new("docker");
    cmd.args(["run", "--rm", "-i", "--net", &format!("container:{id}"), "--cap-add", "NET_ADMIN"]);
    for d in ["/usr", "/lib", "/lib64", "/bin", "/sbin", "/etc/alternatives"] {
        if std::path::Path::new(d).exists() {
            cmd.args(["-v", &format!("{d}:{d}:ro")]);
        }
    }
    cmd.arg(crate::estate::RUNTIME_IMAGE).arg(format!("/usr/sbin/{tool}")).args(args);
    cmd.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| format!("{tool} in the namespace of {id}: {e}"))?;
    if let (Some(text), Some(mut pipe)) = (stdin, child.stdin.take()) {
        pipe.write_all(text.as_bytes()).map_err(|e| format!("{tool} in the namespace of {id}: {e}"))?;
    }
    let out = child.wait_with_output().map_err(|e| format!("{tool} in the namespace of {id}: {e}"))?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        Err(format!("{tool} {} in the namespace of {id}: {}", args.join(" "), String::from_utf8_lossy(&out.stderr).trim()))
    }
}

/// One container of an estate with the address it holds.
#[derive(Debug, Clone, Serialize)]
pub struct Member {
    pub node: String,
    pub id: String,
    pub ip: String,
}

/// A mesh's containers silenced against everything outside the set: each keeps its interface, its
/// address, its sockets, its processes and every other member of the set, and drops all packets
/// to and from the rest of the estate's containers and all UDP to and from the host (the
/// transport's protocol); the host still reaches each one's control API over TCP, so a scenario
/// reads the silenced side's own view. A real network cut, never a container stop.
#[derive(Debug, Clone, Serialize)]
pub struct Silenced {
    pub network: String,
    pub gateway: String,
    pub members: Vec<Member>,
    /// The containers the members no longer hear.
    pub unheard: Vec<Member>,
}

/// Silence `nodes` against the rest of the estate. `fabric_primary` is the node-admin holding the
/// fabric-primary seat: a set that holds it is refused before anything is touched. A member that
/// cannot be silenced leaves the ones already silenced restored, and the error says which.
pub fn silence(estate: &Estate, nodes: &[String], fabric_primary: &str) -> Result<Silenced, String> {
    if nodes.iter().any(|n| n == fabric_primary) {
        return Err(format!("refused: the silenced set {nodes:?} holds the fabric-primary node-admin {fabric_primary}"));
    }
    let gateway = gateway_of(estate)?;
    let mut members = Vec::new();
    for n in nodes {
        let id = estate.container_of(n).ok_or_else(|| format!("{n}: no running container of fabric {}", estate.fabric_id))?;
        members.push(Member { node: n.clone(), ip: address_of(estate, &id)?, id });
    }
    let mut unheard = Vec::new();
    for (n, id) in estate.live_containers().into_iter().filter(|(n, _)| !nodes.contains(n)) {
        unheard.push(Member { node: n, ip: address_of(estate, &id)?, id });
    }
    let silenced = Silenced { network: network_of(estate), gateway, members, unheard };
    let mut done = 0;
    for m in &silenced.members {
        if let Err(e) = in_netns(&m.id, "iptables-restore", &["--noflush"], Some(&silenced.install_rules())) {
            let back: Vec<String> = silenced.members[..done].iter().filter_map(|m| m.lift().err()).collect();
            return Err(format!("{}: {e} ({done} already silenced were restored; failures: {back:?})", m.node));
        }
        done += 1;
    }
    Ok(silenced)
}

impl Silenced {
    fn install_rules(&self) -> String {
        let mut r = format!("*filter\n:{SILENCE_CHAIN} - [0:0]\n");
        for u in &self.unheard {
            r += &format!("-A {SILENCE_CHAIN} -s {ip} -j DROP\n-A {SILENCE_CHAIN} -d {ip} -j DROP\n", ip = u.ip);
        }
        r += &format!("-A {SILENCE_CHAIN} -s {gw} -p udp -j DROP\n-A {SILENCE_CHAIN} -d {gw} -p udp -j DROP\n", gw = self.gateway);
        r += &format!("-I INPUT 1 -j {SILENCE_CHAIN}\n-I OUTPUT 1 -j {SILENCE_CHAIN}\nCOMMIT\n");
        r
    }

    /// The rules each member holds now, as its own packet filter lists them.
    pub fn rules(&self) -> Result<std::collections::BTreeMap<String, String>, String> {
        self.members.iter().map(|m| Ok((m.node.clone(), in_netns(&m.id, "iptables", &["-S"], None)?))).collect()
    }

    /// Every member holds the silence chain, referenced from INPUT and OUTPUT.
    pub fn active(&self) -> Result<bool, String> {
        Ok(self.rules()?.values().all(|r| r.contains(&format!("-A INPUT -j {SILENCE_CHAIN}")) && r.contains(&format!("-A OUTPUT -j {SILENCE_CHAIN}"))))
    }

    /// Restore every member's packet filter. Every failure is named.
    pub fn lift(&self) -> Result<(), String> {
        let failed: Vec<String> = self.members.iter().filter_map(|m| m.lift().err().map(|e| format!("{}: {e}", m.node))).collect();
        if failed.is_empty() {
            Ok(())
        } else {
            Err(failed.join("; "))
        }
    }
}

impl Member {
    fn lift(&self) -> Result<(), String> {
        let rules = format!("*filter\n-D INPUT -j {SILENCE_CHAIN}\n-D OUTPUT -j {SILENCE_CHAIN}\n-F {SILENCE_CHAIN}\n-X {SILENCE_CHAIN}\nCOMMIT\n");
        in_netns(&self.id, "iptables-restore", &["--noflush"], Some(&rules)).map(|_| ())
    }
}

/// Hold a container's processes in place (`docker pause`).
pub fn pause(id: &str) -> Result<(), String> {
    docker(&["pause", id]).map(|_| ())
}

/// Release a paused container (`docker unpause`).
pub fn unpause(id: &str) -> Result<(), String> {
    docker(&["unpause", id]).map(|_| ())
}

/// One container as the Docker daemon holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Inspected {
    pub id: String,
    /// `running`, `paused`, `exited`...
    pub status: String,
    pub running: bool,
    pub pid: u64,
    pub started_at: String,
    pub restart_count: u64,
    pub exit_code: i64,
    pub oom_killed: bool,
    /// The names of the Docker networks the container is attached to now.
    pub networks: Vec<String>,
}

/// Read one container's state from the Docker daemon. A container the daemon no longer holds is an
/// error naming it, never an empty answer.
pub fn inspect(id: &str) -> Result<Inspected, String> {
    let raw = docker(&["inspect", id])?;
    let v: serde_json::Value = serde_json::from_str(&raw).map_err(|e| format!("docker inspect {id}: unreadable ({e})"))?;
    let c = v.get(0).ok_or_else(|| format!("docker inspect {id}: the daemon holds no such container"))?;
    let st = &c["State"];
    let mut networks: Vec<String> = c["NetworkSettings"]["Networks"].as_object().map(|m| m.keys().cloned().collect()).unwrap_or_default();
    networks.sort();
    Ok(Inspected {
        id: c["Id"].as_str().unwrap_or_default().to_string(),
        status: st["Status"].as_str().unwrap_or_default().to_string(),
        running: st["Running"].as_bool().unwrap_or(false),
        pid: st["Pid"].as_u64().unwrap_or(0),
        started_at: st["StartedAt"].as_str().unwrap_or_default().to_string(),
        restart_count: c["RestartCount"].as_u64().unwrap_or(0),
        exit_code: st["ExitCode"].as_i64().unwrap_or(0),
        oom_killed: st["OOMKilled"].as_bool().unwrap_or(false),
        networks,
    })
}
