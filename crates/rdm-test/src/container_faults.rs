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
//! - exact-container faults: [`ExactContainer`] kills, pauses and unpauses ONE container named by
//!   its Docker id, Fabric label, node label and start time; a container the daemon does not hold
//!   as that exact one is refused by name with nothing sent ([`Refusal`]), and every applied fault
//!   is acknowledged by the daemon's own state ([`Applied`]). Each application is one span,
//!   `rdm.testkit.fault.update.via-container-command`.
//! - provider observation: [`inspect`] reads one container's state straight from the Docker
//!   daemon, so a scenario can show that a silenced runtime never stopped, restarted or changed.
//!
//! The fabric-primary node-admin's container is never named by a helper here; a scenario that
//! silences a mesh chooses the side without it ([`silence`] refuses a node set that holds one).


/// The empty runtime image every node container runs from (the provider's `RUNTIME_IMAGE`).
pub const RUNTIME_IMAGE: &str = "rafka-node-runtime:empty";

/// The container estate a fault is aimed into: its Fabric and the running container of each node.
pub trait ContainerEstate {
    /// The Fabric's id: its Docker network is `rafka-<fabric_id>`.
    fn fabric_id(&self) -> &str;
    /// The running container of `node`'s current birth, by the container id its RuntimeFact names.
    fn container_of(&self, node: &str) -> impl std::future::Future<Output = Option<String>> + Send;
    /// Every running container of the Fabric as (node `path.name`, Docker id).
    fn live_containers(&self) -> Vec<(String, String)>;
}
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
pub fn network_of(estate: &impl ContainerEstate) -> String {
    format!("rafka-{}", estate.fabric_id())
}

/// A container disconnected from the fabric network, to be reconnected at the same address.
#[derive(Debug, Clone, Serialize)]
pub struct Unplugged {
    /// The node's `path.name`.
    pub node: String,
    /// The Docker id of the container.
    pub id: String,
    /// The container's address on the Fabric network.
    pub ip: String,
    /// The Fabric's Docker network.
    pub network: String,
}

/// Disconnect the running container of `node` from its Fabric's network.
pub async fn unplug(estate: &impl ContainerEstate, node: &str) -> Result<Unplugged, String> {
    let id = estate.container_of(node).await.ok_or_else(|| format!("{node}: no running container of fabric {}", estate.fabric_id()))?;
    let network = network_of(estate);
    let ip = address_of(estate, &id).map_err(|e| format!("{node}: {e}"))?;
    docker(&["network", "disconnect", "-f", &network, &id])?;
    Ok(Unplugged { node: node.into(), id, ip, network })
}

/// Reconnect an unplugged container at the address it held.
pub fn replug(u: &Unplugged) -> Result<(), String> {
    docker(&["network", "connect", "--ip", &u.ip, &u.network, &u.id]).map(|_| ())
}

/// The Docker network's gateway: the host's address on it, the source of every call the host makes
/// into the estate.
pub fn gateway_of(estate: &impl ContainerEstate) -> Result<String, String> {
    docker(&["network", "inspect", "--format", "{{range .IPAM.Config}}{{.Gateway}}{{end}}", &network_of(estate)])
}

/// A container's address on its Fabric's network. A container that no longer runs holds none.
pub fn address_of(estate: &impl ContainerEstate, id: &str) -> Result<String, String> {
    let network = network_of(estate);
    running_address(&network, id)?.ok_or_else(|| format!("container {id} no longer runs and holds no address on {network}"))
}

/// A container's address on `network`, read with its running state in one daemon read: `None` when
/// the container no longer runs (it exited or was removed after the daemon listed it). An exited
/// container keeps its endpoint with the address released, which the daemon renders as
/// `invalid IP` (Docker 29) or nothing: it holds no address. A running container whose read is not
/// an address is refused by name, so a packet filter is only ever written with addresses.
pub fn running_address(network: &str, id: &str) -> Result<Option<String>, String> {
    let format = format!("{{{{.State.Running}}}} {{{{with index .NetworkSettings.Networks \"{network}\"}}}}{{{{.IPAddress}}}}{{{{end}}}}");
    match docker(&["inspect", "--type", "container", "--format", &format, id]) {
        Ok(read) => running_address_in(network, id, &read),
        Err(e) if e.contains("No such container") => Ok(None),
        Err(e) => Err(e),
    }
}

fn running_address_in(network: &str, id: &str, read: &str) -> Result<Option<String>, String> {
    let (running, ip) = read.trim().split_once(' ').unwrap_or((read.trim(), ""));
    if running != "true" {
        return Ok(None);
    }
    ip.trim()
        .parse::<std::net::IpAddr>()
        .map(|ip| Some(ip.to_string()))
        .map_err(|_| format!("container {id} runs and holds no address on {network} (the daemon reads {:?})", ip.trim()))
}

/// The packet-filter chain a silence installs in each silenced container's own network namespace.
pub const SILENCE_CHAIN: &str = "RDM-SILENCE";

/// One privileged test process, opened before the silence clock starts. Commands
/// use its pipes, so Docker startup/teardown cannot delay a timed filter release.
#[derive(Debug)]
struct NamespaceTool {
    child: std::process::Child,
    input: Option<std::process::ChildStdin>,
    output: std::io::BufReader<std::process::ChildStdout>,
}

impl NamespaceTool {
    fn start(id: &str, install: &str, lift: &str) -> Result<Self, String> {
        use std::io::BufRead;
        use std::process::Stdio;
        let binary = std::fs::canonicalize("/usr/sbin/iptables")
            .map_err(|e| format!("resolve the host's filter implementation: {e}"))?;
        // Both selected xtables implementations accept the command as argv[1].
        // Arguments, including the rules, are passed directly, never shell-expanded.
        let script = r#"
set -o pipefail
printf 'RDM-TOOL-READY\n'
while IFS= read -r command; do
    case "$command" in
        install) printf '%s' "$2" | "$1" iptables-restore --noflush 2>&1 ;;
        rules) "$1" iptables -S 2>&1 ;;
        lift) printf '%s' "$3" | "$1" iptables-restore --noflush 2>&1 ;;
        *) printf 'unknown fixture command: %s\n' "$command"; false ;;
    esac
    status=$?
    printf '\nRDM-TOOL-END:%s\n' "$status"
done
"#;
        let mut child = Command::new("docker")
            .args(["exec", "--user", "0", "--privileged", "-i", id, "/usr/bin/bash", "-c", script, "--"])
            .arg(binary).arg(install).arg(lift)
            .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped())
            .spawn().map_err(|e| format!("start the exact namespace tool for {id}: {e}"))?;
        let input = child.stdin.take();
        let output = std::io::BufReader::new(child.stdout.take().unwrap());
        let mut tool = Self { child, input, output };
        let mut ready = String::new();
        tool.output.read_line(&mut ready).map_err(|e| format!("namespace {id} readiness: {e}"))?;
        if ready.trim() != "RDM-TOOL-READY" {
            return Err(tool.ended(&format!("namespace {id} did not become ready: {ready:?}")));
        }
        Ok(tool)
    }

    fn ended(&mut self, context: &str) -> String {
        use std::io::Read;
        self.input.take();
        let _ = self.child.kill();
        let _ = self.child.wait();
        let mut stderr = String::new();
        if let Some(mut pipe) = self.child.stderr.take() { let _ = pipe.read_to_string(&mut stderr); }
        format!("{context}; stderr {stderr}")
    }

    fn command(&mut self, command: &str) -> Result<String, String> {
        use std::io::{BufRead, Write};
        let input = self.input.as_mut().ok_or("the namespace tool has ended")?;
        writeln!(input, "{command}").and_then(|()| input.flush()).map_err(|e| format!("send {command} to the namespace tool: {e}"))?;
        let mut output = String::new();
        loop {
            let mut line = String::new();
            if self.output.read_line(&mut line).map_err(|e| format!("read namespace {command}: {e}"))? == 0 {
                return Err(self.ended(&format!("namespace tool ended during {command}: {output}")));
            }
            if let Some(status) = line.trim().strip_prefix("RDM-TOOL-END:") {
                return if status == "0" { Ok(output.trim().to_string()) } else { Err(format!("namespace {command} refused ({status}): {output}")) };
            }
            output.push_str(&line);
        }
    }
}

impl Drop for NamespaceTool {
    fn drop(&mut self) {
        // EOF ends the fixture's shell; no process or helper container is left behind.
        self.input.take();
        let _ = self.child.wait();
    }
}

/// One container of an estate with the address it holds.
#[derive(Debug, Clone, Serialize)]
pub struct Member {
    /// The node's `path.name`.
    pub node: String,
    /// The Docker id of the container.
    pub id: String,
    /// The container's address on the Fabric network.
    pub ip: String,
}

/// A mesh's containers silenced against everything outside the set: each keeps its interface, its
/// address, its sockets, its processes and every other member of the set, and drops all packets
/// to and from the rest of the estate's containers and all UDP to and from the host (the
/// transport's protocol). A scenario reads an isolated admin's view with a Node RPC probe
/// running inside that container's network namespace. A real network cut, never a container stop.
#[derive(Debug, Clone, Serialize)]
pub struct Silenced {
    /// The Fabric's Docker network.
    pub network: String,
    /// The network's gateway address on the host.
    pub gateway: String,
    /// The containers on the silenced side.
    pub members: Vec<Member>,
    /// The containers the members no longer hear.
    pub unheard: Vec<Member>,
    /// Whether the members also drop all UDP to and from the host ([`silence`]); a [`cut`] leaves the
    /// host reaching both sides.
    pub host_cut: bool,
    #[serde(skip)]
    tools: std::sync::Arc<std::collections::BTreeMap<String, std::sync::Mutex<NamespaceTool>>>,
}

/// Silence `nodes` against the rest of the estate. `fabric_primary` is the node-admin holding the
/// fabric-primary role: a set that holds it is refused before anything is touched. A member that
/// cannot be silenced leaves the ones already silenced restored, and the error says which.
pub async fn silence(estate: &impl ContainerEstate, nodes: &[String], fabric_primary: &str) -> Result<Silenced, String> {
    let silenced = prepare_silence(estate, nodes, fabric_primary).await?;
    silenced.install()?;
    Ok(silenced)
}

/// Resolve the exact containers and addresses before the timed silence starts; install nothing.
pub async fn prepare_silence(estate: &impl ContainerEstate, nodes: &[String], fabric_primary: &str) -> Result<Silenced, String> {
    if nodes.iter().any(|n| n == fabric_primary) {
        return Err(format!("refused: the silenced set {nodes:?} holds the fabric-primary node-admin {fabric_primary}"));
    }
    let gateway = gateway_of(estate)?;
    let mut members = Vec::new();
    for n in nodes {
        let id = estate.container_of(n).await.ok_or_else(|| format!("{n}: no running container of fabric {}", estate.fabric_id()))?;
        members.push(Member { node: n.clone(), ip: address_of(estate, &id)?, id });
    }
    // The rest of the estate as the daemon lists it running. One that exits before its address is
    // read (a superseded birth's runtime being terminated) holds no address and hears no one: no rule.
    let network = network_of(estate);
    let mut unheard = Vec::new();
    for (n, id) in estate.live_containers().into_iter().filter(|(n, _)| !nodes.contains(n)) {
        if let Some(ip) = running_address(&network, &id)? {
            unheard.push(Member { node: n, ip, id });
        }
    }
    with_tools(Silenced { network, gateway, members, unheard, host_cut: true, tools: Default::default() })
}

/// Cut the containers of `a` from the containers of `b`: in each container of `a`, every packet to and
/// from each container of `b` is dropped. The host and every other container still reach both sides,
/// as the process provider's UDP drop between two port sets does. Installed before it returns;
/// [`Silenced::lift`] ends it. A node that runs in no container of the fabric is refused by name.
pub async fn cut(estate: &impl ContainerEstate, a: &[String], b: &[String]) -> Result<Silenced, String> {
    let gateway = gateway_of(estate)?;
    let mut sides = [Vec::new(), Vec::new()];
    for (side, nodes) in sides.iter_mut().zip([a, b]) {
        for n in nodes {
            let id = estate.container_of(n).await.ok_or_else(|| format!("{n}: no running container of fabric {}", estate.fabric_id()))?;
            side.push(Member { node: n.clone(), ip: address_of(estate, &id)?, id });
        }
    }
    let [members, unheard] = sides;
    let cut = with_tools(Silenced { network: network_of(estate), gateway, members, unheard, host_cut: false, tools: Default::default() })?;
    cut.install()?;
    Ok(cut)
}

/// One namespace tool per member, each opened with the set's install and lift rules.
fn with_tools(mut silenced: Silenced) -> Result<Silenced, String> {
    let install = silenced.install_rules();
    let lift = lift_rules();
    let tools: Result<std::collections::BTreeMap<_, _>, String> = on_members(&silenced.members, |m| {
        NamespaceTool::start(&m.id, &install, &lift).map(|tool| (m.id.clone(), std::sync::Mutex::new(tool)))
    }).into_iter().collect();
    silenced.tools = std::sync::Arc::new(tools?);
    Ok(silenced)
}

fn on_members<T: Send>(members: &[Member], action: impl Fn(&Member) -> T + Sync) -> Vec<T> {
    std::thread::scope(|scope| {
        let action = &action;
        let tasks: Vec<_> = members.iter().map(|m| scope.spawn(move || action(m))).collect();
        tasks.into_iter().map(|task| task.join().expect("the exact container operation completed")).collect()
    })
}

impl Silenced {
    fn command(&self, member: &Member, command: &str) -> Result<String, String> {
        self.tools.get(&member.id).ok_or_else(|| format!("{}: no prepared namespace tool", member.node))?
            .lock().map_err(|_| format!("{}: the namespace tool ended while held", member.node))?
            .command(command).map_err(|e| format!("{}: {e}", member.node))
    }

    /// Install each independent namespace's filter concurrently; restore successes on failure.
    pub fn install(&self) -> Result<(), String> {
        let results = on_members(&self.members, |m| self.command(m, "install"));
        let failed: Vec<_> = self.members.iter().zip(&results).filter_map(|(m, r)| r.as_ref().err().map(|e| format!("{}: {e}", m.node))).collect();
        if failed.is_empty() { return Ok(()); }
        let applied: Vec<_> = self.members.iter().zip(&results).filter(|(_, r)| r.is_ok()).map(|(m, _)| m.clone()).collect();
        let back: Vec<_> = on_members(&applied, |m| self.command(m, "lift")).into_iter().filter_map(Result::err).collect();
        Err(format!("{} ({} already silenced were restored; failures: {back:?})", failed.join("; "), applied.len()))
    }

    /// Read every exact container in one daemon query; an unreadable container is named.
    pub fn inspect(&self) -> Result<std::collections::BTreeMap<String, Inspected>, String> {
        let mut args = vec!["inspect"];
        args.extend(self.members.iter().map(|m| m.id.as_str()));
        let raw = docker(&args)?;
        let rows: Vec<serde_json::Value> = serde_json::from_str(&raw).map_err(|e| format!("docker inspect the silenced containers: {e}"))?;
        self.members.iter().map(|member| {
            let row = rows.iter().find(|row| row["Id"] == member.id)
                .ok_or_else(|| format!("{}: the daemon holds no exact container {}", member.node, member.id))?;
            Ok((member.node.clone(), inspected(row)))
        }).collect()
    }

    fn install_rules(&self) -> String {
        let mut r = format!("*filter\n:{SILENCE_CHAIN} - [0:0]\n");
        for u in &self.unheard {
            r += &format!("-A {SILENCE_CHAIN} -s {ip} -j DROP\n-A {SILENCE_CHAIN} -d {ip} -j DROP\n", ip = u.ip);
        }
        if self.host_cut {
            r += &format!("-A {SILENCE_CHAIN} -s {gw} -p udp -j DROP\n-A {SILENCE_CHAIN} -d {gw} -p udp -j DROP\n", gw = self.gateway);
        }
        r += &format!("-I INPUT 1 -j {SILENCE_CHAIN}\n-I OUTPUT 1 -j {SILENCE_CHAIN}\nCOMMIT\n");
        r
    }

    /// The rules each member holds now, as its own packet filter lists them.
    pub fn rules(&self) -> Result<std::collections::BTreeMap<String, String>, String> {
        on_members(&self.members, |m| Ok((m.node.clone(), self.command(m, "rules")?))).into_iter().collect()
    }

    /// Every member holds the silence chain, referenced from INPUT and OUTPUT.
    pub fn active(&self) -> Result<bool, String> {
        Ok(self.rules()?.values().all(|r| r.contains(&format!("-A INPUT -j {SILENCE_CHAIN}")) && r.contains(&format!("-A OUTPUT -j {SILENCE_CHAIN}"))))
    }

    /// Restore every member's packet filter. Every failure is named, except a member whose exact container
    /// the daemon no longer runs: its filter ended with it (node recovery replaced a silenced birth with a
    /// new one, in a new container outside the silence, as the process provider's re-created node binds new
    /// ports outside its cut).
    pub fn lift(&self) -> Result<(), String> {
        let gone = |m: &Member| docker(&["inspect", "-f", "{{.State.Running}}", &m.id]).map_or(true, |running| running.trim() != "true");
        let failed: Vec<String> = on_members(&self.members, |m| self.command(m, "lift").err().filter(|_| !gone(m)).map(|e| format!("{}: {e}", m.node))).into_iter().flatten().collect();
        if failed.is_empty() {
            Ok(())
        } else {
            Err(failed.join("; "))
        }
    }
}

fn lift_rules() -> String {
    format!("*filter\n-D INPUT -j {SILENCE_CHAIN}\n-D OUTPUT -j {SILENCE_CHAIN}\n-F {SILENCE_CHAIN}\n-X {SILENCE_CHAIN}\nCOMMIT\n")
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
    /// The Docker id of the container.
    pub id: String,
    /// `running`, `paused`, `exited`...
    pub status: String,
    /// Whether the container is running.
    pub running: bool,
    /// The process id.
    pub pid: u64,
    /// When the container started.
    pub started_at: String,
    /// How many times the daemon restarted the container.
    pub restart_count: u64,
    /// The container's exit code.
    pub exit_code: i64,
    /// Whether the daemon killed it for memory.
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
    Ok(inspected(c))
}

fn inspected(c: &serde_json::Value) -> Inspected {
    let st = &c["State"];
    let mut networks: Vec<String> = c["NetworkSettings"]["Networks"].as_object().map(|m| m.keys().cloned().collect()).unwrap_or_default();
    networks.sort();
    Inspected {
        id: c["Id"].as_str().unwrap_or_default().to_string(),
        status: st["Status"].as_str().unwrap_or_default().to_string(),
        running: st["Running"].as_bool().unwrap_or(false),
        pid: st["Pid"].as_u64().unwrap_or(0),
        started_at: st["StartedAt"].as_str().unwrap_or_default().to_string(),
        restart_count: c["RestartCount"].as_u64().unwrap_or(0),
        exit_code: st["ExitCode"].as_i64().unwrap_or(0),
        oom_killed: st["OOMKilled"].as_bool().unwrap_or(false),
        networks,
    }
}

/// The `(rafka.fabric, rafka.node)` labels the provider put on container `id`.
pub fn labels_of(id: &str) -> Result<(String, String), String> {
    let out = docker(&["inspect", "--format", "{{index .Config.Labels \"rafka.fabric\"}} {{index .Config.Labels \"rafka.node\"}}", id])?;
    let (fabric, node) = out.split_once(' ').unwrap_or((out.as_str(), ""));
    Ok((fabric.to_string(), node.to_string()))
}

/// What an exact-container fault does to the container.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// `docker kill`: SIGKILL to the container's init; nothing is flushed. It takes the witness that the
    /// fabric first reached ready: a death during fabric start is not tested (Q74,
    /// [`crate::estate::FabricReady`]).
    Kill(crate::estate::FabricReady),
    /// `docker pause`: the container's processes are frozen, alive and silent.
    Pause,
    /// `docker unpause`: the freeze is released.
    Unpause,
}

impl Fault {
    /// Name.
    pub fn name(self) -> &'static str {
        match self {
            Fault::Kill(_) => "kill",
            Fault::Pause => "pause",
            Fault::Unpause => "unpause",
        }
    }
}

impl Serialize for Fault {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.name())
    }
}

/// Why a fault was not applied. Nothing was sent to the daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "refusal", rename_all = "snake_case")]
pub enum Refusal {
    /// The daemon holds no container with this id.
    NoSuchContainer {
        /// The id.
        id: String,
    },
    /// The daemon could not be read.
    Unreadable {
        /// The id.
        id: String,
        /// The reason.
        reason: String,
    },
    /// The container belongs to another Fabric.
    WrongFabric {
        /// The id.
        id: String,
        /// The published.
        published: String,
        /// The observed.
        observed: String,
    },
    /// The container runs another node.
    WrongNode {
        /// The id.
        id: String,
        /// The published.
        published: String,
        /// The observed.
        observed: String,
    },
    /// The container at this id started at another time: it is another run of the container now.
    NotThisContainer {
        /// The id.
        id: String,
        /// The published started at.
        published_started_at: String,
        /// The observed started at.
        observed_started_at: String,
    },
    /// The container is not running.
    AlreadyExited {
        /// The id.
        id: String,
        /// The status.
        status: String,
        /// The exit code.
        exit_code: i64,
    },
    /// The daemon refused the command.
    CommandFailed {
        /// The id.
        id: String,
        /// The fault.
        fault: Fault,
        /// The reason.
        reason: String,
    },
    /// The command was accepted and the daemon did not show its consequence within the bound.
    NotAcknowledged {
        /// The id.
        id: String,
        /// The fault.
        fault: Fault,
        /// The status.
        status: String,
    },
}

/// The daemon's observation that acknowledged an applied fault.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Applied {
    /// The fault applied.
    pub fault: Fault,
    /// The exact container the fault was applied to.
    pub container: ExactContainer,
    /// The container's status after the command (`paused`, `running`, `exited`).
    pub status_after: String,
    /// The container is no longer running: the terminal observation of a kill.
    pub exited: bool,
    /// The container's exit code.
    pub exit_code: i64,
}

/// ONE container of an estate: its Docker id, the Fabric and node labels it carried and the time
/// it started. A fault acts on it only while the daemon still holds exactly that container.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExactContainer {
    /// The Docker id of the container.
    pub id: String,
    /// The Fabric label the container carried.
    pub fabric_id: String,
    /// The node's `path.name`.
    pub node: String,
    /// When the container started.
    pub started_at: String,
    /// The process id.
    pub pid: u64,
}

impl ExactContainer {
    /// The container the daemon holds for `node` of the estate's Fabric right now.
    pub async fn of(estate: &impl ContainerEstate, node: &str) -> Result<Self, Refusal> {
        let id = estate.container_of(node).await.ok_or_else(|| Refusal::NoSuchContainer { id: format!("{node} of fabric {}", estate.fabric_id()) })?;
        let i = inspect(&id).map_err(|reason| Refusal::Unreadable { id: id.clone(), reason })?;
        let (fabric_id, node) = labels_of(&id).map_err(|reason| Refusal::Unreadable { id: id.clone(), reason })?;
        Ok(Self { id, fabric_id, node, started_at: i.started_at, pid: i.pid })
    }

    /// The same container under another start time: what a restarted container looks like.
    pub fn with_started_at(&self, started_at: &str) -> Self {
        Self { started_at: started_at.into(), ..self.clone() }
    }

    /// The same id under another node label.
    pub fn with_node(&self, node: &str) -> Self {
        Self { node: node.into(), ..self.clone() }
    }

    /// The same id under another Fabric label.
    pub fn with_fabric(&self, fabric_id: &str) -> Self {
        Self { fabric_id: fabric_id.into(), ..self.clone() }
    }

    /// Is the daemon holding exactly this container, running.
    pub fn check(&self) -> Result<Inspected, Refusal> {
        let i = match inspect(&self.id) {
            Ok(i) => i,
            Err(e) if e.to_lowercase().contains("no such") => return Err(Refusal::NoSuchContainer { id: self.id.clone() }),
            Err(reason) => return Err(Refusal::Unreadable { id: self.id.clone(), reason }),
        };
        let (fabric, node) = labels_of(&self.id).map_err(|reason| Refusal::Unreadable { id: self.id.clone(), reason })?;
        if fabric != self.fabric_id {
            return Err(Refusal::WrongFabric { id: self.id.clone(), published: self.fabric_id.clone(), observed: fabric });
        }
        if node != self.node {
            return Err(Refusal::WrongNode { id: self.id.clone(), published: self.node.clone(), observed: node });
        }
        if i.started_at != self.started_at {
            return Err(Refusal::NotThisContainer { id: self.id.clone(), published_started_at: self.started_at.clone(), observed_started_at: i.started_at });
        }
        if !i.running {
            return Err(Refusal::AlreadyExited { id: self.id.clone(), status: i.status, exit_code: i.exit_code });
        }
        Ok(i)
    }

    /// Apply `fault` to this exact container and wait for the daemon to acknowledge it.
    pub fn apply(&self, fault: Fault) -> Result<Applied, Refusal> {
        let span = tracing::info_span!(
            "rdm.testkit.fault.update.via-container-command",
            fault = fault.name(),
            container = %self.id,
            node = %self.node,
            fabric = %self.fabric_id,
            started_at = %self.started_at,
            outcome = tracing::field::Empty,
        );
        let _g = span.enter();
        let out = self.apply_inner(fault);
        span.record(
            "outcome",
            tracing::field::display(match &out {
                Ok(a) => serde_json::json!({"applied": a.fault.name(), "status_after": a.status_after, "exited": a.exited}),
                Err(r) => serde_json::to_value(r).unwrap_or(serde_json::Value::Null),
            }),
        );
        out
    }

    fn apply_inner(&self, fault: Fault) -> Result<Applied, Refusal> {
        let before = self.check()?;
        // A pause of a paused container and an unpause of a running one are the daemon's refusals,
        // named as they come.
        let verb = fault.name();
        let _ = before;
        docker(&[verb, &self.id]).map_err(|reason| Refusal::CommandFailed { id: self.id.clone(), fault, reason })?;
        let until = std::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            let now = inspect(&self.id).map_err(|reason| Refusal::Unreadable { id: self.id.clone(), reason })?;
            let acked = match fault {
                Fault::Kill(_) => !now.running,
                Fault::Pause => now.status == "paused",
                Fault::Unpause => now.status == "running",
            };
            if acked {
                return Ok(Applied { fault, container: self.clone(), status_after: now.status, exited: !now.running, exit_code: now.exit_code });
            }
            if std::time::Instant::now() >= until {
                return Err(Refusal::NotAcknowledged { id: self.id.clone(), fault, status: now.status });
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A container the daemon listed running that exits before its address is read (a birth's
    /// runtime being terminated) keeps its endpoint with the address released: Docker 29 renders it
    /// `invalid IP`. It holds no address and is heard by no one, so the silence writes no rule for
    /// it; a running container that holds no address is refused by name; every address the ruleset
    /// names is an address.
    #[test]
    fn a_container_that_exits_before_its_address_is_read_takes_no_rule_and_every_rule_names_an_address() {
        let net = "rafka-8ndbz2p393dm";
        assert_eq!(running_address_in(net, "a", "true 10.231.0.7\n"), Ok(Some("10.231.0.7".to_string())));
        assert_eq!(running_address_in(net, "b", "false invalid IP"), Ok(None), "an exited container holds no address");
        assert_eq!(running_address_in(net, "c", "false "), Ok(None), "an exited container holds no address");
        assert!(running_address_in(net, "d", "true invalid IP").is_err(), "a running container without an address is refused");
        assert!(running_address_in(net, "e", "true ").is_err(), "a running container off the network is refused");

        // The soak's listing at the failed unheard step: the third container listed had exited.
        let listed = [("mesh1.gateway.1", "true 10.231.0.31"), ("mesh1.broker.1", "true 10.231.0.30"), ("mesh1.gateway.1", "false invalid IP"), ("mesh1.admin.1", "true 10.231.0.2")];
        let mut unheard = Vec::new();
        for (i, (node, read)) in listed.iter().enumerate() {
            if let Some(ip) = running_address_in(net, &i.to_string(), read).expect("a listed container's read") {
                unheard.push(Member { node: node.to_string(), id: i.to_string(), ip });
            }
        }
        let silenced = Silenced { network: net.into(), gateway: "10.231.0.1".into(), members: Vec::new(), unheard, host_cut: true, tools: Default::default() };
        let rules = silenced.install_rules();
        for line in rules.lines().filter(|l| l.starts_with("-A ")) {
            let words: Vec<&str> = line.split_whitespace().collect();
            let at = words.iter().position(|w| *w == "-s" || *w == "-d").unwrap_or_else(|| panic!("a rule names an address: {line}"));
            assert!(words[at + 1].parse::<std::net::IpAddr>().is_ok() && words[at + 2].starts_with('-'), "line names no address: {line}\n{rules}");
        }
    }
}
