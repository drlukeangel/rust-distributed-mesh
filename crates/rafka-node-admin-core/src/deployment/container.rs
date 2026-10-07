//! `ContainerDeploymentProvider`: one container per node, each in its own
//! network namespace on the fabric's own bridge network (PRD §10: the
//! container provider is the namespace/isolation/network-fault gate).
//!
//! Node-admin still assigns every advertised endpoint: each node owns one
//! address on the fabric network (`--ip`), and its sockets bind ports on that
//! address. `WaitForBind` reads the container's own socket table
//! (`/proc/<pid>/net/udp` of its init process), never the host's.
//!
//! The runtime image is empty: the node executable and the host's runtime
//! libraries are mounted read-only, and the node's data dir is mounted at the
//! same path, so the launch environment is identical to the process
//! provider's.

use super::endpoint::EndpointAllocator;
use super::provider::{tail, DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use crate::model::ProviderKind;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Mutex;
use tokio::process::Command;

/// The empty image every node container runs from.
pub const RUNTIME_IMAGE: &str = "rafka-node-runtime:empty";
/// Where the node executable is mounted inside the container.
const BIN_DIR: &str = "/rafka/bin";
/// The Docker daemon's socket a containerised node-admin drives.
const DOCKER_SOCKET: &str = "/var/run/docker.sock";
/// Host directories holding the executable's runtime libraries.
const RUNTIME_MOUNTS: &[&str] = &["/lib", "/lib64", "/usr"];

/// The fabric's bridge network.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricNetwork {
    pub name: String,
    pub base: Ipv4Addr,
    pub prefix: u8,
    /// The host's address on the network: node-admin is reachable here.
    pub gateway: Ipv4Addr,
}

impl FabricNetwork {
    /// Every node address of the network: after the gateway, before broadcast.
    pub fn node_range(&self) -> (Ipv4Addr, Ipv4Addr) {
        let base = u32::from(self.base);
        let size = 1u32 << (32 - self.prefix as u32);
        let first = u32::from(self.gateway).max(base) + 1;
        (Ipv4Addr::from(first), Ipv4Addr::from(base + size - 2))
    }
}

pub struct ContainerDeploymentProvider {
    fabric: String,
    network: FabricNetwork,
    /// Exit codes of containers this provider removed, so `inspect` still
    /// answers for them.
    exited: Mutex<HashMap<String, Option<i32>>>,
    /// Each live container (by immutable id): its data dir, where its logs
    /// are kept on removal, and its deployment.
    data_dirs: Mutex<HashMap<String, (std::path::PathBuf, crate::model::DeploymentId)>>,
    /// The Docker daemon this provider drives: the control domain a
    /// container id means something in.
    domain: String,
}

async fn docker(args: &[&str]) -> Result<String, String> {
    let out = Command::new("docker")
        .args(args)
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .map_err(|e| format!("docker {}: {e}", args.first().unwrap_or(&"")))?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        Err(format!("docker {}: {}", args.first().unwrap_or(&""), String::from_utf8_lossy(&out.stderr).trim()))
    }
}

/// One launch's container: fixed by node and deployment id, so a re-run finds it.
fn container_name(spec: &ResolvedNodeLaunch) -> String {
    format!("rafka-{}-{}", docker_safe(&spec.node.to_string()), docker_safe(&spec.deployment_id.0))
}

/// Container names allow `[a-zA-Z0-9][a-zA-Z0-9_.-]`.
/// Every address a container holds on `network` right now (`docker network inspect`).
pub fn network_addresses(network: &str) -> std::collections::BTreeSet<IpAddr> {
    let out = std::process::Command::new("docker").args(["network", "inspect", "--format", "{{range .Containers}}{{.IPv4Address}} {{end}}", network]).output();
    out.map(|o| String::from_utf8_lossy(&o.stdout).split_whitespace().filter_map(|a| a.split('/').next()?.parse().ok()).collect()).unwrap_or_default()
}

/// `uid:gid` of this process (`/proc/self/status`): a container runs as the user that launched it.
pub fn current_user() -> String {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |k: &str| status.lines().find_map(|l| l.strip_prefix(k)).and_then(|v| v.split_whitespace().next().map(String::from));
    match (field("Uid:"), field("Gid:")) {
        (Some(u), Some(g)) => format!("{u}:{g}"),
        _ => "0:0".into(),
    }
}

/// The group owning the Docker daemon's socket: a containerised node-admin joins it to drive the
/// daemon as the launching user.
pub fn socket_group() -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(DOCKER_SOCKET).ok().map(|m| m.gid())
}

fn docker_safe(s: &str) -> String {
    s.chars().map(|c| if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-') { c } else { '-' }).collect()
}

fn parse_cidr(cidr: &str) -> Option<(Ipv4Addr, u8)> {
    let (ip, prefix) = cidr.split_once('/')?;
    Some((ip.parse().ok()?, prefix.parse().ok()?))
}

/// Is UDP `addr` bound in the network namespace of `pid`? Reads
/// `/proc/<pid>/net/udp`: `sl local_address ...`, address as the in-memory
/// network-order word printed in hex, then `:port` in hex.
pub fn netns_holds_udp(pid: u32, addr: SocketAddr) -> Result<bool, String> {
    netns_table_holds(pid, "udp", addr, None)
}

/// Is TCP `addr` listened on in the network namespace of `pid`
/// (`/proc/<pid>/net/tcp`, state `0A` = LISTEN)?
pub fn netns_listens_tcp(pid: u32, addr: SocketAddr) -> Result<bool, String> {
    netns_table_holds(pid, "tcp", addr, Some("0A"))
}

/// Every UDP socket bound in the network namespace of `pid`, as
/// `/proc/<pid>/net/udp` lists them (IPv4 only; an unspecified bind is kept as such).
pub fn netns_udp_sockets(pid: u32) -> Result<Vec<SocketAddr>, String> {
    let path = format!("/proc/{pid}/net/udp");
    let table = std::fs::read_to_string(&path).map_err(|e| format!("{path}: {e}"))?;
    let mut out: Vec<SocketAddr> = table
        .lines()
        .skip(1)
        .filter_map(|line| {
            let local = line.split_whitespace().nth(1)?;
            let (ip, port) = local.split_once(':')?;
            let (ip, port) = (u32::from_str_radix(ip, 16).ok()?, u16::from_str_radix(port, 16).ok()?);
            Some(SocketAddr::new(IpAddr::from(ip.to_ne_bytes()), port))
        })
        .collect();
    out.sort();
    Ok(out)
}

/// Does process `pid` itself hold a socket at `addr` (`udp`, or a `tcp` listener)? The socket's
/// inode in the namespace table is matched against the process's own descriptors, so a port held
/// by another process (a squatter, or an earlier birth) never passes for this runtime's.
pub fn process_holds(pid: u32, table: &str, addr: SocketAddr, state: Option<&str>) -> Result<bool, String> {
    let path = format!("/proc/{pid}/net/{table}");
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{path}: {e}"))?;
    let inodes: Vec<String> = text
        .lines()
        .skip(1)
        .filter_map(|line| {
            let cols: Vec<&str> = line.split_whitespace().collect();
            if let Some(want) = state {
                if cols.get(3) != Some(&want) {
                    return None;
                }
            }
            let (ip, port) = cols.get(1)?.split_once(':')?;
            let (ip, port) = (u32::from_str_radix(ip, 16).ok()?, u16::from_str_radix(port, 16).ok()?);
            let ip = IpAddr::from(ip.to_ne_bytes());
            (port == addr.port() && (ip == addr.ip() || ip.is_unspecified())).then(|| cols.get(9).map(|s| s.to_string())).flatten()
        })
        .collect();
    if inodes.is_empty() {
        return Ok(false);
    }
    let fds = format!("/proc/{pid}/fd");
    let held = std::fs::read_dir(&fds)
        .map_err(|e| format!("{fds}: {e}"))?
        .flatten()
        .filter_map(|e| std::fs::read_link(e.path()).ok())
        .filter_map(|l| l.to_str().map(String::from))
        .any(|l| inodes.iter().any(|i| l == format!("socket:[{i}]")));
    Ok(held)
}

fn netns_table_holds(pid: u32, table: &str, addr: SocketAddr, state: Option<&str>) -> Result<bool, String> {
    let path = format!("/proc/{pid}/net/{table}");
    let table = std::fs::read_to_string(&path).map_err(|e| format!("{path}: {e}"))?;
    Ok(table.lines().skip(1).any(|line| {
        if let Some(want) = state {
            if line.split_whitespace().nth(3) != Some(want) {
                return false;
            }
        }
        let Some(local) = line.split_whitespace().nth(1) else { return false };
        let Some((ip, port)) = local.split_once(':') else { return false };
        let (Ok(ip), Ok(port)) = (u32::from_str_radix(ip, 16), u16::from_str_radix(port, 16)) else { return false };
        let ip = IpAddr::from(ip.to_ne_bytes());
        port == addr.port() && (ip == addr.ip() || ip.is_unspecified())
    }))
}

/// `RAFKA_CONTAINER_SUBNET_POOL` (default `10.231.0.0/16`): the IPv4 block
/// fabric networks are carved from, one `/24` each.
pub fn subnet_pool_from_env() -> Result<(Ipv4Addr, u8), String> {
    let raw = std::env::var("RAFKA_CONTAINER_SUBNET_POOL").unwrap_or_else(|_| "10.231.0.0/16".into());
    match parse_cidr(&raw) {
        Some((base, prefix)) if prefix <= 24 => Ok((base, prefix)),
        _ => Err(format!("RAFKA_CONTAINER_SUBNET_POOL={raw} is not an IPv4 block of /24 or wider")),
    }
}

/// The `/24`s of `pool`, starting at one picked from `fabric` so concurrent
/// fabrics rarely contend for the same one.
pub fn candidate_subnets(pool: (Ipv4Addr, u8), fabric: &str) -> Vec<Ipv4Addr> {
    let count = 1u32 << (24 - pool.1 as u32);
    let start = fabric.bytes().fold(0u32, |h, b| h.wrapping_mul(31).wrapping_add(b as u32)) % count;
    let base = u32::from(pool.0) & !((1u32 << (32 - pool.1 as u32)) - 1);
    (0..count).map(|i| Ipv4Addr::from(base + (((start + i) % count) << 8))).collect()
}

/// Create the fabric's network on a node-admin-chosen subnet: an `--ip`
/// assignment needs a user-configured subnet. A subnet another network
/// already uses is skipped.
async fn create_fabric_network(fabric: &str, name: &str) -> Result<(), String> {
    let pool = subnet_pool_from_env()?;
    let mut last = String::new();
    for net in candidate_subnets(pool, fabric) {
        let gateway = Ipv4Addr::from(u32::from(net) + 1);
        let subnet = format!("{net}/24");
        match docker(&["network", "create", "--label", &format!("rafka.fabric={fabric}"), "--subnet", &subnet, "--gateway", &gateway.to_string(), name]).await {
            Ok(_) => return Ok(()),
            // A concurrent admin of the same fabric created it first.
            Err(_) if docker(&["network", "inspect", name]).await.is_ok() => return Ok(()),
            Err(e) if e.contains("overlap") => last = e,
            Err(e) => return Err(format!("cannot create network {name} on {subnet}: {e}")),
        }
    }
    Err(format!("cannot create network {name}: every /24 of RAFKA_CONTAINER_SUBNET_POOL is in use ({last})"))
}

impl ContainerDeploymentProvider {
    /// Check the host can run containers, then make sure the runtime image
    /// and the fabric's network exist. An unsupported host is refused by name.
    pub async fn prepare(fabric: &str) -> Result<Self, DeployError> {
        let unsupported = |reason: String| DeployError::Unsupported { provider: ProviderKind::Container, reason };
        if !cfg!(target_os = "linux") {
            return Err(unsupported("the container provider needs Linux network namespaces".into()));
        }
        docker(&["version", "--format", "{{.Server.Version}}"])
            .await
            .map_err(|e| unsupported(format!("no reachable container runtime: {e}")))?;
        if docker(&["image", "inspect", RUNTIME_IMAGE]).await.is_err() {
            let mut child = Command::new("docker")
                .args(["import", "-", RUNTIME_IMAGE])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .map_err(|e| unsupported(format!("docker import: {e}")))?;
            {
                use tokio::io::AsyncWriteExt;
                let mut stdin = child.stdin.take().expect("piped");
                // An empty tar archive: two zero blocks.
                stdin.write_all(&[0u8; 1024]).await.map_err(|e| unsupported(format!("docker import: {e}")))?;
            }
            let out = child.wait_with_output().await.map_err(|e| unsupported(format!("docker import: {e}")))?;
            if !out.status.success() {
                return Err(unsupported(format!("cannot create {RUNTIME_IMAGE}: {}", String::from_utf8_lossy(&out.stderr).trim())));
            }
        }
        let name = format!("rafka-{}", docker_safe(fabric));
        if docker(&["network", "inspect", &name]).await.is_err() {
            create_fabric_network(fabric, &name).await.map_err(unsupported)?;
        }
        let ipam = docker(&["network", "inspect", "--format", "{{range .IPAM.Config}}{{.Subnet}} {{.Gateway}};{{end}}", &name])
            .await
            .map_err(|e| unsupported(e))?;
        let network = ipam
            .split(';')
            .filter_map(|c| {
                let mut parts = c.split_whitespace();
                let (base, prefix) = parse_cidr(parts.next()?)?;
                let gateway = parts.next().and_then(|g| g.parse().ok()).unwrap_or_else(|| Ipv4Addr::from(u32::from(base) + 1));
                (prefix <= 30).then(|| FabricNetwork { name: name.clone(), base, prefix, gateway })
            })
            .next()
            .ok_or_else(|| unsupported(format!("network {name} has no IPv4 subnet ({ipam})")))?;
        let daemon = docker(&["info", "--format", "{{.ID}}"]).await.map_err(unsupported)?;
        let daemon = if daemon.trim().is_empty() { docker(&["info", "--format", "{{.Name}}"]).await.map_err(unsupported)? } else { daemon };
        Ok(Self {
            fabric: fabric.into(),
            network,
            exited: Mutex::new(HashMap::new()),
            data_dirs: Mutex::new(HashMap::new()),
            domain: format!("container:{}", daemon.trim()),
        })
    }

    pub fn network(&self) -> &FabricNetwork {
        &self.network
    }

    /// The allocator for this fabric's nodes: one address each on its network.
    pub fn allocator(&self, ports: (u16, u16)) -> EndpointAllocator {
        let (first, last) = self.network.node_range();
        let network = self.network.name.clone();
        EndpointAllocator::per_node(first, last, ports.0, ports.1).with_held_elsewhere(super::endpoint::HeldElsewhere::new(move || network_addresses(&network)))
    }

    /// Remove every container of the fabric and its network.
    pub async fn remove_fabric(&self) -> Result<(), DeployError> {
        let err = |reason: String| DeployError::Terminate { deployment: format!("fabric {}", self.fabric), reason };
        let ids = docker(&["ps", "-aq", "--filter", &format!("label=rafka.fabric={}", self.fabric)]).await.map_err(err)?;
        for id in ids.split_whitespace() {
            docker(&["rm", "-f", id]).await.map_err(err)?;
        }
        docker(&["network", "rm", &self.network.name]).await.map_err(err)?;
        Ok(())
    }

    /// The handle's immutable container id.
    fn container_of<'a>(&self, handle: &'a DeploymentHandle) -> Result<&'a str, DeployError> {
        handle.container.as_deref().ok_or_else(|| DeployError::Terminate {
            deployment: handle.deployment_id.0.clone(),
            reason: "the handle names no container".into(),
        })
    }

    fn handle(&self, deployment_id: crate::model::DeploymentId, id: String, pid: Option<u32>) -> DeploymentHandle {
        DeploymentHandle { deployment_id, provider: ProviderKind::Container, pid, start: None, container: Some(id), domain: Some(self.domain.clone()) }
    }
}

#[async_trait::async_trait]
impl DeploymentProvider for ContainerDeploymentProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Container
    }

    fn control_domain(&self) -> String {
        self.domain.clone()
    }

    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        let err = |reason: String| DeployError::Spawn { node: spec.node.to_string(), reason };
        let ip = spec.transport.ip();
        if let Some((n, a)) = spec.listeners.iter().find(|(_, a)| a.ip() != ip) {
            return Err(err(format!("listener {n} is assigned {a}, but one container has one address ({ip})")));
        }
        std::fs::create_dir_all(&spec.data_dir).map_err(|e| err(format!("data dir {}: {e}", spec.data_dir.display())))?;
        let exe_name = spec.executable.file_name().ok_or_else(|| err(format!("{} names no file", spec.executable.display())))?;
        let exe_in = Path::new(BIN_DIR).join(exe_name);
        let name = container_name(spec);
        let data = spec.data_dir.display().to_string();
        let mut args: Vec<String> = vec![
            "run".into(),
            "-d".into(),
            "--name".into(),
            name.clone(),
            "--label".into(),
            format!("rafka.fabric={}", self.fabric),
            "--label".into(),
            format!("rafka.node={}", spec.node),
            "--network".into(),
            self.network.name.clone(),
            "--ip".into(),
            ip.to_string(),
            // As the launching user: what the node writes into host directories stays the user's.
            "--user".into(),
            current_user(),
            "-v".into(),
            format!("{}:{}:ro", spec.executable.display(), exe_in.display()),
            "-v".into(),
            format!("{data}:{data}"),
        ];
        for m in RUNTIME_MOUNTS.iter().filter(|m| Path::new(m).exists()) {
            args.extend(["-v".into(), format!("{m}:{m}:ro")]);
        }
        // A node-admin in a container drives the same Docker daemon (one provider control domain)
        // and hands it host paths: the daemon's socket, the binaries and the fabric's data root are
        // mounted at their host paths, so a path it names means the same thing to the daemon.
        if spec.node.kind == rafka_mesh_entity::NodeKind::NodeAdmin {
            if std::env::var_os("DOCKER_HOST").is_none() && Path::new(DOCKER_SOCKET).exists() {
                args.extend(["-v".into(), format!("{DOCKER_SOCKET}:{DOCKER_SOCKET}")]);
                if let Some(gid) = socket_group() {
                    args.extend(["--group-add".into(), gid.to_string()]);
                }
            }
            // It proves a birth bound its sockets from the birth's own socket table
            // (`/proc/<pid>/net/*` of the container's init, a host pid): it sees host pids.
            args.extend(["--pid".into(), "host".into()]);
            // The Docker client keeps its configuration under HOME; the node's data dir is its own.
            args.extend(["-e".into(), format!("HOME={data}")]);
            if let Some(dir) = spec.env.get("RAFKA_BIN_DIR").filter(|d| Path::new(d).is_dir()) {
                args.extend(["-v".into(), format!("{dir}:{dir}:ro")]);
            }
            if let Some(root) = spec.data_dir.parent().filter(|r| r.is_dir()) {
                args.extend(["-v".into(), format!("{}:{}", root.display(), root.display())]);
            }
        }
        // The span evidence directory the launch names, at the same path: a container's spans
        // land beside every process's.
        if let Some(dir) = spec.env.get("RAFKA_EVIDENCE_DIR").filter(|d| Path::new(d).is_dir()) {
            args.extend(["-v".into(), format!("{dir}:{dir}")]);
        }
        for (k, v) in &spec.env {
            args.extend(["-e".into(), format!("{k}={v}")]);
        }
        args.push(RUNTIME_IMAGE.into());
        args.push(exe_in.display().to_string());
        args.extend(spec.args.iter().cloned());
        let argv: Vec<&str> = args.iter().map(String::as_str).collect();
        // A record left by an earlier birth in this data dir is not this one's:
        // the birth waits for the one the pipeline makes available.
        let _ = std::fs::remove_file(spec.data_dir.join(rafka_mesh_entity::runtime::RUNTIME_FILE));
        // `docker run -d` answers the immutable id: the runtime's identity,
        // not the (reusable) name.
        let id = docker(&argv).await.map_err(err)?.trim().to_string();
        self.data_dirs.lock().unwrap().insert(id.clone(), (spec.data_dir.clone(), spec.deployment_id.clone()));
        let pid = docker(&["inspect", "--format", "{{.State.Pid}}", &id]).await.map_err(err)?.parse::<u32>().ok().filter(|p| *p > 0);
        // The pipeline registers this exact handle and makes its runtime fact
        // available to the birth (`MakeRuntimeFactAvailableToBirth`).
        Ok(self.handle(spec.deployment_id.clone(), id, pid))
    }

    async fn terminate(&self, handle: &DeploymentHandle, mode: TerminationMode) -> Result<(), DeployError> {
        let name = self.container_of(handle)?;
        let err = |reason: String| DeployError::Terminate { deployment: handle.deployment_id.0.clone(), reason };
        let grace = match mode {
            TerminationMode::Graceful { grace } => grace.as_secs().max(1),
            TerminationMode::Immediate => 0,
        };
        docker(&["stop", "-t", &grace.to_string(), name]).await.map_err(err)?;
        let code = docker(&["inspect", "--format", "{{.State.ExitCode}}", name]).await.ok().and_then(|c| c.parse().ok());
        // Keep the logs beside the node's data, then remove the container.
        let data_dir = self.data_dirs.lock().unwrap().remove(name).map(|(d, _)| d);
        if let (Some(dir), Ok(logs)) = (data_dir, Command::new("docker").args(["logs", name]).output().await) {
            let _ = std::fs::write(dir.join("container.log"), [&logs.stdout[..], &logs.stderr[..]].concat());
        }
        docker(&["rm", "-f", name]).await.map_err(err)?;
        self.exited.lock().unwrap().insert(name.to_string(), code);
        Ok(())
    }

    async fn inspect(&self, handle: &DeploymentHandle) -> DeploymentStatus {
        let Ok(name) = self.container_of(handle) else { return DeploymentStatus::Unknown };
        if let Some(code) = self.exited.lock().unwrap().get(name) {
            return DeploymentStatus::Exited { code: *code };
        }
        match docker(&["inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}", name]).await {
            Ok(s) => match s.split_once(' ') {
                Some(("running" | "restarting" | "paused", _)) => DeploymentStatus::Running,
                Some(("exited" | "dead", code)) => DeploymentStatus::Exited { code: code.parse().ok() },
                Some(("created", _)) => DeploymentStatus::Running,
                _ => DeploymentStatus::Unknown,
            },
            Err(_) => DeploymentStatus::Unknown,
        }
    }

    async fn signal_stop(&self, handle: &DeploymentHandle) -> Result<(), DeployError> {
        let name = self.container_of(handle)?;
        if self.inspect(handle).await == DeploymentStatus::Running {
            docker(&["kill", "--signal", "TERM", name])
                .await
                .map_err(|reason| DeployError::Terminate { deployment: handle.deployment_id.0.clone(), reason })?;
        }
        Ok(())
    }

    async fn find(&self, spec: &ResolvedNodeLaunch) -> Option<DeploymentHandle> {
        let name = container_name(spec);
        let state = docker(&["inspect", "--format", "{{.State.Status}} {{.State.Pid}} {{.Id}}", &name]).await.ok()?;
        let mut parts = state.split_whitespace();
        let (status, pid, id) = (parts.next()?, parts.next()?, parts.next()?.to_string());
        if status != "running" {
            return None;
        }
        self.data_dirs.lock().unwrap().insert(id.clone(), (spec.data_dir.clone(), spec.deployment_id.clone()));
        Some(self.handle(spec.deployment_id.clone(), id, pid.parse().ok().filter(|p| *p > 0)))
    }

    fn launched(&self) -> Vec<DeploymentHandle> {
        self.data_dirs.lock().unwrap().iter().map(|(id, (_, dep))| self.handle(dep.clone(), id.clone(), None)).collect()
    }

    async fn holds(&self, handle: &DeploymentHandle, addr: SocketAddr, transport: super::endpoint::SlotTransport) -> bool {
        handle.pid.is_some_and(|pid| match transport {
            super::endpoint::SlotTransport::Udp => netns_holds_udp(pid, addr).unwrap_or(false),
            super::endpoint::SlotTransport::Tcp => netns_listens_tcp(pid, addr).unwrap_or(false),
        })
    }

    async fn failure_detail(&self, handle: &DeploymentHandle, _data_dir: &Path) -> String {
        let Ok(name) = self.container_of(handle) else { return String::new() };
        match Command::new("docker").args(["logs", "--tail", "20", name]).output().await {
            Ok(o) => tail(&String::from_utf8_lossy(&[&o.stdout[..], &o.stderr[..]].concat()), 5),
            Err(e) => format!("docker logs: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_socket_is_held_only_by_the_process_whose_descriptor_it_is() {
        let me = std::process::id();
        let udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = udp.local_addr().unwrap();
        assert_eq!(super::process_holds(me, "udp", addr, None), Ok(true), "our own UDP socket");
        let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let taddr = tcp.local_addr().unwrap();
        assert_eq!(super::process_holds(me, "tcp", taddr, Some("0A")), Ok(true), "our own TCP listener");
        // The same ports, asked of a process that is not us (pid 1 holds neither): false, never a
        // host-wide "someone holds it".
        assert_eq!(super::process_holds(1, "udp", addr, None).unwrap_or(false), false);
        drop(udp);
        assert_eq!(super::process_holds(me, "udp", addr, None), Ok(false), "released: nobody holds it");
    }

    use super::*;

    #[test]
    fn node_addresses_start_after_the_gateway_and_stop_before_broadcast() {
        let n = FabricNetwork { name: "n".into(), base: Ipv4Addr::new(172, 18, 0, 0), prefix: 16, gateway: Ipv4Addr::new(172, 18, 0, 1) };
        assert_eq!(n.node_range(), (Ipv4Addr::new(172, 18, 0, 2), Ipv4Addr::new(172, 18, 255, 254)));
    }

    #[test]
    fn the_socket_table_is_read_in_network_byte_order() {
        // Both sides are read while the socket is held: the table is the whole network
        // namespace, so a port freed by a drop can be taken at once by any other binder on the
        // box. While 127.0.0.1:P is held, neither the wildcard nor 127.0.0.2:P can be bound.
        let sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = sock.local_addr().unwrap();
        let other: SocketAddr = (Ipv4Addr::new(127, 0, 0, 2), addr.port()).into();
        assert_eq!(netns_holds_udp(std::process::id(), addr), Ok(true));
        assert_eq!(netns_holds_udp(std::process::id(), other), Ok(false));
        assert!(netns_holds_udp(u32::MAX, addr).unwrap_err().contains("/proc/"));
        drop(sock);
    }

    #[test]
    fn fabric_subnets_are_every_slash_24_of_the_pool_once() {
        let pool = (Ipv4Addr::new(10, 231, 0, 0), 22);
        let nets = candidate_subnets(pool, "fabric-a");
        assert_eq!(nets.len(), 4);
        let mut sorted = nets.clone();
        sorted.sort();
        assert_eq!(sorted, vec![Ipv4Addr::new(10, 231, 0, 0), Ipv4Addr::new(10, 231, 1, 0), Ipv4Addr::new(10, 231, 2, 0), Ipv4Addr::new(10, 231, 3, 0)]);
        assert_eq!(candidate_subnets(pool, "fabric-a"), nets, "deterministic per fabric");
    }

    #[test]
    fn a_tcp_listener_is_found_and_a_bare_socket_is_not() {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        assert_eq!(netns_listens_tcp(std::process::id(), addr), Ok(true));
        drop(l);
        assert_eq!(netns_listens_tcp(std::process::id(), addr), Ok(false));
    }

    #[test]
    fn container_names_carry_only_safe_characters() {
        assert_eq!(docker_safe("mesh1.rpc.2/x y"), "mesh1.rpc.2-x-y");
    }
}
