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

use super::provider::{tail, DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use crate::model::ProviderKind;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
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
    /// The network's name.
    pub name: String,
    /// The network's base address.
    pub base: Ipv4Addr,
    /// The network's prefix length.
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

/// The node addresses this provider handed out on the fabric network.
#[derive(Default)]
struct NodeIps {
    next: u32,
    held: HashMap<crate::model::PathName, IpAddr>,
}

/// Realises runtimes as containers on the fabric's bridge network.
pub struct ContainerDeploymentProvider {
    fabric: String,
    network: FabricNetwork,
    /// Exit codes of containers this provider removed, so `inspect` still
    /// answers for them.
    exited: Mutex<HashMap<String, Option<i32>>>,
    /// Each live container (by immutable id): its data dir, where its logs
    /// are kept on removal, and its deployment.
    ips: Mutex<NodeIps>,
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

/// Every address a container holds on `network` right now (`docker network inspect`). `Err`
/// names the inspect that failed: a daemon that cannot answer never reads as no address taken.
pub(crate) async fn network_addresses(network: &str) -> Result<std::collections::BTreeSet<IpAddr>, String> {
    docker(&["network", "inspect", "--format", "{{range .Containers}}{{.IPv4Address}} {{end}}", network])
        .await
        .map(|out| parse_network_addresses(&out))
        .map_err(|e| format!("the addresses taken on network {network}: {e}"))
}

/// The addresses of a `docker network inspect` listing (`<ip>/<prefix>` separated by spaces).
fn parse_network_addresses(out: &str) -> std::collections::BTreeSet<IpAddr> {
    out.split_whitespace().filter_map(|a| a.split('/').next()?.parse().ok()).collect()
}

/// Does a `docker inspect` failure say no object has that name (and nothing else)?
fn inspect_says_absent(err: &str) -> bool {
    let err = err.to_ascii_lowercase();
    err.contains("no such object") || err.contains("no such container")
}

/// `uid:gid` of this process, from the kernel: a container runs as the user that launched it.
pub fn current_user() -> String {
    // SAFETY: getuid and getgid take no arguments, always succeed and touch no memory.
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    format!("{uid}:{gid}")
}

/// The group owning the Docker daemon's socket at `socket`: a containerised node-admin joins it
/// to drive the daemon as the launching user. `None` when no socket is there; `Err` names a
/// socket path that cannot be looked at. A blocking call: run it on the blocking pool.
fn socket_group_at(socket: &Path) -> Result<Option<u32>, String> {
    use std::os::unix::fs::MetadataExt;
    match std::fs::metadata(socket) {
        Ok(m) => Ok(Some(m.gid())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("docker socket {}: {e}", socket.display())),
    }
}

/// Is there a directory at `path`? `false` when nothing is there; `Err` names a path that cannot
/// be looked at.
fn dir_present(path: &Path) -> Result<bool, String> {
    match std::fs::metadata(path) {
        Ok(m) => Ok(m.is_dir()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

/// What a launch's disk holds for its `docker run`, read in one blocking call.
#[derive(Debug, Default)]
struct LaunchDisk {
    /// The runtime library directories this host has.
    runtime_mounts: Vec<&'static str>,
    /// A node-admin's Docker socket to mount, and the group that owns it.
    docker_socket_gid: Option<u32>,
    /// A node-admin's binary directory (`RDM_BIN_DIR`), when it is a directory.
    bin_dir: Option<String>,
    /// A node-admin's fabric data root: the data dir's parent, when it is a directory.
    data_root: Option<PathBuf>,
    /// A node-admin's explicit executable binding directories.
    binding_dirs: Vec<PathBuf>,
    /// The span evidence directory (`RDM_EVIDENCE_DIR`), when it is a directory.
    evidence_dir: Option<String>,
}

/// Prepare a launch's disk: make the data dir, look at what is mounted, and remove a runtime
/// record an earlier birth left in the data dir (the birth waits for the one the pipeline makes
/// available). Every failure names its file. A blocking call: run it on the blocking pool.
fn launch_disk(data_dir: &Path, admin: bool, env: &std::collections::BTreeMap<String, String>) -> Result<LaunchDisk, String> {
    std::fs::create_dir_all(data_dir).map_err(|e| format!("data dir {}: {e}", data_dir.display()))?;
    let mut disk = LaunchDisk::default();
    for m in RUNTIME_MOUNTS {
        match std::fs::metadata(m) {
            Ok(_) => disk.runtime_mounts.push(m),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(format!("runtime library directory {m}: {e}")),
        }
    }
    if admin {
        if std::env::var_os("DOCKER_HOST").is_none() {
            disk.docker_socket_gid = socket_group_at(Path::new(DOCKER_SOCKET))?;
        }
        if let Some(dir) = env.get("RDM_BIN_DIR") {
            disk.bin_dir = dir_present(Path::new(dir)).map_err(|e| format!("RDM_BIN_DIR {e}"))?.then(|| dir.clone());
        }
        if let Some(root) = data_dir.parent() {
            disk.data_root = dir_present(root).map_err(|e| format!("data root {e}"))?.then(|| root.to_path_buf());
        }
        if let Some(file) = env.get(rafka_mesh_entity::binding::ENV_EXECUTABLE_BINDINGS) {
            let mut dirs: std::collections::BTreeSet<PathBuf> = Path::new(file).parent().map(Path::to_path_buf).into_iter().collect();
            let set = rafka_mesh_entity::binding::BindingSet::load(Path::new(file)).map_err(|e| format!("executable bindings {file}: {e}"))?;
            dirs.extend(set.bindings.iter().filter_map(|b| b.executable.parent().map(Path::to_path_buf)));
            let bin_dir = disk.bin_dir.as_ref().map(PathBuf::from);
            for d in dirs {
                if Some(&d) != bin_dir.as_ref() && !disk.data_root.as_ref().is_some_and(|r| d.starts_with(r)) && dir_present(&d).map_err(|e| format!("executable binding directory {e}"))? {
                    disk.binding_dirs.push(d);
                }
            }
        }
    }
    if let Some(dir) = env.get("RDM_EVIDENCE_DIR") {
        disk.evidence_dir = dir_present(Path::new(dir)).map_err(|e| format!("RDM_EVIDENCE_DIR {e}"))?.then(|| dir.clone());
    }
    let record = data_dir.join(rafka_mesh_entity::runtime::RUNTIME_FILE);
    match std::fs::remove_file(&record) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(format!("an earlier birth's runtime record {} cannot be removed: {e}", record.display())),
    }
    Ok(disk)
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
#[cfg(test)]
pub(crate) fn netns_listens_tcp(pid: u32, addr: SocketAddr) -> Result<bool, String> {
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
#[cfg(test)]
pub(crate) fn process_holds(pid: u32, table: &str, addr: SocketAddr, state: Option<&str>) -> Result<bool, String> {
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

/// `RDM_CONTAINER_SUBNET_POOL` (default `10.231.0.0/16`): the IPv4 block
/// fabric networks are carved from, one `/24` each.
pub(crate) fn subnet_pool_from_env() -> Result<(Ipv4Addr, u8), String> {
    let raw = std::env::var("RDM_CONTAINER_SUBNET_POOL").unwrap_or_else(|_| "10.231.0.0/16".into());
    match parse_cidr(&raw) {
        Some((base, prefix)) if prefix <= 24 => Ok((base, prefix)),
        _ => Err(format!("RDM_CONTAINER_SUBNET_POOL={raw} is not an IPv4 block of /24 or wider")),
    }
}

/// The `/24`s of `pool`, starting at one picked from `fabric` so concurrent
/// fabrics rarely contend for the same one.
pub(crate) fn candidate_subnets(pool: (Ipv4Addr, u8), fabric: &str) -> Vec<Ipv4Addr> {
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
    Err(format!("cannot create network {name}: every /24 of RDM_CONTAINER_SUBNET_POOL is in use ({last})"))
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
            ips: Mutex::new(NodeIps::default()),
            domain: format!("container:{}", daemon.trim()),
        })
    }

    /// The fabric's bridge network.
    pub fn network(&self) -> &FabricNetwork {
        &self.network
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
    /// The live container this launch started, by its name.
    /// `Ok(None)` when no container has the name or it is not running; `Err` names a `docker
    /// inspect` that failed for any other reason, or answered what this build cannot read.
    async fn find_by_name(&self, spec: &ResolvedNodeLaunch) -> Result<Option<DeploymentHandle>, DeployError> {
        let name = container_name(spec);
        let named = |reason: String| DeployError::Spawn { node: spec.node.to_string(), reason: format!("finding container {name}: {reason}") };
        let state = match docker(&["inspect", "--format", "{{.State.Status}} {{.State.Pid}} {{.Id}}", &name]).await {
            Ok(state) => state,
            Err(e) if inspect_says_absent(&e) => return Ok(None),
            Err(e) => return Err(named(e)),
        };
        let mut parts = state.split_whitespace();
        let (Some(status), Some(pid), Some(id)) = (parts.next(), parts.next(), parts.next()) else {
            return Err(named(format!("docker inspect answered {state:?}, not `<status> <pid> <id>`")));
        };
        if status != "running" {
            return Ok(None);
        }
        let id = id.to_string();
        self.data_dirs.lock().unwrap().insert(id.clone(), (spec.data_dir.clone(), spec.deployment_id.clone()));
        Ok(Some(self.handle(spec.deployment_id.clone(), id, pid.parse().ok().filter(|p| *p > 0))))
    }

    fn launched_handles(&self) -> Vec<DeploymentHandle> {
        self.data_dirs.lock().unwrap().iter().map(|(id, (_, dep))| self.handle(dep.clone(), id.clone(), None)).collect()
    }

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
        let exe_name = spec.executable.file_name().ok_or_else(|| err(format!("{} names no file", spec.executable.display())))?;
        let exe_in = Path::new(BIN_DIR).join(exe_name);
        // The data dir, the mounts and an earlier birth's runtime record, on the blocking pool.
        let admin = spec.node.kind == rafka_mesh_entity::NodeKind::NodeAdmin;
        let disk = {
            let (data_dir, env) = (spec.data_dir.clone(), spec.env.clone());
            crate::record_store::blocking_io(move || launch_disk(&data_dir, admin, &env)).await.map_err(err)?
        };
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
        for m in &disk.runtime_mounts {
            args.extend(["-v".into(), format!("{m}:{m}:ro")]);
        }
        // A node-admin in a container drives the same Docker daemon (one provider control domain)
        // and hands it host paths: the daemon's socket, the binaries and the fabric's data root are
        // mounted at their host paths, so a path it names means the same thing to the daemon.
        if admin {
            if let Some(gid) = disk.docker_socket_gid {
                args.extend(["-v".into(), format!("{DOCKER_SOCKET}:{DOCKER_SOCKET}"), "--group-add".into(), gid.to_string()]);
            }
            // It proves a birth bound its sockets from the birth's own socket table
            // (`/proc/<pid>/net/*` of the container's init, a host pid): it sees host pids.
            args.extend(["--pid".into(), "host".into()]);
            // The Docker client keeps its configuration under HOME; the node's data dir is its own.
            args.extend(["-e".into(), format!("HOME={data}")]);
            if let Some(dir) = &disk.bin_dir {
                args.extend(["-v".into(), format!("{dir}:{dir}:ro")]);
            }
            if let Some(root) = &disk.data_root {
                args.extend(["-v".into(), format!("{}:{}", root.display(), root.display())]);
            }
            // Explicit executable bindings: the admin validates and re-hashes them at every launch, so it
            // reads the binding file and every bound executable at their host paths. A directory the
            // admin already has mounted is not mounted twice.
            for d in &disk.binding_dirs {
                args.extend(["-v".into(), format!("{}:{}:ro", d.display(), d.display())]);
            }
        }
        // The span evidence directory the launch names, at the same path: a container's spans
        // land beside every process's.
        if let Some(dir) = &disk.evidence_dir {
            args.extend(["-v".into(), format!("{dir}:{dir}")]);
        }
        for (k, v) in &spec.env {
            args.extend(["-e".into(), format!("{k}={v}")]);
        }
        args.push(RUNTIME_IMAGE.into());
        args.push(exe_in.display().to_string());
        args.extend(spec.args.iter().cloned());
        let argv: Vec<&str> = args.iter().map(String::as_str).collect();
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
        // Keep the logs beside the node's data, then remove the container. Logs that cannot be
        // kept fail the stop by name and leave the stopped container (and its logs) in place, so
        // a re-run keeps them before it removes it.
        let data_dir = self.data_dirs.lock().unwrap().get(name).map(|(d, _)| d.clone());
        if let Some(dir) = data_dir {
            let logs = Command::new("docker").args(["logs", name]).stdin(std::process::Stdio::null()).output().await.map_err(|e| err(format!("docker logs {name}: {e}")))?;
            if !logs.status.success() {
                return Err(err(format!("docker logs {name}: {}", String::from_utf8_lossy(&logs.stderr).trim())));
            }
            let file = dir.join("container.log");
            let written = file.clone();
            crate::record_store::blocking_io(move || std::fs::write(&written, [&logs.stdout[..], &logs.stderr[..]].concat()))
                .await
                .map_err(|e| err(format!("the container's logs cannot be kept at {}: {e}", file.display())))?;
        }
        docker(&["rm", "-f", name]).await.map_err(err)?;
        self.data_dirs.lock().unwrap().remove(name);
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

    async fn find(&self, spec: &ResolvedNodeLaunch) -> Result<Option<DeploymentHandle>, DeployError> {
        self.find_by_name(spec).await
    }

    fn launched(&self) -> Vec<DeploymentHandle> {
        self.launched_handles()
    }

    /// A container is given its own address on the fabric network before it starts (`--ip`): the
    /// one pre-boot address a container needs. Its ports are the operating system's. The
    /// addresses already taken on the network are asked of the daemon (no lock held across it);
    /// a daemon that cannot answer, or a network with no address left, is an error naming it.
    async fn bind_ip(&self, node: &crate::model::PathName) -> Result<IpAddr, DeployError> {
        let err = |reason: String| DeployError::Spawn { node: node.to_string(), reason };
        if let Some(ip) = self.ips.lock().unwrap().held.get(node) {
            return Ok(*ip);
        }
        let on_network = network_addresses(&self.network.name).await.map_err(err)?;
        let mut ips = self.ips.lock().unwrap();
        if let Some(ip) = ips.held.get(node) {
            return Ok(*ip);
        }
        let (first, last) = self.network.node_range();
        let (first, last) = (u32::from(first), u32::from(last));
        let taken: std::collections::BTreeSet<IpAddr> = ips.held.values().copied().chain(on_network).collect();
        let mut next = ips.next.clamp(first, last);
        for _ in 0..=(last - first) {
            let ip = IpAddr::from(Ipv4Addr::from(next));
            next = if next == last { first } else { next + 1 };
            if !taken.contains(&ip) {
                ips.next = next;
                ips.held.insert(node.clone(), ip);
                return Ok(ip);
            }
        }
        Err(err(format!("every node address of the fabric network {} is held ({} taken)", self.network.name, taken.len())))
    }

    async fn failure_detail(&self, handle: &DeploymentHandle, _data_dir: &Path) -> String {
        let name = match self.container_of(handle) {
            Ok(name) => name,
            Err(e) => return e.to_string(),
        };
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

    /// The negative half probes a port this process still owns but does not listen on (a
    /// connected stream's local end), never a port just handed back to the kernel: the table is
    /// the whole network namespace, and a released port is anyone's to listen on next.
    #[test]
    fn a_tcp_listener_is_found_and_a_bare_socket_is_not() {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        assert_eq!(netns_listens_tcp(std::process::id(), addr), Ok(true));
        let bare = std::net::TcpStream::connect(addr).unwrap();
        let bare_addr = bare.local_addr().unwrap();
        assert_ne!(bare_addr, addr);
        assert_eq!(netns_listens_tcp(std::process::id(), bare_addr), Ok(false), "held, connected, not listening");
        drop(bare);
        drop(l);
    }

    /// CONTRACT: `docker inspect` of a name no container has is "not found"; any other failure
    /// (a daemon that cannot be reached) is not, and is answered as the error it is.
    #[test]
    fn only_no_such_object_reads_as_no_container() {
        assert!(inspect_says_absent("docker inspect: error: no such object: rafka-x"));
        assert!(inspect_says_absent("docker inspect: Error: No such object: rafka-x"));
        assert!(inspect_says_absent("docker inspect: Error response from daemon: No such container: rafka-x"));
        assert!(!inspect_says_absent("docker inspect: Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?"));
        assert!(!inspect_says_absent("docker inspect: permission denied while trying to connect to the Docker daemon socket"));
    }

    /// CONTRACT: the addresses `docker network inspect` lists are the addresses taken.
    #[test]
    fn network_addresses_are_read_from_the_inspect_listing() {
        let got = parse_network_addresses("10.231.0.2/24 10.231.0.3/24 ");
        let want: std::collections::BTreeSet<IpAddr> = ["10.231.0.2".parse().unwrap(), "10.231.0.3".parse().unwrap()].into_iter().collect();
        assert_eq!(got, want);
    }

    /// CONTRACT: the container runs as this process's own user, read from the kernel, never a
    /// fallback.
    #[test]
    fn the_container_runs_as_this_process_user() {
        // An oracle independent of the kernel calls: the real ids `/proc/self/status` lists.
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        let real = |k: &str| status.lines().find_map(|l| l.strip_prefix(k)).and_then(|v| v.split_whitespace().next()).unwrap().to_string();
        assert_eq!(current_user(), format!("{}:{}", real("Uid:"), real("Gid:")));
    }

    /// CONTRACT: the Docker socket that is absent is no mount; a socket path that cannot be
    /// looked at is an error naming it, never "no socket".
    #[test]
    fn an_unreadable_docker_socket_is_an_error_naming_it() {
        let dir = crate::record_store::tempdir("container-socket");
        assert_eq!(socket_group_at(&dir.join("missing.sock")), Ok(None));
        let file = dir.join("file");
        std::fs::write(&file, b"x").unwrap();
        let under_file = file.join("docker.sock");
        let err = socket_group_at(&under_file).unwrap_err();
        assert!(err.contains(&under_file.display().to_string()), "{err}");
    }

    /// CONTRACT: a launch's disk is prepared in one blocking call: the data dir made, the runtime
    /// libraries that exist mounted, and a runtime record left by an earlier birth removed. A
    /// record it cannot remove fails the launch naming the file.
    #[test]
    fn a_stale_runtime_record_that_cannot_be_removed_fails_the_launch_naming_it() {
        let dir = crate::record_store::tempdir("container-launch-disk");
        let ok = dir.join("clean");
        let disk = launch_disk(&ok, false, &std::collections::BTreeMap::new()).unwrap();
        assert!(ok.is_dir(), "the data dir is made");
        assert!(disk.runtime_mounts.contains(&"/usr"), "{:?}", disk.runtime_mounts);
        let stale = dir.join("stale");
        let record = stale.join(rafka_mesh_entity::runtime::RUNTIME_FILE);
        std::fs::create_dir_all(record.join("held")).unwrap();
        let err = launch_disk(&stale, false, &std::collections::BTreeMap::new()).unwrap_err();
        assert!(err.contains(&record.display().to_string()), "{err}");
    }

    #[test]
    fn container_names_carry_only_safe_characters() {
        assert_eq!(docker_safe("mesh1.rpc.2/x y"), "mesh1.rpc.2-x-y");
    }
}
