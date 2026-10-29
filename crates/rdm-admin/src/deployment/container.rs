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

use super::provider::{tail, DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, Found, ResolvedNodeLaunch, TerminationMode};
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

/// The node addresses this provider handed out on the fabric network, each held by the birth (its
/// deployment) it was given to until that birth's container is removed.
#[derive(Default)]
struct NodeIps {
    next: u32,
    held: HashMap<crate::model::DeploymentId, IpAddr>,
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
    docker_with_env(args, &std::collections::BTreeMap::new()).await
}

/// [`docker`], the client run with `env` in its own environment as well (each secret a `-e K` names).
async fn docker_with_env(args: &[&str], env: &std::collections::BTreeMap<String, String>) -> Result<String, String> {
    // Killed on drop: a caller's bound (node.runtime.inspect's, Q273) that expires ends the client too.
    let out = Command::new("docker")
        .args(args)
        .envs(env)
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true)
        .output()
        .await
        .map_err(|e| format!("docker {}: {e}", args.first().unwrap_or(&"")))?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        Err(format!("docker {}: {}", args.first().unwrap_or(&""), String::from_utf8_lossy(&out.stderr).trim()))
    }
}

/// The `docker run` environment arguments of `spec`, and the environment the docker client itself is
/// run with (Q150 §0.1, the container boundary). A secret, every declared value set for the kind
/// (`spec.secrets`, decoded here, at the spawn) and RDM's four ([`rdm_core::launch::RDM_SECRETS`]),
/// rides as a bare `-e K` with its value in the client's environment, which keeps it off the
/// command-line arguments; the daemon's container configuration still holds a container's environment,
/// the accepted boundary. Every other entry is `-e K=V`.
pub(crate) fn env_args(spec: &ResolvedNodeLaunch) -> Result<(Vec<String>, std::collections::BTreeMap<String, String>), String> {
    let mut args = Vec::new();
    let mut client = std::collections::BTreeMap::new();
    for (k, v) in &spec.env {
        if rdm_core::launch::RDM_SECRETS.contains(&k.as_str()) {
            args.extend(["-e".into(), k.clone()]);
            client.insert(k.clone(), v.clone());
        } else {
            args.extend(["-e".into(), format!("{k}={v}")]);
        }
    }
    for (k, v) in &spec.secrets {
        args.extend(["-e".into(), k.clone()]);
        client.insert(k.clone(), v.value(k)?);
    }
    Ok((args, client))
}

/// One launch's container: fixed by node and deployment id, so a re-run finds it.
fn container_name(spec: &ResolvedNodeLaunch) -> String {
    format!("rafka-{}-{}", docker_safe(&spec.node.to_string()), docker_safe(&spec.deployment_id.as_str()))
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
/// The record a launch's spawn writes in its data dir, naming the launch, durable before the
/// container is created ([`super::provider::write_launch_record`]): a container this provider removed
/// (its terminate, `remove_fabric`) leaves the daemon nothing by its name, and this record is what says
/// the launch ran ([`ContainerDeploymentProvider::find`]).
const LAUNCH_FILE: &str = "container-launch.json";

/// Write the launch record of `deployment` in `data_dir`. A blocking call: run it on the blocking
/// pool.
fn record_launch(data_dir: &Path, deployment: &crate::model::DeploymentId) -> Result<(), String> {
    super::provider::write_launch_record(data_dir, LAUNCH_FILE, &serde_json::json!({ "deployment_id": deployment.as_str() }))
}

/// Whether the launch record in `data_dir` names `deployment`: `false` when none was written or it
/// names another launch; `Err` names a record that cannot be read or parsed, never read as "never
/// launched". A blocking call: run it on the blocking pool.
fn recorded_launch(data_dir: &Path, deployment: &crate::model::DeploymentId) -> Result<bool, String> {
    let file = data_dir.join(LAUNCH_FILE);
    let raw = match std::fs::read_to_string(&file) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(format!("{}: {e}", file.display())),
    };
    let v: serde_json::Value = serde_json::from_str(&raw).map_err(|e| format!("{}: {e}", file.display()))?;
    Ok(v.get("deployment_id").and_then(|d| d.as_str()) == Some(deployment.as_str()))
}

/// What a container's Docker status says of its launch ([`ContainerDeploymentProvider::find`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContainerLaunch {
    /// `running`: the runtime runs.
    Running,
    /// `exited` or `dead`: the runtime ran and has exited.
    Exited,
    /// `created`: Docker created the container and never started its runtime.
    NeverStarted,
}

/// The launch fact of the Docker status `status`; a status this build cannot place (`paused`,
/// `restarting`, `removing`, or one it does not know) is refused by name, never read as any of them.
fn container_launch(status: &str) -> Result<ContainerLaunch, String> {
    match status {
        "running" => Ok(ContainerLaunch::Running),
        "exited" | "dead" => Ok(ContainerLaunch::Exited),
        "created" => Ok(ContainerLaunch::NeverStarted),
        other => Err(format!("docker inspect answered status {other:?}: this build cannot say whether the launch runs, has exited or never started")),
    }
}

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
    /// A node-admin's executable directories (`RDM_EXECUTABLE_DIRS` of its launch), each once.
    executable_dirs: Vec<PathBuf>,
    /// The span evidence directory (`RDM_EVIDENCE_DIR`), when it is a directory.
    evidence_dir: Option<String>,
    /// The fabric namespace, mounted writable at its supplied absolute path for every node.
    store_dir: Option<PathBuf>,
}

/// Prepare a launch's disk: make the data dir, look at what is mounted, and remove a runtime
/// record an earlier birth left in the data dir (the birth waits for the one the pipeline makes
/// available). Every failure names its file. A blocking call: run it on the blocking pool.
fn launch_disk(data_dir: &Path, admin: bool, env: &std::collections::BTreeMap<String, String>) -> Result<LaunchDisk, String> {
    std::fs::create_dir_all(data_dir).map_err(|e| format!("data dir {}: {e}", data_dir.display()))?;
    let mut disk = LaunchDisk::default();
    if let Some(dir) = env.get(rdm_core::launch::ENV_STORE_DIR) {
        let path = Path::new(dir);
        if !path.is_absolute() {
            return Err(format!("RDM_STORE_DIR {dir:?}: the mounted store root must be an absolute directory"));
        }
        if !dir_present(path).map_err(|e| format!("RDM_STORE_DIR {e}"))? {
            return Err(format!("RDM_STORE_DIR {dir}: the supplied store root is missing or is not a directory"));
        }
        std::fs::read_dir(path).map_err(|e| format!("RDM_STORE_DIR {dir}: the supplied store root cannot be listed: {e}"))?;
        disk.store_dir = Some(path.to_path_buf());
    }

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
        // The executable directories its launch names (proposal executable-bindings-at-create.md §2),
        // which it re-hashes and launches from, at their host paths, read-only: one under the writable
        // data root is mounted again over it, read-only. A named directory that is not there is refused
        // by name: the admin could launch nothing from it.
        let named = rdm_core::launch::decode_executable_dirs(env.get(rdm_core::launch::ENV_EXECUTABLE_DIRS).map(String::as_str).unwrap_or_default())?;
        for d in named {
            if !dir_present(&d).map_err(|e| format!("{} {e}", rdm_core::launch::ENV_EXECUTABLE_DIRS))? {
                return Err(format!("{} names {}, which is not a directory on the host", rdm_core::launch::ENV_EXECUTABLE_DIRS, d.display()));
            }
            disk.executable_dirs.push(d);
        }
    }
    if let Some(dir) = env.get("RDM_EVIDENCE_DIR") {
        disk.evidence_dir = dir_present(Path::new(dir)).map_err(|e| format!("RDM_EVIDENCE_DIR {e}"))?.then(|| dir.clone());
    }
    let record = data_dir.join(rdm_core::runtime::RUNTIME_FILE);
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

/// Is UDP `addr` bound in the network namespace of `pid`? Reads `/proc/<pid>/net/udp`
/// (`udp_table_holds`). The kernel answers that table a page per read and resumes each page by
/// position, so `Ok(true)` proves the bind, and `Ok(false)` is no proof of absence in a table longer
/// than a page that changes between its pages.
pub fn netns_holds_udp(pid: u32, addr: SocketAddr) -> Result<bool, String> {
    Ok(udp_table_holds(&udp_table(pid)?, addr))
}

/// Every UDP socket bound in the network namespace of `pid`, as `/proc/<pid>/net/udp` lists them
/// (`udp_sockets_in`), read as [`netns_holds_udp`] reads it.
pub fn netns_udp_sockets(pid: u32) -> Result<Vec<SocketAddr>, String> {
    Ok(udp_sockets_in(&udp_table(pid)?))
}

fn udp_table(pid: u32) -> Result<String, String> {
    let path = format!("/proc/{pid}/net/udp");
    std::fs::read_to_string(&path).map_err(|e| format!("{path}: {e}"))
}

/// The local addresses a `/proc/<pid>/net/udp` table lists, sorted (IPv4 only; an unspecified bind is
/// kept as such): `sl local_address ...`, the address as the in-memory network-order word printed in
/// hex, then `:port` in hex.
fn udp_sockets_in(table: &str) -> Vec<SocketAddr> {
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
    out
}

/// Whether a `/proc/<pid>/net/udp` table lists `addr` bound: at its address, or unspecified at its port.
fn udp_table_holds(table: &str, addr: SocketAddr) -> bool {
    udp_sockets_in(table).iter().any(|a| a.port() == addr.port() && (a.ip() == addr.ip() || a.ip().is_unspecified()))
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

    /// What became of this launch's container, by its name (the name carries the deployment id):
    /// one that runs is the live runtime; one that exited or is dead ran and has exited; one Docker
    /// created and never started was never launched. None by that name: the launch's record
    /// ([`LAUNCH_FILE`], written before the container is created) in the data dir names it and no
    /// container of it is left, so it has exited (it ran and was removed, or its executor was lost
    /// before the container existed); else it was never launched. `Err` names a `docker inspect` that failed for any other reason, or
    /// answered what this build cannot read (a status it cannot place included), or a launch record
    /// that cannot be read.
    async fn find_by_name(&self, spec: &ResolvedNodeLaunch) -> Result<Found, DeployError> {
        let name = container_name(spec);
        let named = |reason: String| DeployError::Spawn { node: spec.node.to_string(), reason: format!("finding container {name}: {reason}") };
        let state = match docker(&["inspect", "--format", "{{.State.Status}} {{.State.Pid}} {{.Id}}", &name]).await {
            Ok(state) => state,
            Err(e) if inspect_says_absent(&e) => {
                let (dir, deployment) = (spec.data_dir.clone(), spec.deployment_id.clone());
                return match crate::blocking::blocking_io(move || recorded_launch(&dir, &deployment)).await.map_err(named)? {
                    true => Ok(Found::Exited(None)),
                    false => Ok(Found::NeverLaunched),
                };
            }
            Err(e) => return Err(named(e)),
        };
        let mut parts = state.split_whitespace();
        let (Some(status), Some(pid), Some(id)) = (parts.next(), parts.next(), parts.next()) else {
            return Err(named(format!("docker inspect answered {state:?}, not `<status> <pid> <id>`")));
        };
        match container_launch(status).map_err(named)? {
            ContainerLaunch::Running => {}
            ContainerLaunch::Exited => return Ok(Found::Exited(None)),
            ContainerLaunch::NeverStarted => return Ok(Found::NeverLaunched),
        }
        let id = id.to_string();
        self.data_dirs.lock().unwrap().insert(id.clone(), (spec.data_dir.clone(), spec.deployment_id.clone()));
        Ok(Found::Running(self.handle(spec.deployment_id.clone(), id, pid.parse().ok().filter(|p| *p > 0))))
    }

    fn launched_handles(&self) -> Vec<DeploymentHandle> {
        self.data_dirs.lock().unwrap().iter().map(|(id, (_, dep))| self.handle(dep.clone(), id.clone(), None)).collect()
    }

    /// The container `name` of `handle` is gone, and the address its birth held with it.
    fn removed(&self, handle: &DeploymentHandle, name: &str, code: Option<i32>) {
        self.ips.lock().unwrap().held.remove(&handle.deployment_id);
        self.data_dirs.lock().unwrap().remove(name);
        self.exited.lock().unwrap().insert(name.to_string(), code);
    }

    fn container_of<'a>(&self, handle: &'a DeploymentHandle) -> Result<&'a str, DeployError> {
        handle.container.as_deref().ok_or_else(|| DeployError::Terminate {
            deployment: handle.deployment_id.to_string(),
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
        let admin = spec.node.kind == rdm_core::NodeKind::NodeAdmin;
        let disk = {
            let (data_dir, env) = (spec.data_dir.clone(), spec.env.clone());
            crate::blocking::blocking_io(move || launch_disk(&data_dir, admin, &env)).await.map_err(err)?
        };
        // Which launch the container realises, durable before the container exists, so one that runs
        // always has its record and a later `find` knows the launch ran once the container is removed.
        let (dir, deployment) = (spec.data_dir.clone(), spec.deployment_id.clone());
        crate::blocking::blocking_io(move || record_launch(&dir, &deployment)).await.map_err(err)?;
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
        // --mount refuses a missing source instead of creating a fresh namespace. The
        // environment below retains this exact path for ordinary nodes and node-admins.
        if let Some(store) = &disk.store_dir {
            if store != &spec.data_dir && disk.data_root.as_ref() != Some(store) {
                args.extend(["--mount".into(), format!("type=bind,src={},dst={}", store.display(), store.display())]);
            }
        }
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
            // The executable directories: the admin re-hashes what it launches at every launch, so it reads
            // each at its host path, read-only.
            // `RDM_BIN_DIR`, mounted above, is not mounted twice.
            for d in disk.executable_dirs.iter().filter(|d| disk.bin_dir.as_deref() != d.to_str()) {
                args.extend(["-v".into(), format!("{}:{}:ro", d.display(), d.display())]);
            }
        }
        // The span evidence directory the launch names, at the same path: a container's spans
        // land beside every process's.
        if let Some(dir) = &disk.evidence_dir {
            args.extend(["-v".into(), format!("{dir}:{dir}")]);
        }
        // Each secret rides as a bare `-e K`, its value in the docker client's own environment
        // (`env_args`): never on the command-line arguments.
        let (env_args, client_env) = env_args(spec).map_err(err)?;
        args.extend(env_args);
        args.push(RUNTIME_IMAGE.into());
        args.push(exe_in.display().to_string());
        args.extend(spec.args.iter().cloned());
        let argv: Vec<&str> = args.iter().map(String::as_str).collect();
        // `docker run -d` answers the immutable id: the runtime's identity,
        // not the (reusable) name.
        let id = docker_with_env(&argv, &client_env).await.map_err(err)?.trim().to_string();
        self.data_dirs.lock().unwrap().insert(id.clone(), (spec.data_dir.clone(), spec.deployment_id.clone()));
        let pid = docker(&["inspect", "--format", "{{.State.Pid}}", &id]).await.map_err(err)?.parse::<u32>().ok().filter(|p| *p > 0);
        // The pipeline registers this exact handle and makes its runtime fact
        // available to the birth (`MakeRuntimeFactAvailableToBirth`).
        Ok(self.handle(spec.deployment_id.clone(), id, pid))
    }

    async fn terminate(&self, handle: &DeploymentHandle, mode: TerminationMode) -> Result<(), DeployError> {
        let name = self.container_of(handle)?;
        let err = |reason: String| DeployError::Terminate { deployment: handle.deployment_id.to_string(), reason };
        // A runtime already removed terminates as done (Q145): the daemon's "No such container" on
        // any of the calls below is that, so a re-run of the step succeeds.
        let gone = |reason: &String| reason.contains("No such container");
        let code = match mode {
            TerminationMode::Graceful { grace } => match docker(&["stop", "-t", &grace.as_secs().max(1).to_string(), name]).await {
                Ok(_) => docker(&["inspect", "--format", "{{.State.ExitCode}}", name]).await.ok().and_then(|c| c.parse().ok()),
                Err(e) if gone(&e) => return Ok(self.removed(handle, name, None)),
                Err(e) => return Err(err(e)),
            },
            // Forced termination: the daemon's kill (SIGKILL), never a stop, which delivers SIGTERM
            // first and thaws a paused container so it runs its handler (i144 R23). The kill is
            // waited out, and the wait answers the exit code.
            TerminationMode::Immediate => {
                match docker(&["kill", name]).await {
                    Ok(_) => {}
                    Err(e) if gone(&e) => return Ok(self.removed(handle, name, None)),
                    Err(e) => return Err(err(e)),
                }
                let waited = match docker(&["wait", name]).await {
                    Ok(w) => w,
                    Err(e) if gone(&e) => return Ok(self.removed(handle, name, None)),
                    Err(e) => return Err(err(e)),
                };
                let waited = waited.trim();
                Some(waited.parse().map_err(|e| err(format!("docker wait {name} answered {waited:?}, which is not an exit code: {e}")))?)
            }
        };
        // Keep the logs beside the node's data, then remove the container. Logs that cannot be
        // kept fail the stop by name and leave the stopped container (and its logs) in place, so
        // a re-run keeps them before it removes it.
        let data_dir = self.data_dirs.lock().unwrap().get(name).map(|(d, _)| d.clone());
        if let Some(dir) = data_dir {
            let logs = Command::new("docker").args(["logs", name]).stdin(std::process::Stdio::null()).output().await.map_err(|e| err(format!("docker logs {name}: {e}")))?;
            if !logs.status.success() {
                let reason = format!("docker logs {name}: {}", String::from_utf8_lossy(&logs.stderr).trim());
                if gone(&reason) {
                    return Ok(self.removed(handle, name, code));
                }
                return Err(err(reason));
            }
            let file = dir.join("container.log");
            let written = file.clone();
            crate::blocking::blocking_io(move || std::fs::write(&written, [&logs.stdout[..], &logs.stderr[..]].concat()))
                .await
                .map_err(|e| err(format!("the container's logs cannot be kept at {}: {e}", file.display())))?;
        }
        match docker(&["rm", "-f", name]).await {
            Ok(_) => {}
            Err(e) if gone(&e) => {}
            Err(e) => return Err(err(e)),
        }
        self.removed(handle, name, code);
        Ok(())
    }

    async fn inspect(&self, handle: &DeploymentHandle) -> DeploymentStatus {
        let Ok(name) = self.container_of(handle) else { return DeploymentStatus::Unknown };
        if let Some(code) = self.exited.lock().unwrap().get(name) {
            return DeploymentStatus::Exited { code: *code };
        }
        match docker(&["inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}", name]).await {
            Ok(s) => status_of(&s),
            Err(_) => DeploymentStatus::Unknown,
        }
    }

    async fn signal_stop(&self, handle: &DeploymentHandle) -> Result<(), DeployError> {
        let name = self.container_of(handle)?;
        match self.inspect(handle).await {
            DeploymentStatus::Running => docker(&["kill", "--signal", "TERM", name])
                .await
                .map(|_| ())
                .map_err(|reason| DeployError::Terminate { deployment: handle.deployment_id.to_string(), reason }),
            // The daemon unpauses a paused container to deliver a signal (measured 2026-10-15, Docker
            // 29.6.2: `paused` reads `running` right after `docker kill --signal TERM`), and the provider
            // never continues a suspended runtime (deployment-provider.md §5): nothing is sent, by name.
            DeploymentStatus::OsStopped => Err(DeployError::Terminate { deployment: handle.deployment_id.to_string(), reason: format!("{name} is paused (os-stopped): a signal would unpause it, and the provider continues no suspended runtime; nothing was sent") }),
            DeploymentStatus::Exited { .. } | DeploymentStatus::Unknown => Ok(()),
        }
    }

    async fn find(&self, spec: &ResolvedNodeLaunch) -> Result<Found, DeployError> {
        self.find_by_name(spec).await
    }

    fn launched(&self) -> Vec<DeploymentHandle> {
        self.launched_handles()
    }

    /// A container is given its own address on the fabric network before it starts (`--ip`): the
    /// one pre-boot address a container needs. Its ports are the operating system's. The address
    /// belongs to the birth it was given to: that birth binds it again, and a new birth at the same
    /// `path.name` (a replace re-births there while its renamed predecessor's container still runs,
    /// Q79) is given one no container holds. The addresses already taken on the network are asked of
    /// the daemon (no lock held across it); a daemon that cannot answer, or a network with no address
    /// left, is an error naming it.
    async fn bind_ip(&self, node: &crate::model::PathName, birth: &crate::model::DeploymentId) -> Result<IpAddr, DeployError> {
        let err = |reason: String| DeployError::Spawn { node: node.to_string(), reason };
        if let Some(ip) = self.ips.lock().unwrap().held.get(birth) {
            return Ok(*ip);
        }
        let on_network = network_addresses(&self.network.name).await.map_err(err)?;
        let mut ips = self.ips.lock().unwrap();
        if let Some(ip) = ips.held.get(birth) {
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
                ips.held.insert(birth.clone(), ip);
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

/// The daemon's `{{.State.Status}} {{.State.ExitCode}}`, mapped conservatively
/// (deployment-provider.md §2.7): `running`, and `restarting` and `created` (nonterminal and not
/// suspended, nothing more claimed) are `Running`; `paused` is the daemon's frozen cgroup, the
/// container suspended (`OsStopped`); `exited` and `dead` are `Exited` with Docker's own code;
/// anything else cannot be told.
fn status_of(state: &str) -> DeploymentStatus {
    match state.trim().split_once(' ') {
        Some(("running" | "restarting" | "created", _)) => DeploymentStatus::Running,
        Some(("paused", _)) => DeploymentStatus::OsStopped,
        Some(("exited" | "dead", code)) => DeploymentStatus::Exited { code: code.parse().ok() },
        _ => DeploymentStatus::Unknown,
    }
}

#[cfg(test)]
mod tests {

    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:47
    /// "For `container`: `docker inspect <container_name>` verifying the container's
    ///  `rdm.deployment_id` label matches `deployment_id`, reading `.State.Status` and
    ///  `.State.ExitCode`."
    /// CONTRACT: docs/architecture/lifecycles/node-runtime-inspect.md:52
    /// "4. The provider returns the typed answer: `Running` ... `OsStopped` ... `Exited` ... `Unavailable`"
    /// CONTRACT (deployment-provider.md §2.7): the daemon's states map conservatively onto the four
    /// provider states: `running`, `restarting` and `created` are `Running` (nonterminal, not
    /// suspended, nothing more claimed); `paused` is `OsStopped`, never `Running`; `exited` and `dead`
    /// carry Docker's code; an unreadable answer is `Unknown`, never exited verdict.
    #[test]
    fn the_daemons_states_map_to_the_four_provider_states_and_paused_is_os_stopped() {
        for state in ["running 0", "restarting 0", "created 0"] {
            assert_eq!(status_of(state), DeploymentStatus::Running, "{state}");
        }
        assert_eq!(status_of("paused 0"), DeploymentStatus::OsStopped);
        assert_eq!(status_of("exited 137"), DeploymentStatus::Exited { code: Some(137) });
        assert_eq!(status_of("dead 1"), DeploymentStatus::Exited { code: Some(1) });
        assert_eq!(status_of("exited x"), DeploymentStatus::Exited { code: None }, "a code the daemon did not give is none");
        for unreadable in ["", "removing 0", "running", "garbage"] {
            assert_eq!(status_of(unreadable), DeploymentStatus::Unknown, "{unreadable:?}");
        }
    }

    use super::*;

    /// CONTRACT (proposal executable-bindings-at-create.md §2: "`RDM_EXECUTABLE_DIRS`, comma-separated host
    /// directories mounted read-only at their host paths ... the launching admin writes its own value into
    /// every node-admin launch, as it writes the root, so every admin of the fabric mounts the same
    /// directories"; deployment-provider.md §2.7 "Executables"): a node-admin's launch whose environment
    /// names `RDM_EXECUTABLE_DIRS` has each named directory mounted, read-only, at its host path; a
    /// directory named twice is mounted once, and a non-admin launch mounts none. Must NOT happen: a
    /// launched admin that cannot read an executable its fabric accepted.
    #[test]
    fn a_node_admins_launch_mounts_the_executable_dirs_its_environment_names() {
        let root = std::env::temp_dir().join(format!("launch-dirs-{}-{}", std::process::id(), rand::random::<u32>()));
        let (data, a, b) = (root.join("data").join("mesh1.admin.2"), root.join("a"), root.join("b"));
        for d in [&data, &a, &b] {
            std::fs::create_dir_all(d).unwrap();
        }
        let env: std::collections::BTreeMap<String, String> = [(rdm_core::launch::ENV_EXECUTABLE_DIRS.to_string(), format!("{},{},{}", a.display(), b.display(), a.display()))].into();
        let disk = launch_disk(&data, true, &env).unwrap();
        assert_eq!(disk.executable_dirs, vec![a.clone(), b.clone()], "each named directory, once, at its host path");
        let disk = launch_disk(&data, false, &env).unwrap();
        assert!(disk.executable_dirs.is_empty(), "a node that launches nothing mounts none");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn node_addresses_start_after_the_gateway_and_stop_before_broadcast() {
        let n = FabricNetwork { name: "n".into(), base: Ipv4Addr::new(172, 18, 0, 0), prefix: 16, gateway: Ipv4Addr::new(172, 18, 0, 1) };
        assert_eq!(n.node_range(), (Ipv4Addr::new(172, 18, 0, 2), Ipv4Addr::new(172, 18, 255, 254)));
    }

    /// CONTRACT: a `/proc/<pid>/net/udp` line names its local address as the in-memory network-order
    /// word printed in hex (the kernel's own text; on a little-endian host 127.0.0.1 is `0100007F`),
    /// then `:port` in hex, and an unspecified bind holds its port at every address. The table is a
    /// fixture: the host namespace's live table is answered a page per read and each page is resumed
    /// by position, so a socket held throughout reads absent when an earlier line goes between two
    /// pages. Only what a read promises is asserted live: a missing pid's table is an error naming it.
    #[test]
    #[cfg(target_endian = "little")]
    fn the_socket_table_is_read_in_network_byte_order() {
        let table = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops\n\
                     257: 0100007F:1F90 00000000:0000 07 00000000:00000000 00:00000000 00000000  1000        0 41823 2 0000000000000000 0\n\
                     301: 00000000:1FBD 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 41901 2 0000000000000000 0\n";
        let held = SocketAddr::from((Ipv4Addr::LOCALHOST, 8080));
        assert!(udp_table_holds(table, held), "0100007F:1F90 is 127.0.0.1:8080");
        assert!(!udp_table_holds(table, SocketAddr::from((Ipv4Addr::new(127, 0, 0, 2), 8080))), "127.0.0.2:8080 is not bound");
        assert!(udp_table_holds(table, SocketAddr::from((Ipv4Addr::new(10, 0, 0, 7), 8125))), "the unspecified bind holds 8125 at every address");
        assert_eq!(udp_sockets_in(table), vec![SocketAddr::from((Ipv4Addr::UNSPECIFIED, 8125)), held]);
        assert!(netns_holds_udp(u32::MAX, held).unwrap_err().contains("/proc/"));
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
        let dir = crate::test_support::tempdir("container-socket");
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
        let dir = crate::test_support::tempdir("container-launch-disk");
        let ok = dir.join("clean");
        let disk = launch_disk(&ok, false, &std::collections::BTreeMap::new()).unwrap();
        assert!(ok.is_dir(), "the data dir is made");
        assert!(disk.runtime_mounts.contains(&"/usr"), "{:?}", disk.runtime_mounts);
        let stale = dir.join("stale");
        let record = stale.join(rdm_core::runtime::RUNTIME_FILE);
        std::fs::create_dir_all(record.join("held")).unwrap();
        let err = launch_disk(&stale, false, &std::collections::BTreeMap::new()).unwrap_err();
        assert!(err.contains(&record.display().to_string()), "{err}");
    }

    #[test]
    fn container_names_carry_only_safe_characters() {
        assert_eq!(docker_safe("mesh1.rpc.2/x y"), "mesh1.rpc.2-x-y");
    }

    /// CONTRACT (lifecycles/node-create.md: exact provider proof of a runtime's state): the launch
    /// record a spawn writes says a launch whose container is gone ran, only when it names that launch;
    /// none, or one naming another launch, says never launched; one that cannot be read or parsed is
    /// refused by name. Must NOT happen: an unreadable record read as "never launched", which would
    /// start a birth that ran again.
    #[test]
    fn a_launch_record_says_a_launch_ran_only_when_it_names_it_and_an_unreadable_one_is_refused() {
        let dir = std::env::temp_dir().join(format!("container-launch-record-{}", crate::model::DeploymentId::mint().as_str()));
        std::fs::create_dir_all(&dir).unwrap();
        let (this, other) = (crate::model::DeploymentId::mint(), crate::model::DeploymentId::mint());
        assert_eq!(recorded_launch(&dir, &this), Ok(false), "no record: never launched");
        record_launch(&dir, &other).unwrap();
        assert_eq!(recorded_launch(&dir, &this), Ok(false), "the record of another launch");
        record_launch(&dir, &this).unwrap();
        assert_eq!(recorded_launch(&dir, &this), Ok(true), "the record names this launch");
        std::fs::write(dir.join(LAUNCH_FILE), b"not json").unwrap();
        assert!(recorded_launch(&dir, &this).expect_err("an unparsable record").contains(LAUNCH_FILE));
        std::fs::remove_file(dir.join(LAUNCH_FILE)).unwrap();
        std::fs::create_dir_all(dir.join(LAUNCH_FILE)).unwrap();
        assert!(recorded_launch(&dir, &this).expect_err("an unreadable record").contains(LAUNCH_FILE));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CONTRACT (lifecycles/node-create.md: exact provider proof of a runtime's state): a container's
    /// Docker status places its launch: `running` runs, `exited` and `dead` ran and exited, `created`
    /// never started; any other status is refused by name. Must NOT happen: an exited container read
    /// as a launch never made, or an unplaced status read as any of the three.
    #[test]
    fn a_containers_status_places_its_launch_or_is_refused_by_name() {
        assert_eq!(container_launch("running"), Ok(ContainerLaunch::Running));
        assert_eq!(container_launch("exited"), Ok(ContainerLaunch::Exited));
        assert_eq!(container_launch("dead"), Ok(ContainerLaunch::Exited));
        assert_eq!(container_launch("created"), Ok(ContainerLaunch::NeverStarted));
        for other in ["paused", "restarting", "removing", "unheard-of"] {
            let refused = container_launch(other).expect_err(other);
            assert!(refused.contains(other), "{refused}");
        }
    }

    /// CONTRACT (Q150 §0.1, the container boundary): `docker run` carries each secret as a bare `-e K`,
    /// its value in the docker client's own environment, never on the command-line arguments: every
    /// declared value set for the kind and RDM's four (`RDM_CA_SECRET`, `RDM_ISSUING_KEY`,
    /// `RDM_FABRIC_CA_KEY`, `RDM_DECLARED_SECRETS`). Every other entry stays `-e K=V`.
    #[test]
    fn a_secret_rides_the_docker_clients_environment_and_never_its_arguments() {
        let secrets = [
            (rdm_core::launch::ENV_CA_SECRET, "root-secret-hex"),
            (rdm_core::launch::ENV_ISSUING_KEY, "issuing-key-hex"),
            (rdm_core::launch::ENV_FABRIC_CA_KEY, "ca-key-hex"),
            (rdm_core::launch::ENV_DECLARED_SECRETS, "{\"v\":1,\"declared\":[]}"),
        ];
        let spec = ResolvedNodeLaunch {
            secrets: std::collections::BTreeMap::from([("APP_KEY".to_string(), rdm_core::launch::SecretHex::of_value("app,value=with\nnewline")), ("APP_EMPTY".to_string(), rdm_core::launch::SecretHex::of_value(""))]),
            withhold: vec!["APP_KEY".into(), "APP_EMPTY".into()],
            node: "mesh1.admin.2".parse().unwrap(),
            deployment_id: crate::model::DeploymentId::mint(),
            executable: "/bin/true".into(),
            args: vec![],
            env: secrets.iter().map(|(k, v)| (k.to_string(), v.to_string())).chain([("RDM_FABRIC".to_string(), "fabric1".to_string())]).collect(),
            data_dir: "/tmp/x".into(),
            transport: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![],
        };
        let (args, client) = env_args(&spec).unwrap();
        let argv = args.join(" ");
        for (k, v) in secrets.iter().copied().chain([("APP_KEY", "app,value=with\nnewline"), ("APP_EMPTY", "")]) {
            assert!(args.windows(2).any(|w| w[0] == "-e" && w[1] == k), "{k} rides as a bare -e {k}: {argv}");
            assert_eq!(client.get(k).map(String::as_str), Some(v), "{k}'s value is in the docker client's environment");
            if !v.is_empty() {
                assert!(!argv.contains(v), "{k}'s value is never on the arguments: {argv}");
            }
        }
        assert!(args.windows(2).any(|w| w[0] == "-e" && w[1] == "RDM_FABRIC=fabric1"), "a non-secret entry stays -e K=V: {argv}");
        assert!(!client.contains_key("RDM_FABRIC"), "only secrets ride the client's environment");
    }
}
