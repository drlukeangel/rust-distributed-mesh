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
    /// The docker client every command runs through.
    docker: Docker,
}

/// The docker client every command of this provider runs through ([`super::client`]): each
/// invocation under the deadline its caller hands it, with its own span. The program is `docker`;
/// a test stands in another.
#[derive(Debug, Clone)]
pub(crate) struct Docker {
    program: String,
}

/// What one docker invocation acts on, for its span and its failure.
#[derive(Default, Clone, Copy)]
pub(crate) struct On<'a> {
    deployment: Option<&'a str>,
    container: Option<&'a str>,
    env: Option<&'a std::collections::BTreeMap<String, String>>,
}

/// A docker command that gave no answer: the provider's error text, and the typed deadline it stays.
#[derive(Debug, Clone)]
pub(crate) struct DockerError {
    failure: super::client::Failure,
    operation: &'static str,
    deployment: Option<String>,
    text: String,
}

impl DockerError {
    /// The provider's error for it: a deadline stays [`DeployError::Deadline`], every other failure
    /// is `wrap`ped with its text.
    pub(crate) fn into_deploy(self, wrap: impl FnOnce(String) -> DeployError) -> DeployError {
        match self.failure {
            super::client::Failure::Deadline { elapsed, bound, mutation } => DeployError::Deadline {
                operation: self.operation.to_string(),
                deployment: self.deployment.unwrap_or_default(),
                elapsed,
                bound,
                mutation,
            },
            _ => wrap(self.text),
        }
    }

    /// Whether the daemon answered that no object has the name (and nothing else). A deadline or a
    /// client that could not run never says so.
    pub(crate) fn says_absent(&self) -> bool {
        matches!(self.failure, super::client::Failure::Answered { .. }) && inspect_says_absent(&self.text)
    }

    /// Whether the daemon answered with `needle` in its reply.
    fn answered(&self, needle: &str) -> bool {
        matches!(self.failure, super::client::Failure::Answered { .. }) && self.text.contains(needle)
    }
}

impl std::fmt::Display for DockerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

impl Default for Docker {
    fn default() -> Self {
        Self { program: "docker".into() }
    }
}

/// Every verb that changes a runtime, a container or a network: past its deadline its outcome is
/// unknown.
const MUTATIONS: &[&str] = &["run", "rm", "stop", "kill", "create", "import", "pause", "unpause"];

impl Docker {
    /// The client run as `program` (tests).
    #[cfg(test)]
    pub(crate) fn program(program: impl Into<String>) -> Self {
        Self { program: program.into() }
    }

    /// `args` under `deadline` in control domain `domain`: the client's stdout and stderr.
    async fn run(&self, domain: Option<&str>, args: &[&str], deadline: super::deadline::Deadline, on: On<'_>, stderr_is_data: bool) -> Result<super::client::Answer, DockerError> {
        let empty = std::collections::BTreeMap::new();
        let (object, verb) = match args {
            ["network", v, ..] => ("network", *v),
            ["image", v, ..] => ("image", *v),
            ["version", ..] | ["info", ..] => ("daemon", args[0]),
            [v, ..] => ("container", *v),
            [] => ("container", ""),
        };
        let operation: &'static str = operation_name(object, verb);
        let mutation = MUTATIONS.contains(&verb);
        let call = super::client::Invocation { operation, args, env: on.env.unwrap_or(&empty), deployment: on.deployment, container: on.container, mutation, stderr_is_data };
        super::client::run(&self.program, domain, &call, deadline).await.map_err(|failure| DockerError {
            text: failure.message(&self.program, args.first().unwrap_or(&"")),
            failure,
            operation,
            deployment: on.deployment.map(str::to_string),
        })
    }

    /// `args` under `deadline`: the client's stdout, trimmed.
    async fn text(&self, domain: Option<&str>, args: &[&str], deadline: super::deadline::Deadline, on: On<'_>) -> Result<String, DockerError> {
        self.run(domain, args, deadline, on, false).await.map(|a| String::from_utf8_lossy(&a.stdout).trim().to_string())
    }
}

/// The span name of a docker verb on `object` (`container.run`, `network.inspect`, …).
fn operation_name(object: &str, verb: &str) -> &'static str {
    match (object, verb) {
        ("container", "run") => "container.run",
        ("container", "inspect") => "container.inspect",
        ("container", "stop") => "container.stop",
        ("container", "kill") => "container.kill",
        ("container", "wait") => "container.wait",
        ("container", "logs") => "container.logs",
        ("container", "rm") => "container.rm",
        ("container", "ps") => "container.ps",
        ("network", "inspect") => "network.inspect",
        ("network", "create") => "network.create",
        ("network", "rm") => "network.rm",
        ("image", "inspect") => "image.inspect",
        ("image", "import") => "image.import",
        ("daemon", "version") => "daemon.version",
        ("daemon", "info") => "daemon.info",
        _ => "container.other",
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
pub(crate) async fn network_addresses(docker: &Docker, domain: Option<&str>, network: &str, deadline: super::deadline::Deadline) -> Result<std::collections::BTreeSet<IpAddr>, DockerError> {
    docker
        .text(domain, &["network", "inspect", "--format", "{{range .Containers}}{{.IPv4Address}} {{end}}", network], deadline, On::default())
        .await
        .map(|out| parse_network_addresses(&out))
        .map_err(|mut e| {
            e.text = format!("the addresses taken on network {network}: {}", e.text);
            e
        })
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
async fn create_fabric_network(docker: &Docker, fabric: &str, name: &str, deadline: super::deadline::Deadline) -> Result<(), String> {
    let pool = subnet_pool_from_env()?;
    let mut last = String::new();
    for net in candidate_subnets(pool, fabric) {
        let gateway = Ipv4Addr::from(u32::from(net) + 1);
        let subnet = format!("{net}/24");
        match docker.text(None, &["network", "create", "--label", &format!("rafka.fabric={fabric}"), "--subnet", &subnet, "--gateway", &gateway.to_string(), name], deadline, On::default()).await {
            Ok(_) => return Ok(()),
            // A concurrent admin of the same fabric created it first.
            Err(_) if docker.text(None, &["network", "inspect", name], deadline, On::default()).await.is_ok() => return Ok(()),
            Err(e) if e.answered("overlap") => last = e.to_string(),
            Err(e) => return Err(format!("cannot create network {name} on {subnet}: {e}")),
        }
    }
    Err(format!("cannot create network {name}: every /24 of RDM_CONTAINER_SUBNET_POOL is in use ({last})"))
}

impl ContainerDeploymentProvider {
    /// Check the host can run containers, then make sure the runtime image
    /// and the fabric's network exist. An unsupported host is refused by name.
    pub async fn prepare(fabric: &str) -> Result<Self, DeployError> {
        Self::prepare_with(Docker::default(), fabric, super::deadline::Deadline::step()).await
    }

    /// [`Self::prepare`] through `docker`, every command under `deadline`.
    pub(crate) async fn prepare_with(docker: Docker, fabric: &str, deadline: super::deadline::Deadline) -> Result<Self, DeployError> {
        let unsupported = |reason: String| DeployError::Unsupported { provider: ProviderKind::Container, reason };
        if !cfg!(target_os = "linux") {
            return Err(unsupported("the container provider needs Linux network namespaces".into()));
        }
        docker
            .text(None, &["version", "--format", "{{.Server.Version}}"], deadline, On::default())
            .await
            .map_err(|e| e.into_deploy(|e| unsupported(format!("no reachable container runtime: {e}"))))?;
        if docker.text(None, &["image", "inspect", RUNTIME_IMAGE], deadline, On::default()).await.is_err() {
            let mut child = Command::new(&docker.program)
                .args(["import", "-", RUNTIME_IMAGE])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .map_err(|e| unsupported(format!("docker import: {e}")))?;
            {
                use tokio::io::AsyncWriteExt;
                let mut stdin = child.stdin.take().expect("piped");
                // An empty tar archive: two zero blocks.
                stdin.write_all(&[0u8; 1024]).await.map_err(|e| unsupported(format!("docker import: {e}")))?;
            }
            // Under the same deadline as every other command: past it the client is dropped, and
            // killed with it.
            let out = deadline
                .run(child.wait_with_output())
                .await
                .map_err(|elapsed| DeployError::Deadline { operation: "image.import".into(), deployment: String::new(), elapsed, bound: deadline.bound(), mutation: true })?
                .map_err(|e| unsupported(format!("docker import: {e}")))?;
            if !out.status.success() {
                return Err(unsupported(format!("cannot create {RUNTIME_IMAGE}: {}", String::from_utf8_lossy(&out.stderr).trim())));
            }
        }
        let name = format!("rafka-{}", docker_safe(fabric));
        if docker.text(None, &["network", "inspect", &name], deadline, On::default()).await.is_err() {
            create_fabric_network(&docker, fabric, &name, deadline).await.map_err(unsupported)?;
        }
        let ipam = docker
            .text(None, &["network", "inspect", "--format", "{{range .IPAM.Config}}{{.Subnet}} {{.Gateway}};{{end}}", &name], deadline, On::default())
            .await
            .map_err(|e| e.into_deploy(unsupported))?;
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
        let daemon = docker.text(None, &["info", "--format", "{{.ID}}"], deadline, On::default()).await.map_err(|e| e.into_deploy(unsupported))?;
        let daemon = if daemon.trim().is_empty() { docker.text(None, &["info", "--format", "{{.Name}}"], deadline, On::default()).await.map_err(|e| e.into_deploy(unsupported))? } else { daemon };
        Ok(Self {
            fabric: fabric.into(),
            network,
            exited: Mutex::new(HashMap::new()),
            data_dirs: Mutex::new(HashMap::new()),
            ips: Mutex::new(NodeIps::default()),
            domain: format!("container:{}", daemon.trim()),
            docker,
        })
    }

    /// The fabric's bridge network.
    pub fn network(&self) -> &FabricNetwork {
        &self.network
    }

    /// Remove every container of the fabric and its network, each command under a step's deadline.
    pub async fn remove_fabric(&self) -> Result<(), DeployError> {
        let err = |reason: String| DeployError::Terminate { deployment: format!("fabric {}", self.fabric), reason };
        let ids = self.docker_text(&["ps", "-aq", "--filter", &format!("label=rafka.fabric={}", self.fabric)], super::deadline::Deadline::step(), On::default()).await.map_err(|e| e.into_deploy(err))?;
        for id in ids.split_whitespace() {
            self.docker_text(&["rm", "-f", id], super::deadline::Deadline::step(), On { container: Some(id), ..On::default() }).await.map_err(|e| e.into_deploy(err))?;
        }
        self.docker_text(&["network", "rm", &self.network.name], super::deadline::Deadline::step(), On::default()).await.map_err(|e| e.into_deploy(err))?;
        Ok(())
    }

    /// One docker command of this provider, in its control domain, under `deadline`.
    async fn docker_text(&self, args: &[&str], deadline: super::deadline::Deadline, on: On<'_>) -> Result<String, DockerError> {
        self.docker.text(Some(&self.domain), args, deadline, on).await
    }

    /// What became of this launch's container, by its name (the name carries the deployment id),
    /// every command under `deadline`: one that runs is the live runtime; one that exited or is dead
    /// ran and has exited. One Docker created and never started is never read as absence (Q304): that
    /// exact container is removed without `--force` (one that started meanwhile refuses it) and read
    /// absent before the launch record decides; a removal or a read that fails or passes its
    /// deadline fails the find, and nothing is launched. None by that name: the launch's record
    /// ([`LAUNCH_FILE`], written before the container is created) in the data dir names it and no
    /// container of it is left, so it has exited (it ran and was removed, or its executor was lost
    /// before the container existed); else it was never launched. `Err` names a `docker inspect` that
    /// failed for any other reason, passed its deadline ([`DeployError::Deadline`]), or answered what
    /// this build cannot read (a status it cannot place included), or a launch record that cannot be
    /// read.
    async fn find_by_name(&self, spec: &ResolvedNodeLaunch, deadline: super::deadline::Deadline) -> Result<Found, DeployError> {
        let name = container_name(spec);
        let named = |reason: String| DeployError::Spawn { node: spec.node.to_string(), reason: format!("finding container {name}: {reason}") };
        let deployment = spec.deployment_id.to_string();
        let on = On { deployment: Some(deployment.as_str()), ..On::default() };
        let state = match self.docker_text(&["inspect", "--format", "{{.State.Status}} {{.State.Pid}} {{.Id}}", &name], deadline, on).await {
            Ok(state) => state,
            Err(e) if e.says_absent() => return self.by_launch_record(spec).await.map_err(named),
            Err(e) => return Err(e.into_deploy(named)),
        };
        let mut parts = state.split_whitespace();
        let (Some(status), Some(pid), Some(id)) = (parts.next(), parts.next(), parts.next()) else {
            return Err(named(format!("docker inspect answered {state:?}, not `<status> <pid> <id>`")));
        };
        match container_launch(status).map_err(named)? {
            ContainerLaunch::Running => {}
            ContainerLaunch::Exited => return Ok(Found::Exited(None)),
            ContainerLaunch::NeverStarted => {
                let on = On { deployment: Some(deployment.as_str()), container: Some(id), ..On::default() };
                self.docker_text(&["rm", id], deadline, on)
                    .await
                    .map_err(|e| e.into_deploy(|r| named(format!("the created container {id} was not removed, so nothing is launched: {r}"))))?;
                match self.docker_text(&["inspect", "--format", "{{.Id}}", id], deadline, on).await {
                    Err(e) if e.says_absent() => {}
                    Ok(_) => return Err(named(format!("the created container {id} is still there after its removal, so nothing is launched"))),
                    Err(e) => return Err(e.into_deploy(|r| named(format!("the created container {id} was removed, but its absence could not be read, so nothing is launched: {r}")))),
                }
                tracing::info_span!("rdm.node_admin.deployment.remove.via-created-container", node = %spec.node, deployment_id = %spec.deployment_id, container = id)
                    .in_scope(|| tracing::info!("a container Docker created and never started is removed by its exact id and read absent before the launch record decides"));
                return self.by_launch_record(spec).await.map_err(named);
            }
        }
        let id = id.to_string();
        self.data_dirs.lock().unwrap().insert(id.clone(), (spec.data_dir.clone(), spec.deployment_id.clone()));
        Ok(Found::Running(self.handle(spec.deployment_id.clone(), id, pid.parse().ok().filter(|p| *p > 0))))
    }

    /// No container of the launch is left: its record says whether it ran ([`Found::Exited`]) or was
    /// never launched.
    async fn by_launch_record(&self, spec: &ResolvedNodeLaunch) -> Result<Found, String> {
        let (dir, deployment) = (spec.data_dir.clone(), spec.deployment_id.clone());
        Ok(match crate::blocking::blocking_io(move || recorded_launch(&dir, &deployment)).await? {
            true => Found::Exited(None),
            false => Found::NeverLaunched,
        })
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

    async fn spawn(&self, spec: &ResolvedNodeLaunch, deadline: super::deadline::Deadline) -> Result<DeploymentHandle, DeployError> {
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
        // Past the deadline the run's outcome is unknown (Q304): never a launch that did not happen;
        // the next attempt's find resolves this exact launch before anything is launched again.
        let deployment = spec.deployment_id.to_string();
        let id = self
            .docker_text(&argv, deadline, On { deployment: Some(deployment.as_str()), env: Some(&client_env), ..On::default() })
            .await
            .map_err(|e| e.into_deploy(err))?;
        self.data_dirs.lock().unwrap().insert(id.clone(), (spec.data_dir.clone(), spec.deployment_id.clone()));
        let pid = self
            .docker_text(&["inspect", "--format", "{{.State.Pid}}", &id], deadline, On { deployment: Some(deployment.as_str()), container: Some(&id), ..On::default() })
            .await
            .map_err(|e| e.into_deploy(err))?
            .parse::<u32>()
            .ok()
            .filter(|p| *p > 0);
        // The pipeline registers this exact handle and makes its runtime fact
        // available to the birth (`MakeRuntimeFactAvailableToBirth`).
        Ok(self.handle(spec.deployment_id.clone(), id, pid))
    }

    async fn terminate(&self, handle: &DeploymentHandle, mode: TerminationMode, deadline: super::deadline::Deadline) -> Result<(), DeployError> {
        let name = self.container_of(handle)?;
        let err = |reason: String| DeployError::Terminate { deployment: handle.deployment_id.to_string(), reason };
        let deployment = handle.deployment_id.to_string();
        let on = On { deployment: Some(deployment.as_str()), container: Some(name), ..On::default() };
        // A runtime already removed terminates as done (Q145): the daemon's "No such container" on
        // any of the calls below is that, so a re-run of the step succeeds. A call past its deadline
        // is never that: the terminate is not done, and its outcome is unknown.
        let code = match mode {
            TerminationMode::Graceful { grace } => match self.docker_text(&["stop", "-t", &grace.as_secs().max(1).to_string(), name], deadline, on).await {
                Ok(_) => self.docker_text(&["inspect", "--format", "{{.State.ExitCode}}", name], deadline, on).await.ok().and_then(|c| c.parse().ok()),
                Err(e) if e.says_absent() => return Ok(self.removed(handle, name, None)),
                Err(e) => return Err(e.into_deploy(err)),
            },
            // Forced termination: the daemon's kill (SIGKILL), never a stop, which delivers SIGTERM
            // first and thaws a paused container so it runs its handler (i144 R23). The kill is
            // waited out, and the wait answers the exit code.
            TerminationMode::Immediate => {
                match self.docker_text(&["kill", name], deadline, on).await {
                    Ok(_) => {}
                    Err(e) if e.says_absent() => return Ok(self.removed(handle, name, None)),
                    Err(e) => return Err(e.into_deploy(err)),
                }
                let waited = match self.docker_text(&["wait", name], deadline, on).await {
                    Ok(w) => w,
                    Err(e) if e.says_absent() => return Ok(self.removed(handle, name, None)),
                    Err(e) => return Err(e.into_deploy(err)),
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
            let logs = match self.docker.run(Some(&self.domain), &["logs", name], deadline, on, true).await {
                Ok(logs) => logs,
                Err(e) if e.says_absent() => return Ok(self.removed(handle, name, code)),
                Err(e) => return Err(e.into_deploy(err)),
            };
            let file = dir.join("container.log");
            let written = file.clone();
            crate::blocking::blocking_io(move || std::fs::write(&written, [&logs.stdout[..], &logs.stderr[..]].concat()))
                .await
                .map_err(|e| err(format!("the container's logs cannot be kept at {}: {e}", file.display())))?;
        }
        match self.docker_text(&["rm", "-f", name], deadline, on).await {
            Ok(_) => {}
            Err(e) if e.says_absent() => {}
            Err(e) => return Err(e.into_deploy(err)),
        }
        self.removed(handle, name, code);
        Ok(())
    }

    /// A read past its deadline is `Unknown`, never absent or exited.
    async fn inspect(&self, handle: &DeploymentHandle, deadline: super::deadline::Deadline) -> DeploymentStatus {
        let Ok(name) = self.container_of(handle) else { return DeploymentStatus::Unknown };
        if let Some(code) = self.exited.lock().unwrap().get(name) {
            return DeploymentStatus::Exited { code: *code };
        }
        let deployment = handle.deployment_id.to_string();
        match self.docker_text(&["inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}", name], deadline, On { deployment: Some(deployment.as_str()), container: Some(name), ..On::default() }).await {
            Ok(s) => status_of(&s),
            Err(_) => DeploymentStatus::Unknown,
        }
    }

    async fn signal_stop(&self, handle: &DeploymentHandle, deadline: super::deadline::Deadline) -> Result<(), DeployError> {
        let name = self.container_of(handle)?;
        let err = |reason: String| DeployError::Terminate { deployment: handle.deployment_id.to_string(), reason };
        match self.inspect(handle, deadline).await {
            DeploymentStatus::Running => {
                let deployment = handle.deployment_id.to_string();
                self.docker_text(&["kill", "--signal", "TERM", name], deadline, On { deployment: Some(deployment.as_str()), container: Some(name), ..On::default() })
                    .await
                    .map(|_| ())
                    .map_err(|e| e.into_deploy(err))
            }
            // The daemon unpauses a paused container to deliver a signal (measured 2026-10-15, Docker
            // 29.6.2: `paused` reads `running` right after `docker kill --signal TERM`), and the provider
            // never continues a suspended runtime (deployment-provider.md §5): nothing is sent, by name.
            DeploymentStatus::OsStopped => Err(err(format!("{name} is paused (os-stopped): a signal would unpause it, and the provider continues no suspended runtime; nothing was sent"))),
            DeploymentStatus::Exited { .. } | DeploymentStatus::Unknown => Ok(()),
        }
    }

    async fn find(&self, spec: &ResolvedNodeLaunch, deadline: super::deadline::Deadline) -> Result<Found, DeployError> {
        self.find_by_name(spec, deadline).await
    }

    fn launched(&self) -> Vec<DeploymentHandle> {
        self.launched_handles()
    }

    /// A container is given its own address on the fabric network before it starts (`--ip`): the
    /// one pre-boot address a container needs. Its ports are the operating system's. The address
    /// belongs to the birth it was given to: that birth binds it again, and a new birth at the same
    /// `path.name` (a replace re-births there while its renamed predecessor's container still runs,
    /// Q79) is given one no container holds. The addresses already taken on the network are asked of
    /// the daemon (no lock held across it); a daemon that cannot answer by the deadline, or a network
    /// with no address left, is an error naming it.
    async fn bind_ip(&self, node: &crate::model::PathName, birth: &crate::model::DeploymentId, deadline: super::deadline::Deadline) -> Result<IpAddr, DeployError> {
        let err = |reason: String| DeployError::Spawn { node: node.to_string(), reason };
        if let Some(ip) = self.ips.lock().unwrap().held.get(birth) {
            return Ok(*ip);
        }
        let on_network = network_addresses(&self.docker, Some(&self.domain), &self.network.name, deadline).await.map_err(|e| e.into_deploy(err))?;
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

    async fn failure_detail(&self, handle: &DeploymentHandle, _data_dir: &Path, deadline: super::deadline::Deadline) -> String {
        let name = match self.container_of(handle) {
            Ok(name) => name,
            Err(e) => return e.to_string(),
        };
        let deployment = handle.deployment_id.to_string();
        match self.docker.run(Some(&self.domain), &["logs", "--tail", "20", name], deadline, On { deployment: Some(deployment.as_str()), container: Some(name), ..On::default() }, true).await {
            Ok(o) => tail(&String::from_utf8_lossy(&[&o.stdout[..], &o.stderr[..]].concat()), 5),
            Err(e) => e.to_string(),
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

    // ---- Q304 (rdm#241): every docker command under its step's deadline, the launch resolved ----

    /// A stand-in for the docker client in its own directory: it logs every call and answers from its
    /// state file (`absent`, `created`, `running`); a `hang-<verb>` file makes that verb never answer.
    struct FakeDocker {
        dir: std::path::PathBuf,
    }

    impl FakeDocker {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("rdm-fake-docker-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(dir.join("data")).unwrap();
            let s = dir.display();
            let script = format!(
                r#"#!/bin/bash
S={s}
echo "$*" >> $S/calls
st=$(cat $S/state 2>/dev/null || echo absent)
case "$1" in
  version) echo 29.6.2 ;;
  image) exit 0 ;;
  info) echo fake-daemon ;;
  network) case "$*" in *IPAM*) echo "10.231.7.0/24 10.231.7.1;" ;; *Containers*) echo "" ;; *) echo net ;; esac ;;
  inspect)
    [ -f $S/hang-inspect ] && exec sleep 600
    case "$st" in
      absent) echo "Error: No such object: ${{@: -1}}" >&2; exit 1 ;;
      created) echo "created 0 cid1" ;;
      running) echo "running 4242 cid1" ;;
    esac ;;
  rm)
    [ -f $S/hang-rm ] && exec sleep 600
    if [ -f $S/start-before-rm ]; then echo running > $S/state; echo "Error response from daemon: cannot remove container cid1: container is running: stop the container before removing or force remove" >&2; exit 1; fi
    echo absent > $S/state; echo cid1 ;;
  run)
    echo created > $S/state
    if [ -f $S/hang-run ]; then echo running > $S/state; exec sleep 600; fi
    echo running > $S/state; echo cid1 ;;
  *) exit 0 ;;
esac
"#
            );
            let bin = dir.join("docker");
            std::fs::write(&bin, script).unwrap();
            std::fs::set_permissions(&bin, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
            Self { dir }
        }
        fn set(&self, state: &str) {
            std::fs::write(self.dir.join("state"), state).unwrap();
        }
        fn flag(&self, f: &str) {
            std::fs::write(self.dir.join(f), "").unwrap();
        }
        fn calls(&self) -> Vec<String> {
            std::fs::read_to_string(self.dir.join("calls")).unwrap_or_default().lines().map(str::to_string).collect()
        }
        fn runs(&self) -> usize {
            self.calls().iter().filter(|c| c.starts_with("run ")).count()
        }
        async fn provider(&self) -> ContainerDeploymentProvider {
            ContainerDeploymentProvider::prepare_with(Docker::program(self.dir.join("docker").display().to_string()), "fab", super::super::deadline::Deadline::step()).await.unwrap()
        }
        fn spec(&self) -> ResolvedNodeLaunch {
            ResolvedNodeLaunch {
                secrets: Default::default(),
                withhold: Vec::new(),
                node: "mesh1.rpc.1".parse().unwrap(),
                deployment_id: crate::model::DeploymentId::mint(),
                executable: "/bin/true".into(),
                args: vec![],
                env: Default::default(),
                data_dir: self.dir.join("data"),
                transport: "10.231.7.5:0".parse().unwrap(),
                listeners: vec![],
            }
        }
    }

    impl Drop for FakeDocker {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn short() -> super::super::deadline::Deadline {
        super::super::deadline::Deadline::after(std::time::Duration::from_millis(400))
    }

    /// rdm#241 proofs 1 and 2: a run that hangs ends at its deadline with its outcome unknown, never a
    /// launch that did not happen; the run had in fact started the container, and the next find
    /// adopts exactly that one runtime: nothing is launched a second time.
    #[tokio::test]
    async fn a_run_past_its_deadline_is_an_unknown_outcome_and_the_next_find_adopts_the_one_runtime() {
        let fake = FakeDocker::new("lost-reply");
        let p = fake.provider().await;
        let spec = fake.spec();
        fake.flag("hang-run");
        match p.spawn(&spec, short()).await {
            Err(DeployError::Deadline { operation, mutation: true, .. }) => assert_eq!(operation, "container.run"),
            other => panic!("a hung run is a deadline with its outcome unknown, got {other:?}"),
        }
        match p.find(&spec, super::super::deadline::Deadline::step()).await.unwrap() {
            Found::Running(h) => assert_eq!(h.container.as_deref(), Some("cid1"), "the exact container is adopted"),
            other => panic!("the launch that did start is found running, got {other:?}"),
        }
        assert_eq!(fake.runs(), 1, "exactly one run was ever issued: {:?}", fake.calls());
    }

    /// rdm#241 proof 3: a container Docker created is never absence; when it starts between the
    /// inspection and its cleanup, the removal (without `--force`) is refused, the find fails, and
    /// nothing is force-killed or launched again.
    #[tokio::test]
    async fn a_created_container_that_starts_before_its_cleanup_is_neither_forced_nor_launched_again() {
        let fake = FakeDocker::new("created-starts");
        let p = fake.provider().await;
        let spec = fake.spec();
        fake.set("created");
        fake.flag("start-before-rm");
        let err = p.find(&spec, super::super::deadline::Deadline::step()).await.expect_err("a refused removal fails the find");
        assert!(err.to_string().contains("nothing is launched"), "{err}");
        let calls = fake.calls();
        assert!(calls.iter().any(|c| c == "rm cid1"), "the exact id is removed without force: {calls:?}");
        assert!(!calls.iter().any(|c| c.starts_with("rm -f") || c.starts_with("kill")), "never forced: {calls:?}");
        assert_eq!(fake.runs(), 0);
        std::fs::remove_file(fake.dir.join("start-before-rm")).unwrap();
        assert!(matches!(p.find(&spec, super::super::deadline::Deadline::step()).await.unwrap(), Found::Running(_)), "the next find adopts the runtime that started");
    }

    /// A created container whose removal succeeds and whose absence is read: the launch record
    /// decides, so a launch whose record names it reads exited, and the removal was by exact id.
    #[tokio::test]
    async fn a_created_container_is_removed_by_its_exact_id_and_read_absent_before_the_record_decides() {
        let fake = FakeDocker::new("created-removed");
        let p = fake.provider().await;
        let spec = fake.spec();
        record_launch(&spec.data_dir, &spec.deployment_id).unwrap();
        fake.set("created");
        assert_eq!(p.find(&spec, super::super::deadline::Deadline::step()).await.unwrap(), Found::Exited(None));
        let calls = fake.calls();
        let rm = calls.iter().position(|c| c == "rm cid1").expect("removed by exact id");
        assert!(calls[rm + 1..].iter().any(|c| c.starts_with("inspect --format {{.Id}} cid1")), "its absence is read after the removal: {calls:?}");
    }

    /// rdm#241 proof 4: a cleanup that passes its deadline launches no replacement.
    #[tokio::test]
    async fn a_cleanup_past_its_deadline_launches_no_replacement() {
        let fake = FakeDocker::new("cleanup-hangs");
        let p = fake.provider().await;
        let spec = fake.spec();
        fake.set("created");
        fake.flag("hang-rm");
        match p.find(&spec, short()).await {
            Err(DeployError::Deadline { operation, mutation: true, .. }) => assert_eq!(operation, "container.rm"),
            other => panic!("a hung cleanup is a deadline, got {other:?}"),
        }
        assert_eq!(fake.runs(), 0, "no replacement is launched");
    }

    /// rdm#241 proof 5: an inspection past its deadline is never absent or exited.
    #[tokio::test]
    async fn an_inspection_past_its_deadline_is_never_absent_or_exited() {
        let fake = FakeDocker::new("inspect-hangs");
        let p = fake.provider().await;
        let spec = fake.spec();
        record_launch(&spec.data_dir, &spec.deployment_id).unwrap();
        fake.set("running");
        let handle = p.handle(spec.deployment_id.clone(), "cid1".into(), None);
        fake.flag("hang-inspect");
        match p.find(&spec, short()).await {
            Err(DeployError::Deadline { mutation: false, .. }) => {}
            other => panic!("a hung inspection fails the find, never absent or exited: {other:?}"),
        }
        assert_eq!(p.inspect(&handle, short()).await, DeploymentStatus::Unknown);
        assert_eq!(fake.runs(), 0);
    }

    /// rdm#241 proof 6: an executor lost after its run was submitted: its successor (another
    /// provider instance over the same daemon) resolves the same durable launch, by its name, to the
    /// one runtime, and launches nothing.
    #[tokio::test]
    async fn a_successor_resolves_the_same_durable_launch_after_its_executor_is_lost() {
        let fake = FakeDocker::new("successor");
        let spec = fake.spec();
        fake.flag("hang-run");
        let lost = fake.provider().await;
        assert!(matches!(lost.spawn(&spec, short()).await, Err(DeployError::Deadline { .. })));
        drop(lost);
        let successor = fake.provider().await;
        assert!(matches!(successor.find(&spec, super::super::deadline::Deadline::step()).await.unwrap(), Found::Running(_)));
        let name = container_name(&spec);
        assert!(fake.calls().iter().any(|c| c.ends_with(&name)), "found by the launch's own name {name}");
        assert_eq!(fake.runs(), 1);
    }
}
