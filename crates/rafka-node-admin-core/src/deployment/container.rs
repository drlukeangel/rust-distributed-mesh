//! `ContainerDeploymentProvider`: one container per node, each in its own
//! network namespace on the fabric's own bridge network (PRD §10: the
//! container provider is the namespace/isolation/network-fault gate).
//!
//! Node-admin still assigns every advertised endpoint: each node owns one
//! address on the fabric network (`--ip`), and its slots bind ports on that
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
    /// Each live container's data dir, where its logs are kept on removal.
    data_dirs: Mutex<HashMap<String, std::path::PathBuf>>,
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

/// Container names allow `[a-zA-Z0-9][a-zA-Z0-9_.-]`.
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
    let path = format!("/proc/{pid}/net/udp");
    let table = std::fs::read_to_string(&path).map_err(|e| format!("{path}: {e}"))?;
    Ok(table.lines().skip(1).any(|line| {
        let Some(local) = line.split_whitespace().nth(1) else { return false };
        let Some((ip, port)) = local.split_once(':') else { return false };
        let (Ok(ip), Ok(port)) = (u32::from_str_radix(ip, 16), u16::from_str_radix(port, 16)) else { return false };
        let ip = IpAddr::from(ip.to_ne_bytes());
        port == addr.port() && (ip == addr.ip() || ip.is_unspecified())
    }))
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
            if let Err(e) = docker(&["network", "create", "--label", &format!("rafka.fabric={fabric}"), &name]).await {
                // A concurrent admin of the same fabric may have created it.
                docker(&["network", "inspect", &name]).await.map_err(|_| unsupported(format!("cannot create network {name}: {e}")))?;
            }
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
        Ok(Self { fabric: fabric.into(), network, exited: Mutex::new(HashMap::new()), data_dirs: Mutex::new(HashMap::new()) })
    }

    pub fn network(&self) -> &FabricNetwork {
        &self.network
    }

    /// The allocator for this fabric's nodes: one address each on its network.
    pub fn allocator(&self, ports: (u16, u16)) -> EndpointAllocator {
        let (first, last) = self.network.node_range();
        EndpointAllocator::per_node(first, last, ports.0, ports.1)
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

    fn container_of<'a>(&self, handle: &'a DeploymentHandle) -> Result<&'a str, DeployError> {
        handle.container.as_deref().ok_or_else(|| DeployError::Terminate {
            deployment: handle.deployment_id.0.clone(),
            reason: "the handle names no container".into(),
        })
    }
}

#[async_trait::async_trait]
impl DeploymentProvider for ContainerDeploymentProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Container
    }

    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        let err = |reason: String| DeployError::Spawn { node: spec.node.to_string(), reason };
        let ip = spec.endpoints.first().map(|e| e.addr.ip()).ok_or_else(|| err("no endpoint assigned".into()))?;
        if let Some(e) = spec.endpoints.iter().find(|e| e.addr.ip() != ip) {
            return Err(err(format!("slot {} is assigned {}, but one container has one address ({ip})", e.slot, e.addr)));
        }
        std::fs::create_dir_all(&spec.data_dir).map_err(|e| err(format!("data dir {}: {e}", spec.data_dir.display())))?;
        let exe_name = spec.executable.file_name().ok_or_else(|| err(format!("{} names no file", spec.executable.display())))?;
        let exe_in = Path::new(BIN_DIR).join(exe_name);
        let name = format!("rafka-{}-{}", docker_safe(&spec.node.to_string()), docker_safe(&spec.deployment_id.0));
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
            "-v".into(),
            format!("{}:{}:ro", spec.executable.display(), exe_in.display()),
            "-v".into(),
            format!("{data}:{data}"),
        ];
        for m in RUNTIME_MOUNTS.iter().filter(|m| Path::new(m).exists()) {
            args.extend(["-v".into(), format!("{m}:{m}:ro")]);
        }
        for (k, v) in &spec.env {
            args.extend(["-e".into(), format!("{k}={v}")]);
        }
        args.push(RUNTIME_IMAGE.into());
        args.push(exe_in.display().to_string());
        args.extend(spec.args.iter().cloned());
        let argv: Vec<&str> = args.iter().map(String::as_str).collect();
        docker(&argv).await.map_err(err)?;
        self.data_dirs.lock().unwrap().insert(name.clone(), spec.data_dir.clone());
        let pid = docker(&["inspect", "--format", "{{.State.Pid}}", &name]).await.map_err(err)?.parse::<u32>().ok().filter(|p| *p > 0);
        Ok(DeploymentHandle { deployment_id: spec.deployment_id.clone(), provider: ProviderKind::Container, pid, container: Some(name) })
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
        let data_dir = self.data_dirs.lock().unwrap().remove(name);
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

    async fn holds_udp(&self, handle: &DeploymentHandle, addr: SocketAddr) -> bool {
        handle.pid.is_some_and(|pid| netns_holds_udp(pid, addr).unwrap_or(false))
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
    use super::*;

    #[test]
    fn node_addresses_start_after_the_gateway_and_stop_before_broadcast() {
        let n = FabricNetwork { name: "n".into(), base: Ipv4Addr::new(172, 18, 0, 0), prefix: 16, gateway: Ipv4Addr::new(172, 18, 0, 1) };
        assert_eq!(n.node_range(), (Ipv4Addr::new(172, 18, 0, 2), Ipv4Addr::new(172, 18, 255, 254)));
    }

    #[test]
    fn the_socket_table_is_read_in_network_byte_order() {
        let sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = sock.local_addr().unwrap();
        assert_eq!(netns_holds_udp(std::process::id(), addr), Ok(true));
        drop(sock);
        assert_eq!(netns_holds_udp(std::process::id(), addr), Ok(false));
        assert!(netns_holds_udp(u32::MAX, addr).unwrap_err().contains("/proc/"));
    }

    #[test]
    fn container_names_carry_only_safe_characters() {
        assert_eq!(docker_safe("mesh1.rpc.2/x y"), "mesh1.rpc.2-x-y");
    }
}
