//! `ProcessDeploymentProvider`: one OS process per node on the shared host
//! network namespace (PRD §10: the process provider is the port-collision gate).
//!
//! The process is started detached in its own process group so it outlives the
//! admin that launched it. Its runtime is the pid together with the kernel's
//! start time of that pid, within this host's boot and pid namespace (the
//! control domain): a successor admin adopts it from the birth's published
//! runtime fact, and a recycled pid never matches the old birth.

use super::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
#[cfg(test)]
use rafka_mesh_entity::RuntimeLocator;
use crate::model::ProviderKind;
use rafka_mesh_entity::runtime::{process_control_domain, process_start_token};
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Mutex;
use std::time::Duration;

/// Written into the node's data dir at spawn: `{"deployment_id", "pid"}`.
pub const DEPLOYMENT_FILE: &str = "deployment.json";

/// One child this provider started: its deployment, and its exit once seen.
#[derive(Debug, Clone)]
struct Launched {
    deployment_id: crate::model::DeploymentId,
    start: Option<u64>,
    /// `None` while it runs; `Some(code)` once it exited (reaped).
    exit: Option<Option<i32>>,
}

pub struct ProcessDeploymentProvider {
    /// Children this provider started. A watcher thread per child waits on
    /// it, so an exit is reaped at once (no zombie) and its code recorded.
    children: std::sync::Arc<Mutex<HashMap<u32, Launched>>>,
    /// This host's boot and pid namespace.
    domain: String,
}

impl Default for ProcessDeploymentProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessDeploymentProvider {
    pub fn new() -> Self {
        Self { children: Default::default(), domain: process_control_domain().unwrap_or_else(|e| format!("process:unknown:{e}")) }
    }

    fn handle(&self, deployment_id: crate::model::DeploymentId, pid: u32, start: Option<u64>) -> DeploymentHandle {
        DeploymentHandle { deployment_id, provider: ProviderKind::Process, pid: Some(pid), start, container: None, domain: Some(self.domain.clone()) }
    }
}

/// `pid` is the process `start` names: alive, and started then. A handle
/// without a start token is trusted on the pid alone only for a child this
/// provider holds.
#[cfg(unix)]
fn is(pid: u32, start: Option<u64>) -> bool {
    match start {
        Some(s) => process_start_token(pid) == Some(s) && alive(pid),
        None => alive(pid),
    }
}

#[cfg(unix)]
fn signal(pid: u32, sig: i32) -> bool {
    // SAFETY: kill(2) with a pid we were handed; no memory is touched.
    unsafe { libc::kill(pid as i32, sig) == 0 }
}

#[cfg(unix)]
fn alive(pid: u32) -> bool {
    // A process is gone only once every task of its thread group has exited. The leader's own
    // state is not proof: during `exit_group` the leader can already read as a zombie while its
    // sibling threads are still exiting, and the group's files (its sockets) are released only by
    // the last of them. Judging the leader alone let a restart spawn the next birth while the
    // old one still held the transport port (`WaitForBind: Address already in use`).
    match std::fs::read_dir(format!("/proc/{pid}/task")) {
        Ok(tasks) => tasks.flatten().any(|t| std::fs::read_to_string(t.path().join("stat")).is_ok_and(|stat| !is_zombie(&stat))),
        // No task directory: the pid is gone, or /proc is not readable here; kill(2) with signal 0
        // answers whether the pid exists at all.
        Err(_) => std::fs::metadata(format!("/proc/{pid}")).is_ok() && signal(pid, 0),
    }
}

/// `stat` (a `/proc/<pid>/stat` or `/proc/<pid>/task/<tid>/stat` line) names a zombie.
fn is_zombie(stat: &str) -> bool {
    stat.rsplit(')').next().is_some_and(|rest| rest.trim_start().starts_with('Z'))
}

#[async_trait::async_trait]
impl DeploymentProvider for ProcessDeploymentProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }

    fn control_domain(&self) -> String {
        self.domain.clone()
    }

    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        let err = |reason: String| DeployError::Spawn { node: spec.node.to_string(), reason };
        std::fs::create_dir_all(&spec.data_dir).map_err(|e| err(format!("data dir {}: {e}", spec.data_dir.display())))?;
        let log = |name: &str| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(spec.data_dir.join(name))
                .map(Stdio::from)
                .map_err(|e| err(format!("{name}: {e}")))
        };
        // A record left by an earlier birth in this data dir is not this one's:
        // the birth waits for the one the pipeline makes available.
        let _ = std::fs::remove_file(spec.data_dir.join(rafka_mesh_entity::runtime::RUNTIME_FILE));
        rafka_mesh_entity::runtime::ExitRecord::clear(&spec.data_dir);
        let mut cmd = std::process::Command::new(&spec.executable);
        cmd.args(&spec.args).envs(&spec.env).stdin(Stdio::null()).stdout(log("stdout.log")?).stderr(log("stderr.log")?);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            cmd.process_group(0);
        }
        let child = cmd.spawn().map_err(|e| err(format!("{}: {e}", spec.executable.display())))?;
        let pid = child.id();
        let start = process_start_token(pid);
        // Which deployment this pid realises, so a re-run can find it.
        let record = serde_json::json!({ "deployment_id": spec.deployment_id.0, "pid": pid, "start": start });
        std::fs::write(spec.data_dir.join(DEPLOYMENT_FILE), record.to_string()).map_err(|e| err(format!("{DEPLOYMENT_FILE}: {e}")))?;
        self.children.lock().unwrap().insert(pid, Launched { deployment_id: spec.deployment_id.clone(), start, exit: None });
        let children = self.children.clone();
        std::thread::Builder::new()
            .name(format!("reap-{pid}"))
            .spawn(move || {
                let mut child = child;
                let code = child.wait().ok().and_then(|s| s.code());
                if let Some(l) = children.lock().unwrap().get_mut(&pid) {
                    l.exit = Some(code);
                }
            })
            .map_err(|e| err(format!("watching pid {pid}: {e}")))?;
        // The pipeline registers this exact handle and makes its runtime fact
        // available to the birth (`MakeRuntimeFactAvailableToBirth`).
        Ok(self.handle(spec.deployment_id.clone(), pid, start))
    }

    async fn terminate(&self, handle: &DeploymentHandle, mode: TerminationMode) -> Result<(), DeployError> {
        let Some(pid) = handle.pid else {
            return Err(DeployError::Terminate { deployment: handle.deployment_id.0.clone(), reason: "no pid".into() });
        };
        #[cfg(unix)]
        if !self.is_ours(pid, handle.start) {
            // Exited, or the pid now names another process: nothing of this birth to stop.
            return Ok(());
        }
        #[cfg(unix)]
        {
            let grace = match mode {
                TerminationMode::Graceful { grace } => {
                    signal(pid, libc::SIGTERM);
                    grace
                }
                TerminationMode::Immediate => Duration::ZERO,
            };
            let until = tokio::time::Instant::now() + grace;
            loop {
                if !self.running(pid, handle.start) {
                    break;
                }
                if tokio::time::Instant::now() >= until {
                    signal(pid, libc::SIGKILL);
                    for _ in 0..100 {
                        if !self.running(pid, handle.start) {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        Ok(())
    }

    async fn signal_stop(&self, handle: &DeploymentHandle) -> Result<(), DeployError> {
        let Some(pid) = handle.pid else {
            return Err(DeployError::Terminate { deployment: handle.deployment_id.0.clone(), reason: "no pid".into() });
        };
        #[cfg(unix)]
        if self.running(pid, handle.start) {
            signal(pid, libc::SIGTERM);
        }
        Ok(())
    }

    async fn find(&self, spec: &ResolvedNodeLaunch) -> Option<DeploymentHandle> {
        let raw = std::fs::read_to_string(spec.data_dir.join(DEPLOYMENT_FILE)).ok()?;
        let v: serde_json::Value = serde_json::from_str(&raw).ok()?;
        if v.get("deployment_id")?.as_str()? != spec.deployment_id.0 {
            return None;
        }
        let pid = u32::try_from(v.get("pid")?.as_u64()?).ok()?;
        let start = v.get("start").and_then(|s| s.as_u64());
        #[cfg(unix)]
        if !Self::runs(pid, &spec.executable) || start.is_some_and(|s| process_start_token(pid) != Some(s)) {
            return None;
        }
        Some(self.handle(spec.deployment_id.clone(), pid, start.or_else(|| process_start_token(pid))))
    }

    fn launched(&self) -> Vec<DeploymentHandle> {
        self.children
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, l)| l.exit.is_none())
            .map(|(pid, l)| self.handle(l.deployment_id.clone(), *pid, l.start))
            .collect()
    }

    async fn inspect(&self, handle: &DeploymentHandle) -> DeploymentStatus {
        let Some(pid) = handle.pid else { return DeploymentStatus::Unknown };
        if let Some(l) = self.children.lock().unwrap().get(&pid).filter(|l| handle.start.is_none() || l.start == handle.start) {
            return match l.exit {
                Some(code) => DeploymentStatus::Exited { code },
                None => DeploymentStatus::Running,
            };
        }
        // A recycled pid runs another process: this birth has exited.
        #[cfg(unix)]
        if is(pid, handle.start) {
            return DeploymentStatus::Running;
        }
        DeploymentStatus::Exited { code: None }
    }
}

impl ProcessDeploymentProvider {
    /// `pid` is alive and runs `executable` (a recycled pid runs something else).
    #[cfg(unix)]
    fn runs(pid: u32, executable: &std::path::Path) -> bool {
        alive(pid)
            && std::fs::read_link(format!("/proc/{pid}/exe")).is_ok_and(|exe| {
                exe == executable || std::fs::canonicalize(executable).is_ok_and(|c| c == exe)
            })
    }

    /// Still running: a child of ours not yet seen to exit, or the live
    /// process `start` names (an adopted runtime).
    fn running(&self, pid: u32, start: Option<u64>) -> bool {
        match self.children.lock().unwrap().get(&pid).filter(|l| start.is_none() || l.start == start) {
            Some(l) => l.exit.is_none(),
            #[cfg(unix)]
            None => is(pid, start),
            #[cfg(not(unix))]
            None => false,
        }
    }

    /// The handle's process: our child, or the exact adopted process.
    #[cfg(unix)]
    fn is_ours(&self, pid: u32, start: Option<u64>) -> bool {
        self.running(pid, start)
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use crate::deployment::provider::adopt;
    use crate::model::DeploymentId;

    fn launch(dir: &std::path::Path) -> ResolvedNodeLaunch {
        ResolvedNodeLaunch {
            node: "mesh1.rpc.1".parse().unwrap(),
            deployment_id: DeploymentId::mint(),
            executable: "/bin/sleep".into(),
            args: vec!["30".into()],
            env: Default::default(),
            data_dir: dir.to_path_buf(),
            transport: "127.0.0.1:0".parse().unwrap(),
            listeners: vec![],
        }
    }

    #[tokio::test]
    async fn a_successor_controls_the_exact_process_and_never_a_recycled_pid() {
        let dir = std::env::temp_dir().join(format!("rafka-process-adopt-{}", std::process::id()));
        let launcher = ProcessDeploymentProvider::new();
        let h = launcher.spawn(&launch(&dir)).await.unwrap();
        // The handle names exactly the runtime it started; making its fact
        // available to the birth is the pipeline's step, not the provider's.
        let fact = h.fact().expect("an exact handle");
        assert!(rafka_mesh_entity::RuntimeFact::read_record(&dir).is_none());
        let successor = ProcessDeploymentProvider::new();
        // The same pid under another start token is another process: never ours to signal.
        let mut forged = fact.clone();
        let RuntimeLocator::Process { pid, start } = fact.locator.clone() else { panic!() };
        forged.locator = RuntimeLocator::Process { pid, start: start + 1 };
        let forged = adopt(&successor, &forged).unwrap();
        assert!(matches!(successor.inspect(&forged).await, DeploymentStatus::Exited { .. }), "a recycled pid is not the old birth");
        successor.terminate(&forged, TerminationMode::Immediate).await.unwrap();
        assert!(alive(pid), "nothing was signalled through a recycled pid");
        // The exact fact: the successor inspects and stops it.
        let adopted = adopt(&successor, &fact).unwrap();
        assert_eq!(successor.inspect(&adopted).await, DeploymentStatus::Running);
        successor.terminate(&adopted, TerminationMode::Graceful { grace: Duration::from_secs(2) }).await.unwrap();
        for _ in 0..100 {
            if launcher.inspect(&h).await != DeploymentStatus::Running {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(matches!(launcher.inspect(&h).await, DeploymentStatus::Exited { .. }));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
