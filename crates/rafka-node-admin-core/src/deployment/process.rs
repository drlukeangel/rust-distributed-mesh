//! `ProcessDeploymentProvider`: one OS process per node on the shared host
//! network namespace (PRD §10: the process provider is the port-collision gate).
//!
//! The process is started detached in its own process group so it outlives the
//! admin that launched it; a successor admin manages it by pid.

use super::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use crate::model::ProviderKind;
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
    /// `None` while it runs; `Some(code)` once it exited (reaped).
    exit: Option<Option<i32>>,
}

#[derive(Default)]
pub struct ProcessDeploymentProvider {
    /// Children this provider started. A watcher thread per child waits on
    /// it, so an exit is reaped at once (no zombie) and its code recorded.
    children: std::sync::Arc<Mutex<HashMap<u32, Launched>>>,
}

impl ProcessDeploymentProvider {
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(unix)]
fn signal(pid: u32, sig: i32) -> bool {
    // SAFETY: kill(2) with a pid we were handed; no memory is touched.
    unsafe { libc::kill(pid as i32, sig) == 0 }
}

#[cfg(unix)]
fn alive(pid: u32) -> bool {
    // A zombie is not alive: read its state from /proc when available.
    if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        return !stat.rsplit(')').next().is_some_and(|rest| rest.trim_start().starts_with('Z'));
    }
    signal(pid, 0)
}

#[async_trait::async_trait]
impl DeploymentProvider for ProcessDeploymentProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
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
        let mut cmd = std::process::Command::new(&spec.executable);
        cmd.args(&spec.args).envs(&spec.env).stdin(Stdio::null()).stdout(log("stdout.log")?).stderr(log("stderr.log")?);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            cmd.process_group(0);
        }
        let child = cmd.spawn().map_err(|e| err(format!("{}: {e}", spec.executable.display())))?;
        let pid = child.id();
        // Which deployment this pid realises, so a re-run can find it.
        let record = serde_json::json!({ "deployment_id": spec.deployment_id.0, "pid": pid });
        std::fs::write(spec.data_dir.join(DEPLOYMENT_FILE), record.to_string()).map_err(|e| err(format!("{DEPLOYMENT_FILE}: {e}")))?;
        self.children.lock().unwrap().insert(pid, Launched { deployment_id: spec.deployment_id.clone(), exit: None });
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
        Ok(DeploymentHandle { deployment_id: spec.deployment_id.clone(), provider: ProviderKind::Process, pid: Some(pid), container: None })
    }

    async fn terminate(&self, handle: &DeploymentHandle, mode: TerminationMode) -> Result<(), DeployError> {
        let Some(pid) = handle.pid else {
            return Err(DeployError::Terminate { deployment: handle.deployment_id.0.clone(), reason: "no pid".into() });
        };
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
                if !self.running(pid) {
                    break;
                }
                if tokio::time::Instant::now() >= until {
                    signal(pid, libc::SIGKILL);
                    for _ in 0..100 {
                        if !self.running(pid) {
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
        if self.running(pid) {
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
        #[cfg(unix)]
        if !Self::runs(pid, &spec.executable) {
            return None;
        }
        Some(DeploymentHandle { deployment_id: spec.deployment_id.clone(), provider: ProviderKind::Process, pid: Some(pid), container: None })
    }

    fn launched(&self) -> Vec<DeploymentHandle> {
        self.children
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, l)| l.exit.is_none())
            .map(|(pid, l)| DeploymentHandle { deployment_id: l.deployment_id.clone(), provider: ProviderKind::Process, pid: Some(*pid), container: None })
            .collect()
    }

    async fn inspect(&self, handle: &DeploymentHandle) -> DeploymentStatus {
        let Some(pid) = handle.pid else { return DeploymentStatus::Unknown };
        if let Some(l) = self.children.lock().unwrap().get(&pid) {
            return match l.exit {
                Some(code) => DeploymentStatus::Exited { code },
                None => DeploymentStatus::Running,
            };
        }
        #[cfg(unix)]
        if alive(pid) {
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

    /// Still running: a child of ours not yet seen to exit, or any other
    /// live process with that pid.
    fn running(&self, pid: u32) -> bool {
        match self.children.lock().unwrap().get(&pid) {
            Some(l) => l.exit.is_none(),
            #[cfg(unix)]
            None => alive(pid),
            #[cfg(not(unix))]
            None => false,
        }
    }
}
