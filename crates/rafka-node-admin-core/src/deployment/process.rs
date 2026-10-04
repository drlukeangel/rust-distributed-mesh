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

#[derive(Default)]
pub struct ProcessDeploymentProvider {
    /// Children this admin spawned, so their exit can be reaped.
    children: Mutex<HashMap<u32, std::process::Child>>,
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
        self.children.lock().unwrap().insert(pid, child);
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
                self.reap(pid);
                if !alive(pid) {
                    break;
                }
                if tokio::time::Instant::now() >= until {
                    signal(pid, libc::SIGKILL);
                    for _ in 0..100 {
                        self.reap(pid);
                        if !alive(pid) {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        self.children.lock().unwrap().remove(&pid);
        Ok(())
    }

    async fn signal_stop(&self, handle: &DeploymentHandle) -> Result<(), DeployError> {
        let Some(pid) = handle.pid else {
            return Err(DeployError::Terminate { deployment: handle.deployment_id.0.clone(), reason: "no pid".into() });
        };
        #[cfg(unix)]
        {
            self.reap(pid);
            if alive(pid) {
                signal(pid, libc::SIGTERM);
            }
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
        self.reap(pid);
        #[cfg(unix)]
        if !Self::runs(pid, &spec.executable) {
            return None;
        }
        Some(DeploymentHandle { deployment_id: spec.deployment_id.clone(), provider: ProviderKind::Process, pid: Some(pid), container: None })
    }

    async fn inspect(&self, handle: &DeploymentHandle) -> DeploymentStatus {
        let Some(pid) = handle.pid else { return DeploymentStatus::Unknown };
        if let Some(c) = self.children.lock().unwrap().get_mut(&pid) {
            return match c.try_wait() {
                Ok(Some(s)) => DeploymentStatus::Exited { code: s.code() },
                Ok(None) => DeploymentStatus::Running,
                Err(_) => DeploymentStatus::Unknown,
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

    fn reap(&self, pid: u32) {
        if let Some(c) = self.children.lock().unwrap().get_mut(&pid) {
            let _ = c.try_wait();
        }
    }
}
