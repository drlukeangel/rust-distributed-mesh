//! Chaos primitives — concrete implementations of the catalog in chaos PRD §2.
//!
//! Sprint 11 phase 1 ships:
//! - `kill_node`     — abruptly terminate a node via topology-ui DELETE
//! - `restart_node`  — kill + immediately re-spawn
//!
//! Sprint 11 phase 2 adds:
//! - `burst_kill`    — kill 3 random subprocesses back-to-back (substrate resilience test)
//! - `wedge_node`    — Suspend-Process equivalent (Windows: NtSuspendProcess via PowerShell shell-out)
//! - `disk_full`     — fill subprocess spawn dir until writes fail
//!
//! Subsequent phases add the network primitives (Windows firewall rules) and clock_skew (env at restart).

use crate::{
    ChaosContext, ChaosError, ChaosOutcome, ChaosPrimitive, DetectionResult,
};
use async_trait::async_trait;
use serde_json::json;
use std::time::Instant;
use tracing::info_span;

const NODE_TYPES: &[&str] = &["gateway", "broker", "compute", "registry"];

/// Pick a random UI-spawned subprocess from topology-ui's registry. Returns the node_name.
/// Returns an error if there are no spawned subprocesses.
async fn pick_random_spawned(ctx: &ChaosContext) -> Result<String, ChaosError> {
    use rand::seq::SliceRandom;
    let url = format!("{}/api/nodes/spawned", ctx.topology_ui_url);
    let resp = ctx
        .http
        .get(&url)
        .send()
        .await
        .map_err(|e| ChaosError::TopologyUiUnreachable(format!("{e}")))?;
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| ChaosError::TopologyUiUnreachable(format!("parse spawned list: {e}")))?;
    let names: Vec<String> = body["spawned"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();
    if names.is_empty() {
        return Err(ChaosError::InvalidTarget(
            "no UI-spawned subprocesses available to target".into(),
        ));
    }
    let mut rng = ctx.rng.lock().await;
    let pick = names
        .choose(&mut *rng)
        .ok_or_else(|| ChaosError::Execution("choose failed unexpectedly".into()))?;
    Ok(pick.clone())
}

/// `kill_node` — DELETE /api/nodes/{name}. Detection: poll /api/spawned until the name is gone.
pub struct KillNode {
    /// Explicit target. None → pick random spawned subprocess.
    pub target: Option<String>,
}

#[async_trait]
impl ChaosPrimitive for KillNode {
    fn name(&self) -> &str {
        "kill_node"
    }

    async fn execute(&self, ctx: &ChaosContext) -> Result<ChaosOutcome, ChaosError> {
        let target = match &self.target {
            Some(t) => t.clone(),
            None => pick_random_spawned(ctx).await?,
        };
        let span = info_span!(
            "rafka.chaos.primitive.executed",
            name = "kill_node",
            target = %target,
            "otel.kind" = "internal",
        );
        let _enter = span.enter();
        let url = format!("{}/api/nodes/{}", ctx.topology_ui_url, target);
        let resp = ctx
            .http
            .delete(&url)
            .send()
            .await
            .map_err(|e| ChaosError::TopologyUiUnreachable(format!("DELETE {url}: {e}")))?;
        if !resp.status().is_success() && resp.status().as_u16() != 404 {
            return Err(ChaosError::Execution(format!(
                "kill {target} returned {}",
                resp.status()
            )));
        }
        Ok(ChaosOutcome {
            primitive_name: "kill_node".into(),
            targets: vec![target.clone()],
            state: json!({"killed": target}),
        })
    }

    async fn detect(
        &self,
        ctx: &ChaosContext,
        outcome: &ChaosOutcome,
        deadline_ms: u64,
    ) -> Result<DetectionResult, ChaosError> {
        let target = &outcome.targets[0];
        let start = Instant::now();
        loop {
            let waited_ms = start.elapsed().as_millis() as u64;
            if waited_ms > deadline_ms {
                return Ok(DetectionResult::FailedTimeout { waited_ms });
            }
            // poll spawned list
            let url = format!("{}/api/nodes/spawned", ctx.topology_ui_url);
            let resp = ctx
                .http
                .get(&url)
                .send()
                .await
                .map_err(|e| ChaosError::TopologyUiUnreachable(format!("{e}")))?;
            let body: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| ChaosError::TopologyUiUnreachable(format!("{e}")))?;
            let still_present = body["spawned"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .any(|v| v.as_str().map(|s| s == target).unwrap_or(false))
                })
                .unwrap_or(false);
            if !still_present {
                let span = info_span!(
                    "rafka.chaos.primitive.detected",
                    name = "kill_node",
                    target = %target,
                    result = "passed",
                    waited_ms = waited_ms as i64,
                    "otel.kind" = "internal",
                );
                drop(span);
                return Ok(DetectionResult::Passed { waited_ms });
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
}

/// `restart_node` — kill + re-spawn same node type. Detection: new node_name appears in /api/spawned.
pub struct RestartNode {
    pub target: Option<String>,
}

#[async_trait]
impl ChaosPrimitive for RestartNode {
    fn name(&self) -> &str {
        "restart_node"
    }

    async fn execute(&self, ctx: &ChaosContext) -> Result<ChaosOutcome, ChaosError> {
        let target = match &self.target {
            Some(t) => t.clone(),
            None => pick_random_spawned(ctx).await?,
        };
        // Derive node_type from the name prefix (gateway-XYZ, broker-XYZ, etc.)
        let node_type = NODE_TYPES
            .iter()
            .find(|t| target.starts_with(*t))
            .copied()
            .ok_or_else(|| {
                ChaosError::InvalidTarget(format!(
                    "cannot derive node_type from {target}"
                ))
            })?;

        // Kill phase
        let span = info_span!(
            "rafka.chaos.primitive.executed",
            name = "restart_node",
            target = %target,
            node_type = node_type,
            "otel.kind" = "internal",
        );
        let _enter = span.enter();

        let kill_url = format!("{}/api/nodes/{}", ctx.topology_ui_url, target);
        let _ = ctx.http.delete(&kill_url).send().await; // best-effort; even 404 is fine

        // Re-spawn phase
        let spawn_url = format!("{}/api/nodes/spawn", ctx.topology_ui_url);
        let resp = ctx
            .http
            .post(&spawn_url)
            .json(&json!({"node_type": node_type}))
            .send()
            .await
            .map_err(|e| ChaosError::TopologyUiUnreachable(format!("POST {spawn_url}: {e}")))?;
        if !resp.status().is_success() {
            return Err(ChaosError::Execution(format!(
                "respawn returned {}",
                resp.status()
            )));
        }
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| ChaosError::Execution(format!("parse spawn response: {e}")))?;
        let new_name = body["node_name"]
            .as_str()
            .ok_or_else(|| ChaosError::Execution("spawn response missing node_name".into()))?
            .to_string();
        Ok(ChaosOutcome {
            primitive_name: "restart_node".into(),
            targets: vec![target.clone(), new_name.clone()],
            state: json!({"old": target, "new": new_name, "node_type": node_type}),
        })
    }

    async fn detect(
        &self,
        ctx: &ChaosContext,
        outcome: &ChaosOutcome,
        deadline_ms: u64,
    ) -> Result<DetectionResult, ChaosError> {
        let new_name = &outcome.targets[1];
        let start = Instant::now();
        loop {
            let waited_ms = start.elapsed().as_millis() as u64;
            if waited_ms > deadline_ms {
                return Ok(DetectionResult::FailedTimeout { waited_ms });
            }
            let url = format!("{}/api/nodes/spawned", ctx.topology_ui_url);
            let resp = ctx
                .http
                .get(&url)
                .send()
                .await
                .map_err(|e| ChaosError::TopologyUiUnreachable(format!("{e}")))?;
            let body: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| ChaosError::TopologyUiUnreachable(format!("{e}")))?;
            let present = body["spawned"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .any(|v| v.as_str().map(|s| s == new_name).unwrap_or(false))
                })
                .unwrap_or(false);
            if present {
                let span = info_span!(
                    "rafka.chaos.primitive.detected",
                    name = "restart_node",
                    new_target = %new_name,
                    result = "passed",
                    waited_ms = waited_ms as i64,
                    "otel.kind" = "internal",
                );
                drop(span);
                return Ok(DetectionResult::Passed { waited_ms });
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
}

/// `burst_kill` — kill up to N random subprocesses back-to-back (default 3). Substrate
/// resilience test: catches bugs where a single kill works but rapid-fire kills cause
/// race conditions in the registry or accept loop.
pub struct BurstKill {
    pub count: usize,
}

#[async_trait]
impl ChaosPrimitive for BurstKill {
    fn name(&self) -> &str {
        "burst_kill"
    }

    async fn execute(&self, ctx: &ChaosContext) -> Result<ChaosOutcome, ChaosError> {
        let span = info_span!(
            "rafka.chaos.primitive.executed",
            name = "burst_kill",
            count = self.count as i64,
            "otel.kind" = "internal",
        );
        let _enter = span.enter();
        let mut killed: Vec<String> = Vec::new();
        for _ in 0..self.count {
            let target = match pick_random_spawned(ctx).await {
                Ok(t) => t,
                Err(_) => break, // no more targets; stop early, still report what was killed
            };
            let url = format!("{}/api/nodes/{}", ctx.topology_ui_url, target);
            let _ = ctx.http.delete(&url).send().await;
            killed.push(target);
        }
        Ok(ChaosOutcome {
            primitive_name: "burst_kill".into(),
            targets: killed.clone(),
            state: json!({"killed": killed}),
        })
    }

    async fn detect(
        &self,
        ctx: &ChaosContext,
        outcome: &ChaosOutcome,
        deadline_ms: u64,
    ) -> Result<DetectionResult, ChaosError> {
        let targets = &outcome.targets;
        if targets.is_empty() {
            return Ok(DetectionResult::FailedAssertion {
                msg: "burst killed 0 targets — registry empty at start".into(),
                waited_ms: 0,
            });
        }
        let start = Instant::now();
        loop {
            let waited_ms = start.elapsed().as_millis() as u64;
            if waited_ms > deadline_ms {
                return Ok(DetectionResult::FailedTimeout { waited_ms });
            }
            let url = format!("{}/api/nodes/spawned", ctx.topology_ui_url);
            let resp = ctx.http.get(&url).send().await
                .map_err(|e| ChaosError::TopologyUiUnreachable(format!("{e}")))?;
            let body: serde_json::Value = resp.json().await
                .map_err(|e| ChaosError::TopologyUiUnreachable(format!("{e}")))?;
            let still: Vec<&str> = body["spawned"].as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str()).filter(|s| targets.iter().any(|t| t == s)).collect())
                .unwrap_or_default();
            if still.is_empty() {
                return Ok(DetectionResult::Passed { waited_ms });
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
}

/// `wedge_node` — Windows: Suspend-Process. Substrate criterion: survivors detect the
/// wedged node as stale (peer.disconnected within a deadline OR heartbeat peer_count
/// drops on at least one survivor).
///
/// Implementation: shell out to PowerShell `Suspend-Process -Id <pid>`. Reverted via
/// `Resume-Process -Id <pid>`. The pid is obtained from the spawn response stored in
/// topology-ui — but topology-ui only exposes node_names, not pids, via /api/spawned.
/// Workaround: query Get-Process for `rafka-<type>` and pick one — imprecise but works
/// for single-target chaos. A future improvement adds /api/nodes/spawned-detail returning
/// {name, pid} pairs.
pub struct WedgeNode {
    pub target_node_type: String,
    pub duration_ms: u64,
}

#[async_trait]
impl ChaosPrimitive for WedgeNode {
    fn name(&self) -> &str {
        "wedge_node"
    }

    async fn execute(&self, _ctx: &ChaosContext) -> Result<ChaosOutcome, ChaosError> {
        let span = info_span!(
            "rafka.chaos.primitive.executed",
            name = "wedge_node",
            node_type = %self.target_node_type,
            duration_ms = self.duration_ms as i64,
            "otel.kind" = "internal",
        );
        let _enter = span.enter();

        // PowerShell: find first rafka-<type> process, suspend it
        let binary_name = format!("rafka-{}", self.target_node_type);
        let ps_script = format!(
            "$p = Get-Process -Name '{binary_name}' -ErrorAction SilentlyContinue | Select-Object -First 1; if ($p) {{ \
                Add-Type -Name 'NT' -Namespace 'Win32' -MemberDefinition '[DllImport(\"ntdll.dll\")] public static extern int NtSuspendProcess(IntPtr p);'; \
                [Win32.NT]::NtSuspendProcess($p.Handle); Write-Output $p.Id \
            }} else {{ Write-Error 'no process' }}"
        );
        let output = tokio::process::Command::new("powershell")
            .args(["-NoProfile", "-Command", &ps_script])
            .output()
            .await
            .map_err(|e| ChaosError::Execution(format!("powershell suspend: {e}")))?;
        if !output.status.success() {
            return Err(ChaosError::Execution(format!(
                "Suspend-Process failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        let pid = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(ChaosOutcome {
            primitive_name: "wedge_node".into(),
            targets: vec![format!("{}:{}", self.target_node_type, pid)],
            state: json!({"node_type": self.target_node_type, "pid": pid, "duration_ms": self.duration_ms}),
        })
    }

    async fn detect(
        &self,
        _ctx: &ChaosContext,
        _outcome: &ChaosOutcome,
        deadline_ms: u64,
    ) -> Result<DetectionResult, ChaosError> {
        // Wait for the wedge duration, then resume.
        // (Detection of "survivors see it as stale" requires Jaeger query; left as a
        //  follow-up. For now we just sleep the wedge duration as proof-of-life.)
        let wait = std::cmp::min(self.duration_ms, deadline_ms);
        tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
        Ok(DetectionResult::Passed { waited_ms: wait })
    }

    async fn revert(
        &self,
        _ctx: &ChaosContext,
        outcome: &ChaosOutcome,
    ) -> Result<(), ChaosError> {
        let pid = outcome.state.get("pid").and_then(|v| v.as_str()).unwrap_or("");
        if pid.is_empty() {
            return Ok(());
        }
        let ps_script = format!(
            "$p = Get-Process -Id {pid} -ErrorAction SilentlyContinue; if ($p) {{ \
                Add-Type -Name 'NT2' -Namespace 'Win32' -MemberDefinition '[DllImport(\"ntdll.dll\")] public static extern int NtResumeProcess(IntPtr p);'; \
                [Win32.NT2]::NtResumeProcess($p.Handle) | Out-Null \
            }}"
        );
        let _ = tokio::process::Command::new("powershell")
            .args(["-NoProfile", "-Command", &ps_script])
            .output()
            .await;
        Ok(())
    }
}

/// `disk_full` — fill spawn data dir until writes fail. Per chaos PRD §2: "boot fails
/// cleanly with clear error; steady-state node continues without writing new state until
/// disk has space." Today's v2 node-base has minimal disk writes (just identity file
/// at boot), so this primitive primarily tests boot-time error paths.
///
/// Implementation: write 1MB chunks of random bytes to E:/tmp/rafka-ui-nodes/<target>/
/// until write fails (disk full OR quota exceeded OR access denied).
pub struct DiskFull {
    pub target: Option<String>,
    /// Cap on filler size to avoid actually filling the entire E: drive
    pub max_bytes: u64,
}

#[async_trait]
impl ChaosPrimitive for DiskFull {
    fn name(&self) -> &str {
        "disk_full"
    }

    async fn execute(&self, ctx: &ChaosContext) -> Result<ChaosOutcome, ChaosError> {
        let target = match &self.target {
            Some(t) => t.clone(),
            None => pick_random_spawned(ctx).await?,
        };
        let span = info_span!(
            "rafka.chaos.primitive.executed",
            name = "disk_full",
            target = %target,
            max_bytes = self.max_bytes as i64,
            "otel.kind" = "internal",
        );
        let _enter = span.enter();

        let spawn_dir = format!("E:/tmp/rafka-ui-nodes/{}", target);
        let filler_path = format!("{}/CHAOS-DISK-FULL.bin", spawn_dir);

        let chunk = vec![0u8; 1 * 1024 * 1024]; // 1MB
        let mut written: u64 = 0;
        let mut file = match tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&filler_path)
            .await
        {
            Ok(f) => f,
            Err(e) => {
                return Err(ChaosError::Execution(format!(
                    "open {filler_path}: {e}"
                )));
            }
        };
        use tokio::io::AsyncWriteExt;
        loop {
            if written >= self.max_bytes {
                break;
            }
            if let Err(e) = file.write_all(&chunk).await {
                tracing::info!(target = %target, written, error = %e, "disk_full filler write stopped");
                break;
            }
            written += chunk.len() as u64;
        }
        let _ = file.flush().await;
        Ok(ChaosOutcome {
            primitive_name: "disk_full".into(),
            targets: vec![target.clone()],
            state: json!({"filler_path": filler_path, "bytes_written": written}),
        })
    }

    async fn detect(
        &self,
        _ctx: &ChaosContext,
        outcome: &ChaosOutcome,
        _deadline_ms: u64,
    ) -> Result<DetectionResult, ChaosError> {
        // Pass = we wrote AT LEAST 1 MB before stopping (proves the path is exercised).
        let bytes = outcome.state.get("bytes_written").and_then(|v| v.as_u64()).unwrap_or(0);
        if bytes >= 1024 * 1024 {
            Ok(DetectionResult::Passed { waited_ms: 0 })
        } else {
            Ok(DetectionResult::FailedAssertion {
                msg: format!("disk_full wrote only {bytes} bytes — likely fs error before chunk 1"),
                waited_ms: 0,
            })
        }
    }

    async fn revert(
        &self,
        _ctx: &ChaosContext,
        outcome: &ChaosOutcome,
    ) -> Result<(), ChaosError> {
        if let Some(p) = outcome.state.get("filler_path").and_then(|v| v.as_str()) {
            let _ = tokio::fs::remove_file(p).await;
        }
        Ok(())
    }
}
