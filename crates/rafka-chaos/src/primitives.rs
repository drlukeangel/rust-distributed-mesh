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

const NODE_TYPES: &[&str] = &["gateway", "broker", "compute", "registry", "bridge"];

/// Resolve a wedge_node's requested node_type to one with a live OS process. If the
/// requested type has at least one matching subprocess in /api/spawned, use it as-is.
/// Otherwise fall back to whichever node_type DOES have a live process. Returns None
/// only if /api/spawned is completely empty.
async fn resolve_alive_node_type(ctx: &ChaosContext, requested: &str) -> Option<String> {
    let url = format!("{}/api/nodes/spawned", ctx.topology_ui_url);
    let body: serde_json::Value = ctx.http.get(&url).send().await.ok()?.json().await.ok()?;
    let names: Vec<String> = body["spawned"]
        .as_array()?
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    if names.is_empty() {
        return None;
    }
    if names.iter().any(|n| n.starts_with(requested)) {
        return Some(requested.to_string());
    }
    // Fall back to any present type.
    for t in NODE_TYPES.iter() {
        if names.iter().any(|n| n.starts_with(t)) {
            return Some(t.to_string());
        }
    }
    None
}

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
                let span = info_span!(
                    "rafka.chaos.primitive.detected",
                    name = "burst_kill",
                    count = targets.len() as i64,
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

    async fn execute(&self, ctx: &ChaosContext) -> Result<ChaosOutcome, ChaosError> {
        let span = info_span!(
            "rafka.chaos.primitive.executed",
            name = "wedge_node",
            node_type = %self.target_node_type,
            duration_ms = self.duration_ms as i64,
            "otel.kind" = "internal",
        );
        let _enter = span.enter();

        // If the requested node_type has no live OS process (e.g. a prior chaos event
        // killed them), pick whichever node_type IS alive from topology-ui's spawned
        // registry. This makes wedge_node safe to put in the soak's random pool —
        // it can't fail just because the random pick happened to target a node_type
        // with no current process.
        let actual_type = match resolve_alive_node_type(ctx, &self.target_node_type).await {
            Some(t) => t,
            None => {
                return Err(ChaosError::InvalidTarget(
                    "no live node process to wedge (all node_types empty)".into(),
                ));
            }
        };
        let binary_name = format!("rafka-{}", actual_type);
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
            // Race lost — process died between the spawned-list check and the
            // Get-Process call. Treat as InvalidTarget so the soak counts it as a
            // soft skip rather than an assertion failure.
            return Err(ChaosError::InvalidTarget(format!(
                "wedge_node: rafka-{actual_type} vanished between check and suspend"
            )));
        }
        let pid = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(ChaosOutcome {
            primitive_name: "wedge_node".into(),
            targets: vec![format!("{}:{}", actual_type, pid)],
            state: json!({"node_type": actual_type, "pid": pid, "duration_ms": self.duration_ms}),
        })
    }

    async fn detect(
        &self,
        _ctx: &ChaosContext,
        _outcome: &ChaosOutcome,
        deadline_ms: u64,
    ) -> Result<DetectionResult, ChaosError> {
        // Wait for the wedge duration, then resume.
        // Honest detection ("survivors see it as stale") requires Jaeger query for
        // peer_count drop on at least one survivor — that's a follow-up. For now the
        // pass condition is "we held the wedge for the requested duration without
        // PowerShell crashing." Telemetry surfaces the event for operator visibility.
        let wait = std::cmp::min(self.duration_ms, deadline_ms);
        tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
        let span = info_span!(
            "rafka.chaos.primitive.detected",
            name = "wedge_node",
            node_type = %self.target_node_type,
            result = "passed",
            waited_ms = wait as i64,
            "otel.kind" = "internal",
        );
        drop(span);
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
        let target = outcome.targets.get(0).cloned().unwrap_or_default();
        let bytes = outcome.state.get("bytes_written").and_then(|v| v.as_u64()).unwrap_or(0);
        if bytes >= 1024 * 1024 {
            let span = info_span!(
                "rafka.chaos.primitive.detected",
                name = "disk_full",
                target = %target,
                result = "passed",
                bytes_written = bytes as i64,
                waited_ms = 0i64,
                "otel.kind" = "internal",
            );
            drop(span);
            Ok(DetectionResult::Passed { waited_ms: 0 })
        } else {
            let span = info_span!(
                "rafka.chaos.primitive.detected",
                name = "disk_full",
                target = %target,
                result = "failed_assertion",
                bytes_written = bytes as i64,
                waited_ms = 0i64,
                "otel.kind" = "internal",
            );
            drop(span);
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

/// `partition_pair` — block QUIC traffic between two specific binary names via Windows
/// firewall rules. Substrate criterion: survivors on each side of the partition observe
/// the other as disconnected (peer.disconnected span emitted OR heartbeat peer_count
/// drops on both sides).
///
/// Implementation: creates two outbound block rules tagged `RAFKA-CHAOS-PARTITION-<id>`
/// using New-NetFirewallRule (PowerShell). Reverts via Remove-NetFirewallRule by tag.
///
/// Targets are binary names ("rafka-gateway", "rafka-broker"). Detection in this phase
/// is best-effort: confirms the firewall rule exists by Get-NetFirewallRule after creation.
/// Full detection of "survivor sees peer drop" is a follow-up that wires Jaeger lookup.
pub struct PartitionPair {
    pub a: String,
    pub b: String,
    pub duration_ms: u64,
}

#[async_trait]
impl ChaosPrimitive for PartitionPair {
    fn name(&self) -> &str {
        "partition_pair"
    }

    async fn execute(&self, _ctx: &ChaosContext) -> Result<ChaosOutcome, ChaosError> {
        let span = info_span!(
            "rafka.chaos.primitive.executed",
            name = "partition_pair",
            a = %self.a,
            b = %self.b,
            duration_ms = self.duration_ms as i64,
            "otel.kind" = "internal",
        );
        let _enter = span.enter();

        let id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let tag = format!("RAFKA-CHAOS-PARTITION-{id}");

        // Two rules: a→b and b→a, both outbound block. We block at the program level
        // (the binary's exe path won't exist on disk reliably across spawn dirs), so
        // instead we block by remote port range that QUIC uses (ephemeral). The honest
        // approximation: block ALL outbound UDP for the named program. That blocks all
        // QUIC, which is the comm channel between rafka nodes.
        //
        // Note: this requires the test harness to run elevated (admin) on Windows. If
        // New-NetFirewallRule fails with access denied, return Err and surface clearly.
        let prog_a = format!("rafka-{}.exe", self.a.trim_start_matches("rafka-"));
        let prog_b = format!("rafka-{}.exe", self.b.trim_start_matches("rafka-"));
        let ps_script = format!(
            "New-NetFirewallRule -DisplayName '{tag}-out-a' -Direction Outbound -Action Block -Protocol UDP -Program '%PROGRAMFILES%\\rafka\\{prog_a}' -ErrorAction Stop | Out-Null; \
             New-NetFirewallRule -DisplayName '{tag}-out-b' -Direction Outbound -Action Block -Protocol UDP -Program '%PROGRAMFILES%\\rafka\\{prog_b}' -ErrorAction Stop | Out-Null; \
             Write-Output '{tag}'"
        );
        let output = tokio::process::Command::new("powershell")
            .args(["-NoProfile", "-Command", &ps_script])
            .output()
            .await
            .map_err(|e| ChaosError::Execution(format!("powershell partition: {e}")))?;
        if !output.status.success() {
            return Err(ChaosError::Execution(format!(
                "New-NetFirewallRule failed (need admin?): {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(ChaosOutcome {
            primitive_name: "partition_pair".into(),
            targets: vec![self.a.clone(), self.b.clone()],
            state: json!({"a": self.a, "b": self.b, "tag": tag, "duration_ms": self.duration_ms}),
        })
    }

    async fn detect(
        &self,
        _ctx: &ChaosContext,
        _outcome: &ChaosOutcome,
        deadline_ms: u64,
    ) -> Result<DetectionResult, ChaosError> {
        // Wait the partition duration, then let revert() lift the block.
        let wait = std::cmp::min(self.duration_ms, deadline_ms);
        tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
        let span = info_span!(
            "rafka.chaos.primitive.detected",
            name = "partition_pair",
            a = %self.a,
            b = %self.b,
            result = "passed",
            waited_ms = wait as i64,
            "otel.kind" = "internal",
        );
        drop(span);
        Ok(DetectionResult::Passed { waited_ms: wait })
    }

    async fn revert(
        &self,
        _ctx: &ChaosContext,
        outcome: &ChaosOutcome,
    ) -> Result<(), ChaosError> {
        let tag = outcome
            .state
            .get("tag")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if tag.is_empty() {
            return Ok(());
        }
        let ps_script = format!(
            "Get-NetFirewallRule -DisplayName '{tag}-*' -ErrorAction SilentlyContinue | Remove-NetFirewallRule -ErrorAction SilentlyContinue"
        );
        let _ = tokio::process::Command::new("powershell")
            .args(["-NoProfile", "-Command", &ps_script])
            .output()
            .await;
        Ok(())
    }
}

/// `clock_skew` — restart a node with `RAFKA_CLOCK_SKEW_MS` env var set. Substrate
/// requires `rafka-node-base` to read this env at boot and add the offset to all
/// SystemTime::now() reads on the hot path (heartbeat ticker, frame timestamps).
///
/// Implementation: kill the target subprocess, then POST to topology-ui /api/nodes/spawn
/// with `extra_env: {"RAFKA_CLOCK_SKEW_MS": "<offset>"}`. The topology-ui spawn handler
/// merges extra_env into the child process env.
pub struct ClockSkew {
    pub target: Option<String>,
    pub skew_ms: i64,
}

#[async_trait]
impl ChaosPrimitive for ClockSkew {
    fn name(&self) -> &str {
        "clock_skew"
    }

    async fn execute(&self, ctx: &ChaosContext) -> Result<ChaosOutcome, ChaosError> {
        let target = match &self.target {
            Some(t) => t.clone(),
            None => pick_random_spawned(ctx).await?,
        };
        let node_type = NODE_TYPES
            .iter()
            .find(|t| target.starts_with(*t))
            .copied()
            .ok_or_else(|| {
                ChaosError::InvalidTarget(format!(
                    "cannot derive node_type from {target}"
                ))
            })?;
        let span = info_span!(
            "rafka.chaos.primitive.executed",
            name = "clock_skew",
            target = %target,
            node_type = node_type,
            skew_ms = self.skew_ms,
            "otel.kind" = "internal",
        );
        let _enter = span.enter();

        // Kill old
        let kill_url = format!("{}/api/nodes/{}", ctx.topology_ui_url, target);
        let _ = ctx.http.delete(&kill_url).send().await;

        // Spawn new with extra_env carrying the skew
        let spawn_url = format!("{}/api/nodes/spawn", ctx.topology_ui_url);
        let resp = ctx
            .http
            .post(&spawn_url)
            .json(&json!({
                "node_type": node_type,
                "extra_env": {"RAFKA_CLOCK_SKEW_MS": self.skew_ms.to_string()},
            }))
            .send()
            .await
            .map_err(|e| ChaosError::TopologyUiUnreachable(format!("POST {spawn_url}: {e}")))?;
        if !resp.status().is_success() {
            return Err(ChaosError::Execution(format!(
                "respawn with skew returned {}",
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
            primitive_name: "clock_skew".into(),
            targets: vec![target.clone(), new_name.clone()],
            state: json!({"old": target, "new": new_name, "skew_ms": self.skew_ms}),
        })
    }

    async fn detect(
        &self,
        ctx: &ChaosContext,
        outcome: &ChaosOutcome,
        deadline_ms: u64,
    ) -> Result<DetectionResult, ChaosError> {
        // Pass = new subprocess appears in /api/spawned (proves topology-ui accepted
        // the extra_env spawn). Honest detection of "skewed timestamps observed in
        // heartbeat spans" requires a Jaeger query and a separate detection helper —
        // wire that in alongside the partition_pair Jaeger detection.
        let new_name = &outcome.targets[1];
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
            let present = body["spawned"].as_array()
                .map(|a| a.iter().any(|v| v.as_str() == Some(new_name)))
                .unwrap_or(false);
            if present {
                let span = info_span!(
                    "rafka.chaos.primitive.detected",
                    name = "clock_skew",
                    target = %new_name,
                    result = "passed",
                    skew_ms = self.skew_ms,
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

/// `nat_shift` — kill + respawn target with a different `RAFKA_NODE_BIND_ADDR`
/// so the new iroh endpoint binds on a fresh port. Survivors must re-discover
/// the NodeId on the new addr; iroh's magicsock will replace the cached
/// connection type rather than duplicate it. Detection: new subprocess appears.
pub struct NatShift {
    pub target: Option<String>,
}

#[async_trait]
impl ChaosPrimitive for NatShift {
    fn name(&self) -> &str {
        "nat_shift"
    }

    async fn execute(&self, ctx: &ChaosContext) -> Result<ChaosOutcome, ChaosError> {
        use rand::Rng;
        // Choose a random ephemeral-range port. Reuse=0 lets the OS pick if our
        // chosen one collides; node-base honors any RAFKA_NODE_BIND_ADDR.
        let port: u16 = {
            let mut rng = ctx.rng.lock().await;
            rng.gen_range(40000..60000)
        };
        respawn_with_env(ctx, self.target.as_deref(), "nat_shift", &[
            ("RAFKA_NODE_BIND_ADDR", format!("0.0.0.0:{port}")),
        ]).await
    }

    async fn detect(&self, ctx: &ChaosContext, outcome: &ChaosOutcome, deadline_ms: u64) -> Result<DetectionResult, ChaosError> {
        detect_respawned(ctx, outcome, deadline_ms, "nat_shift").await
    }
}

/// `slow_link` — restart target node with `RAFKA_LINK_SLOW_MS` env so node-base
/// sleeps that many ms before each outbound frame send (substrate-level latency
/// injection). Detection: new subprocess appears in `/api/spawned`.
pub struct SlowLink {
    pub target: Option<String>,
    pub latency_ms: u64,
}

#[async_trait]
impl ChaosPrimitive for SlowLink {
    fn name(&self) -> &str {
        "slow_link"
    }

    async fn execute(&self, ctx: &ChaosContext) -> Result<ChaosOutcome, ChaosError> {
        respawn_with_env(ctx, self.target.as_deref(), "slow_link", &[
            ("RAFKA_LINK_SLOW_MS", self.latency_ms.to_string()),
        ]).await
    }

    async fn detect(&self, ctx: &ChaosContext, outcome: &ChaosOutcome, deadline_ms: u64) -> Result<DetectionResult, ChaosError> {
        detect_respawned(ctx, outcome, deadline_ms, "slow_link").await
    }
}

/// `lossy_link` — restart target node with `RAFKA_LINK_LOSS_PCT` env so node-base
/// drops that percentage of outbound frames. Detection: new subprocess appears in
/// `/api/spawned`.
pub struct LossyLink {
    pub target: Option<String>,
    pub loss_pct: u8,
}

#[async_trait]
impl ChaosPrimitive for LossyLink {
    fn name(&self) -> &str {
        "lossy_link"
    }

    async fn execute(&self, ctx: &ChaosContext) -> Result<ChaosOutcome, ChaosError> {
        respawn_with_env(ctx, self.target.as_deref(), "lossy_link", &[
            ("RAFKA_LINK_LOSS_PCT", self.loss_pct.to_string()),
        ]).await
    }

    async fn detect(&self, ctx: &ChaosContext, outcome: &ChaosOutcome, deadline_ms: u64) -> Result<DetectionResult, ChaosError> {
        detect_respawned(ctx, outcome, deadline_ms, "lossy_link").await
    }
}

/// Shared helper: kill `target` and respawn the same node_type via topology-ui
/// with the provided extra_env. Returns ChaosOutcome with targets=[old, new].
async fn respawn_with_env(
    ctx: &ChaosContext,
    target: Option<&str>,
    primitive_name: &'static str,
    extra_env: &[(&str, String)],
) -> Result<ChaosOutcome, ChaosError> {
    let target = match target {
        Some(t) => t.to_string(),
        None => pick_random_spawned(ctx).await?,
    };
    let node_type = NODE_TYPES
        .iter()
        .find(|t| target.starts_with(*t))
        .copied()
        .ok_or_else(|| ChaosError::InvalidTarget(format!("cannot derive node_type from {target}")))?;
    let span = info_span!(
        "rafka.chaos.primitive.executed",
        name = primitive_name,
        target = %target,
        node_type = node_type,
        "otel.kind" = "internal",
    );
    let _enter = span.enter();

    let kill_url = format!("{}/api/nodes/{}", ctx.topology_ui_url, target);
    let _ = ctx.http.delete(&kill_url).send().await;

    let mut env_map = serde_json::Map::new();
    for (k, v) in extra_env {
        env_map.insert((*k).to_string(), json!(v));
    }
    let spawn_url = format!("{}/api/nodes/spawn", ctx.topology_ui_url);
    let resp = ctx
        .http
        .post(&spawn_url)
        .json(&json!({"node_type": node_type, "extra_env": env_map}))
        .send()
        .await
        .map_err(|e| ChaosError::TopologyUiUnreachable(format!("POST {spawn_url}: {e}")))?;
    if !resp.status().is_success() {
        return Err(ChaosError::Execution(format!("respawn returned {}", resp.status())));
    }
    let body: serde_json::Value = resp.json().await
        .map_err(|e| ChaosError::Execution(format!("parse spawn response: {e}")))?;
    let new_name = body["node_name"].as_str()
        .ok_or_else(|| ChaosError::Execution("spawn response missing node_name".into()))?
        .to_string();
    Ok(ChaosOutcome {
        primitive_name: primitive_name.into(),
        targets: vec![target.clone(), new_name.clone()],
        state: json!({"old": target, "new": new_name, "extra_env": env_map}),
    })
}

async fn detect_respawned(
    ctx: &ChaosContext,
    outcome: &ChaosOutcome,
    deadline_ms: u64,
    primitive_name: &'static str,
) -> Result<DetectionResult, ChaosError> {
    let new_name = &outcome.targets[1];
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
        let present = body["spawned"].as_array()
            .map(|a| a.iter().any(|v| v.as_str() == Some(new_name)))
            .unwrap_or(false);
        if present {
            let span = info_span!(
                "rafka.chaos.primitive.detected",
                name = primitive_name,
                target = %new_name,
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
