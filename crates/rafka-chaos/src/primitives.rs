//! Chaos primitives — concrete implementations of the catalog in chaos PRD §2.
//!
//! Sprint 11 phase 1 ships:
//! - `kill_node`   — abruptly terminate a node via topology-ui DELETE
//! - `restart_node` — kill + immediately re-spawn
//!
//! Subsequent phases add the network + system primitives.

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
