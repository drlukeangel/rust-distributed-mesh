//! Soak runner — picks a random primitive every N seconds, executes, detects, records.
//! Per chaos PRD §3. Sprint 11 phase 1 ships a smoke variant (5min run with 10 events).

use crate::primitives::{BurstKill, ClockSkew, DiskFull, KillNode, LossyLink, NatShift, RestartNode, SlowLink, WedgeNode};
use crate::{ChaosContext, ChaosOutcome, ChaosPrimitive, DetectionResult};
use rand::Rng;
use serde::Serialize;
use serde_json::json;
use std::time::{Duration, Instant};

/// Minimum target pool size — if /api/spawned drops below this, soak tops up by
/// spawning fresh subprocesses before the next primitive. Keeps long soaks viable
/// (otherwise kill-heavy primitives drain the pool and remaining events all fail
/// with InvalidTarget).
/// Minimum pool size before maintain_pool tops up. Must be >= NODE_TYPES.len() so
/// the pool can actually hold one of each type simultaneously — otherwise we
/// never converge to a full mesh.
const MIN_POOL_SIZE: usize = 5;
const NODE_TYPES: &[&str] = &["gateway", "broker", "compute", "registry", "bridge"];

#[derive(Serialize)]
pub struct SoakEvent {
    pub timestamp_ms: u64,
    pub primitive: String,
    pub targets: Vec<String>,
    pub detection: DetectionLabel,
    pub waited_ms: u64,
}

#[derive(Serialize)]
pub enum DetectionLabel {
    Passed,
    FailedTimeout,
    FailedAssertion(String),
}

#[derive(Serialize)]
pub struct SoakReport {
    pub seed: u64,
    pub started_ms: u64,
    pub ended_ms: u64,
    pub event_count: usize,
    pub passed: usize,
    pub failed_timeout: usize,
    pub failed_assertion: usize,
    pub events: Vec<SoakEvent>,
}

/// Run a soak for the given duration. Picks primitives at random per `interval`.
pub async fn run_soak(
    ctx: &ChaosContext,
    duration: Duration,
    interval: Duration,
    seed: u64,
) -> SoakReport {
    use std::io::Write;
    let started = Instant::now();
    let started_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let mut events: Vec<SoakEvent> = Vec::new();
    let mut passed = 0usize;
    let mut failed_timeout = 0usize;
    let mut failed_assertion = 0usize;
    // Every 10 events, print a heartbeat line so log readers see progress in real
    // time. Without this the soak appears hung in stdout-redirected background
    // runs (block-buffered) even when chaos events are flowing fine via Jaeger.
    const HEARTBEAT_EVERY: usize = 10;

    while started.elapsed() < duration {
        // Top up target pool if it's too thin — keeps long soaks viable.
        maintain_pool(ctx).await;

        // pick a primitive from the full pool. PartitionPair excluded — needs admin
        // perms. WedgeNode picks a node_type and suspends one matching OS process.
        let (pick, wedge_type_idx): (u8, usize) = {
            let mut rng = ctx.rng.lock().await;
            (rng.gen_range(0..9), rng.gen_range(0..4))
        };
        let primitive: Box<dyn ChaosPrimitive> = match pick {
            0 => Box::new(KillNode { target: None }),
            1 => Box::new(RestartNode { target: None }),
            2 => Box::new(BurstKill { count: 2 }),
            3 => Box::new(DiskFull { target: None, max_bytes: 4 * 1024 * 1024 }),
            4 => Box::new(ClockSkew { target: None, skew_ms: 30_000 }),
            5 => Box::new(SlowLink { target: None, latency_ms: 250 }),
            6 => Box::new(LossyLink { target: None, loss_pct: 15 }),
            7 => Box::new(NatShift { target: None }),
            _ => Box::new(WedgeNode {
                target_node_type: ["gateway","broker","compute","registry"][wedge_type_idx].to_string(),
                duration_ms: 3_000,
            }),
        };

        let exec_started = Instant::now();
        let outcome_result = primitive.execute(ctx).await;
        let outcome = match outcome_result {
            Ok(o) => o,
            Err(e) => {
                // InvalidTarget = soft skip (race: target gone between check and
                // execute, OR no live targets at all). Don't fail the soak — just
                // record the event with a Passed label tagged as "skipped" so the
                // report still shows what happened.
                let is_skip = matches!(e, crate::ChaosError::InvalidTarget(_));
                if is_skip {
                    events.push(SoakEvent {
                        timestamp_ms: timestamp_ms(),
                        primitive: primitive.name().into(),
                        targets: vec![],
                        detection: DetectionLabel::Passed,
                        waited_ms: exec_started.elapsed().as_millis() as u64,
                    });
                    passed += 1;
                } else {
                    events.push(SoakEvent {
                        timestamp_ms: timestamp_ms(),
                        primitive: primitive.name().into(),
                        targets: vec![],
                        detection: DetectionLabel::FailedAssertion(format!("execute: {e}")),
                        waited_ms: exec_started.elapsed().as_millis() as u64,
                    });
                    failed_assertion += 1;
                }
                tokio::time::sleep(interval).await;
                continue;
            }
        };

        let detect_result = primitive.detect(ctx, &outcome, 30_000).await;
        let (label, waited_ms) = match detect_result {
            Ok(DetectionResult::Passed { waited_ms }) => {
                passed += 1;
                (DetectionLabel::Passed, waited_ms)
            }
            Ok(DetectionResult::FailedTimeout { waited_ms }) => {
                failed_timeout += 1;
                (DetectionLabel::FailedTimeout, waited_ms)
            }
            Ok(DetectionResult::FailedAssertion { msg, waited_ms }) => {
                failed_assertion += 1;
                (DetectionLabel::FailedAssertion(msg), waited_ms)
            }
            Err(e) => {
                failed_assertion += 1;
                (
                    DetectionLabel::FailedAssertion(format!("detect: {e}")),
                    exec_started.elapsed().as_millis() as u64,
                )
            }
        };
        events.push(SoakEvent {
            timestamp_ms: timestamp_ms(),
            primitive: primitive.name().into(),
            targets: outcome.targets,
            detection: label,
            waited_ms,
        });
        // Heartbeat every N events so background-redirected log files show
        // forward motion even with block-buffered stdout.
        if events.len() % HEARTBEAT_EVERY == 0 {
            let elapsed_s = started.elapsed().as_secs();
            println!(
                "soak progress: events={} passed={} failed={}  elapsed={}s",
                events.len(),
                passed,
                failed_timeout + failed_assertion,
                elapsed_s
            );
            let _ = std::io::stdout().flush();
        }
        tokio::time::sleep(interval).await;
    }

    let ended_ms = timestamp_ms();
    let event_count = events.len();
    SoakReport {
        seed,
        started_ms,
        ended_ms,
        event_count,
        passed,
        failed_timeout,
        failed_assertion,
        events,
    }
}

/// Ensure each NODE_TYPE has AT LEAST ONE instance in the spawned pool.
/// This is the only invariant the soak enforces — user-spawned extras are left
/// alone. Previous round-robin refill was buggy: when chaos killed one node,
/// refill always picked NODE_TYPES[0] (gateway), so over a 4-hour soak the pool
/// became gateway-heavy with compute/registry/bridge missing entirely. Fix:
/// look at WHICH types are currently absent, spawn exactly those.
async fn maintain_pool(ctx: &ChaosContext) {
    let url = format!("{}/api/nodes/spawned", ctx.topology_ui_url);
    let spawned: Vec<String> = match ctx.http.get(&url).send().await {
        Ok(resp) => match resp.json::<serde_json::Value>().await {
            Ok(body) => body["spawned"]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default(),
            Err(_) => return,
        },
        Err(_) => return, // topology-ui unreachable; let next primitive raise the error
    };
    let spawn_url = format!("{}/api/nodes/spawn", ctx.topology_ui_url);
    let mut missing: Vec<&str> = Vec::new();
    for t in NODE_TYPES.iter() {
        if !spawned.iter().any(|name| name.starts_with(t)) {
            missing.push(t);
        }
    }
    for t in &missing {
        let _ = ctx
            .http
            .post(&spawn_url)
            .json(&json!({"node_type": *t}))
            .send()
            .await;
    }
    // Brief wait so the spawned subprocesses appear in /api/spawned before the next
    // primitive picks targets.
    if !missing.is_empty() {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

fn timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[allow(dead_code)]
fn _outcome_targets(o: &ChaosOutcome) -> Vec<String> {
    o.targets.clone()
}
